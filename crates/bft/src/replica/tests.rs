//! Four replicas on an in-memory network that delivers every message.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::task::noop_waker_ref;
use oleaph_consensus::{Duty, NodeId, Peer, Reads, Replicated, Route, Transaction as _};
use oleaph_shard::ShardState;
use oleaph_storage::{Command, KvState, Outcome, WriteBatch};
use oleaph_wal::Options;
use parking_lot::Mutex;
use tempfile::TempDir;

use super::{Host, Journal, Ledger, Replica, Settled};
use crate::block::Order;
use crate::checkpoint::{Height, Interval};
use crate::core::{Config, Core};
use crate::fixture::{self, DOMAIN, change_to, committee, key};
use crate::message::Message;
use crate::rejection::Rejection;
use crate::view::Epoch;
use crate::view::View;

/// Messages in flight, as (from, to, message).
type Wire = Arc<Mutex<VecDeque<(NodeId, NodeId, Message)>>>;

#[derive(Debug)]
struct Loopback {
    node: NodeId,
    wire: Wire,
    /// The timers armed, oldest first.
    wakes: Arc<Mutex<Vec<(View, Duration)>>>,
    /// The members it learned of, and where they are.
    met: Arc<Mutex<BTreeMap<NodeId, Peer>>>,
}

impl Host for Loopback {
    fn send(&self, to: NodeId, message: &Message) {
        self.wire.lock().push_back((self.node, to, message.clone()));
    }

    fn wake(&self, view: View, after: Duration) {
        self.wakes.lock().push((view, after));
    }

    fn meet(&self, node: NodeId, peer: &Peer) {
        drop(self.met.lock().insert(node, peer.clone()));
    }
}

struct Cluster {
    dir: TempDir,
    wire: Wire,
    replicas: BTreeMap<NodeId, Replica<Loopback>>,
}

fn open(dir: &TempDir, wire: &Wire, node: NodeId) -> Replica<Loopback> {
    open_on(dir, wire, node, ShardState::default(), Interval::DEFAULT)
}

/// Opens node `node`'s replica, starting from `state`, with a checkpoint
/// at every `interval`.
fn open_on(
    dir: &TempDir,
    wire: &Wire,
    node: NodeId,
    state: ShardState,
    interval: Interval,
) -> Replica<Loopback> {
    let path = dir.path().join(node.to_string());
    let (journal, recovered) = Journal::open(path, Options::default()).unwrap();
    let ledger = Ledger::replay(state, &recovered).every(interval);
    let config = Config::new(node, key(node.get()), committee(), DOMAIN);
    let core = Core::recover(config, recovered);
    let host = Loopback {
        node,
        wire: Arc::clone(wire),
        wakes: Arc::default(),
        met: Arc::default(),
    };
    Replica::start(core, journal, ledger, BTreeMap::new(), host)
}

impl Cluster {
    fn new() -> Self {
        Self::starting(|_| ShardState::default(), Interval::DEFAULT)
    }

    /// Four replicas, each starting from the state `state` gives its node,
    /// with a checkpoint at every `interval`.
    fn starting(state: impl Fn(NodeId) -> ShardState, interval: Interval) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let wire = Wire::default();
        let replicas = (1..=4)
            .map(NodeId::new)
            .map(|node| (node, open_on(&dir, &wire, node, state(node), interval)))
            .collect();
        Self {
            dir,
            wire,
            replicas,
        }
    }

    /// Delivers up to `count` messages.
    fn pump(&self, count: usize) {
        for _ in 0..count {
            let Some((from, to, message)) = self.wire.lock().pop_front() else {
                return;
            };
            if let Some(replica) = self.replicas.get(&to) {
                replica.receive(from, message);
            }
        }
    }

    /// Runs `future` to its end, delivering messages while it waits, and
    /// firing the timers armed last once none are in flight.
    fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(noop_waker_ref());
        for _ in 0..1000 {
            if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
                return value;
            }
            self.pump(50);
            if self.wire.lock().is_empty() {
                self.expire();
            }
        }
        panic!("never finished");
    }

    /// Fires the timer each replica armed last.
    fn expire(&self) {
        for replica in self.replicas.values() {
            let armed = replica.host.wakes.lock().last().copied();
            if let Some((view, _after)) = armed {
                replica.expire(view);
            }
        }
    }

    /// Delivers messages and fires timers until `done` holds.
    fn until(&self, done: impl Fn(&Self) -> bool) {
        for _ in 0..100 {
            if done(self) {
                return;
            }
            self.pump(2000);
            self.expire();
        }
        panic!("never got there");
    }

    fn replica(&self, node: u64) -> &Replica<Loopback> {
        &self.replicas[&NodeId::new(node)]
    }
}

fn put(key: &str, value: &str) -> Command {
    Command::Kv(WriteBatch::new().put(key, value))
}

#[test]
fn every_replica_takes_writes_and_reads_what_it_decided() {
    let cluster = Cluster::new();
    let outcome = cluster
        .run(cluster.replica(2).propose(put("a", "1")))
        .unwrap();
    let Outcome::Committed(version) = outcome else {
        panic!("{outcome:?}");
    };
    // Every replica decides the block in the end, and then the committee
    // goes quiet: there is nothing more to decide.
    cluster.pump(2000);
    assert!(cluster.wire.lock().is_empty(), "quiet");
    for replica in cluster.replicas.values() {
        let value = cluster.run(replica.read(|kv| kv.get(b"a").map(<[u8]>::to_vec)));
        assert_eq!(value.unwrap(), Some(b"1".to_vec()), "{}", replica.node());
        assert!(
            cluster.run(replica.read(KvState::version)).unwrap() >= version,
            "applied"
        );
        assert_eq!(replica.route(), Route::Here, "takes writes");
        assert!((1..=4).contains(&replica.node().get()), "its node");
    }
    assert_eq!(
        <Replica<Loopback> as Replicated>::READS,
        Reads::Decided,
        "reads"
    );
}

#[test]
fn a_transaction_commits_on_the_decided_state() {
    let cluster = Cluster::new();
    let replica = cluster.replica(3);
    let _put = cluster.run(replica.propose(put("k", "v"))).unwrap();
    let mut txn = cluster.run(replica.begin()).unwrap();
    assert_eq!(
        txn.get(b"k").unwrap(),
        Some(b"v".to_vec()),
        "reads its state"
    );
    txn.put("k", "w");
    txn.delete("gone");
    let all = txn
        .range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();
    assert_eq!(
        all,
        vec![(b"k".to_vec(), b"w".to_vec())],
        "its own writes count"
    );
    let committed = cluster.run(txn.commit()).unwrap();
    let value = cluster.run(replica.read(|kv| kv.get(b"k").map(<[u8]>::to_vec)));
    assert_eq!(value.unwrap(), Some(b"w".to_vec()), "committed");
    assert!(committed.get() > 0, "a version");
    let empty = cluster.run(replica.begin()).unwrap();
    let read_at = cluster.run(replica.read(KvState::version)).unwrap();
    assert!(
        cluster.run(empty.commit()).unwrap() <= read_at,
        "writes nothing"
    );
}

#[test]
fn one_replica_at_a_time_runs_the_chores() {
    let cluster = Cluster::new();
    let mine = cluster
        .replicas
        .values()
        .filter(|replica| replica.duty() == Duty::Mine)
        .count();
    assert_eq!(mine, 1, "the first view's leader");
    let members = cluster.replica(1).members();
    assert!(
        (1..=4).all(|node| members.votes(NodeId::new(node))),
        "voters"
    );
    assert_eq!(
        cluster.replica(1).shard(),
        ShardState::default().shard().clone(),
        "the shard"
    );
}

#[test]
fn a_restarted_replica_keeps_what_it_decided() {
    let mut cluster = Cluster::new();
    let _put = cluster
        .run(cluster.replica(1).propose(put("a", "1")))
        .unwrap();
    cluster.pump(2000);
    let before = cluster.run(cluster.replica(4).read(Clone::clone)).unwrap();
    drop(cluster.replicas.remove(&NodeId::new(4)));
    let again = open(&cluster.dir, &cluster.wire, NodeId::new(4));
    let after = cluster.run(again.read(Clone::clone)).unwrap();
    assert_eq!(after, before, "the same state");
    assert!(after.get(b"a").is_some(), "with the write");
}

#[test]
fn a_view_without_progress_times_out_ever_more_slowly() {
    let cluster = Cluster::new();
    let replica = cluster.replica(1);
    let first = (View::new(1), Duration::from_secs(1));
    assert_eq!(replica.host.wakes.lock().last(), Some(&first), "on start");
    // Nobody hears it: it gives up alone, and waits twice as long.
    cluster.wire.lock().clear();
    replica.expire(View::new(1));
    let again = (View::new(1), Duration::from_secs(2));
    assert_eq!(replica.host.wakes.lock().last(), Some(&again), "backs off");
    assert!(
        cluster
            .wire
            .lock()
            .iter()
            .any(|(_, _, message)| matches!(message, Message::Timeout(_))),
        "tells the others"
    );
}

/// A checkpoint at every decided block.
fn every_block() -> Interval {
    Interval::new(std::num::NonZeroU64::MIN)
}

#[test]
fn the_committee_certifies_the_state_it_decided() {
    let cluster = Cluster::starting(|_| ShardState::default(), every_block());
    let _put = cluster
        .run(cluster.replica(1).propose(put("a", "1")))
        .unwrap();
    cluster.pump(2000);
    for replica in cluster.replicas.values() {
        let certificate = replica.checkpoint().unwrap();
        let checkpoint = certificate.checkpoint();
        assert!(checkpoint.height() >= Height::new(1), "{}", replica.node());
        assert_eq!(
            certificate.verify(&committee(), DOMAIN),
            Ok(()),
            "{}",
            replica.node()
        );
        let state = cluster.run(replica.read(Clone::clone)).unwrap();
        assert!(state.get(b"a").is_some(), "applied");
        assert!(
            cluster.run(replica.propose(put("b", "2"))).is_ok(),
            "still serves"
        );
    }
}

#[test]
fn a_replica_whose_state_differs_stops() {
    let odd = NodeId::new(4);
    let cluster = Cluster::starting(
        |node| {
            let mut state = ShardState::default();
            if node == odd {
                let _applied = state.apply(oleaph_storage::Version::new(1), put("x", "y"));
            }
            state
        },
        every_block(),
    );
    let _put = cluster
        .run(cluster.replica(1).propose(put("a", "1")))
        .unwrap();
    cluster.pump(2000);
    let replica = cluster.replica(4);
    let certified = replica.checkpoint().unwrap();
    assert_eq!(
        certified.verify(&committee(), DOMAIN),
        Ok(()),
        "the others certified theirs"
    );
    let refused = cluster.run(replica.propose(put("b", "2")));
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("is not the one the committee certified"),
        "stopped"
    );
    // The others go on without it.
    let _again = cluster
        .run(cluster.replica(2).propose(put("c", "3")))
        .unwrap();
}

#[test]
fn a_replica_restarts_from_its_certified_state() {
    let mut cluster = Cluster::starting(|_| ShardState::default(), every_block());
    for key in ["a", "b", "c"] {
        let _put = cluster
            .run(cluster.replica(1).propose(put(key, "1")))
            .unwrap();
    }
    cluster.pump(2000);
    let node = NodeId::new(4);
    let before = cluster.run(cluster.replica(4).read(Clone::clone)).unwrap();
    let certified = cluster.replica(4).checkpoint().unwrap();
    drop(cluster.replicas.remove(&node));
    let states = cluster.dir.path().join(node.to_string()).join("states");
    assert_eq!(
        std::fs::read_dir(&states).unwrap().count(),
        1,
        "the certified state only"
    );
    let again = open_on(
        &cluster.dir,
        &cluster.wire,
        node,
        ShardState::default(),
        every_block(),
    );
    let after = cluster.run(again.read(Clone::clone)).unwrap();
    assert_eq!(after, before, "the same state");
    assert!(
        again
            .checkpoint()
            .is_none_or(|known| known.height() >= certified.height()),
        "no older checkpoint"
    );
    drop(cluster.replicas.insert(node, again));
    let _later = cluster
        .run(cluster.replica(2).propose(put("d", "1")))
        .unwrap();
    cluster.pump(2000);
    let held = cluster.run(
        cluster
            .replica(4)
            .read(|kv| kv.get(b"d").map(<[u8]>::to_vec)),
    );
    assert_eq!(held.unwrap(), Some(b"1".to_vec()), "and goes on deciding");
}

#[test]
fn a_replica_far_behind_takes_the_certified_state_from_the_others() {
    let mut cluster = Cluster::starting(|_| ShardState::default(), every_block());
    let node = NodeId::new(4);
    // It hears nothing while the others decide, certify and let the
    // blocks go.
    let away = cluster.replicas.remove(&node).unwrap();
    for key in ["a", "b", "c"] {
        let _put = cluster
            .run(cluster.replica(1).propose(put(key, "1")))
            .unwrap();
    }
    cluster.pump(2000);
    let certified = cluster.replica(1).checkpoint().unwrap();
    assert!(certified.height() >= Height::new(3), "certified");
    drop(cluster.replicas.insert(node, away));
    let _put = cluster
        .run(cluster.replica(2).propose(put("d", "1")))
        .unwrap();
    for _ in 0..20 {
        cluster.pump(2000);
        cluster.expire();
    }
    let theirs = cluster.run(cluster.replica(1).read(Clone::clone)).unwrap();
    let mine = cluster.run(cluster.replica(4).read(Clone::clone)).unwrap();
    assert_eq!(mine, theirs, "the same state");
    assert!(
        cluster
            .replica(4)
            .checkpoint()
            .is_some_and(|known| known.height() >= certified.height()),
        "the certified checkpoint"
    );
    let _later = cluster
        .run(cluster.replica(4).propose(put("e", "1")))
        .unwrap();
    cluster.pump(2000);
    let held = cluster.run(
        cluster
            .replica(1)
            .read(|kv| kv.get(b"e").map(<[u8]>::to_vec)),
    );
    assert_eq!(held.unwrap(), Some(b"1".to_vec()), "and goes on deciding");
    cluster.pump(2000);
    let before = cluster.run(cluster.replica(4).read(Clone::clone)).unwrap();
    drop(cluster.replicas.remove(&node));
    let again = open_on(
        &cluster.dir,
        &cluster.wire,
        node,
        ShardState::default(),
        every_block(),
    );
    let after = cluster.run(again.read(Clone::clone)).unwrap();
    assert_eq!(after, before, "it restarts from the state it took");
}

/// How many parts of a checkpoint's state `node` asked for, in flight.
fn wants_from(cluster: &Cluster, node: NodeId) -> usize {
    cluster
        .wire
        .lock()
        .iter()
        .filter(|(from, _, message)| *from == node && matches!(message, Message::Want(..)))
        .count()
}

#[test]
fn only_a_replica_behind_the_certified_state_asks_for_it() {
    let mut cluster = Cluster::starting(|_| ShardState::default(), every_block());
    let node = NodeId::new(4);
    let away = cluster.replicas.remove(&node).unwrap();
    for key in ["a", "b"] {
        let _put = cluster
            .run(cluster.replica(1).propose(put(key, "1")))
            .unwrap();
    }
    cluster.pump(2000);
    cluster.wire.lock().clear();
    let certified = cluster.replica(1).checkpoint().unwrap();

    // One as far as the checkpoint has nothing to ask.
    let level = NodeId::new(1);
    cluster
        .replica(1)
        .receive(NodeId::new(2), Message::Certified(certified.clone()));
    assert_eq!(wants_from(&cluster, level), 0, "level");

    // One behind asks for the manifest, once however often it hears.
    away.receive(level, Message::Certified(certified.clone()));
    assert_eq!(wants_from(&cluster, node), 1, "the manifest");
    away.receive(NodeId::new(2), Message::Certified(certified));
    assert_eq!(wants_from(&cluster, node), 1, "asked already");

    // An expired view asks again: the answer may have been lost.
    cluster.wire.lock().clear();
    let armed = away.host.wakes.lock().last().copied().unwrap();
    away.expire(armed.0);
    assert_eq!(wants_from(&cluster, node), 1, "asks again");
}

#[test]
fn a_committee_hands_the_shard_on_to_the_next() {
    let mut cluster = Cluster::new();
    let _put = cluster
        .run(cluster.replica(2).propose(put("a", "1")))
        .unwrap();
    let refused = cluster.run(
        cluster
            .replica(2)
            .submit(Order::Change(change_to(2, &[1, 2]))),
    );
    assert_eq!(
        refused.unwrap(),
        Settled::Refused(Rejection::OtherEpoch(Epoch::new(2), Epoch::FIRST)),
        "decided, and changes nothing"
    );
    let epoch = cluster.run(cluster.replica(1).change(change_to(1, &[1, 2, 3])));
    assert_eq!(epoch.unwrap(), Epoch::new(1), "the next epoch");
    cluster.until(|cluster| (1..=3).all(|node| cluster.replica(node).epoch() == Epoch::new(1)));
    let retired = cluster.run(cluster.replica(4).propose(put("x", "1")));
    assert!(retired.is_err(), "the member left out decides no more");
    let again = cluster.run(cluster.replica(1).change(change_to(1, &[1, 2, 3])));
    assert!(again.is_err(), "a change for an epoch that has begun");
    // The new committee decides, in a domain of its own.
    let _later = cluster
        .run(cluster.replica(3).propose(put("b", "2")))
        .unwrap();
    cluster.pump(2000);
    for node in 1..=3 {
        let state = cluster
            .run(cluster.replica(node).read(Clone::clone))
            .unwrap();
        assert_eq!(state.get(b"a"), Some(b"1".as_slice()), "{node} kept");
        assert_eq!(state.get(b"b"), Some(b"2".as_slice()), "{node} decided");
        let members = cluster.replica(node).members();
        assert!(!members.votes(NodeId::new(4)), "{node} decides without 4");
    }
    // A member restarted with the charter goes on in the epoch it
    // reached.
    let node = NodeId::new(1);
    let before = cluster.run(cluster.replica(1).read(Clone::clone)).unwrap();
    drop(cluster.replicas.remove(&node));
    let restarted = open(&cluster.dir, &cluster.wire, node);
    assert_eq!(restarted.epoch(), Epoch::new(1), "the epoch it reached");
    let after = cluster.run(restarted.read(Clone::clone)).unwrap();
    assert_eq!(after, before, "the same state");
    drop(cluster.replicas.insert(node, restarted));
    let _restarted = cluster
        .run(cluster.replica(1).propose(put("c", "3")))
        .unwrap();
}

#[test]
fn a_member_that_missed_the_end_of_an_epoch_follows_on() {
    let mut cluster = Cluster::new();
    let late = NodeId::new(3);
    // It hears nothing while the others decide the change and end the
    // epoch.
    let away = cluster.replicas.remove(&late).unwrap();
    let _put = cluster
        .run(cluster.replica(1).propose(put("a", "1")))
        .unwrap();
    let epoch = cluster.run(cluster.replica(1).change(change_to(1, &[1, 2, 3])));
    assert_eq!(epoch.unwrap(), Epoch::new(1), "the next epoch");
    cluster.until(|cluster| (1..=2).all(|node| cluster.replica(node).epoch() == Epoch::new(1)));
    assert_eq!(away.epoch(), Epoch::FIRST, "missed it");

    // The next committee has no quorum without it. Back, it times out in
    // the epoch it knows; the others show it how that epoch ended, and it
    // takes the state there and follows on.
    drop(cluster.replicas.insert(late, away));
    cluster.until(|cluster| cluster.replica(3).epoch() == Epoch::new(1));
    let _later = cluster
        .run(cluster.replica(3).propose(put("b", "2")))
        .unwrap();
    cluster.pump(2000);
    for node in 1..=3 {
        let state = cluster
            .run(cluster.replica(node).read(Clone::clone))
            .unwrap();
        assert_eq!(state.get(b"a"), Some(b"1".as_slice()), "{node} kept");
        assert_eq!(state.get(b"b"), Some(b"2".as_slice()), "{node} decided");
    }
}

/// Decides `command` through `node` and waits until every member of the
/// shard's newest epoch certified a checkpoint past the one that started
/// it.
fn certify_past_the_start(cluster: &Cluster, node: u64, command: Command) {
    let start = cluster.replica(node).checkpoint().unwrap();
    let _decided = cluster.run(cluster.replica(node).propose(command)).unwrap();
    cluster.until(|cluster| {
        cluster
            .replica(node)
            .checkpoint()
            .is_some_and(|newest| newest.supersedes(&start))
    });
}

#[test]
fn a_newcomer_takes_the_newest_state_and_joins() {
    let mut cluster = Cluster::starting(|_| ShardState::default(), every_block());
    let left = NodeId::new(4);
    let away = cluster.replicas.remove(&left).unwrap();
    let _put = cluster
        .run(cluster.replica(1).propose(put("a", "1")))
        .unwrap();
    let epoch = cluster.run(cluster.replica(1).change(change_to(1, &[1, 2, 3, 5])));
    assert_eq!(epoch.unwrap(), Epoch::new(1), "the next epoch");
    cluster.until(|cluster| (1..=3).all(|node| cluster.replica(node).epoch() == Epoch::new(1)));
    // The state at the end is gone once the new committee certifies one
    // of its own.
    certify_past_the_start(&cluster, 1, put("b", "2"));

    // Started with the charter, a newcomer knows only the first epoch.
    // The members show it how the epochs succeeded and the newest state,
    // and it takes that state and joins.
    let newcomer = NodeId::new(5);
    let joining = open_on(
        &cluster.dir,
        &cluster.wire,
        newcomer,
        ShardState::default(),
        every_block(),
    );
    assert_eq!(joining.epoch(), Epoch::FIRST, "the charter's epoch");
    // The members learned where it is from the change.
    for node in 1..=3 {
        let met = cluster.replica(node).host.met.lock().clone();
        assert_eq!(
            met,
            fixture::peers(&[1, 2, 3, 5]),
            "{node} met the committee"
        );
    }
    drop(cluster.replicas.insert(newcomer, joining));
    cluster.until(|cluster| cluster.replica(5).epoch() == Epoch::new(1));
    let _joined = cluster
        .run(cluster.replica(5).propose(put("c", "3")))
        .unwrap();
    cluster.pump(2000);
    let state = cluster.run(cluster.replica(5).read(Clone::clone)).unwrap();
    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
        assert_eq!(state.get(key.as_bytes()), Some(value.as_bytes()), "{key}");
    }

    // A member left out that missed it all retires, with nothing to take.
    drop(cluster.replicas.insert(left, away));
    cluster.until(|cluster| {
        cluster
            .run(cluster.replica(4).propose(put("x", "1")))
            .is_err()
    });
}

#[test]
fn a_member_that_missed_the_first_checkpoint_of_an_epoch_takes_it() {
    let mut cluster = Cluster::starting(|_| ShardState::default(), every_block());
    let late = NodeId::new(3);
    let away = cluster.replicas.remove(&late).unwrap();
    let _put = cluster
        .run(cluster.replica(1).propose(put("a", "1")))
        .unwrap();
    // The same members, in an epoch of their own, decide without it.
    let epoch = cluster.run(cluster.replica(1).change(change_to(1, &[1, 2, 3, 4])));
    assert_eq!(epoch.unwrap(), Epoch::new(1), "the next epoch");
    cluster.until(|cluster| {
        [1, 2, 4]
            .iter()
            .all(|node| cluster.replica(*node).epoch() == Epoch::new(1))
    });
    certify_past_the_start(&cluster, 1, put("b", "2"));

    drop(cluster.replicas.insert(late, away));
    cluster.until(|cluster| cluster.replica(3).epoch() == Epoch::new(1));
    let state = cluster.run(cluster.replica(3).read(Clone::clone)).unwrap();
    assert_eq!(state.get(b"b"), Some(b"2".as_slice()), "the newest state");
}
