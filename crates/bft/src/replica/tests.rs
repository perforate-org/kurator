//! Four replicas on an in-memory network that delivers every message.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::task::noop_waker_ref;
use kurator_consensus::{Duty, NodeId, Reads, Replicated, Route, Transaction as _};
use kurator_shard::ShardState;
use kurator_storage::{Command, KvState, Outcome, WriteBatch};
use kurator_wal::Options;
use parking_lot::Mutex;
use tempfile::TempDir;

use super::{Host, Journal, Ledger, Replica};
use crate::checkpoint::{Height, Interval};
use crate::core::{Config, Core};
use crate::fixture::{DOMAIN, committee, key};
use crate::message::Message;
use crate::view::View;

/// Messages in flight, as (from, to, message).
type Wire = Arc<Mutex<VecDeque<(NodeId, NodeId, Message)>>>;

#[derive(Debug)]
struct Loopback {
    node: NodeId,
    wire: Wire,
    /// The timers armed, oldest first.
    wakes: Arc<Mutex<Vec<(View, Duration)>>>,
}

impl Host for Loopback {
    fn send(&self, to: NodeId, message: &Message) {
        self.wire.lock().push_back((self.node, to, message.clone()));
    }

    fn wake(&self, view: View, after: Duration) {
        self.wakes.lock().push((view, after));
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
                let _applied = state.apply(kurator_storage::Version::new(1), put("x", "y"));
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
