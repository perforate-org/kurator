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
    let path = dir.path().join(node.to_string());
    let (journal, recovered) = Journal::open(path, Options::default()).unwrap();
    let ledger = Ledger::replay(ShardState::default(), &recovered);
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
        let dir = tempfile::tempdir().unwrap();
        let wire = Wire::default();
        let replicas = (1..=4)
            .map(NodeId::new)
            .map(|node| (node, open(&dir, &wire, node)))
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

    /// Runs `future` to its end, delivering messages while it waits.
    fn run<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(noop_waker_ref());
        for _ in 0..1000 {
            if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
                return value;
            }
            self.pump(50);
        }
        panic!("never finished");
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
    // Every replica decides the block in the end.
    cluster.pump(2000);
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
