//! Splits: a shard becomes two Raft groups on the same nodes, each half
//! keeps its keys and goes on taking writes, a node that missed the split
//! comes to hold both halves, and a write routed before a split follows
//! its key.
#![expect(
    clippy::expect_used,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeSet;

use std::ops::Bound::Unbounded;

use kurator_raft::openraft::AsyncRuntime as _;
use kurator_raft::{Error, NodeId};
use kurator_shard::ShardId;
use kurator_sim::{Cluster, Faults, Holdings, Outcome, Transacted};
use kurator_storage::{Version, WriteBatch};
use openraft_rt_sim::SimRuntime;

/// Writes `key-<n>` for every `n` in `range`; returns the keys
/// acknowledged.
async fn write_keys(cluster: &Cluster, range: std::ops::Range<u32>) -> BTreeSet<Vec<u8>> {
    let mut acked = BTreeSet::new();
    for index in range {
        let key = format!("key-{index:03}").into_bytes();
        if cluster
            .write(WriteBatch::new().put(key.clone(), b"v".to_vec()))
            .await
            == Outcome::Acked
        {
            let _: bool = acked.insert(key);
        }
    }
    acked
}

/// Asserts that every node holds the same shards, that each holds every
/// acknowledged key, and returns what the first node holds.
fn agreed(cluster: &Cluster, acked: &BTreeSet<Vec<u8>>) -> Holdings {
    let states = cluster.states();
    let first = states.values().next().expect("a node").clone();
    for (id, holdings) in &states {
        assert_eq!(holdings, &first, "{id} agrees");
    }
    for key in acked {
        assert!(
            first.get(key).is_some(),
            "{} survives",
            String::from_utf8_lossy(key)
        );
    }
    first
}

#[test]
fn both_halves_keep_their_keys_and_take_writes() {
    SimRuntime::with_seed(11).block_on(async {
        let mut cluster = Cluster::start(11, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = write_keys(&cluster, 0..20).await;
        assert_eq!(cluster.split(b"key-010").await, Outcome::Acked, "split");
        assert_eq!(cluster.shards().len(), 2, "two shards");
        assert_eq!(
            cluster.split(b"key-010").await,
            Outcome::Rejected,
            "a shard starts there already"
        );
        acked.extend(write_keys(&cluster, 20..40).await);
        assert_eq!(cluster.split(b"key-030").await, Outcome::Acked, "again");
        acked.extend(write_keys(&cluster, 0..40).await);
        cluster.settle().await.unwrap();

        let held = agreed(&cluster, &acked);
        let shards: Vec<(u64, usize)> = held
            .shards()
            .map(|state| (state.shard().id().get(), state.kv().iter().count()))
            .collect();
        // The rejected split used up the id 3.
        assert_eq!(shards, [(1, 10), (2, 20), (4, 10)], "keys by shard");
        for state in held.shards() {
            let span = state.shard().span();
            assert!(
                state.kv().keys().all(|key| span.contains(key)),
                "{} holds only its keys",
                state.shard()
            );
        }
        // Versions go on from the shard that split: the newest version of
        // the new shard is past the split, which is past the older writes.
        let first = held.shard(ShardId::FIRST).unwrap().kv().version();
        let newest = held.shard(ShardId::new(4)).unwrap().kv().version();
        assert!(newest > first, "{newest} after {first}");
        assert!(first > Version::new(20), "{first}");
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_node_that_missed_the_split_comes_to_hold_both_halves() {
    SimRuntime::with_seed(12).block_on(async {
        let mut cluster = Cluster::start(12, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = write_keys(&cluster, 0..20).await;
        let behind = cluster
            .ids()
            .into_iter()
            .find(|id| Some(*id) != cluster.leader())
            .unwrap();
        cluster.crash(behind).await.unwrap();
        assert_eq!(cluster.split(b"key-100").await, Outcome::Acked, "split");
        // Enough writes on both sides that both shards purge their logs:
        // the node that was down gets snapshots, not the split entry.
        acked.extend(write_keys(&cluster, 20..80).await);
        acked.extend(write_keys(&cluster, 100..160).await);
        let purged = cluster
            .statuses()
            .into_iter()
            .find(|(id, _)| Some(*id) == cluster.leader())
            .and_then(|(_, status)| status.purged());
        assert!(purged.is_some(), "the first shard purged its log");
        cluster.restart(behind).await.unwrap();
        cluster.settle().await.unwrap();
        let held = agreed(&cluster, &acked);
        assert_eq!(held.shards().count(), 2, "both halves");
        let states = cluster.states();
        assert_eq!(states[&behind], held, "the node that was down caught up");
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_split_survives_a_crash_of_every_node() {
    SimRuntime::with_seed(13).block_on(async {
        let mut cluster = Cluster::start(13, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = write_keys(&cluster, 0..10).await;
        assert_eq!(cluster.split(b"key-005").await, Outcome::Acked, "split");
        for id in cluster.up() {
            cluster.crash(id).await.unwrap();
        }
        for id in cluster.ids() {
            cluster.restart(id).await.unwrap();
        }
        acked.extend(write_keys(&cluster, 0..10).await);
        cluster.settle().await.unwrap();
        let held = agreed(&cluster, &acked);
        assert_eq!(held.shards().count(), 2, "both halves");
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_single_node_splits_and_serves_both_halves() {
    SimRuntime::with_seed(14).block_on(async {
        let mut cluster = Cluster::start(14, 1, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = write_keys(&cluster, 0..10).await;
        assert_eq!(cluster.split(b"key-005").await, Outcome::Acked, "split");
        acked.extend(write_keys(&cluster, 0..10).await);
        cluster.settle().await.unwrap();
        let held = agreed(&cluster, &acked);
        assert_eq!(held.shards().count(), 2, "both halves");
        assert_eq!(
            cluster.leader_of(ShardId::new(2)),
            Some(NodeId::new(1)),
            "leads"
        );
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_node_that_joins_after_a_split_joins_every_shard() {
    SimRuntime::with_seed(15).block_on(async {
        let mut cluster = Cluster::start(15, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = write_keys(&cluster, 0..20).await;
        assert_eq!(cluster.split(b"key-010").await, Outcome::Acked, "split");
        let joined = cluster.join(NodeId::new(4)).await.unwrap();
        assert_eq!(joined, Outcome::Acked, "joined");
        acked.extend(write_keys(&cluster, 20..30).await);
        cluster.settle().await.unwrap();
        let held = agreed(&cluster, &acked);
        assert_eq!(cluster.states().len(), 4, "four nodes");
        assert_eq!(held.shards().count(), 2, "both halves on each");
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_transaction_reads_only_the_keys_of_its_shard() {
    SimRuntime::with_seed(16).block_on(async {
        let mut cluster = Cluster::start(16, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let _acked = write_keys(&cluster, 0..20).await;
        assert_eq!(cluster.split(b"key-010").await, Outcome::Acked, "split");
        let mut seen = Vec::new();
        let mut beyond = None;
        let outcome = cluster
            .transact(|txn| {
                txn.put("key-0055", "mine");
                seen = txn.range(Unbounded, Unbounded)?;
                beyond = Some(txn.get(b"key-015"));
                Ok(())
            })
            .await;
        assert!(matches!(outcome, Transacted::Committed(_)), "{outcome:?}");
        let keys: Vec<String> = seen
            .iter()
            .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
            .collect();
        let mut expected: Vec<String> = (0..10).map(|index| format!("key-{index:03}")).collect();
        expected.push("key-0055".to_owned());
        expected.sort();
        assert_eq!(keys, expected, "the first shard's keys and its own write");
        assert!(
            matches!(beyond, Some(Err(Error::Misrouted { .. }))),
            "a key of the other shard: {beyond:?}"
        );
        cluster.shutdown().await.unwrap();
    });
}
