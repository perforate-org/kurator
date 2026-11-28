//! The meta shard: it hands out the ids of new shards, records where every
//! shard is, and the client routes by what it records, through splits,
//! crashes and replica moves.
#![expect(
    clippy::expect_used,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeSet;

use oleaph_raft::NodeId;
use oleaph_raft::openraft::AsyncRuntime as _;
use oleaph_shard::ShardId;
use oleaph_sim::{Cluster, Faults, Outcome};
use oleaph_storage::WriteBatch;
use openraft_rt_sim::SimRuntime;

fn ids(ids: impl IntoIterator<Item = u64>) -> BTreeSet<NodeId> {
    ids.into_iter().map(NodeId::new).collect()
}

/// Where the registry records the replicas of `shard`.
async fn recorded(cluster: &Cluster, shard: ShardId) -> BTreeSet<NodeId> {
    let registry = cluster.registry().await.expect("a meta leader");
    registry
        .placement(shard)
        .expect("recorded")
        .replicas()
        .clone()
}

#[test]
fn the_registry_records_every_split_and_routes_the_client() {
    SimRuntime::with_seed(21).block_on(async {
        let mut cluster = Cluster::start(21, 3, Faults::default())
            .await
            .expect("start");
        cluster.settle().await.expect("settle");
        assert_eq!(cluster.split(b"m").await, Outcome::Acked, "split");
        assert_eq!(cluster.split(b"t").await, Outcome::Acked, "again");
        cluster.settle().await.expect("settle");
        let registry = cluster.registry().await.expect("a meta leader");
        assert_eq!(registry.next(), ShardId::new(4), "two ids taken");
        let map = registry.map().expect("the registry tiles the keys");
        let shards: Vec<_> = map.iter().cloned().collect();
        assert_eq!(shards, cluster.shards(), "the client routes by it");
        for shard in &shards {
            let placement = registry.placement(shard.id()).expect("recorded");
            assert_eq!(placement.replicas(), &ids(1..=3), "{shard} everywhere");
        }
        assert_eq!(
            registry.placement(ShardId::META),
            None,
            "the meta shard is not its own record"
        );
        cluster.shutdown().await.expect("shutdown");
    });
}

#[test]
fn an_id_is_never_handed_out_twice_across_a_meta_leader_crash() {
    SimRuntime::with_seed(22).block_on(async {
        let mut cluster = Cluster::start(22, 3, Faults::default())
            .await
            .expect("start");
        cluster.settle().await.expect("settle");
        assert_eq!(cluster.split(b"m").await, Outcome::Acked, "split");
        let leader = cluster.leader_of(ShardId::META).expect("a meta leader");
        cluster.crash(leader).await.expect("crash");
        assert_eq!(cluster.split(b"t").await, Outcome::Acked, "split");
        cluster.settle().await.expect("settle");
        let held: Vec<u64> = cluster
            .shards()
            .iter()
            .map(|shard| shard.id().get())
            .collect();
        assert_eq!(held, [1, 2, 3], "each split had an id of its own");
        cluster.shutdown().await.expect("shutdown");
    });
}

#[test]
fn a_replica_moves_and_the_registry_follows() {
    SimRuntime::with_seed(23).block_on(async {
        let mut cluster = Cluster::start(23, 3, Faults::default())
            .await
            .expect("start");
        cluster.settle().await.expect("settle");
        assert_eq!(cluster.split(b"m").await, Outcome::Acked, "split");
        let write = WriteBatch::new().put(b"a".to_vec(), b"v".to_vec());
        assert_eq!(cluster.write(write).await, Outcome::Acked, "write");
        let first = ShardId::FIRST;
        let right = ShardId::new(2);

        cluster.add_node(NodeId::new(5)).await.expect("a new node");
        cluster.settle().await.expect("settle");
        assert!(
            !cluster.ids().contains(&NodeId::new(5)),
            "a node no shard took is forgotten"
        );
        cluster.add_node(NodeId::new(4)).await.expect("a new node");
        let held = cluster
            .move_replica(first, NodeId::new(1), NodeId::new(2))
            .await;
        assert_eq!(held, Outcome::Rejected, "not onto a node that holds it");
        let onto_new = cluster
            .move_replica(first, NodeId::new(1), NodeId::new(4))
            .await;
        assert_eq!(onto_new, Outcome::Acked, "onto the new node");
        cluster.settle().await.expect("settle");
        assert_eq!(recorded(&cluster, first).await, ids(2..=4), "moved");
        assert_eq!(recorded(&cluster, right).await, ids(1..=3), "untouched");

        // Back onto the node that still has the replica it left behind.
        let back = cluster
            .move_replica(first, NodeId::new(2), NodeId::new(1))
            .await;
        assert_eq!(back, Outcome::Acked, "back");
        cluster.settle().await.expect("settle");
        assert_eq!(recorded(&cluster, first).await, ids([1, 3, 4]), "back");

        let states = cluster.states();
        let holds = |id: u64| states[&NodeId::new(id)].shard(first).is_some();
        assert!(!holds(2), "the node it moved off holds it no more");
        assert!(holds(1) && holds(4), "the nodes it moved to hold it");
        for id in [1, 3, 4] {
            let key = states[&NodeId::new(id)].get(b"a");
            assert!(key.is_some(), "{id} holds the keys written before");
        }
        assert_eq!(
            cluster
                .write(WriteBatch::new().put(b"b".to_vec(), b"v".to_vec()))
                .await,
            Outcome::Acked,
            "the moved shard takes writes"
        );
        cluster.shutdown().await.expect("shutdown");
    });
}

#[test]
fn settling_waits_for_a_new_leader_after_the_leader_moves_off() {
    SimRuntime::with_seed(24).block_on(async {
        let mut cluster = Cluster::start(24, 3, Faults::default())
            .await
            .expect("start");
        cluster.settle().await.expect("settle");
        let first = ShardId::FIRST;
        let leader = cluster.leader_of(first).expect("a leader");
        cluster.add_node(NodeId::new(4)).await.expect("a new node");
        let moved = cluster.move_replica(first, leader, NodeId::new(4)).await;
        assert_eq!(moved, Outcome::Acked, "moved off the leader");
        cluster.settle().await.expect("settle");
        let next = cluster.leader_of(first).expect("a new leader");
        assert_ne!(next, leader, "the old leader stepped down");
        let voters = cluster.voters_of(first).expect("a leader");
        assert!(voters.contains(&next), "{next} votes");
        cluster.shutdown().await.expect("shutdown");
    });
}
