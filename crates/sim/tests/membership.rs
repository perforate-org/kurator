//! Membership changes: nodes join as learners and vote only once they
//! caught up, nodes leave, and a partition lets only the majority change
//! the membership.
#![expect(
    clippy::expect_used,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeSet;
use std::time::Duration;

use kurator_raft::openraft::AsyncRuntime as _;
use kurator_raft::{NodeId, Progress};
use kurator_sim::{Cluster, Faults, Outcome};
use kurator_storage::WriteBatch;
use openraft_rt_sim::SimRuntime;

const fn id(raw: u64) -> NodeId {
    NodeId::new(raw)
}

/// Writes `count` keys starting at `from`; returns those acknowledged.
async fn write_keys(cluster: &Cluster, from: u32, count: u32) -> BTreeSet<Vec<u8>> {
    let mut acked = BTreeSet::new();
    for index in from..from.saturating_add(count) {
        let key = index.to_le_bytes().to_vec();
        if let Outcome::Acked(_) = cluster
            .write(WriteBatch::new().put(key.clone(), b"v".to_vec()))
            .await
        {
            let _: bool = acked.insert(key);
        }
    }
    acked
}

fn voters(cluster: &Cluster) -> BTreeSet<NodeId> {
    cluster.voters().expect("a leader").into_iter().collect()
}

#[test]
fn a_new_node_votes_only_after_it_caught_up_and_then_counts_in_the_quorum() {
    SimRuntime::with_seed(11).block_on(async {
        let mut cluster = Cluster::start(11, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = write_keys(&cluster, 0, 120).await;

        // The learner is added and goes down before it receives anything.
        let added = cluster.add_learner(id(4)).await.unwrap();
        assert!(matches!(added, Outcome::Acked(_)), "added: {added:?}");
        cluster.crash(id(4)).await.unwrap();
        acked.extend(write_keys(&cluster, 120, 20).await);
        assert!(
            matches!(cluster.progress(id(4)), Some(Progress::Behind { .. })),
            "behind: {:?}",
            cluster.progress(id(4))
        );
        assert_eq!(
            cluster.promote(id(4)).await,
            Outcome::Rejected,
            "a learner that is behind cannot vote"
        );
        assert_eq!(voters(&cluster), BTreeSet::from([id(1), id(2), id(3)]));

        // Back up, it catches up from the snapshot and the log, and votes.
        cluster.restart(id(4)).await.unwrap();
        let mut promoted = Outcome::Rejected;
        for _ in 0..100 {
            promoted = cluster.promote(id(4)).await;
            if promoted != Outcome::Rejected {
                break;
            }
            SimRuntime::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            matches!(promoted, Outcome::Acked(_)),
            "promoted: {promoted:?}"
        );
        assert!(voters(&cluster).contains(&id(4)), "votes");
        let joined = cluster
            .statuses()
            .into_iter()
            .find(|(node, _)| *node == id(4))
            .unwrap()
            .1;
        assert!(
            joined.snapshot().is_some(),
            "caught up from a snapshot: {joined:?}"
        );

        // With one old voter gone and another down, the quorum needs node 4.
        let leader = cluster.leader().unwrap();
        let old = [id(1), id(2), id(3)];
        let leaving = old.into_iter().find(|node| *node != leader).unwrap();
        let left = cluster.leave(leaving).await.unwrap();
        assert!(matches!(left, Outcome::Acked(_)), "left: {left:?}");
        let down = old
            .into_iter()
            .find(|node| *node != leader && *node != leaving)
            .unwrap();
        cluster.crash(down).await.unwrap();
        let late = write_keys(&cluster, 140, 10).await;
        assert!(!late.is_empty(), "the leader and node 4 form a quorum");
        acked.extend(late);

        cluster.settle().await.unwrap();
        let states = cluster.states();
        assert!(!states.contains_key(&leaving), "the node that left is gone");
        let newcomer = &states[&id(4)];
        for key in &acked {
            assert!(newcomer.get(key).is_some(), "node 4 holds every write");
        }
        for state in states.values() {
            assert_eq!(state, newcomer, "replicas agree");
        }
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn the_leader_can_remove_itself_and_the_rest_carry_on() {
    SimRuntime::with_seed(12).block_on(async {
        let mut cluster = Cluster::start(12, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = write_keys(&cluster, 0, 10).await;
        let leader = cluster.leader().unwrap();
        let left = cluster.leave(leader).await.unwrap();
        assert!(matches!(left, Outcome::Acked(_)), "left: {left:?}");
        acked.extend(write_keys(&cluster, 10, 10).await);
        cluster.settle().await.unwrap();
        let remaining = voters(&cluster);
        assert_eq!(remaining.len(), 2, "two voters: {remaining:?}");
        assert!(!remaining.contains(&leader), "the old leader left");
        assert_ne!(cluster.leader(), Some(leader), "a new leader");
        for state in cluster.states().values() {
            for key in &acked {
                assert!(state.get(key).is_some(), "every write survives");
            }
        }
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_cut_off_leader_cannot_change_the_membership() {
    SimRuntime::with_seed(13).block_on(async {
        let mut cluster = Cluster::start(13, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let old = cluster.leader().unwrap();
        let victim = cluster.ids().into_iter().find(|node| *node != old).unwrap();
        cluster.isolate(old);
        let cut_off = cluster.leave_via(old, victim).await.unwrap();
        assert!(
            !matches!(cut_off, Outcome::Acked(_)),
            "no quorum, no change: {cut_off:?}"
        );
        SimRuntime::sleep(Duration::from_secs(2)).await;

        // Meanwhile the majority admits a new node.
        let joined = cluster.join(id(4)).await.unwrap();
        assert!(matches!(joined, Outcome::Acked(_)), "joined: {joined:?}");
        cluster.settle().await.unwrap();
        assert_eq!(
            voters(&cluster),
            BTreeSet::from([id(1), id(2), id(3), id(4)]),
            "the victim stays; node 4 joined"
        );
        let states = cluster.states();
        assert_eq!(
            states.len(),
            4,
            "every node is up: {:?}",
            cluster.statuses()
        );
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn the_majority_removes_a_cut_off_node_and_admits_another() {
    SimRuntime::with_seed(14).block_on(async {
        let mut cluster = Cluster::start(14, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let leader = cluster.leader().unwrap();
        let cut_off = cluster
            .ids()
            .into_iter()
            .find(|node| *node != leader)
            .unwrap();
        cluster.isolate(cut_off);
        let mut acked = write_keys(&cluster, 0, 10).await;
        let left = cluster.leave(cut_off).await.unwrap();
        assert!(matches!(left, Outcome::Acked(_)), "left: {left:?}");
        let joined = cluster.join(id(4)).await.unwrap();
        assert!(matches!(joined, Outcome::Acked(_)), "joined: {joined:?}");
        acked.extend(write_keys(&cluster, 10, 10).await);
        cluster.heal();
        cluster.settle().await.unwrap();
        let expected: BTreeSet<NodeId> = [id(1), id(2), id(3), id(4)]
            .into_iter()
            .filter(|node| *node != cut_off)
            .collect();
        assert_eq!(voters(&cluster), expected, "membership");
        for state in cluster.states().values() {
            for key in &acked {
                assert!(state.get(key).is_some(), "every write survives");
            }
        }
        cluster.shutdown().await.unwrap();
    });
}
