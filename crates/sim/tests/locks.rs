//! Locks a coordinator of a transaction across shards left behind: they
//! hold a split back, and settling resolves them by asking the primary.

use std::time::Duration;

use kurator_raft::openraft::AsyncRuntime as _;
use kurator_sim::{Cluster, Coordinator, Faults, Outcome};
use kurator_storage::WriteBatch;
use openraft_rt_sim::SimRuntime;

#[test]
fn a_lock_left_behind_holds_a_split_back_until_settling_resolves_it() {
    SimRuntime::with_seed(31).block_on(async {
        let mut cluster = Cluster::start(31, 3, Faults::default())
            .await
            .expect("start");
        cluster.settle().await.expect("settle");
        let opening = WriteBatch::new().put("a", "1").put("z", "1");
        assert_eq!(cluster.write(opening).await, Outcome::Acked, "opened");
        assert_eq!(cluster.split(b"m").await, Outcome::Acked, "split");
        cluster.settle().await.expect("settle");
        assert!(cluster.unlocked(), "nothing prepared yet");

        let abandoned = SimRuntime::timeout(
            Duration::from_millis(1),
            Coordinator::new(&cluster).transact(|txn| {
                txn.put(b"a", "2")?;
                txn.put(b"z", "2")
            }),
        )
        .await;
        assert!(abandoned.is_err(), "the coordinator gave up");
        SimRuntime::sleep(Duration::from_secs(1)).await;
        assert!(!cluster.unlocked(), "its first part stays prepared");
        assert_eq!(cluster.split(b"c").await, Outcome::Rejected, "held back");

        cluster.settle().await.expect("settle");
        assert!(cluster.unlocked(), "resolved");
        for (id, holdings) in cluster.states() {
            let values = (holdings.get(b"a"), holdings.get(b"z"));
            let unchanged = (Some(b"1".as_slice()), Some(b"1".as_slice()));
            assert_eq!(values, unchanged, "{id}: aborted, never decided");
        }
        assert_eq!(cluster.split(b"c").await, Outcome::Acked, "then splits");
        cluster.shutdown().await.expect("shutdown");
    });
}
