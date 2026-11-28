//! Locks a coordinator of a transaction across shards left behind: they
//! hold writes and a split back until their deadline passes, and then
//! whoever runs into them, or settling, resolves them at the primary. Long
//! after, the decisions are forgotten.
#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "test code: a failure should panic with context"
)]

use std::time::Duration;

use oleaph_client::{Call, Collected, Collector, Coordinator, Reply, Shards as _, Transacted};
use oleaph_consensus::Transaction as _;
use oleaph_raft::openraft::AsyncRuntime as _;
use oleaph_shard::{NamespaceId, ShardId};
use oleaph_sim::{Cluster, Faults, Holdings, Outcome};
use oleaph_storage::{Command, Deadline, Decide, Decision, Draft, Outcome as Applied, WriteBatch};
use openraft_rt_sim::SimRuntime;

/// Longer than a coordinator means to take over a transaction.
const PAST_THE_DEADLINE: Duration = Duration::from_secs(6);

/// A settled cluster that holds `a = 1` on the first shard and `z = 1` on
/// the second.
async fn cluster(seed: u64) -> Cluster {
    let mut cluster = Cluster::start(seed, 3, Faults::default())
        .await
        .expect("start");
    cluster.settle().await.expect("settle");
    let opening = WriteBatch::new().put("a", "1").put("z", "1");
    assert_eq!(cluster.write(opening).await, Outcome::Acked, "opened");
    assert_eq!(cluster.split(b"m").await, Outcome::Acked, "split");
    cluster.settle().await.expect("settle");
    assert!(cluster.unlocked(), "nothing prepared yet");
    cluster
}

/// Writes `value` to `a` and `z` in one transaction across both shards.
async fn put_both(cluster: &Cluster, value: &str) -> Transacted {
    let mut txn = Coordinator::new(cluster).begin(NamespaceId::DEFAULT);
    for key in [b"a", b"z"] {
        if txn.put(key, value).await.is_err() {
            return Transacted::Rejected;
        }
    }
    txn.commit().await
}

/// Reads `a` and writes `z` in one transaction across both shards.
async fn read_a_write_z(cluster: &Cluster) -> Transacted {
    let mut txn = Coordinator::new(cluster).begin(NamespaceId::DEFAULT);
    if txn.get(b"a").await.is_err() || txn.put(b"z", "3").await.is_err() {
        return Transacted::Rejected;
    }
    txn.commit().await
}

/// Starts a transaction that writes `a` and `z` and gives up on it as
/// soon as it prepared its first part, on the first shard.
async fn abandon(cluster: &Cluster) {
    let mut txn = Coordinator::new(cluster).begin(NamespaceId::DEFAULT);
    txn.put(b"a", "2").await.expect("a");
    txn.put(b"z", "2").await.expect("z");
    let abandoned = SimRuntime::timeout(Duration::from_millis(1), txn.commit()).await;
    assert!(abandoned.is_err(), "the coordinator gave up");
    SimRuntime::sleep(Duration::from_secs(1)).await;
    assert!(!cluster.unlocked(), "its first part stays prepared");
}

/// The values of `a` and `z` on a replica.
type Values = (Option<Vec<u8>>, Option<Vec<u8>>);

/// The values of `a` and `z` on every replica.
fn values(cluster: &Cluster) -> Vec<Values> {
    cluster
        .states()
        .values()
        .map(|holdings| {
            let get = |key: &[u8]| holdings.get(key).map(<[u8]>::to_vec);
            (get(b"a"), get(b"z"))
        })
        .collect()
}

#[test]
fn a_coordinator_tells_every_shard() {
    SimRuntime::with_seed(31).block_on(async {
        let cluster = cluster(31).await;
        let moved = put_both(&cluster, "0").await;
        assert!(matches!(moved, Transacted::Committed(_)), "{moved:?}");
        assert!(cluster.unlocked(), "every shard was told");
        cluster.shutdown().await.expect("shutdown");
    });
}

#[test]
fn a_lock_left_behind_holds_a_split_back_until_settling_resolves_it() {
    SimRuntime::with_seed(32).block_on(async {
        let mut cluster = cluster(32).await;
        abandon(&cluster).await;
        assert_eq!(cluster.split(b"c").await, Outcome::Rejected, "held back");
        cluster.settle().await.expect("settle");
        assert!(cluster.unlocked(), "resolved");
        let one = Some(b"1".to_vec());
        for value in values(&cluster) {
            assert_eq!(value, (one.clone(), one.clone()), "aborted, never decided");
        }
        assert_eq!(cluster.split(b"c").await, Outcome::Acked, "then splits");
        cluster.shutdown().await.expect("shutdown");
    });
}

#[test]
fn whoever_runs_into_a_lock_past_its_deadline_resolves_it() {
    SimRuntime::with_seed(33).block_on(async {
        let cluster = cluster(33).await;
        abandon(&cluster).await;
        let write = || cluster.write(WriteBatch::new().put("a", "3"));
        assert_eq!(write().await, Outcome::Rejected, "held back");
        let across = || read_a_write_z(&cluster);
        assert_eq!(across().await, Transacted::Aborted, "held back");
        assert!(!cluster.unlocked(), "not before the deadline");
        SimRuntime::sleep(PAST_THE_DEADLINE).await;
        assert_eq!(write().await, Outcome::Acked, "a write resolves it");
        assert!(cluster.unlocked(), "resolved");

        abandon(&cluster).await;
        SimRuntime::sleep(PAST_THE_DEADLINE).await;
        assert_eq!(across().await, Transacted::Aborted, "in the way");
        assert!(cluster.unlocked(), "a coordinator resolves it");

        abandon(&cluster).await;
        SimRuntime::sleep(PAST_THE_DEADLINE).await;
        let reads = cluster
            .transact(|txn| {
                let _a = txn.get(b"a")?;
                txn.put(b"b", "1");
                Ok(())
            })
            .await;
        assert_eq!(reads, Transacted::Aborted, "in the way");
        assert!(cluster.unlocked(), "a transaction on one shard resolves it");
        let three = (Some(b"3".to_vec()), Some(b"1".to_vec()));
        for value in values(&cluster) {
            assert_eq!(value, three, "only the write after the deadline");
        }
        cluster.shutdown().await.expect("shutdown");
    });
}

/// How many decisions on transactions across shards the replicas keep.
fn decisions(cluster: &Cluster) -> usize {
    cluster
        .states()
        .values()
        .flat_map(Holdings::shards)
        .map(|state| state.kv().decisions().count())
        .sum()
}

#[test]
fn decisions_long_past_their_deadline_are_forgotten() {
    SimRuntime::with_seed(34).block_on(async {
        let mut cluster = cluster(34).await;
        let moved = put_both(&cluster, "0").await;
        assert!(matches!(moved, Transacted::Committed(_)), "{moved:?}");
        abandon(&cluster).await;
        assert!(decisions(&cluster) > 0, "kept while it may be asked");
        assert_eq!(
            Collector::new(&cluster).collect(Duration::ZERO).await,
            Collected::Forgotten,
            "nothing to forget yet"
        );
        assert!(decisions(&cluster) > 0, "still kept");
        SimRuntime::sleep(2 * PAST_THE_DEADLINE).await;
        assert_eq!(
            Collector::new(&cluster)
                .collect(Duration::from_secs(1))
                .await,
            Collected::Forgotten,
            "collected"
        );
        assert!(cluster.unlocked(), "the lock left behind was resolved");
        cluster.settle().await.expect("settle");
        assert_eq!(decisions(&cluster), 0, "forgotten on every replica");
        let committed = (Some(b"0".to_vec()), Some(b"0".to_vec()));
        for value in values(&cluster) {
            assert_eq!(value, committed, "the commit stands, the abandoned one not");
        }
        let again = put_both(&cluster, "4").await;
        assert!(matches!(again, Transacted::Committed(_)), "{again:?}");
        cluster.shutdown().await.expect("shutdown");
    });
}

/// The shard that holds `z`.
fn second(cluster: &Cluster) -> ShardId {
    cluster
        .shards()
        .into_iter()
        .find(|shard| shard.span().contains(b"z"))
        .expect("the second shard")
        .id()
}

/// Proposes `command` on `shard`'s leader: how it applied.
async fn propose(cluster: &Cluster, shard: ShardId, command: Command) -> Applied {
    match cluster.call(shard, Call::Propose(command)).await {
        Some(Reply::Applied(applied)) => applied,
        other => panic!("{shard}: {other:?}"),
    }
}

/// Writes `a = 2` and `z = 2` in a transaction across both shards whose
/// coordinator stops right after the commit point: the primary, the first
/// shard, committed it, and the second shard still holds its part locked.
async fn commit_on_the_primary_only(cluster: &Cluster) {
    let shards = [ShardId::FIRST, second(cluster)];
    let id = cluster.next_txn();
    let deadline = Deadline::new(cluster.now().millis().saturating_add(5000));
    for (shard, key) in shards.into_iter().zip([b"a", b"z"]) {
        let Some(Reply::Begun { at, .. }) = cluster.call(shard, Call::Begin).await else {
            panic!("{shard} begins");
        };
        let mut draft = Draft::new(at);
        draft.put(key, "2");
        let prepare = draft.into_prepare(id, b"a".to_vec(), deadline);
        let prepared = propose(cluster, shard, Command::Prepare(prepare)).await;
        assert!(matches!(prepared, Applied::Prepared(_)), "{prepared:?}");
    }
    let decide = Decide::new(id, b"a".to_vec(), deadline, Decision::Commit);
    let committed = propose(cluster, ShardId::FIRST, Command::Decide(decide)).await;
    assert!(matches!(committed, Applied::Committed(_)), "{committed:?}");
}

#[test]
fn a_read_only_transaction_does_not_see_half_of_one_across_shards() {
    SimRuntime::with_seed(35).block_on(async {
        let cluster = cluster(35).await;
        commit_on_the_primary_only(&cluster).await;
        // A second transaction reads what the first wrote on the first
        // shard, and writes `y` on the second.
        let mut txn = Coordinator::new(&cluster).begin(NamespaceId::DEFAULT);
        assert_eq!(txn.get(b"a").await.expect("a"), Some(b"2".to_vec()), "a");
        txn.put(b"y", "after").await.expect("y");
        let after = txn.commit().await;
        assert!(matches!(after, Transacted::Committed(_)), "{after:?}");
        // Seeing `y` but not `z` puts this read after the second
        // transaction, which comes after the first, and before the first.
        let mut seen = None;
        let read = cluster
            .transact_in(second(&cluster), |reader| {
                seen = Some((reader.get(b"y")?, reader.get(b"z")?));
                Ok(())
            })
            .await;
        let skewed = Some((Some(b"after".to_vec()), Some(b"1".to_vec())));
        assert!(
            !(matches!(read, Transacted::Committed(_)) && seen == skewed),
            "read skew: {read:?} saw {seen:?}"
        );
        cluster.shutdown().await.expect("shutdown");
    });
}
