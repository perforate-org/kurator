//! Clusters under random faults, membership changes, splits and replica
//! moves: whatever happens, an acknowledged write is never lost, every
//! replica of a shard ends with the same state, the shards hold every key
//! once, and the same seed replays the same run.
//!
//! `OLEAPH_SIM_SEEDS=<n>` runs more seeds than the default, and
//! `OLEAPH_SIM_SEED=<seed>` runs just one, to reproduce a failure.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code: a failure should panic with context"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use oleaph_consensus::{Duty, Route};
use oleaph_raft::NodeId;
use oleaph_raft::openraft::AsyncRuntime as _;
use oleaph_shard::{Shard, ShardId, ShardMap, ShardState};
use oleaph_sim::{Cluster, Faults, Holdings, Outcome, Rng, SimError};
use oleaph_storage::WriteBatch;
use openraft_rt_sim::SimRuntime;

const DEFAULT_SEEDS: u64 = 12;
const STEPS: u64 = 120;
/// The longest gap between two steps, in milliseconds of simulated time.
const MAX_GAP_MS: u64 = 200;

/// What one simulated run did and how it ended.
#[derive(Debug, PartialEq, Eq)]
struct Run {
    /// Every step and its outcome, for comparing runs.
    trace: Vec<String>,
    /// Keys whose writes were acknowledged.
    acked: BTreeSet<Vec<u8>>,
    /// Keys whose writes may or may not have happened.
    unknown: BTreeSet<Vec<u8>>,
    /// What every node held after the cluster settled.
    states: BTreeMap<NodeId, Holdings>,
    /// The id the next node to join gets. Ids are never reused: a node
    /// that left keeps its directory.
    next_id: u64,
}

/// The fewest and the most nodes the schedule keeps in the cluster.
const MIN_NODES: usize = 3;
const MAX_NODES: usize = 5;

fn seeds() -> Vec<u64> {
    if let Some(seed) = env_u64("OLEAPH_SIM_SEED") {
        return vec![seed];
    }
    let count = env_u64("OLEAPH_SIM_SEEDS").unwrap_or(DEFAULT_SEEDS);
    let mut seeds: Vec<u64> = (0..count).collect();
    seeds.extend(REGRESSIONS.iter().filter(|seed| **seed >= count));
    seeds
}

/// Seeds that once failed, run every time:
/// - 16: a leave ran before the client learned of a split, and missed the
///   new shard.
/// - 205: a split whose outcome was unknown applied after settle first
///   looked.
const REGRESSIONS: [u64; 2] = [16, 205];

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect(name))
}

/// Runs a random schedule of writes, crashes, restarts, partitions, joins,
/// leaves, splits, moves and pauses for `seed`, then settles the cluster.
fn run(seed: u64) -> Run {
    SimRuntime::with_seed(seed).block_on(async {
        let mut rng = Rng::new(seed);
        let mut cluster = Cluster::start(seed, 3, Faults::default()).await.unwrap();
        let mut run = Run {
            trace: Vec::new(),
            acked: BTreeSet::new(),
            unknown: BTreeSet::new(),
            states: BTreeMap::new(),
            next_id: 4,
        };
        for step in 0..STEPS {
            let event = act(&mut cluster, &mut rng, step, &mut run).await.unwrap();
            run.trace.push(format!("{step}: {event}"));
            SimRuntime::sleep(Duration::from_millis(rng.below(MAX_GAP_MS))).await;
        }
        cluster
            .settle()
            .await
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
        run.states = cluster.states();
        run.trace.push(format!("settled: {:?}", cluster.statuses()));
        cluster.shutdown().await.unwrap();
        run
    })
}

/// Takes one random step.
async fn act(
    cluster: &mut Cluster,
    rng: &mut Rng,
    step: u64,
    run: &mut Run,
) -> Result<String, SimError> {
    let roll = rng.below(100);
    let event = match roll {
        0..52 => {
            let key = format!("key-{step}").into_bytes();
            let outcome = cluster
                .write(WriteBatch::new().put(key.clone(), b"value".to_vec()))
                .await;
            match outcome {
                Outcome::Acked => drop(run.acked.insert(key)),
                Outcome::Unknown => drop(run.unknown.insert(key)),
                Outcome::Rejected => {}
                _ => panic!("an outcome added to oleaph-sim: {outcome:?}"),
            }
            format!("write -> {outcome:?}")
        }
        52..56 => {
            let at = format!("key-{}", rng.below(STEPS)).into_bytes();
            let outcome = cluster.split(&at).await;
            format!("split {} -> {outcome:?}", String::from_utf8_lossy(&at))
        }
        56..66 => match rng.pick(&cluster.up()).copied() {
            Some(id) => {
                cluster.crash(id).await?;
                assert!(cluster.down().contains(&id), "{id} is down");
                format!("crash {id}")
            }
            None => "crash: none up".to_owned(),
        },
        66..78 => match rng.pick(&cluster.down()).copied() {
            Some(id) => {
                cluster.restart(id).await?;
                format!("restart {id}")
            }
            None => "restart: none down".to_owned(),
        },
        78..84 => rng.pick(&cluster.ids()).copied().map_or_else(
            || "isolate: no replicas".to_owned(),
            |id| {
                cluster.isolate(id);
                format!("isolate {id}")
            },
        ),
        84..90 => {
            cluster.heal();
            "heal".to_owned()
        }
        90..92 => {
            for id in cluster.up() {
                cluster.crash(id).await?;
            }
            for id in cluster.ids() {
                cluster.restart(id).await?;
            }
            "restart all".to_owned()
        }
        92..95 if cluster.ids().len() < MAX_NODES => {
            let id = NodeId::new(run.next_id);
            run.next_id = run.next_id.saturating_add(1);
            let outcome = cluster.join(id).await?;
            format!("join {id} -> {outcome:?}")
        }
        95..97 if cluster.ids().len() > MIN_NODES => match rng.pick(&cluster.ids()).copied() {
            Some(id) => {
                let outcome = cluster.leave(id).await?;
                format!("leave {id} -> {outcome:?}")
            }
            None => "leave: no replicas".to_owned(),
        },
        97..99 => {
            let shard = rng.pick(&cluster.shards()).map(Shard::id);
            let voters = shard.and_then(|shard| cluster.voters_of(shard));
            let others: Vec<NodeId> = cluster
                .ids()
                .into_iter()
                .filter(|id| voters.as_ref().is_some_and(|voters| !voters.contains(id)))
                .collect();
            let from = voters.as_ref().and_then(|voters| rng.pick(voters).copied());
            match (shard, from, rng.pick(&others).copied()) {
                (Some(shard), Some(from), Some(to)) => {
                    let outcome = cluster.move_replica(shard, from, to).await;
                    format!("move {shard} {from} -> {to}: {outcome:?}")
                }
                _ => "move: nowhere".to_owned(),
            }
        }
        _ => {
            let pause = Duration::from_millis(rng.between(10, 2000));
            SimRuntime::sleep(pause).await;
            format!("pause {pause:?}")
        }
    };
    Ok(event)
}

/// Checks what every run must satisfy.
fn check(seed: u64, run: &Run) {
    let mut shards: BTreeMap<ShardId, &ShardState> = BTreeMap::new();
    for (id, holdings) in &run.states {
        for state in holdings.shards() {
            let first = *shards.entry(state.shard().id()).or_insert(state);
            assert_eq!(
                state,
                first,
                "seed {seed}: {id} diverged on {}",
                state.shard()
            );
        }
    }
    let tiles = ShardMap::from_shards(shards.values().map(|state| state.shard().clone()));
    assert!(
        tiles.is_some(),
        "seed {seed}: the shards do not hold every key once: {:?}",
        shards
            .values()
            .map(|state| state.shard().to_string())
            .collect::<Vec<_>>()
    );
    let holder = |key: &[u8]| {
        shards
            .values()
            .find(|state| state.shard().span().contains(key))
            .expect("a shard holds every key")
    };
    for key in &run.acked {
        assert!(
            holder(key).kv().get(key).is_some(),
            "seed {seed}: acknowledged write {} was lost\n{}",
            String::from_utf8_lossy(key),
            run.trace.join("\n")
        );
    }
    for (key, _) in shards.values().flat_map(|state| state.kv().iter()) {
        assert!(
            run.acked.contains(key) || run.unknown.contains(key),
            "seed {seed}: {} appeared without a write",
            String::from_utf8_lossy(key)
        );
    }
}

#[test]
fn random_faults_never_lose_an_acknowledged_write() {
    let mut acked = 0;
    let mut split = 0;
    let mut faults = BTreeSet::new();
    for seed in seeds() {
        let run = run(seed);
        check(seed, &run);
        acked += run.acked.len();
        for event in &run.trace {
            let kind = event.split_whitespace().nth(1).unwrap_or_default();
            if [
                "crash", "restart", "isolate", "join", "leave", "split", "move",
            ]
            .contains(&kind)
            {
                let _: bool = faults.insert(kind.to_owned());
            }
            if kind == "split" && event.ends_with("Acked") {
                split += 1;
            }
        }
    }
    assert!(acked > 0, "some writes succeed");
    assert!(split > 0, "some splits succeed");
    assert_eq!(
        faults.len(),
        7,
        "replicas crashed, restarted, were cut off, joined, left and moved, shards split: {faults:?}"
    );
}

#[test]
fn the_same_seed_replays_the_same_run() {
    let seed = seeds().first().copied().unwrap_or(0);
    let first = run(seed);
    let second = run(seed);
    assert_eq!(first.trace, second.trace, "same steps and outcomes");
    assert_eq!(first, second, "same end state");
}

#[test]
fn writes_and_chores_go_to_the_leader() {
    SimRuntime::with_seed(1).block_on(async {
        let mut cluster = Cluster::start(1, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let leader = cluster.leader().unwrap();
        let routes = cluster.routes();
        assert_eq!(routes.len(), 3, "every replica");
        for (id, route, duty) in routes {
            if id == leader {
                assert_eq!((route, duty), (Route::Here, Duty::Mine), "the leader");
            } else {
                assert_eq!(
                    (route, duty),
                    (Route::Leader(leader), Duty::Others),
                    "follower {id}"
                );
            }
        }
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_follower_that_missed_purged_entries_catches_up_from_a_snapshot() {
    SimRuntime::with_seed(1).block_on(async {
        let mut cluster = Cluster::start(1, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let behind = cluster
            .ids()
            .into_iter()
            .find(|id| Some(*id) != cluster.leader())
            .unwrap();
        cluster.crash(behind).await.unwrap();
        let mut acked = BTreeSet::new();
        for index in 0..120_u32 {
            let key = index.to_le_bytes().to_vec();
            if cluster
                .write(WriteBatch::new().put(key.clone(), b"v".to_vec()))
                .await
                == Outcome::Acked
            {
                let _: bool = acked.insert(key);
            }
        }
        let leader = cluster
            .statuses()
            .into_iter()
            .find(|(id, _)| Some(*id) == cluster.leader())
            .unwrap()
            .1;
        assert!(
            leader.purged().is_some(),
            "the leader purged its log: {leader:?}"
        );
        cluster.restart(behind).await.unwrap();
        cluster.settle().await.unwrap();
        let (_, caught_up) = cluster
            .statuses()
            .into_iter()
            .find(|(id, _)| *id == behind)
            .unwrap();
        assert!(
            caught_up.snapshot().is_some(),
            "installed a snapshot: {caught_up:?}"
        );
        let states = cluster.states();
        assert_eq!(states.get(&behind), states.values().next(), "caught up");
        for key in &acked {
            assert!(states[&behind].get(key).is_some(), "has every write");
        }
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn a_cut_off_leader_cannot_commit_and_its_writes_never_appear() {
    SimRuntime::with_seed(2).block_on(async {
        let mut cluster = Cluster::start(2, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let old = cluster.leader().unwrap();
        cluster.isolate(old);
        // Only the cut-off leader takes this write; it cannot reach a quorum.
        let cut_off = cluster
            .write_to(old, WriteBatch::new().put(b"lost".to_vec(), b"v".to_vec()))
            .await;
        assert!(cut_off != Outcome::Acked, "no quorum, no ack: {cut_off:?}");
        SimRuntime::sleep(Duration::from_secs(2)).await;
        let outcome = cluster
            .write(WriteBatch::new().put(b"kept".to_vec(), b"v".to_vec()))
            .await;
        assert!(
            outcome == Outcome::Acked,
            "the majority elects a new leader: {outcome:?}"
        );
        cluster.settle().await.unwrap();
        let states = cluster.states();
        for state in states.values() {
            assert!(state.get(b"kept").is_some(), "the majority's write");
            assert!(state.get(b"lost").is_none(), "the minority's write is gone");
        }
        cluster.shutdown().await.unwrap();
    });
}

#[test]
fn every_acknowledged_write_survives_a_full_cluster_restart() {
    SimRuntime::with_seed(3).block_on(async {
        let mut cluster = Cluster::start(3, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut acked = Vec::new();
        for index in 0..60_u32 {
            let key = index.to_le_bytes().to_vec();
            if cluster
                .write(WriteBatch::new().put(key.clone(), b"v".to_vec()))
                .await
                == Outcome::Acked
            {
                acked.push(key);
            }
        }
        assert!(!acked.is_empty(), "writes succeed");
        for id in cluster.ids() {
            cluster.crash(id).await.unwrap();
        }
        for id in cluster.ids() {
            cluster.restart(id).await.unwrap();
        }
        cluster.settle().await.unwrap();
        let leader = cluster.leader().unwrap();
        let statuses = cluster.statuses();
        let term = statuses.first().unwrap().1.term();
        assert!(term > 1, "the restart elected a new leader: {statuses:?}");
        for (_, status) in &statuses {
            assert_eq!(status.leader(), Some(leader), "one leader: {statuses:?}");
            assert_eq!(status.term(), term, "one term: {statuses:?}");
            assert_eq!(
                status.last_log(),
                status.applied().map(|applied| applied.index),
                "applied everything logged: {statuses:?}"
            );
        }
        let read = cluster
            .read(|state| acked.iter().all(|key| state.get(key).is_some()))
            .await;
        assert_eq!(read, Some(true), "a linearizable read sees every write");
        cluster.shutdown().await.unwrap();
    });
}
