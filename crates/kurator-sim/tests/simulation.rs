//! Clusters under random faults and membership changes: whatever happens,
//! an acknowledged write is never lost, every replica ends with the same
//! state, and the same seed replays the same run.
//!
//! `KURATOR_SIM_SEEDS=<n>` runs more seeds than the default, and
//! `KURATOR_SIM_SEED=<seed>` runs just one, to reproduce a failure.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code: a failure should panic with context"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kurator_raft::NodeId;
use kurator_raft::openraft::AsyncRuntime as _;
use kurator_sim::{Cluster, Faults, Outcome, Rng, SimError};
use kurator_storage::{KvState, WriteBatch};
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
    /// The state of every replica after the cluster settled.
    states: BTreeMap<NodeId, KvState>,
    /// The id the next node to join gets. Ids are never reused: a node
    /// that left keeps its directory.
    next_id: u64,
}

/// The fewest and the most nodes the schedule keeps in the cluster.
const MIN_NODES: usize = 3;
const MAX_NODES: usize = 5;

fn seeds() -> Vec<u64> {
    if let Some(seed) = env_u64("KURATOR_SIM_SEED") {
        return vec![seed];
    }
    (0..env_u64("KURATOR_SIM_SEEDS").unwrap_or(DEFAULT_SEEDS)).collect()
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect(name))
}

/// Runs a random schedule of writes, crashes, restarts, partitions, joins,
/// leaves and pauses for `seed`, then settles the cluster.
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
        0..56 => {
            let key = format!("key-{step}").into_bytes();
            let outcome = cluster
                .write(WriteBatch::new().put(key.clone(), b"value".to_vec()))
                .await;
            match outcome {
                Outcome::Acked(_) => drop(run.acked.insert(key)),
                Outcome::Unknown => drop(run.unknown.insert(key)),
                Outcome::Rejected => {}
                _ => panic!("an outcome added to kurator-sim: {outcome:?}"),
            }
            format!("write -> {outcome:?}")
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
    let mut states = run.states.values();
    let first = states.next().expect("replicas are up");
    for state in states {
        assert_eq!(state, first, "seed {seed}: replicas diverged");
    }
    for key in &run.acked {
        assert!(
            first.get(key).is_some(),
            "seed {seed}: acknowledged write {} was lost\n{}",
            String::from_utf8_lossy(key),
            run.trace.join("\n")
        );
    }
    for (key, _) in first.iter() {
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
    let mut faults = BTreeSet::new();
    for seed in seeds() {
        let run = run(seed);
        check(seed, &run);
        acked += run.acked.len();
        for event in &run.trace {
            let kind = event.split_whitespace().nth(1).unwrap_or_default();
            if ["crash", "restart", "isolate", "join", "leave"].contains(&kind) {
                let _: bool = faults.insert(kind.to_owned());
            }
        }
    }
    assert!(acked > 0, "some writes succeed");
    assert_eq!(
        faults.len(),
        5,
        "replicas crashed, restarted, were cut off, joined and left: {faults:?}"
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
            if let Outcome::Acked(_) = cluster
                .write(WriteBatch::new().put(key.clone(), b"v".to_vec()))
                .await
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
        assert!(
            !matches!(cut_off, Outcome::Acked(_)),
            "no quorum, no ack: {cut_off:?}"
        );
        SimRuntime::sleep(Duration::from_secs(2)).await;
        let outcome = cluster
            .write(WriteBatch::new().put(b"kept".to_vec(), b"v".to_vec()))
            .await;
        assert!(
            matches!(outcome, Outcome::Acked(_)),
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
            if let Outcome::Acked(_) = cluster
                .write(WriteBatch::new().put(key.clone(), b"v".to_vec()))
                .await
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
