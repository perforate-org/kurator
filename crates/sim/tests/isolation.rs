//! Isolation under faults, checked as Elle checks it: concurrent
//! transactions append to lists and read them, on one shard and across
//! two, while replicas crash, restart and get cut off, and decisions are
//! forgotten. Each run records what every transaction read and appended
//! and how it ended, and [`oleaph_history`] looks for a history that no
//! serial order of the committed transactions explains.
//!
//! The claim checked is the one the storage crate makes: committed
//! transactions are serializable. Real-time order is not checked, and
//! neither are the reads of transactions that did not commit.
//!
//! `OLEAPH_SIM_SEEDS=<n>` runs more seeds than the default, and
//! `OLEAPH_SIM_SEED=<seed>` runs just one, to reproduce a failure.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::time::Duration;

use futures_util::future::join_all;
use oleaph_client::{Call, Collector, Coordinator, Reply, Shards as _, Transacted};
use oleaph_consensus::Transaction as _;
use oleaph_history::{Checked, Dependency, Element, Fate, History, Key, Txn, check};
use oleaph_raft::openraft::AsyncRuntime as _;
use oleaph_shard::{NamespaceId, ShardId};
use oleaph_sim::{Cluster, Faults, Outcome, Rng};
use oleaph_storage::{Command, Deadline, Decide, Decision, Draft, Outcome as Applied};
use openraft_rt_sim::SimRuntime;

const DEFAULT_SEEDS: u64 = 8;
const STEPS: u64 = 60;
/// The keys, first and last: keys `0..=2` on the first shard, the rest on
/// the second, which starts at [`SPLIT_AT`].
const EVERY_KEY: (u64, u64) = (0, 5);
const FIRST_SHARD: (u64, u64) = (0, 2);
const SECOND_SHARD: (u64, u64) = (SPLIT_AT, 5);
const SPLIT_AT: u64 = 3;
/// How many transactions run at once.
const CONCURRENT: u64 = 4;
/// The longest gap between two steps, in milliseconds of simulated time.
const MAX_GAP_MS: u64 = 200;

fn seeds() -> Vec<u64> {
    if let Some(seed) = env_u64("OLEAPH_SIM_SEED") {
        return vec![seed];
    }
    (0..env_u64("OLEAPH_SIM_SEEDS").unwrap_or(DEFAULT_SEEDS)).collect()
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect(name))
}

fn key(index: u64) -> Vec<u8> {
    format!("list/{index}").into_bytes()
}

/// A list as a value: its elements, eight bytes each.
fn encode(list: &[Element]) -> Vec<u8> {
    list.iter()
        .flat_map(|element| element.value().to_le_bytes())
        .collect()
}

fn decode(value: Option<&[u8]>) -> Vec<Element> {
    value
        .unwrap_or_default()
        .chunks_exact(8)
        .map(|bytes| Element::new(u64::from_le_bytes(bytes.try_into().unwrap())))
        .collect()
}

/// One step a transaction plans: read a key's list, or append an element
/// to it (which reads it first).
#[derive(Debug, Clone, Copy)]
enum Step {
    Read(u64),
    Append(u64, Element),
}

/// Where a transaction runs.
#[derive(Debug, Clone, Copy)]
enum Route {
    /// On one shard's leader, committing in one log entry, or in none
    /// when it only read.
    Local(ShardId),
    /// Across shards, with the client as the coordinator of two phases.
    Across,
    /// Across shards, by a coordinator that stops after this many
    /// milliseconds of committing, whatever it got done by then: a
    /// coordinator that crashed.
    Abandoned(u64),
    /// Across shards, by a coordinator that stops right after the commit
    /// point: the primary committed, the other shards still hold their
    /// parts locked until someone resolves them past their deadline.
    Stranded,
}

/// A transaction to run: where, and its steps.
#[derive(Debug, Clone)]
struct Plan {
    route: Route,
    steps: Vec<Step>,
}

/// Plans transactions: about a third of them only read.
struct Planner {
    shards: [ShardId; 2],
    next: u64,
}

impl Planner {
    fn plan(&mut self, rng: &mut Rng) -> Plan {
        let (route, (first, last)) = match rng.below(3) {
            0 => (Route::Local(self.shards[0]), FIRST_SHARD),
            1 => (Route::Local(self.shards[1]), SECOND_SHARD),
            2 if rng.chance(150) => (Route::Abandoned(rng.below(40)), EVERY_KEY),
            2 if rng.chance(150) => (Route::Stranded, EVERY_KEY),
            _ => (Route::Across, EVERY_KEY),
        };
        let reading = rng.chance(333);
        let steps = (0..rng.between(1, 4))
            .map(|_| {
                let index = rng.between(first, last);
                if reading || rng.chance(400) {
                    Step::Read(index)
                } else {
                    self.next = self.next.checked_add(1).unwrap();
                    Step::Append(index, Element::new(self.next))
                }
            })
            .collect();
        Plan { route, steps }
    }
}

/// How a client saw a transaction end.
const fn fate(outcome: Transacted) -> Fate {
    match outcome {
        Transacted::Committed(_) => Fate::Committed,
        Transacted::Unknown => Fate::Unknown,
        Transacted::Aborted | Transacted::Rejected | _ => Fate::Aborted,
    }
}

/// Runs `plan` on one shard, recording what it saw.
async fn local(cluster: &Cluster, shard: ShardId, steps: &[Step]) -> (Txn, Fate) {
    let mut seen = Txn::new();
    let outcome = cluster
        .transact_in(shard, |txn| {
            for step in steps {
                let (index, appended) = match *step {
                    Step::Read(index) => (index, None),
                    Step::Append(index, element) => (index, Some(element)),
                };
                let mut list = decode(txn.get(&key(index))?.as_deref());
                seen.read(Key::new(index), list.clone());
                if let Some(element) = appended {
                    seen.append(Key::new(index), element);
                    list.push(element);
                    txn.put(key(index), encode(&list));
                }
            }
            Ok(())
        })
        .await;
    (seen, fate(outcome))
}

/// Runs `plan` across shards, recording what it saw. A coordinator that
/// stops after `patience` cannot tell how it ended.
async fn across(cluster: &Cluster, steps: &[Step], patience: Option<Duration>) -> (Txn, Fate) {
    let mut seen = Txn::new();
    let mut txn = Coordinator::new(cluster).begin(NamespaceId::DEFAULT);
    for step in steps {
        let (index, appended) = match *step {
            Step::Read(index) => (index, None),
            Step::Append(index, element) => (index, Some(element)),
        };
        let Ok(value) = txn.get(&key(index)).await else {
            return (seen, Fate::Aborted);
        };
        let mut list = decode(value.as_deref());
        seen.read(Key::new(index), list.clone());
        if let Some(element) = appended {
            seen.append(Key::new(index), element);
            list.push(element);
            if txn.put(&key(index), encode(&list)).await.is_err() {
                return (seen, Fate::Aborted);
            }
        }
    }
    let Some(patience) = patience else {
        return (seen, fate(txn.commit().await));
    };
    let fate = SimRuntime::timeout(patience, txn.commit())
        .await
        .map_or(Fate::Unknown, fate);
    (seen, fate)
}

/// Runs `steps` across shards as [`Coordinator`] does, but stops right
/// after the primary committed: the client cannot tell how it ended.
async fn stranded(cluster: &Cluster, steps: &[Step]) -> (Txn, Fate) {
    let mut seen = Txn::new();
    let mut parts: BTreeMap<ShardId, Draft> = BTreeMap::new();
    for step in steps {
        let (index, appended) = match *step {
            Step::Read(index) => (index, None),
            Step::Append(index, element) => (index, Some(element)),
        };
        let Some(draft) = part(cluster, &mut parts, index).await else {
            return (seen, Fate::Aborted);
        };
        let Some(value) = read(cluster, draft, index).await else {
            return (seen, Fate::Aborted);
        };
        let mut list = decode(value.as_deref());
        seen.read(Key::new(index), list.clone());
        if let Some(element) = appended {
            seen.append(Key::new(index), element);
            list.push(element);
            draft.put(key(index), encode(&list));
        }
    }
    let Some((primary, primary_key)) = parts
        .iter()
        .find_map(|(shard, draft)| Some((*shard, draft.first_write()?.to_vec())))
    else {
        return (seen, Fate::Aborted);
    };
    let id = cluster.next_txn();
    let deadline = Deadline::new(cluster.now().millis().saturating_add(5_000));
    for (shard, draft) in parts {
        let prepare = draft.into_prepare(id, primary_key.clone(), deadline);
        let reply = cluster
            .call(shard, Call::Propose(Command::Prepare(prepare)))
            .await;
        if !matches!(reply, Some(Reply::Applied(Applied::Prepared(_)))) {
            return (seen, Fate::Unknown);
        }
    }
    let decide = Decide::new(id, primary_key, deadline, Decision::Commit);
    let _committed: Option<Reply> = cluster
        .call(primary, Call::Propose(Command::Decide(decide)))
        .await;
    (seen, Fate::Unknown)
}

/// The draft of the shard that holds key `index`, begun on its leader
/// when first touched; `None` when it does not answer.
async fn part<'parts>(
    cluster: &Cluster,
    parts: &'parts mut BTreeMap<ShardId, Draft>,
    index: u64,
) -> Option<&'parts mut Draft> {
    let shard = cluster.locate(NamespaceId::DEFAULT, &key(index))?;
    if let Entry::Vacant(vacant) = parts.entry(shard) {
        let Some(Reply::Begun { at, .. }) = cluster.call(shard, Call::Begin).await else {
            return None;
        };
        let _draft: &mut Draft = vacant.insert(Draft::new(at));
    }
    parts.get_mut(&shard)
}

/// The value of key `index` as `draft` reads it: its own write, or the
/// shard's value at its read version.
async fn read(cluster: &Cluster, draft: &mut Draft, index: u64) -> Option<Option<Vec<u8>>> {
    let key = key(index);
    if let Some(written) = draft.written(&key) {
        return Some(written.map(<[u8]>::to_vec));
    }
    let shard = cluster.locate(NamespaceId::DEFAULT, &key)?;
    let call = Call::Get {
        at: draft.read_at(),
        key: key.clone(),
    };
    let Some(Reply::Value(value)) = cluster.call(shard, call).await else {
        return None;
    };
    draft.note_read(&key);
    Some(value)
}

async fn run_plan(cluster: &Cluster, plan: Plan) -> (Txn, Fate) {
    match plan.route {
        Route::Local(shard) => local(cluster, shard, &plan.steps).await,
        Route::Across => across(cluster, &plan.steps, None).await,
        Route::Abandoned(millis) => {
            let patience = Duration::from_millis(millis);
            across(cluster, &plan.steps, Some(patience)).await
        }
        Route::Stranded => stranded(cluster, &plan.steps).await,
    }
}

/// A few transactions at once.
async fn concurrently(
    cluster: &Cluster,
    planner: &mut Planner,
    rng: &mut Rng,
    history: &mut History,
) {
    let plans: Vec<Plan> = (0..CONCURRENT).map(|_| planner.plan(rng)).collect();
    let ended = join_all(plans.into_iter().map(|plan| run_plan(cluster, plan))).await;
    for (txn, fate) in ended {
        let _index = history.record(txn, fate);
    }
}

async fn crash_one(cluster: &mut Cluster, rng: &mut Rng) {
    let Some(id) = rng.pick(&cluster.up()).copied() else {
        return;
    };
    cluster.crash(id).await.unwrap();
}

async fn restart_one(cluster: &mut Cluster, rng: &mut Rng) {
    let Some(id) = rng.pick(&cluster.down()).copied() else {
        return;
    };
    cluster.restart(id).await.unwrap();
}

/// The two shards, the first holding the keys before [`SPLIT_AT`].
async fn split(cluster: &mut Cluster) -> [ShardId; 2] {
    let split = cluster.split(&key(SPLIT_AT)).await;
    assert_eq!(split, Outcome::Acked, "split");
    cluster.settle().await.unwrap();
    let second = cluster
        .shards()
        .into_iter()
        .find(|shard| shard.span().contains(&key(SPLIT_AT)))
        .expect("the second shard")
        .id();
    [ShardId::FIRST, second]
}

fn run(seed: u64) -> Checked {
    SimRuntime::with_seed(seed).block_on(async {
        let mut rng = Rng::new(seed);
        let mut cluster = Cluster::start(seed, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let mut planner = Planner {
            shards: split(&mut cluster).await,
            next: 0,
        };
        let mut history = History::new();
        for _ in 0..STEPS {
            match rng.below(100) {
                0..65 => concurrently(&cluster, &mut planner, &mut rng, &mut history).await,
                65..75 => crash_one(&mut cluster, &mut rng).await,
                75..88 => restart_one(&mut cluster, &mut rng).await,
                88..94 => cluster
                    .leader()
                    .into_iter()
                    .for_each(|id| cluster.isolate(id)),
                94..97 => {
                    let collect = Collector::new(&cluster).collect(Duration::ZERO);
                    let (_collected, ()) = futures_util::future::join(
                        collect,
                        concurrently(&cluster, &mut planner, &mut rng, &mut history),
                    )
                    .await;
                }
                _ => cluster.heal(),
            }
            SimRuntime::sleep(Duration::from_millis(rng.below(MAX_GAP_MS))).await;
        }
        cluster
            .settle()
            .await
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
        // A last read of every key, so that each key's order is known up
        // to its last version.
        let last = Plan {
            route: Route::Across,
            steps: (EVERY_KEY.0..=EVERY_KEY.1).map(Step::Read).collect(),
        };
        let (txn, fate) = run_plan(&cluster, last).await;
        assert_eq!(fate, Fate::Committed, "seed {seed}: the last read");
        let _index = history.record(txn, fate);
        cluster.shutdown().await.unwrap();
        check(&history).unwrap_or_else(|violations| panic!("seed {seed}: {violations}"))
    })
}

#[test]
fn committed_transactions_are_serializable() {
    let checked: Vec<Checked> = seeds().into_iter().map(run).collect();
    let committed: usize = checked.iter().map(Checked::committed).sum();
    assert!(committed > 0, "transactions commit: {checked:?}");
    for dependency in Dependency::ALL {
        let found: usize = checked
            .iter()
            .map(|checked| checked.dependencies(dependency))
            .sum();
        assert!(found > 0, "the histories hold {dependency:?} dependencies");
    }
}
