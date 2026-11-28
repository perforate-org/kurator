//! Transactions under faults: concurrent transfers between accounts while
//! replicas crash, restart and get cut off, and decisions are forgotten,
//! on one shard and across two.
//! Whatever happens, no transfer is applied by half or twice, so the
//! accounts always hold the money they started with, and every replica
//! ends with the same state.
//!
//! `OLEAPH_SIM_SEEDS=<n>` runs more seeds than the default, and
//! `OLEAPH_SIM_SEED=<seed>` runs just one, to reproduce a failure.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code: a failure should panic with context"
)]

use std::time::Duration;

use futures_util::future::join;
use oleaph_client::{Across, Collected, Collector, Coordinator, Error, Transacted};
use oleaph_consensus::Transaction as _;
use oleaph_raft::Txn;
use oleaph_raft::openraft::AsyncRuntime as _;
use oleaph_shard::NamespaceId;
use oleaph_sim::{Cluster, Faults, Holdings, Outcome, Rng, Sim};
use oleaph_storage::WriteBatch;
use openraft_rt_sim::SimRuntime;

const DEFAULT_SEEDS: u64 = 8;
const STEPS: u64 = 60;
const ACCOUNTS: u64 = 4;
const OPENING: u64 = 100;
/// The account the second shard starts at, when the accounts are split.
const SPLIT_AT: u64 = 2;
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

fn account(index: u64) -> Vec<u8> {
    format!("account/{index}").into_bytes()
}

fn amount(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("an amount"))
}

fn balance(txn: &mut Txn<'_, Sim>, index: u64) -> oleaph_consensus::Result<u64> {
    Ok(txn.get(&account(index))?.map_or(0, |bytes| amount(&bytes)))
}

async fn balance_across(txn: &mut Across<'_, Cluster>, index: u64) -> Result<u64, Error> {
    Ok(txn
        .get(&account(index))
        .await?
        .map_or(0, |bytes| amount(&bytes)))
}

/// Moves `sum` from one account to another in `txn`, as [`transfer`] does.
async fn transfer_across(
    txn: &mut Across<'_, Cluster>,
    from: u64,
    to: u64,
    sum: u64,
) -> Result<(), Error> {
    let source = balance_across(txn, from).await?;
    let target = balance_across(txn, to).await?;
    let Some(left) = left(from, to, source, sum) else {
        return Ok(());
    };
    txn.put(&account(from), left.to_le_bytes()).await?;
    txn.put(&account(to), target.checked_add(sum).unwrap().to_le_bytes())
        .await
}

/// The money in every account a replica holds.
fn total(holdings: &Holdings) -> u64 {
    (0..ACCOUNTS)
        .filter_map(|index| holdings.get(&account(index)))
        .map(amount)
        .sum()
}

/// What is left of `source` after `sum` moves out of it, if it holds that
/// much and the money moves between two accounts.
fn left(from: u64, to: u64, source: u64, sum: u64) -> Option<u64> {
    source.checked_sub(sum).filter(|_| from != to)
}

/// How a run's transactions reach the accounts.
#[derive(Debug, Clone, Copy)]
enum Client {
    /// Every account is on the first shard, and a transaction commits on
    /// it alone.
    OneShard,
    /// The accounts are split over two shards, and a transaction commits
    /// in two phases across them.
    Across,
}

/// Moves `sum` from one account to another, if the first holds that much
/// and the two differ.
async fn transfer(cluster: &Cluster, client: Client, from: u64, to: u64, sum: u64) -> Transacted {
    match client {
        Client::OneShard => {
            cluster
                .transact(|txn| {
                    let (source, target) = (balance(txn, from)?, balance(txn, to)?);
                    let Some(left) = left(from, to, source, sum) else {
                        return Ok(());
                    };
                    txn.put(account(from), left.to_le_bytes());
                    txn.put(account(to), target.checked_add(sum).unwrap().to_le_bytes());
                    Ok(())
                })
                .await
        }
        Client::Across => {
            let mut txn = Coordinator::new(cluster).begin(NamespaceId::DEFAULT);
            match transfer_across(&mut txn, from, to, sum).await {
                Ok(()) => txn.commit().await,
                Err(_) => Transacted::Rejected,
            }
        }
    }
}

/// How the transactions of a run ended.
#[derive(Debug, Default)]
struct Tally {
    committed: u64,
    aborted: u64,
    other: u64,
}

impl Tally {
    const fn count(&mut self, outcome: Transacted) {
        let counter = match outcome {
            Transacted::Committed(_) => &mut self.committed,
            Transacted::Aborted => &mut self.aborted,
            Transacted::Rejected | Transacted::Unknown | _ => &mut self.other,
        };
        *counter = counter.saturating_add(1);
    }
}

/// Two transfers at once, often touching the same account, so that one of
/// them conflicts.
async fn transfers(cluster: &Cluster, client: Client, rng: &mut Rng, tally: &mut Tally) {
    let mut pick = || (rng.below(ACCOUNTS), rng.below(ACCOUNTS), rng.below(60));
    let (first, second) = (pick(), pick());
    let (first, second) = join(
        transfer(cluster, client, first.0, first.1, first.2),
        transfer(cluster, client, second.0, second.1, second.2),
    )
    .await;
    tally.count(first);
    tally.count(second);
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

fn run(client: Client, seed: u64) -> Tally {
    SimRuntime::with_seed(seed).block_on(async {
        let mut rng = Rng::new(seed);
        let mut cluster = Cluster::start(seed, 3, Faults::default()).await.unwrap();
        cluster.settle().await.unwrap();
        let opening = (0..ACCOUNTS).fold(WriteBatch::new(), |batch, index| {
            batch.put(account(index), OPENING.to_le_bytes())
        });
        let outcome = cluster.write(opening).await;
        assert!(outcome == Outcome::Acked, "opened: {outcome:?}");
        if matches!(client, Client::Across) {
            let split = cluster.split(&account(SPLIT_AT)).await;
            assert_eq!(split, Outcome::Acked, "split");
            cluster.settle().await.unwrap();
        }
        let mut tally = Tally::default();
        for _ in 0..STEPS {
            match rng.below(100) {
                0..65 => transfers(&cluster, client, &mut rng, &mut tally).await,
                65..75 => crash_one(&mut cluster, &mut rng).await,
                75..88 => restart_one(&mut cluster, &mut rng).await,
                88..94 => cluster
                    .leader()
                    .into_iter()
                    .for_each(|id| cluster.isolate(id)),
                94..97 => {
                    let (_collected, ()) = join(
                        Collector::new(&cluster).collect(Duration::ZERO),
                        transfers(&cluster, client, &mut rng, &mut tally),
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
        // Past the deadline of every transaction, so none is still kept.
        SimRuntime::sleep(Duration::from_secs(6)).await;
        let collected = Collector::new(&cluster).collect(Duration::ZERO).await;
        assert_eq!(collected, Collected::Forgotten, "seed {seed}: collected");
        cluster
            .settle()
            .await
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
        let states = cluster.states();
        let kept = states
            .values()
            .flat_map(Holdings::shards)
            .map(|state| state.kv().decisions().count())
            .sum::<usize>();
        assert_eq!(kept, 0, "seed {seed}: every decision forgotten");
        let first = states.values().next().expect("a replica");
        for (id, state) in &states {
            assert_eq!(state, first, "seed {seed}: {id} agrees");
        }
        assert_eq!(
            total(first),
            ACCOUNTS * OPENING,
            "seed {seed}: no money made or lost"
        );
        cluster.shutdown().await.unwrap();
        tally
    })
}

fn check(client: Client) {
    let tallies: Vec<Tally> = seeds().into_iter().map(|seed| run(client, seed)).collect();
    let committed: u64 = tallies.iter().map(|tally| tally.committed).sum();
    let aborted: u64 = tallies.iter().map(|tally| tally.aborted).sum();
    assert!(committed > 0, "transfers commit: {tallies:?}");
    assert!(aborted > 0, "concurrent transfers conflict: {tallies:?}");
}

#[test]
fn concurrent_transfers_neither_make_nor_lose_money() {
    check(Client::OneShard);
}

#[test]
fn concurrent_transfers_across_shards_neither_make_nor_lose_money() {
    check(Client::Across);
}
