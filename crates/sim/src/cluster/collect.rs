//! Forgetting the decisions on transactions across shards long past their
//! deadline: every shard fenced first, then every shard forgets.

use std::collections::BTreeSet;
use std::time::Duration;

use kurator_raft::Error;
use kurator_shard::{Shard, ShardId};
use kurator_storage::{Abort, Collect, Deadline};

use super::{Cluster, Lock, Outcome, now};

/// How often a fence is tried on one shard, resolving what holds it back
/// in between.
const FENCE_TRIES: u32 = 8;

/// The client as the collector of decisions no one can ask about any
/// more.
#[derive(Debug, Clone, Copy)]
pub struct Collector<'cluster> {
    cluster: &'cluster Cluster,
}

impl<'cluster> Collector<'cluster> {
    /// A collector on `cluster`.
    #[must_use]
    pub const fn new(cluster: &'cluster Cluster) -> Self {
        Self { cluster }
    }

    /// Forgets the decisions on the transactions whose deadline passed
    /// more than `grace` ago. Every shard is fenced first, the ones splits
    /// make meanwhile among them, resolving the transactions they hold
    /// prepared from before the fence; only then does any shard forget.
    /// `Acked` once every shard forgot, `Unknown` when one did not answer
    /// or stayed held.
    pub async fn collect(self, grace: Duration) -> Outcome {
        let grace = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX);
        let horizon = Deadline::new(now().millis().saturating_sub(grace));
        let Some(fenced) = self.fence_all(horizon).await else {
            return Outcome::Unknown;
        };
        for shard in fenced {
            if self.step(shard, Collect::Forget(horizon)).await.is_none() {
                return Outcome::Unknown;
            }
        }
        Outcome::Acked
    }

    /// Fences every shard at `horizon`, until the map shows none that is
    /// not: a shard split off one already fenced is fenced too, but one
    /// split off before is not. The shards fenced, or `None` when one
    /// could not be.
    async fn fence_all(self, horizon: Deadline) -> Option<BTreeSet<ShardId>> {
        let mut fenced = BTreeSet::new();
        loop {
            self.cluster.refresh().await;
            let unfenced: Vec<ShardId> = self
                .cluster
                .shards()
                .iter()
                .map(Shard::id)
                .filter(|shard| !fenced.contains(shard))
                .collect();
            if unfenced.is_empty() {
                return Some(fenced);
            }
            for shard in unfenced {
                self.fence(shard, horizon).await?;
                let _new: bool = fenced.insert(shard);
            }
        }
    }

    /// Fences `shard` at `horizon`, resolving each transaction it holds
    /// prepared from before; `None` when it stays unfenced.
    async fn fence(self, shard: ShardId, horizon: Deadline) -> Option<()> {
        for _ in 0..FENCE_TRIES {
            let Err(abort) = self.step(shard, Collect::Fence(horizon)).await? else {
                return Some(());
            };
            let Abort::Held {
                by,
                primary,
                deadline,
            } = abort
            else {
                return None;
            };
            let lock = Lock::new(by, primary, deadline);
            self.cluster.resolve_lock(shard, lock).await;
        }
        None
    }

    /// Takes `step` on `shard`'s leader: whether it applied, or why not;
    /// `None` when no leader answered.
    async fn step(self, shard: ShardId, step: Collect) -> Option<Result<(), Abort>> {
        self.cluster
            .on_leader(shard, |replica| async move {
                match replica.collect(step).await {
                    Ok(_) => Ok(Ok(())),
                    Err(Error::Storage(kurator_storage::Error::Aborted(abort))) => Ok(Err(abort)),
                    Err(error) => Err(error),
                }
            })
            .await
    }
}
