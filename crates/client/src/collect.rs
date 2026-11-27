//! Forgetting the decisions on transactions across shards long past their
//! deadline: every shard fenced first, then every shard forgets.

use std::collections::BTreeSet;
use std::time::Duration;

use kurator_shard::ShardId;
use kurator_storage::{Abort, Collect, Command, Deadline, Outcome};

use crate::remote::{Call, Reply};
use crate::resolve::Lock;
use crate::shards::Shards;

/// How often a fence is tried on one shard, resolving what holds it back
/// in between.
const FENCE_TRIES: u32 = 8;

/// How a collection came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Collected {
    /// Every shard forgot the decisions past the horizon.
    Forgotten,
    /// A shard did not answer, or stayed held: try again later.
    Unfinished,
}

/// The client as the collector of decisions no one can ask about any
/// more.
#[derive(Debug)]
pub struct Collector<'shards, S> {
    shards: &'shards S,
}

impl<S> Clone for Collector<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Collector<'_, S> {}

impl<'shards, S: Shards> Collector<'shards, S> {
    /// A collector on `shards`.
    #[must_use]
    pub const fn new(shards: &'shards S) -> Self {
        Self { shards }
    }

    /// Forgets the decisions on the transactions whose deadline passed
    /// more than `grace` ago. Every shard is fenced first, the ones splits
    /// make meanwhile among them, resolving the transactions they hold
    /// prepared from before the fence; only then does any shard forget.
    pub async fn collect(self, grace: Duration) -> Collected {
        let grace = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX);
        let horizon = Deadline::new(self.shards.now().millis().saturating_sub(grace));
        let Some(fenced) = self.fence_all(horizon).await else {
            return Collected::Unfinished;
        };
        for shard in fenced {
            if self.step(shard, Collect::Forget(horizon)).await.is_none() {
                return Collected::Unfinished;
            }
        }
        Collected::Forgotten
    }

    /// Fences every shard at `horizon`, until the map shows none that is
    /// not: a shard split off one already fenced is fenced too, but one
    /// split off before is not. The shards fenced, or `None` when one
    /// could not be.
    async fn fence_all(self, horizon: Deadline) -> Option<BTreeSet<ShardId>> {
        let mut fenced = BTreeSet::new();
        loop {
            let unfenced: Vec<ShardId> = self
                .shards
                .refresh()
                .await
                .into_iter()
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
            Lock::new(by, primary, deadline)
                .resolve(self.shards, shard)
                .await;
        }
        None
    }

    /// Takes `step` on `shard`'s leader: whether it applied, or why not;
    /// `None` when no leader answered.
    async fn step(self, shard: ShardId, step: Collect) -> Option<Result<(), Abort>> {
        let reply = self
            .shards
            .call(shard, Call::Propose(Command::Collect(step)))
            .await?;
        let Reply::Applied(outcome) = reply else {
            return None;
        };
        match outcome {
            Outcome::Committed(_) | Outcome::Prepared(_) => Some(Ok(())),
            Outcome::Aborted(abort) => Some(Err(abort)),
            _ => None,
        }
    }
}
