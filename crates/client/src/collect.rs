//! Forgetting the decisions on transactions across shards long past their
//! deadline: every shard fenced first, then every shard forgets.

use std::collections::BTreeSet;
use std::time::Duration;

use oleaph_shard::ShardId;
use oleaph_storage::{Abort, Collect, Command, Deadline, Outcome};

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::FutureExt as _;
    use oleaph_shard::ShardId;
    use oleaph_storage::{Abort, Collect, Command, Deadline, Decision, Outcome, TxnId, Version};
    use parking_lot::Mutex;

    use super::{Collected, Collector, FENCE_TRIES};
    use crate::remote::{Call, Reply};
    use crate::shards::Shards;

    /// What a shard was asked.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Step {
        Fence(u64),
        Forget(u64),
        Decide(Decision),
    }

    /// Shards 1 and 2, then shard 3 split off after the first look. Shard
    /// 2 is held by a transaction decided on shard 1 for its first
    /// `held` fences; `failing` answers every forget with a failure.
    struct Split {
        looks: Mutex<u32>,
        held: Mutex<u32>,
        failing: Option<ShardId>,
        steps: Mutex<Vec<(u64, Step)>>,
    }

    impl Split {
        fn new(held: u32, failing: Option<ShardId>) -> Self {
            Self {
                looks: Mutex::new(0),
                held: Mutex::new(held),
                failing,
                steps: Mutex::new(Vec::new()),
            }
        }

        fn steps(&self) -> Vec<(u64, Step)> {
            self.steps.lock().clone()
        }

        /// Whether `shard` is still held, counting this fence.
        fn holds(&self, shard: ShardId) -> bool {
            let mut held = self.held.lock();
            if shard != ShardId::new(2) || *held == 0 {
                return false;
            }
            *held = held.checked_sub(1).unwrap();
            true
        }
    }

    impl Shards for Split {
        fn locate(&self, _key: &[u8]) -> ShardId {
            ShardId::FIRST
        }

        async fn refresh(&self) -> Vec<ShardId> {
            let first = {
                let mut looks = self.looks.lock();
                *looks = looks.checked_add(1).unwrap();
                *looks == 1
            };
            let shards = if first { 2 } else { 3 };
            (1..=shards).map(ShardId::new).collect()
        }

        async fn call(&self, shard: ShardId, call: Call) -> Option<Reply> {
            let Call::Propose(command) = call else {
                return None;
            };
            let (step, reply) = match command {
                Command::Collect(Collect::Fence(at)) if self.holds(shard) => {
                    let held = Abort::Held {
                        by: TxnId::new(4),
                        primary: b"a".to_vec(),
                        deadline: Deadline::new(1),
                    };
                    (
                        Step::Fence(at.millis()),
                        Reply::Applied(Outcome::Aborted(held)),
                    )
                }
                Command::Collect(Collect::Fence(at)) => (
                    Step::Fence(at.millis()),
                    Reply::Applied(Outcome::Committed(Version::new(1))),
                ),
                Command::Collect(Collect::Forget(at)) if self.failing == Some(shard) => {
                    (Step::Forget(at.millis()), Reply::Failed("down".into()))
                }
                Command::Collect(Collect::Forget(at)) => (
                    Step::Forget(at.millis()),
                    Reply::Applied(Outcome::Committed(Version::new(1))),
                ),
                Command::Decide(decide) => (
                    Step::Decide(decide.decision()),
                    Reply::Applied(Outcome::Aborted(Abort::RolledBack(decide.id()))),
                ),
                other @ (Command::Kv(_) | Command::Txn(_) | Command::Prepare(_) | _) => {
                    return Some(Reply::Failed(format!("{other:?}")));
                }
            };
            self.steps.lock().push((shard.get(), step));
            Some(reply)
        }

        fn now(&self) -> Deadline {
            Deadline::new(10_000)
        }

        fn next_txn(&self) -> TxnId {
            TxnId::new(1)
        }
    }

    fn collect(shards: &Split) -> Collected {
        Collector::new(shards)
            .collect(Duration::from_secs(1))
            .now_or_never()
            .unwrap()
    }

    #[test]
    fn every_shard_is_fenced_before_any_forgets() {
        let shards = Split::new(1, None);
        assert_eq!(collect(&shards), Collected::Forgotten);
        let abort = Step::Decide(Decision::Abort);
        let expected = [
            (1, Step::Fence(9_000)),
            (2, Step::Fence(9_000)),
            (1, abort),
            (2, abort),
            (2, Step::Fence(9_000)),
            (3, Step::Fence(9_000)),
            (1, Step::Forget(9_000)),
            (2, Step::Forget(9_000)),
            (3, Step::Forget(9_000)),
        ];
        assert_eq!(shards.steps(), expected, "held, resolved, split, forgotten");
    }

    #[test]
    fn a_shard_that_stays_held_is_not_forgotten() {
        let shards = Split::new(FENCE_TRIES, None);
        assert_eq!(collect(&shards), Collected::Unfinished);
        let forgets = shards
            .steps()
            .into_iter()
            .filter(|(_, step)| matches!(step, Step::Forget(_)))
            .count();
        assert_eq!(forgets, 0, "nothing forgotten");
    }

    #[test]
    fn a_shard_that_fails_to_forget_leaves_it_unfinished() {
        let shards = Split::new(0, Some(ShardId::new(2)));
        assert_eq!(collect(&shards), Collected::Unfinished);
        let last = shards.steps().last().copied();
        assert_eq!(last, Some((2, Step::Forget(9_000))), "stopped there");
    }
}
