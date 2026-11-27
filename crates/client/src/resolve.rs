//! Deciding transactions across shards whose coordinator gave up: whoever
//! runs into one of their locks past its deadline asks the primary how it
//! came out, aborting it there when it never decided.

use kurator_shard::ShardId;
use kurator_storage::{
    Abort, Command, Deadline, Decide, Decided, Decision, Outcome, Prepare, TxnId,
};

use crate::remote::{Call, Reply};
use crate::shards::Shards;

/// The lock a transaction across shards holds on a shard: which one, the
/// key whose shard decides it, and when its coordinator means to have
/// decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lock {
    id: TxnId,
    primary: Vec<u8>,
    deadline: Deadline,
}

impl Lock {
    /// The lock of transaction `id`, decided where `primary` is, by
    /// `deadline`.
    #[must_use]
    pub const fn new(id: TxnId, primary: Vec<u8>, deadline: Deadline) -> Self {
        Self {
            id,
            primary,
            deadline,
        }
    }

    /// The lock `prepared` holds.
    #[must_use]
    pub fn of(prepared: &Prepare) -> Self {
        Self::new(
            prepared.id(),
            prepared.primary().to_vec(),
            prepared.deadline(),
        )
    }

    /// The lock that `abort` ran into, if it ran into one past its
    /// deadline at `now`.
    #[must_use]
    pub fn expired(abort: &Abort, now: Deadline) -> Option<Self> {
        let Abort::Locked {
            by,
            primary,
            deadline,
            ..
        } = abort
        else {
            return None;
        };
        deadline
            .has_passed(now)
            .then(|| Self::new(*by, primary.clone(), *deadline))
    }

    /// Decides the transaction that holds this lock on `shard` as its
    /// primary decided it, or, when the primary never did, aborted there
    /// first. The primary's log orders that abort and its coordinator's
    /// commit, so whichever came first stands.
    pub async fn resolve(self, shards: &impl Shards, shard: ShardId) {
        let Self {
            id,
            primary,
            deadline,
        } = self;
        let primary_shard = shards.locate(&primary);
        let abort = Decide::new(id, primary.clone(), deadline, Decision::Abort);
        let Some(decided) = decide(shards, primary_shard, abort).await else {
            return;
        };
        let outcome = Decide::new(id, primary, deadline, decision(decided));
        let _decided: Option<Decided> = decide(shards, shard, outcome).await;
    }
}

/// Resolves the transaction whose lock `abort` ran into on `shard`, when
/// its deadline passed; whether it did.
pub async fn resolve_expired(shards: &impl Shards, shard: ShardId, abort: &Abort) -> bool {
    let Some(lock) = Lock::expired(abort, shards.now()) else {
        return false;
    };
    lock.resolve(shards, shard).await;
    true
}

/// Tells `shard`'s leader `decide`: how the shard decided, or `None` when
/// it did not say.
pub(crate) async fn decide(
    shards: &impl Shards,
    shard: ShardId,
    decide: Decide,
) -> Option<Decided> {
    let reply = shards
        .call(shard, Call::Propose(Command::Decide(decide)))
        .await?;
    let Reply::Applied(outcome) = reply else {
        return None;
    };
    match outcome {
        Outcome::Aborted(Abort::RolledBack(_)) => Some(Decided::Aborted),
        Outcome::Committed(version) | Outcome::Prepared(version) => {
            Some(Decided::Committed(version))
        }
        Outcome::Aborted(_) | _ => None,
    }
}

/// The decision a shard's answer stands for.
pub(crate) const fn decision(decided: Decided) -> Decision {
    match decided {
        Decided::Committed(_) => Decision::Commit,
        Decided::Aborted | _ => Decision::Abort,
    }
}
