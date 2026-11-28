//! Deciding transactions across shards whose coordinator gave up: whoever
//! runs into one of their locks past its deadline asks the primary how it
//! came out, aborting it there when it never decided.

use oleaph_shard::{Shard, ShardId};
use oleaph_storage::{
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
    ///
    /// A transaction stays in one namespace, so its primary is a key of
    /// `shard`'s namespace. When the client knows no shards of it, the
    /// lock is left.
    pub async fn resolve(self, shards: &impl Shards, shard: &Shard) {
        let Self {
            id,
            primary,
            deadline,
        } = self;
        let Some(primary_shard) = shards.locate(shard.namespace(), &primary) else {
            return;
        };
        let abort = Decide::new(id, primary.clone(), deadline, Decision::Abort);
        let Some(decided) = decide(shards, primary_shard, abort).await else {
            return;
        };
        let outcome = Decide::new(id, primary, deadline, decision(decided));
        let _decided: Option<Decided> = decide(shards, shard.id(), outcome).await;
    }
}

/// Resolves the transaction whose lock `abort` ran into on `shard`, when
/// its deadline passed; whether it did.
pub async fn resolve_expired(shards: &impl Shards, shard: &Shard, abort: &Abort) -> bool {
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

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;
    use oleaph_shard::{Epoch, NamespaceId, Shard, ShardId};
    use oleaph_storage::{Abort, Command, Deadline, Decision, Outcome, TxnId, Version};
    use oleaph_storage::{End, Span};
    use parking_lot::Mutex;

    use super::{Lock, resolve_expired};
    use crate::remote::{Call, Reply};
    use crate::shards::Shards;

    /// Keys before `m` on the first shard, where every transaction was
    /// committed already; the rest on shard 2. The clock reads 100.
    #[derive(Default)]
    struct Decided {
        told: Mutex<Vec<(ShardId, Decision)>>,
    }

    /// Shard 2, which holds the keys from `m` on.
    fn second() -> Shard {
        let span = Span::new("m", End::Unbounded).unwrap();
        Shard::new(NamespaceId::DEFAULT, ShardId::new(2), span, Epoch::new(1))
    }

    impl Shards for Decided {
        fn locate(&self, namespace: NamespaceId, key: &[u8]) -> Option<ShardId> {
            if namespace != NamespaceId::DEFAULT {
                return None;
            }
            Some(if key < b"m".as_slice() {
                ShardId::FIRST
            } else {
                ShardId::new(2)
            })
        }

        async fn refresh(&self) -> Vec<Shard> {
            Vec::new()
        }

        async fn call(&self, shard: ShardId, call: Call) -> Option<Reply> {
            let Call::Propose(Command::Decide(decide)) = call else {
                return None;
            };
            self.told.lock().push((shard, decide.decision()));
            Some(Reply::Applied(Outcome::Committed(Version::new(5))))
        }

        fn now(&self) -> Deadline {
            Deadline::new(100)
        }

        fn next_txn(&self) -> TxnId {
            TxnId::new(1)
        }
    }

    fn locked(deadline: u64) -> Abort {
        Abort::Locked {
            key: b"z".to_vec(),
            by: TxnId::new(3),
            primary: b"a".to_vec(),
            deadline: Deadline::new(deadline),
        }
    }

    #[test]
    fn only_a_lock_past_its_deadline_is_expired() {
        let now = Deadline::new(100);
        let lock = Lock::new(TxnId::new(3), b"a".to_vec(), Deadline::new(99));
        assert_eq!(Lock::expired(&locked(99), now), Some(lock), "past");
        assert_eq!(Lock::expired(&locked(100), now), None, "not yet");
        let other = Abort::RolledBack(TxnId::new(3));
        assert_eq!(Lock::expired(&other, now), None, "no lock");
    }

    #[test]
    fn a_lock_is_decided_as_its_primary_decided() {
        let shards = Decided::default();
        let abort = locked(99);
        let second = second();
        let resolved = resolve_expired(&shards, &second, &abort);
        assert!(resolved.now_or_never().unwrap(), "resolved");
        let told = shards.told.lock().clone();
        let expected = [
            (ShardId::FIRST, Decision::Abort),
            (ShardId::new(2), Decision::Commit),
        ];
        assert_eq!(
            told, expected,
            "abort asked at the primary, its commit told"
        );
    }

    #[test]
    fn a_lock_before_its_deadline_is_left() {
        let shards = Decided::default();
        let abort = locked(100);
        let second = second();
        let resolved = resolve_expired(&shards, &second, &abort);
        assert!(!resolved.now_or_never().unwrap(), "left");
        assert!(shards.told.lock().is_empty(), "nothing told");
    }

    #[test]
    fn a_lock_whose_namespace_is_unknown_is_left() {
        let shards = Decided::default();
        let abort = locked(99);
        let elsewhere = Shard::whole(NamespaceId::new(4), ShardId::new(9));
        let resolved = resolve_expired(&shards, &elsewhere, &abort);
        assert!(resolved.now_or_never().unwrap(), "expired");
        assert!(shards.told.lock().is_empty(), "nothing told");
    }
}
