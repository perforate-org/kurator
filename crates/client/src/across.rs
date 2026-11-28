//! Transactions across shards, with the client as their coordinator.
//!
//! The client reads each shard from its leader at the version that leader
//! applied last when the transaction first touched the shard, and keeps a
//! [`Draft`] per shard. To commit, it prepares each shard's part, then
//! tells the primary, the shard of the first key written, how the
//! transaction came out: that decision is final. The other shards are
//! told after it. A transaction whose coordinator gave up half way leaves
//! its locks behind; whoever runs into them past their deadline resolves
//! them ([`Lock::resolve`](crate::Lock::resolve)).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::time::Duration;

use oleaph_shard::{NamespaceId, Shard, ShardId};
use oleaph_storage::{
    Abort, Command, Deadline, Decide, Decided, Decision, Draft, Outcome, Prepare, Version,
};

use crate::remote::{Call, Reply};
use crate::resolve::{decide, decision, resolve_expired};
use crate::shards::{Error, Shards};

/// How long a coordinator means to take over a transaction across shards,
/// from when it starts to prepare it: past that, whoever runs into its
/// locks resolves it.
pub const LOCK_TTL: Duration = Duration::from_secs(5);

/// How a transaction came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Transacted {
    /// A quorum holds the writes, made at this version; or the
    /// transaction wrote nothing and read this version.
    Committed(Version),
    /// A write after its read version touched what it read, or a lock
    /// was in the way: it wrote nothing.
    Aborted,
    /// It never reached the log: no leader took it, or a read failed.
    Rejected,
    /// It may or may not take effect: it timed out, or its leader lost
    /// leadership after logging it.
    Unknown,
}

/// The client as the coordinator of transactions across shards.
#[derive(Debug)]
pub struct Coordinator<'shards, S> {
    shards: &'shards S,
}

impl<S> Clone for Coordinator<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Coordinator<'_, S> {}

impl<'shards, S: Shards> Coordinator<'shards, S> {
    /// A coordinator for transactions on `shards`.
    #[must_use]
    pub const fn new(shards: &'shards S) -> Self {
        Self { shards }
    }

    /// Starts a transaction across every shard of `namespace` it will
    /// touch. Its keys are all keys of that namespace.
    #[must_use]
    pub const fn begin(self, namespace: NamespaceId) -> Across<'shards, S> {
        Across {
            coordinator: self,
            namespace,
            parts: BTreeMap::new(),
        }
    }

    /// Prepares `prepare` on `shard`'s leader. A lock in the way whose
    /// deadline passed is resolved, for a later transaction to pass.
    async fn prepare(self, shard: &Shard, prepare: Prepare) -> Prepared {
        let reply = self
            .shards
            .call(shard.id(), Call::Propose(Command::Prepare(prepare)))
            .await;
        match prepared(reply) {
            Prepared::Aborted(abort) => {
                let _resolved: bool = resolve_expired(self.shards, shard, &abort).await;
                Prepared::Refused
            }
            answer @ (Prepared::Locked | Prepared::Refused | Prepared::Unknown) => answer,
        }
    }

    /// When a coordinator that starts now means to have decided.
    fn deadline(self) -> Deadline {
        let ttl = u64::try_from(LOCK_TTL.as_millis()).unwrap_or(u64::MAX);
        Deadline::new(self.shards.now().millis().saturating_add(ttl))
    }
}

/// A transaction across shards of one namespace being built: what it
/// read and writes on each shard it touched so far. Dropping it writes
/// nothing.
#[derive(Debug)]
pub struct Across<'shards, S> {
    coordinator: Coordinator<'shards, S>,
    namespace: NamespaceId,
    parts: BTreeMap<ShardId, Part>,
}

/// One shard's part: the shard as its leader held it when the transaction
/// first touched it, and what the transaction read and writes there.
#[derive(Debug)]
struct Part {
    shard: Shard,
    draft: Draft,
}

/// How a shard answered a prepare.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Prepared {
    /// It holds the part, locked.
    Locked,
    /// It refused; the part locked nothing.
    Refused,
    /// It refused for this reason: a read changed, another transaction
    /// holds a lock in the way, or the transaction was decided already.
    /// The part locked nothing.
    Aborted(Abort),
    /// No answer: the part may be locked.
    Unknown,
}

impl<S: Shards> Across<'_, S> {
    /// The value of `key`, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Unmapped`] when the client knows no shards of the
    /// transaction's namespace, [`Error::Unanswered`] when the key's shard
    /// has no leader up, [`Error::Misrouted`] when it does not hold the
    /// key, and [`Error::Refused`] when it cannot read the version.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let shards = self.coordinator.shards;
        let part = self.part(key).await?;
        if let Some(written) = part.draft.written(key) {
            return Ok(written.map(<[u8]>::to_vec));
        }
        let call = Call::Get {
            at: part.draft.read_at(),
            key: key.to_vec(),
        };
        match shards.call(part.shard.id(), call).await {
            Some(Reply::Value(value)) => {
                part.draft.note_read(key);
                Ok(value)
            }
            Some(other) => Err(Error::of(other)),
            None => Err(Error::Unanswered),
        }
    }

    /// Sets `key` to `value` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get`].
    pub async fn put(&mut self, key: &[u8], value: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.part(key).await?.draft.put(key, value);
        Ok(())
    }

    /// Removes `key` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get`].
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.part(key).await?.draft.delete(key);
        Ok(())
    }

    /// Commits the transaction in two phases, with this client as its
    /// coordinator. `Committed` carries the version of the commit on the
    /// primary.
    pub async fn commit(self) -> Transacted {
        let Self {
            coordinator, parts, ..
        } = self;
        let parts: Vec<(ShardId, Shard, Draft)> = parts
            .into_iter()
            .map(|(id, part)| (id, part.shard, part.draft))
            .collect();
        let Some((primary_shard, primary_key)) = primary(&parts) else {
            return Transacted::Committed(Version::ZERO);
        };
        let id = coordinator.shards.next_txn();
        let deadline = coordinator.deadline();
        let mut proposed = Decision::Commit;
        let mut told = Vec::new();
        for (shard, held, draft) in parts {
            let prepare = draft.into_prepare(id, primary_key.clone(), deadline);
            let prepared = coordinator.prepare(&held, prepare).await;
            if prepared != Prepared::Refused {
                told.push(shard);
            }
            if prepared != Prepared::Locked {
                proposed = Decision::Abort;
                break;
            }
        }
        let proposal = Decide::new(id, primary_key.clone(), deadline, proposed);
        let Some(decided) = decide(coordinator.shards, primary_shard, proposal).await else {
            return Transacted::Unknown;
        };
        let outcome = Decide::new(id, primary_key, deadline, decision(decided));
        for shard in told.into_iter().filter(|shard| *shard != primary_shard) {
            let _decided: Option<Decided> =
                decide(coordinator.shards, shard, outcome.clone()).await;
        }
        match decided {
            Decided::Committed(version) => Transacted::Committed(version),
            Decided::Aborted | _ => Transacted::Aborted,
        }
    }

    /// The part of the shard that holds `key`, started when the
    /// transaction first touches the shard.
    async fn part(&mut self, key: &[u8]) -> Result<&mut Part, Error> {
        let shards = self.coordinator.shards;
        let namespace = self.namespace;
        let shard = shards
            .locate(namespace, key)
            .ok_or(Error::Unmapped(namespace))?;
        let part = match self.parts.entry(shard) {
            Entry::Occupied(part) => part.into_mut(),
            Entry::Vacant(vacant) => {
                let (held, read_at) = match shards.call(shard, Call::Begin).await {
                    Some(Reply::Begun { shard, at }) => (shard, at),
                    Some(other) => return Err(Error::of(other)),
                    None => return Err(Error::Unanswered),
                };
                vacant.insert(Part {
                    shard: held,
                    draft: Draft::new(read_at),
                })
            }
        };
        if part.shard.namespace() != namespace || !part.shard.span().contains(key) {
            return Err(Error::Misrouted(Box::new(part.shard.clone())));
        }
        Ok(part)
    }
}

/// How a shard answered a prepare with `reply`.
fn prepared(reply: Option<Reply>) -> Prepared {
    match reply {
        Some(Reply::Applied(Outcome::Committed(_) | Outcome::Prepared(_))) => Prepared::Locked,
        Some(Reply::Applied(Outcome::Aborted(abort))) => Prepared::Aborted(abort),
        Some(Reply::Misrouted(_)) => Prepared::Refused,
        _ => Prepared::Unknown,
    }
}

/// The primary of a transaction with `parts`: the shard and the first key
/// it writes, or, when it writes nothing, the start of the first shard it
/// read. `None` when it touched no shard.
fn primary(parts: &[(ShardId, Shard, Draft)]) -> Option<(ShardId, Vec<u8>)> {
    let written = parts
        .iter()
        .find_map(|(id, _, draft)| Some((*id, draft.first_write()?.to_vec())));
    written.or_else(|| {
        let (id, shard, _) = parts.first()?;
        Some((*id, shard.span().start().to_vec()))
    })
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;
    use oleaph_shard::{Epoch, NamespaceId, Shard, ShardId};
    use oleaph_storage::{
        Abort, Command, Deadline, Decision, End, KvOp, Outcome, Span, TxnId, Version,
    };
    use parking_lot::Mutex;

    use super::{Coordinator, Transacted};
    use crate::remote::{Call, Reply};
    use crate::shards::{Error, Shards};

    /// Keys before `m` on the first shard, the rest on shard 2.
    fn held(id: ShardId) -> Shard {
        let span = if id == ShardId::FIRST {
            Span::new("", End::Before(b"m".to_vec()))
        } else {
            Span::new("m", End::Unbounded)
        };
        Shard::new(NamespaceId::DEFAULT, id, span.unwrap(), Epoch::new(1))
    }

    /// Two shards that begin at version 1 and decide as told; `refusing`
    /// answers every prepare with its reply instead.
    struct Two {
        refusing: Option<(ShardId, Reply)>,
        decided: Mutex<Vec<(ShardId, Decision)>>,
        /// The writes each prepare carried.
        prepared: Mutex<Vec<(ShardId, Vec<KvOp>)>>,
    }

    impl Two {
        fn new(refusing: Option<(ShardId, Reply)>) -> Self {
            Self {
                refusing,
                decided: Mutex::new(Vec::new()),
                prepared: Mutex::new(Vec::new()),
            }
        }

        fn refused(&self, id: ShardId) -> Option<Reply> {
            let (shard, reply) = self.refusing.as_ref()?;
            (*shard == id).then(|| reply.clone())
        }
    }

    impl Shards for Two {
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
            vec![held(ShardId::FIRST), held(ShardId::new(2))]
        }

        async fn call(&self, shard: ShardId, call: Call) -> Option<Reply> {
            let id = shard;
            Some(match call {
                Call::Begin => Reply::Begun {
                    shard: held(id),
                    at: Version::new(1),
                },
                Call::Propose(Command::Prepare(_)) if self.refused(id).is_some() => {
                    self.refused(id)?
                }
                Call::Propose(Command::Prepare(prepare)) => {
                    let writes = prepare.transaction().writes().ops().to_vec();
                    self.prepared.lock().push((id, writes));
                    Reply::Applied(Outcome::Prepared(Version::new(2)))
                }
                Call::Get { at, .. } if at == Version::new(1) => {
                    Reply::Value(Some(b"old".to_vec()))
                }
                Call::Propose(Command::Decide(decide)) => {
                    self.decided.lock().push((id, decide.decision()));
                    Reply::Applied(match decide.decision() {
                        Decision::Commit => Outcome::Committed(Version::new(3)),
                        Decision::Abort | _ => Outcome::Aborted(Abort::RolledBack(decide.id())),
                    })
                }
                other @ (Call::Get { .. } | Call::Scan { .. } | Call::Propose(_)) => {
                    Reply::Failed(format!("{other:?}"))
                }
            })
        }

        fn now(&self) -> Deadline {
            Deadline::new(0)
        }

        fn next_txn(&self) -> TxnId {
            TxnId::new(1)
        }
    }

    fn put_a_and_z(shards: &Two) -> Transacted {
        async {
            let mut txn = Coordinator::new(shards).begin(NamespaceId::DEFAULT);
            txn.put(b"a", "1").await.unwrap();
            txn.put(b"z", "1").await.unwrap();
            txn.commit().await
        }
        .now_or_never()
        .unwrap()
    }

    #[test]
    fn reads_see_the_shard_and_then_the_writes() {
        let shards = Two::new(None);
        let committed = async {
            let mut txn = Coordinator::new(&shards).begin(NamespaceId::DEFAULT);
            let old = Some(b"old".to_vec());
            assert_eq!(txn.get(b"a").await.unwrap(), old, "the shard");
            txn.put(b"a", "1").await.unwrap();
            assert_eq!(txn.get(b"a").await.unwrap(), Some(b"1".to_vec()), "own");
            txn.delete(b"z").await.unwrap();
            assert_eq!(txn.get(b"z").await.unwrap(), None, "own delete");
            txn.commit().await
        }
        .now_or_never()
        .unwrap();
        assert_eq!(committed, Transacted::Committed(Version::new(3)));
        let prepared = shards.prepared.lock().clone();
        let put = KvOp::Put {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        };
        let delete = KvOp::Delete { key: b"z".to_vec() };
        let expected = [(ShardId::FIRST, vec![put]), (ShardId::new(2), vec![delete])];
        assert_eq!(prepared, expected, "each shard's writes");
    }

    #[test]
    fn every_shard_hears_a_commit() {
        let shards = Two::new(None);
        assert_eq!(put_a_and_z(&shards), Transacted::Committed(Version::new(3)));
        let decided = shards.decided.lock().clone();
        let commit = Decision::Commit;
        assert_eq!(
            decided,
            [(ShardId::FIRST, commit), (ShardId::new(2), commit)]
        );
    }

    #[test]
    fn a_shard_that_refused_to_prepare_is_not_told() {
        let second = ShardId::new(2);
        let conflict = Abort::Conflict {
            key: b"z".to_vec(),
            version: Version::new(2),
        };
        for refusal in [
            Reply::Misrouted(held(second)),
            Reply::Applied(Outcome::Aborted(conflict)),
        ] {
            let shards = Two::new(Some((second, refusal.clone())));
            assert_eq!(put_a_and_z(&shards), Transacted::Aborted, "{refusal:?}");
            let decided = shards.decided.lock().clone();
            let primary = [(ShardId::FIRST, Decision::Abort)];
            assert_eq!(decided, primary, "only the primary: {refusal:?}");
        }
    }

    #[test]
    fn a_namespace_without_shards_is_refused() {
        let shards = Two::new(None);
        let other = NamespaceId::new(3);
        let refused = async {
            let mut txn = Coordinator::new(&shards).begin(other);
            txn.put(b"a", "1").await
        }
        .now_or_never()
        .unwrap();
        assert_eq!(refused, Err(Error::Unmapped(other)), "unmapped");
    }

    #[test]
    fn a_shard_of_another_namespace_is_misrouted() {
        let shards = Elsewhere;
        let misrouted = async {
            let mut txn = Coordinator::new(&shards).begin(NamespaceId::DEFAULT);
            txn.put(b"a", "1").await
        }
        .now_or_never()
        .unwrap();
        let held = Shard::whole(NamespaceId::new(5), ShardId::FIRST);
        assert_eq!(
            misrouted,
            Err(Error::Misrouted(Box::new(held))),
            "another namespace"
        );
    }

    /// A shard that answers a begin as a shard of another namespace.
    struct Elsewhere;

    impl Shards for Elsewhere {
        fn locate(&self, _namespace: NamespaceId, _key: &[u8]) -> Option<ShardId> {
            Some(ShardId::FIRST)
        }

        async fn refresh(&self) -> Vec<Shard> {
            Vec::new()
        }

        async fn call(&self, _shard: ShardId, _call: Call) -> Option<Reply> {
            Some(Reply::Begun {
                shard: Shard::whole(NamespaceId::new(5), ShardId::FIRST),
                at: Version::new(1),
            })
        }

        fn now(&self) -> Deadline {
            Deadline::new(0)
        }

        fn next_txn(&self) -> TxnId {
            TxnId::new(1)
        }
    }

    #[test]
    fn a_shard_that_did_not_answer_is_told() {
        let second = ShardId::new(2);
        let shards = Two::new(Some((second, Reply::Failed("lost".into()))));
        assert_eq!(put_a_and_z(&shards), Transacted::Aborted);
        let decided = shards.decided.lock().clone();
        let abort = Decision::Abort;
        assert_eq!(decided, [(ShardId::FIRST, abort), (second, abort)], "both");
    }
}
