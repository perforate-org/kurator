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
//!
//! A transaction may span namespaces. Namespaces alike in assurance mix
//! freely; a crash fault tolerant one and a Byzantine fault tolerant one
//! mix only with the leave the registry gives that pair, a [`Mixed`]. The
//! transaction is then decided on the Byzantine fault tolerant side, but
//! is only as safe as its crash fault tolerant side, whose operator it
//! trusts.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use oleaph_meta::{Assurance, Mix, Registry};
use oleaph_shard::{NamespaceId, Shard, ShardId};
use oleaph_storage::{
    Abort, Command, Deadline, Decide, Decided, Decision, Draft, Outcome, Prepare, Primary, Version,
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

/// Leave for one transaction to span two namespaces of different
/// assurances, as the registry gives it: see [`Across::admit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mixed(Mix);

impl Mixed {
    /// Leave to mix `one` and `other`, if `registry` gives it.
    #[must_use]
    pub fn of(registry: &Registry, one: NamespaceId, other: NamespaceId) -> Option<Self> {
        Mix::new(one, other)
            .filter(|_| registry.mixes(one, other))
            .map(Self)
    }

    /// The leave the registry gave as `mix`.
    pub(crate) const fn given(mix: Mix) -> Self {
        Self(mix)
    }
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

    /// Starts a transaction across every shard it will touch, whose keys
    /// are keys of `namespace` unless named in another.
    #[must_use]
    pub const fn begin(self, namespace: NamespaceId) -> Across<'shards, S> {
        Across {
            coordinator: self,
            home: namespace,
            mixes: BTreeSet::new(),
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

/// A transaction across shards being built: what it read and writes on
/// each shard it touched so far, and the namespaces it may mix. Dropping
/// it writes nothing.
#[derive(Debug)]
pub struct Across<'shards, S> {
    coordinator: Coordinator<'shards, S>,
    home: NamespaceId,
    mixes: BTreeSet<Mix>,
    parts: BTreeMap<ShardId, Part>,
}

/// One shard's part: the shard as its leader held it when the transaction
/// first touched it, its namespace's assurance, and what the transaction
/// read and writes there.
#[derive(Debug)]
struct Part {
    shard: Shard,
    assurance: Assurance,
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
    /// Lets the transaction span the two namespaces `mixed` names, of
    /// different assurances.
    pub fn admit(&mut self, mixed: Mixed) {
        let _new: bool = self.mixes.insert(mixed.0);
    }

    /// The value of `key`, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// As [`Across::get_in`].
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.get_in(self.home, key).await
    }

    /// Sets `key` to `value` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get_in`].
    pub async fn put(&mut self, key: &[u8], value: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.put_in(self.home, key, value).await
    }

    /// Removes `key` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get_in`].
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.delete_in(self.home, key).await
    }

    /// The value of `key` of `namespace`, with the transaction's own
    /// writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Unmapped`] when the client knows no shards of
    /// `namespace`, [`Error::Unmixed`] when the transaction touched a
    /// namespace of another assurance without leave to mix the two,
    /// [`Error::Unanswered`] when the key's shard has no leader up,
    /// [`Error::Misrouted`] when it does not hold the key, and
    /// [`Error::Refused`] when it cannot read the version.
    pub async fn get_in(
        &mut self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        let shards = self.coordinator.shards;
        let part = self.part(namespace, key).await?;
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

    /// Sets `key` of `namespace` to `value` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get_in`].
    pub async fn put_in(
        &mut self,
        namespace: NamespaceId,
        key: &[u8],
        value: impl Into<Vec<u8>>,
    ) -> Result<(), Error> {
        self.part(namespace, key).await?.draft.put(key, value);
        Ok(())
    }

    /// Removes `key` of `namespace` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get_in`].
    pub async fn delete_in(&mut self, namespace: NamespaceId, key: &[u8]) -> Result<(), Error> {
        self.part(namespace, key).await?.draft.delete(key);
        Ok(())
    }

    /// Commits the transaction in two phases, with this client as its
    /// coordinator. `Committed` carries the version of the commit on the
    /// primary.
    pub async fn commit(self) -> Transacted {
        let Self {
            coordinator, parts, ..
        } = self;
        let parts: Vec<(ShardId, Part)> = parts.into_iter().collect();
        let Some((primary_shard, primary_key)) = primary(&parts) else {
            return Transacted::Committed(Version::ZERO);
        };
        let id = coordinator.shards.next_txn();
        let deadline = coordinator.deadline();
        let mut proposed = Decision::Commit;
        let mut told = Vec::new();
        for (
            shard,
            Part {
                shard: held, draft, ..
            },
        ) in parts
        {
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

    /// The part of the shard that holds `key` of `namespace`, started
    /// when the transaction first touches the shard.
    async fn part(&mut self, namespace: NamespaceId, key: &[u8]) -> Result<&mut Part, Error> {
        let shards = self.coordinator.shards;
        let unmapped = || Error::Unmapped(namespace);
        let assurance = shards.assurance(namespace).ok_or_else(unmapped)?;
        let shard = shards.locate(namespace, key).ok_or_else(unmapped)?;
        self.admits(namespace, assurance)?;
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
                    assurance,
                    draft: Draft::new(read_at),
                })
            }
        };
        if part.shard.namespace() != namespace || !part.shard.span().contains(key) {
            return Err(Error::Misrouted(Box::new(part.shard.clone())));
        }
        Ok(part)
    }

    /// Whether the transaction may touch `namespace`, kept as `assurance`
    /// says: each namespace it touched of another assurance must have
    /// leave to mix with it.
    fn admits(&self, namespace: NamespaceId, assurance: Assurance) -> Result<(), Error> {
        let unmixed = self.parts.values().find_map(|part| {
            let mix = Mix::new(part.shard.namespace(), namespace)?;
            let mixes = part.assurance != assurance;
            (mixes && !self.mixes.contains(&mix)).then_some(mix)
        });
        unmixed.map_or(Ok(()), |mix| Err(Error::Unmixed(mix)))
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
/// read. When it touched a Byzantine fault tolerant namespace, it is
/// decided there, so only its parts are looked at. `None` when it touched
/// no shard.
fn primary(parts: &[(ShardId, Part)]) -> Option<(ShardId, Primary)> {
    let trusted: Vec<&(ShardId, Part)> = parts
        .iter()
        .filter(|(_, part)| part.assurance == Assurance::Bft)
        .collect();
    if trusted.is_empty() {
        first(&parts.iter().collect::<Vec<_>>())
    } else {
        first(&trusted)
    }
}

/// The first key that `parts` write, with its shard, or, when they write
/// nothing, the start of the first of them.
fn first(parts: &[&(ShardId, Part)]) -> Option<(ShardId, Primary)> {
    let at =
        |(id, part): &(ShardId, Part), key: &[u8]| (*id, Primary::new(part.shard.namespace(), key));
    let written = parts.iter().find_map(|entry| {
        let key = entry.1.draft.first_write()?;
        Some(at(entry, key))
    });
    written.or_else(|| {
        let entry = parts.first()?;
        Some(at(entry, entry.1.shard.span().start()))
    })
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;
    use oleaph_meta::{Assurance, Mix};
    use oleaph_shard::{Epoch, NamespaceId, Shard, ShardId};
    use oleaph_storage::{
        Abort, Command, Deadline, Decision, End, KvOp, Outcome, Primary, Span, TxnId, Version,
    };
    use parking_lot::Mutex;

    use super::{Coordinator, Mixed, Transacted};
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

        fn assurance(&self, namespace: NamespaceId) -> Option<Assurance> {
            (namespace == NamespaceId::DEFAULT).then_some(Assurance::Cft)
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

        fn assurance(&self, _namespace: NamespaceId) -> Option<Assurance> {
            Some(Assurance::Cft)
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

    /// The default namespace and `ns1`, crash fault tolerant, and `ns2`,
    /// Byzantine fault tolerant, each one shard: shards 1, 3 and 4. It
    /// records where each decision was told, and the primary it named.
    #[derive(Default)]
    struct Spaces {
        decided: Mutex<Vec<(ShardId, Decision, Primary)>>,
    }

    const CFT: NamespaceId = NamespaceId::new(1);
    const BFT: NamespaceId = NamespaceId::new(2);

    fn shard_of(namespace: NamespaceId) -> Option<Shard> {
        let id = match namespace.get() {
            0 => ShardId::FIRST,
            1 => ShardId::new(3),
            2 => ShardId::new(4),
            _ => return None,
        };
        Some(Shard::whole(namespace, id))
    }

    impl Shards for Spaces {
        fn locate(&self, namespace: NamespaceId, _key: &[u8]) -> Option<ShardId> {
            Some(shard_of(namespace)?.id())
        }

        fn assurance(&self, namespace: NamespaceId) -> Option<Assurance> {
            let _known: Shard = shard_of(namespace)?;
            Some(if namespace == BFT {
                Assurance::Bft
            } else {
                Assurance::Cft
            })
        }

        async fn refresh(&self) -> Vec<Shard> {
            Vec::new()
        }

        async fn call(&self, shard: ShardId, call: Call) -> Option<Reply> {
            let namespace = NamespaceId::new(match shard.get() {
                1 => 0,
                3 => 1,
                _ => 2,
            });
            Some(match call {
                Call::Begin => Reply::Begun {
                    shard: shard_of(namespace)?,
                    at: Version::new(1),
                },
                Call::Get { .. } => Reply::Value(None),
                Call::Propose(Command::Prepare(_)) => {
                    Reply::Applied(Outcome::Prepared(Version::new(2)))
                }
                Call::Propose(Command::Decide(decide)) => {
                    let told = (shard, decide.decision(), decide.primary().clone());
                    self.decided.lock().push(told);
                    Reply::Applied(Outcome::Committed(Version::new(3)))
                }
                other @ (Call::Scan { .. } | Call::Propose(_)) => {
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

    /// Where `shards` were told the decision on a transaction, in turn.
    fn told(shards: &Spaces) -> Vec<ShardId> {
        shards.decided.lock().iter().map(|(id, _, _)| *id).collect()
    }

    /// The primary the first decision named.
    fn primary_of(shards: &Spaces) -> Primary {
        shards.decided.lock().first().unwrap().2.clone()
    }

    #[test]
    fn namespaces_alike_mix_freely() {
        let shards = Spaces::default();
        let committed = async {
            let mut txn = Coordinator::new(&shards).begin(NamespaceId::DEFAULT);
            txn.put(b"a", "1").await.unwrap();
            txn.put_in(CFT, b"b", "2").await.unwrap();
            txn.commit().await
        }
        .now_or_never()
        .unwrap();
        assert_eq!(committed, Transacted::Committed(Version::new(3)), "commits");
        assert_eq!(told(&shards), [ShardId::FIRST, ShardId::new(3)], "both");
        let primary = Primary::new(NamespaceId::DEFAULT, "a");
        assert_eq!(primary_of(&shards), primary, "the first write");
    }

    #[test]
    fn assurances_mix_only_with_leave() {
        let shards = Spaces::default();
        let refused = async {
            let mut txn = Coordinator::new(&shards).begin(NamespaceId::DEFAULT);
            txn.put(b"a", "1").await.unwrap();
            txn.put_in(BFT, b"b", "2").await
        }
        .now_or_never()
        .unwrap();
        let mix = Mix::new(NamespaceId::DEFAULT, BFT).unwrap();
        assert_eq!(refused, Err(Error::Unmixed(mix)), "no leave");
        let from_bft = async {
            let mut txn = Coordinator::new(&shards).begin(BFT);
            let _value = txn.get(b"b").await.unwrap();
            txn.delete_in(CFT, b"c").await
        }
        .now_or_never()
        .unwrap();
        let other = Mix::new(CFT, BFT).unwrap();
        assert_eq!(from_bft, Err(Error::Unmixed(other)), "either way");
        let wrong = async {
            let mut txn = Coordinator::new(&shards).begin(NamespaceId::DEFAULT);
            txn.admit(Mixed::given(other));
            txn.put(b"a", "1").await.unwrap();
            txn.put_in(BFT, b"b", "2").await
        }
        .now_or_never()
        .unwrap();
        assert_eq!(wrong, Err(Error::Unmixed(mix)), "leave for another pair");
        assert!(shards.decided.lock().is_empty(), "nothing told");
    }

    #[test]
    fn a_mixed_transaction_is_decided_on_the_bft_side() {
        let shards = Spaces::default();
        let mixed = Mixed::given(Mix::new(NamespaceId::DEFAULT, BFT).unwrap());
        let committed = async {
            let mut txn = Coordinator::new(&shards).begin(NamespaceId::DEFAULT);
            txn.admit(mixed);
            txn.put(b"a", "1").await.unwrap();
            txn.put_in(BFT, b"b", "2").await.unwrap();
            txn.commit().await
        }
        .now_or_never()
        .unwrap();
        assert_eq!(committed, Transacted::Committed(Version::new(3)), "commits");
        assert_eq!(
            told(&shards),
            [ShardId::new(4), ShardId::FIRST],
            "bft first"
        );
        assert_eq!(primary_of(&shards), Primary::new(BFT, "b"), "its write");

        let read = Spaces::default();
        let reading = async {
            let mut txn = Coordinator::new(&read).begin(NamespaceId::DEFAULT);
            txn.admit(mixed);
            txn.put(b"a", "1").await.unwrap();
            let _value = txn.get_in(BFT, b"b").await.unwrap();
            txn.commit().await
        }
        .now_or_never()
        .unwrap();
        assert_eq!(reading, Transacted::Committed(Version::new(3)), "commits");
        assert_eq!(told(&read), [ShardId::new(4), ShardId::FIRST], "bft first");
        assert_eq!(primary_of(&read), Primary::new(BFT, ""), "where it read");
    }
}
