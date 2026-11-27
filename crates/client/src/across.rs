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

use kurator_shard::{Shard, ShardId};
use kurator_storage::{
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

    /// Starts a transaction across every shard it will touch.
    #[must_use]
    pub const fn begin(self) -> Across<'shards, S> {
        Across {
            coordinator: self,
            parts: BTreeMap::new(),
        }
    }

    /// Prepares `prepare` on `shard`'s leader. A lock in the way whose
    /// deadline passed is resolved, for a later transaction to pass.
    async fn prepare(self, shard: ShardId, prepare: Prepare) -> Prepared {
        let reply = self
            .shards
            .call(shard, Call::Propose(Command::Prepare(prepare)))
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
/// each shard it touched so far. Dropping it writes nothing.
#[derive(Debug)]
pub struct Across<'shards, S> {
    coordinator: Coordinator<'shards, S>,
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
    /// [`Error::Unanswered`] when the key's shard has no leader up,
    /// [`Error::Misrouted`] when it does not hold the key, and
    /// [`Error::Refused`] when it cannot read the version.
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
        let Self { coordinator, parts } = self;
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
        for (shard, _, draft) in parts {
            let prepare = draft.into_prepare(id, primary_key.clone(), deadline);
            let prepared = coordinator.prepare(shard, prepare).await;
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
        let shard = shards.locate(key);
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
        if !part.shard.span().contains(key) {
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
