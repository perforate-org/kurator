//! Transactions across shards, with the client as their coordinator.
//!
//! The client reads each shard from its leader at the version that leader
//! applied last, and keeps a [`Draft`] per shard. To commit, it prepares
//! each shard's part, then tells the primary, the shard of the first key
//! written, how the transaction came out: that decision is final. The
//! other shards are told after it. A transaction whose coordinator gave
//! up half way leaves its locks behind; [`Cluster::settle`] resolves them
//! by asking the primary, aborting there what it never decided.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;

use kurator_raft::{Error, Replica};
use kurator_shard::{Shard, ShardId};
use kurator_storage::{Decide, Decided, Decision, Draft, Prepare, Version};

use super::{Cluster, Transacted};
use crate::Sim;

/// A transaction across shards being built: what it read and writes on
/// each shard it touched so far.
#[derive(Debug)]
pub struct Across<'cluster> {
    cluster: &'cluster Cluster,
    parts: BTreeMap<ShardId, Part>,
}

/// One shard's part: the shard as its leader held it when the transaction
/// first touched it, and what the transaction read and writes there.
#[derive(Debug)]
struct Part {
    replica: Arc<Replica<Sim>>,
    shard: Shard,
    draft: Draft,
}

/// How a shard answered a prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prepared {
    /// It holds the part, locked.
    Locked,
    /// It refused; the part locked nothing.
    Refused,
    /// No answer: the part may be locked.
    Unknown,
}

impl Across<'_> {
    /// The value of `key`, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] when the key's shard has no leader up, and
    /// [`Error::Misrouted`] when it does not hold the key: the shard
    /// split. The transaction should then be dropped.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let part = self.part(key)?;
        let read = part
            .replica
            .read_local(|state| part.draft.get(state.kv(), key));
        Ok(read.map_err(kurator_storage::Error::from)?)
    }

    /// Sets `key` to `value` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get`].
    pub fn put(&mut self, key: &[u8], value: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.part(key)?.draft.put(key, value);
        Ok(())
    }

    /// Removes `key` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Across::get`].
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.part(key)?.draft.delete(key);
        Ok(())
    }

    /// The part of the shard that holds `key`, started when the
    /// transaction first touches the shard.
    fn part(&mut self, key: &[u8]) -> Result<&mut Part, Error> {
        let shard = self.cluster.locate(key);
        let part = match self.parts.entry(shard) {
            Entry::Occupied(part) => part.into_mut(),
            Entry::Vacant(vacant) => {
                let replica = self
                    .cluster
                    .leader_replica(shard)
                    .ok_or(Error::NotLeader { leader: None })?;
                let (held, read_at) =
                    replica.read_local(|state| (state.shard().clone(), state.kv().version()));
                vacant.insert(Part {
                    replica,
                    shard: held,
                    draft: Draft::new(read_at),
                })
            }
        };
        if !part.shard.span().contains(key) {
            return Err(Error::Misrouted {
                shard: Box::new(part.shard.clone()),
            });
        }
        Ok(part)
    }
}

/// The client as the coordinator of transactions across shards.
#[derive(Debug, Clone, Copy)]
pub struct Coordinator<'cluster> {
    cluster: &'cluster Cluster,
}

impl<'cluster> Coordinator<'cluster> {
    /// A coordinator for transactions on `cluster`.
    #[must_use]
    pub const fn new(cluster: &'cluster Cluster) -> Self {
        Self { cluster }
    }

    /// Runs `body` as one transaction across every shard it touches, and
    /// commits it in two phases, with this client as its coordinator.
    /// `Committed` carries the version of the
    /// commit on the primary. A failing `body` writes nothing.
    pub async fn transact<F>(self, body: F) -> Transacted
    where
        F: FnOnce(&mut Across<'_>) -> Result<(), Error>,
    {
        let mut across = Across {
            cluster: self.cluster,
            parts: BTreeMap::new(),
        };
        if body(&mut across).is_err() {
            return Transacted::Rejected;
        }
        let parts: Vec<(ShardId, Shard, Draft)> = across
            .parts
            .into_iter()
            .map(|(id, part)| (id, part.shard, part.draft))
            .collect();
        let Some(primary) = primary(&parts) else {
            return Transacted::Committed(Version::ZERO);
        };
        let id = self.cluster.next_txn();
        let (primary_shard, primary_key) = primary;
        let mut proposed = Decision::Commit;
        let mut told = Vec::new();
        for (shard, _, draft) in parts {
            let prepare = draft.into_prepare(id, primary_key.clone());
            let prepared = self.prepare(shard, prepare).await;
            if prepared != Prepared::Refused {
                told.push(shard);
            }
            if prepared != Prepared::Locked {
                proposed = Decision::Abort;
                break;
            }
        }
        let proposal = Decide::new(id, primary_key.clone(), proposed);
        let Some(decided) = self.cluster.decide(primary_shard, proposal).await else {
            return Transacted::Unknown;
        };
        let outcome = Decide::new(id, primary_key, super::decision(decided));
        for shard in told.into_iter().filter(|shard| *shard != primary_shard) {
            let _decided: Option<Decided> = self.cluster.decide(shard, outcome.clone()).await;
        }
        match decided {
            Decided::Committed(version) => Transacted::Committed(version),
            Decided::Aborted | _ => Transacted::Aborted,
        }
    }

    /// Prepares `prepare` on `shard`'s leader.
    async fn prepare(self, shard: ShardId, prepare: Prepare) -> Prepared {
        let answer = self
            .cluster
            .on_leader(shard, |replica| {
                let prepare = prepare.clone();
                async move {
                    match replica.prepare(prepare).await {
                        Ok(_) => Ok(Prepared::Locked),
                        Err(Error::Storage(_) | Error::Misrouted { .. }) => Ok(Prepared::Refused),
                        Err(Error::Discarded { .. }) => Ok(Prepared::Unknown),
                        Err(error) => Err(error),
                    }
                }
            })
            .await;
        answer.unwrap_or(Prepared::Unknown)
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
