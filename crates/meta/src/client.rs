//! The operations on the meta shard, each run on its leader.

use kurator_raft::{Config, Replica};
use kurator_shard::ShardId;

use crate::error::{MetaError, Result};
use crate::placement::{Placement, Reported};
use crate::registry::{NEXT, Registry, next_id, shard_key};

/// How many times a transaction that another one overtook is run again.
const ATTEMPTS: usize = 8;

/// Takes the id the next split names its new shard, through `meta`, the
/// meta shard's leader. No two calls get the same id, and an id is never
/// handed out again, even when its split then fails.
///
/// # Errors
///
/// [`MetaError::Raft`] when `meta` is not the leader or the commit fails,
/// [`MetaError::Exhausted`] when every id is taken, and
/// [`MetaError::Contended`] when other transactions kept overtaking it.
pub async fn allocate<C: Config>(meta: &Replica<C>) -> Result<ShardId> {
    for _ in 0..ATTEMPTS {
        match try_allocate(meta).await {
            Err(MetaError::Raft(error)) if aborted(&error) => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_allocate<C: Config>(meta: &Replica<C>) -> Result<ShardId> {
    let mut txn = meta.begin().await?;
    let next = next_id(txn.get(NEXT)?.as_deref())?;
    let after = next.get().checked_add(1).ok_or(MetaError::Exhausted)?;
    txn.put(NEXT, after.to_le_bytes());
    let _version = txn.commit().await?;
    Ok(next)
}

/// Records `placement` through `meta`, the meta shard's leader, unless the
/// registry records a newer placement of the shard, or the same one.
///
/// # Errors
///
/// [`MetaError::Raft`] when `meta` is not the leader or the commit fails,
/// [`MetaError::Record`] when the record there does not decode, and
/// [`MetaError::Contended`] when other transactions kept overtaking it.
pub async fn report<C: Config>(meta: &Replica<C>, placement: &Placement) -> Result<Reported> {
    for _ in 0..ATTEMPTS {
        match try_report(meta, placement).await {
            Err(MetaError::Raft(error)) if aborted(&error) => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_report<C: Config>(meta: &Replica<C>, placement: &Placement) -> Result<Reported> {
    let key = shard_key(placement.id());
    let mut txn = meta.begin().await?;
    if let Some(bytes) = txn.get(&key)? {
        let recorded = Placement::decode(&bytes)?;
        if !placement.supersedes(&recorded) {
            return Ok(Reported::Stale);
        }
    }
    let mut value = Vec::new();
    placement.encode(&mut value)?;
    txn.put(key, value);
    let _version = txn.commit().await?;
    Ok(Reported::Recorded)
}

/// The registry as `meta`, the meta shard's leader, holds it now: a
/// linearizable read.
///
/// # Errors
///
/// [`MetaError::Raft`] when `meta` is not the leader, and
/// [`MetaError::Record`] when a record does not decode.
pub async fn registry<C: Config>(meta: &Replica<C>) -> Result<Registry> {
    Ok(meta.read(Registry::read).await??)
}

/// Whether `error` says a transaction was overtaken, so that running it
/// again may succeed.
const fn aborted(error: &kurator_raft::Error) -> bool {
    matches!(
        error,
        kurator_raft::Error::Storage(kurator_storage::Error::Aborted(_))
    )
}
