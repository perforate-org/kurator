//! The operations on the meta shard, each run on its leader.

use std::collections::BTreeSet;

use oleaph_consensus::{NodeId, Replicated, Transaction as _};
use oleaph_shard::{Shard, ShardId};

use crate::error::{MetaError, Result};
use crate::namespace::{Assurance, Namespace, NamespaceName};
use crate::placement::{Placement, Reported};
use crate::registry::{
    NEXT, NEXT_NAMESPACE, Registry, namespace_key, next_id, next_namespace, shard_key,
};

/// How many times a transaction that another one overtook is run again.
const ATTEMPTS: usize = 8;

/// Takes the id the next split names its new shard, through `meta`, the
/// meta shard's leader. No two calls get the same id, and an id is never
/// handed out again, even when its split then fails.
///
/// # Errors
///
/// [`MetaError::Consensus`] when `meta` is not the leader or the commit fails,
/// [`MetaError::Exhausted`] when every id is taken, and
/// [`MetaError::Contended`] when other transactions kept overtaking it.
pub async fn allocate(meta: &impl Replicated) -> Result<ShardId> {
    for _ in 0..ATTEMPTS {
        match try_allocate(meta).await {
            Err(MetaError::Consensus(error)) if error.is_aborted() => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_allocate(meta: &impl Replicated) -> Result<ShardId> {
    let mut txn = meta.begin().await?;
    let next = take_id(&mut txn)?;
    let _version = txn.commit().await?;
    Ok(next)
}

/// Takes the next shard id in `txn`.
fn take_id(txn: &mut impl oleaph_consensus::Transaction) -> Result<ShardId> {
    let next = next_id(txn.get(NEXT)?.as_deref())?;
    let after = next.get().checked_add(1).ok_or(MetaError::Exhausted)?;
    txn.put(NEXT, after.to_le_bytes());
    Ok(next)
}

/// What [`create`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Created {
    /// The namespace is new, and this is its first shard, recorded on the
    /// replicas asked for: they are to start it.
    Founded(Shard),
    /// A namespace of that name is there already: this one. Nothing
    /// changed.
    Taken(Namespace),
}

/// Makes a namespace called `name`, kept as `assurance` says, through
/// `meta`, the meta shard's leader.
///
/// It takes an id for the namespace and one for its first shard, which
/// holds every key of it, and records that shard on `replicas` at
/// membership `membership`. The replicas are then to start the shard,
/// which [`Created::Founded`] names; until they do, its keys cannot be
/// reached.
///
/// # Errors
///
/// As [`allocate`], and [`MetaError::Record`] when a record there does
/// not decode.
pub async fn create(
    meta: &impl Replicated,
    name: &NamespaceName,
    assurance: Assurance,
    replicas: &BTreeSet<NodeId>,
    membership: u64,
) -> Result<Created> {
    for _ in 0..ATTEMPTS {
        match try_create(meta, name, assurance, replicas, membership).await {
            Err(MetaError::Consensus(error)) if error.is_aborted() => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_create(
    meta: &impl Replicated,
    name: &NamespaceName,
    assurance: Assurance,
    replicas: &BTreeSet<NodeId>,
    membership: u64,
) -> Result<Created> {
    if *name == NamespaceName::default_name() {
        return Ok(Created::Taken(Namespace::founding()));
    }
    let key = namespace_key(name);
    let mut txn = meta.begin().await?;
    if let Some(bytes) = txn.get(&key)? {
        return Ok(Created::Taken(Namespace::decode(&bytes)?));
    }
    let id = next_namespace(txn.get(NEXT_NAMESPACE)?.as_deref())?;
    let after = id.get().checked_add(1).ok_or(MetaError::Exhausted)?;
    txn.put(NEXT_NAMESPACE, after.to_le_bytes());
    let mut record = Vec::new();
    Namespace::new(id, assurance).encode(&mut record)?;
    txn.put(key, record);
    let shard = Shard::whole(id, take_id(&mut txn)?);
    let mut placement = Vec::new();
    Placement::new(shard.clone(), replicas.clone(), membership).encode(&mut placement)?;
    txn.put(shard_key(shard.id()), placement);
    let _version = txn.commit().await?;
    Ok(Created::Founded(shard))
}

/// Records `placement` through `meta`, the meta shard's leader, unless the
/// registry records a newer placement of the shard, or the same one.
///
/// # Errors
///
/// [`MetaError::Consensus`] when `meta` is not the leader or the commit fails,
/// [`MetaError::Record`] when the record there does not decode, and
/// [`MetaError::Contended`] when other transactions kept overtaking it.
pub async fn report(meta: &impl Replicated, placement: &Placement) -> Result<Reported> {
    for _ in 0..ATTEMPTS {
        match try_report(meta, placement).await {
            Err(MetaError::Consensus(error)) if error.is_aborted() => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_report(meta: &impl Replicated, placement: &Placement) -> Result<Reported> {
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
/// [`MetaError::Consensus`] when `meta` is not the leader, and
/// [`MetaError::Record`] when a record does not decode.
pub async fn registry(meta: &impl Replicated) -> Result<Registry> {
    Ok(meta.read(Registry::read).await??)
}
