//! The operations on the meta shard, each run on its leader.

use std::collections::BTreeSet;

use oleaph_consensus::{NodeId, Replicated, Transaction as _};
use oleaph_shard::{NamespaceId, Shard, ShardId};

use crate::error::{MetaError, Result};
use crate::mix::Mix;
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
    /// The namespace's id or its shard's, as a charter names them, was
    /// handed out already, or is the default's. Nothing changed.
    Spent,
}

/// Makes a crash fault tolerant namespace called `name` through `meta`,
/// the meta shard's leader.
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
    replicas: &BTreeSet<NodeId>,
    membership: u64,
) -> Result<Created> {
    for _ in 0..ATTEMPTS {
        match try_create(meta, name, replicas, membership).await {
            Err(MetaError::Consensus(error)) if error.is_aborted() => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_create(
    meta: &impl Replicated,
    name: &NamespaceName,
    replicas: &BTreeSet<NodeId>,
    membership: u64,
) -> Result<Created> {
    let mut txn = meta.begin().await?;
    if let Some(recorded) = recorded(&mut txn, name)? {
        return Ok(Created::Taken(recorded));
    }
    let id = next_namespace(txn.get(NEXT_NAMESPACE)?.as_deref())?;
    let after = id.get().checked_add(1).ok_or(MetaError::Exhausted)?;
    txn.put(NEXT_NAMESPACE, after.to_le_bytes());
    let mut record = Vec::new();
    Namespace::new(id, Assurance::Cft).encode(&mut record)?;
    txn.put(namespace_key(name), record);
    let shard = Shard::whole(id, take_id(&mut txn)?);
    let mut placement = Vec::new();
    Placement::new(shard.clone(), replicas.clone(), membership).encode(&mut placement)?;
    txn.put(shard_key(shard.id()), placement);
    let _version = txn.commit().await?;
    Ok(Created::Founded(shard))
}

/// Records a Byzantine fault tolerant namespace that a charter names,
/// through `meta`, the meta shard's leader, so that clients find it.
///
/// The namespace is called `name` and has id `namespace`; the committee
/// of `members` decides its one shard, `shard`.
///
/// A charter fixes the ids, so the registry takes them only if it never
/// handed them out, and hands out only later ones from then on. Recording
/// the same namespace again changes nothing, and answers as the first
/// time did.
///
/// # Errors
///
/// As [`create`].
pub async fn charter(
    meta: &impl Replicated,
    name: &NamespaceName,
    namespace: NamespaceId,
    shard: ShardId,
    members: &BTreeSet<NodeId>,
) -> Result<Created> {
    for _ in 0..ATTEMPTS {
        match try_charter(meta, name, namespace, shard, members).await {
            Err(MetaError::Consensus(error)) if error.is_aborted() => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_charter(
    meta: &impl Replicated,
    name: &NamespaceName,
    namespace: NamespaceId,
    shard: ShardId,
    members: &BTreeSet<NodeId>,
) -> Result<Created> {
    let chartered = Namespace::new(namespace, Assurance::Bft);
    let whole = Shard::whole(namespace, shard);
    let mut txn = meta.begin().await?;
    if let Some(recorded) = recorded(&mut txn, name)? {
        return Ok(if recorded == chartered {
            Created::Founded(whole)
        } else {
            Created::Taken(recorded)
        });
    }
    let next = next_namespace(txn.get(NEXT_NAMESPACE)?.as_deref())?;
    let next_shard = next_id(txn.get(NEXT)?.as_deref())?;
    if namespace < next || shard < next_shard {
        return Ok(Created::Spent);
    }
    let after = namespace.get().checked_add(1).ok_or(MetaError::Exhausted)?;
    let after_shard = shard.get().checked_add(1).ok_or(MetaError::Exhausted)?;
    txn.put(NEXT_NAMESPACE, after.to_le_bytes());
    txn.put(NEXT, after_shard.to_le_bytes());
    let mut record = Vec::new();
    chartered.encode(&mut record)?;
    txn.put(namespace_key(name), record);
    let mut placement = Vec::new();
    Placement::new(whole.clone(), members.clone(), 0).encode(&mut placement)?;
    txn.put(shard_key(shard), placement);
    let _version = txn.commit().await?;
    Ok(Created::Founded(whole))
}

/// What [`mix`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mixing {
    /// One transaction may span the two namespaces from now on.
    Allowed(Mix),
    /// They are one namespace, or alike in assurance: a transaction may
    /// span them without leave. Nothing changed.
    Alike,
    /// There is no namespace of this name. Nothing changed.
    Unknown(NamespaceName),
}

/// Gives one transaction leave to span the namespaces called `one` and
/// `other`, of different assurances, through `meta`, the meta shard's
/// leader. Giving it twice changes nothing.
///
/// # Errors
///
/// As [`create`].
pub async fn mix(
    meta: &impl Replicated,
    one: &NamespaceName,
    other: &NamespaceName,
) -> Result<Mixing> {
    for _ in 0..ATTEMPTS {
        match try_mix(meta, one, other).await {
            Err(MetaError::Consensus(error)) if error.is_aborted() => {}
            outcome => return outcome,
        }
    }
    Err(MetaError::Contended)
}

async fn try_mix(
    meta: &impl Replicated,
    one: &NamespaceName,
    other: &NamespaceName,
) -> Result<Mixing> {
    let mut txn = meta.begin().await?;
    let Some(first) = recorded(&mut txn, one)? else {
        return Ok(Mixing::Unknown(one.clone()));
    };
    let Some(second) = recorded(&mut txn, other)? else {
        return Ok(Mixing::Unknown(other.clone()));
    };
    let mix = Mix::new(first.id(), second.id());
    let Some(mix) = mix.filter(|_| first.assurance() != second.assurance()) else {
        return Ok(Mixing::Alike);
    };
    txn.put(mix.key(), Vec::new());
    let _version = txn.commit().await?;
    Ok(Mixing::Allowed(mix))
}

/// The namespace called `name`, as `txn` reads its record.
fn recorded(
    txn: &mut impl oleaph_consensus::Transaction,
    name: &NamespaceName,
) -> Result<Option<Namespace>> {
    if *name == NamespaceName::default_name() {
        return Ok(Some(Namespace::founding()));
    }
    let record = txn.get(&namespace_key(name))?;
    Ok(record.map(|bytes| Namespace::decode(&bytes)).transpose()?)
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
