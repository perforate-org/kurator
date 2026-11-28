//! Namespaces made from the console: the meta shard records the namespace
//! and its first shard, and this node starts that shard on the meta
//! shard's voters.
//!
//! The meta shard takes the namespace's id and its first shard's id in
//! one transaction, which also records the shard on the voters, so that
//! clients route to it at once. This node then seeds its own replica of
//! the shard with that membership; it campaigns, and the other voters
//! receive the shard from it as it leads.

use std::collections::{BTreeMap, BTreeSet};

use oleaph_consensus::Replicated as _;
use oleaph_meta::{Assurance, Created, Creation, Meta, MetaError, Namespace, NamespaceName};
use oleaph_net::{Quic, Store};
use oleaph_raft::{NodeId, Peer};
use oleaph_shard::{NamespaceId, Shard, ShardId};

/// Why a namespace was not made, or not started.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Uncreated {
    /// The node holds no replica of the meta shard to find its leader by.
    #[error("this node holds no replica of the meta shard")]
    NoMeta,
    /// The node does not vote in the meta shard, so it would not be a
    /// voter of the new shard either, and could not start it.
    #[error("this node does not vote in the meta shard")]
    NotVoter,
    /// A namespace of that name is there already: this one.
    #[error("there is a namespace of that name already: {0}")]
    Taken(Namespace),
    /// The meta shard did not record the namespace.
    #[error(transparent)]
    Meta(#[from] MetaError),
    /// The meta shard recorded the namespace, but its first shard did not
    /// start on this node.
    #[error("{shard} is recorded, but did not start: {error}")]
    Unstarted {
        /// The shard.
        shard: Shard,
        /// Why.
        error: oleaph_raft::Error,
    },
}

/// Makes a namespace called `name`, kept as `assurance` says, and starts
/// its first shard on the meta shard's voters, this node among them.
/// Returns that shard.
///
/// # Errors
///
/// As [`Uncreated`] says.
pub async fn create(
    store: &Store,
    ask: &Quic,
    name: &NamespaceName,
    assurance: Assurance,
) -> Result<Shard, Uncreated> {
    let meta = store
        .inbox()
        .replica(ShardId::META)
        .ok_or(Uncreated::NoMeta)?;
    let members = meta.members();
    if !members.votes(store.id()) {
        return Err(Uncreated::NotVoter);
    }
    let voters: BTreeMap<NodeId, Peer> = members
        .peers()
        .filter(|(id, _)| members.votes(*id))
        .map(|(id, peer)| (id, peer.clone()))
        .collect();
    let replicas: BTreeSet<NodeId> = voters.keys().copied().collect();
    // The shard's seed is the first entry of its log.
    let creation = Creation::new(name.clone(), assurance, replicas, 0);
    let shard = match Meta::new(&meta, ask).create(creation).await? {
        Created::Founded(shard) => shard,
        Created::Taken(namespace) => return Err(Uncreated::Taken(namespace)),
        other => return Err(MetaError::Remote(format!("unexpected {other:?}")).into()),
    };
    store
        .found_shard(shard.clone(), voters)
        .await
        .map_err(|error| Uncreated::Unstarted {
            shard: shard.clone(),
            error,
        })?;
    Ok(shard)
}

/// The id of the namespace called `name`, as the meta shard records it,
/// asked through this node's replica of the meta shard and `ask`. The
/// default namespace needs no asking.
///
/// # Errors
///
/// [`Unnamed::NoMeta`] when the node holds no replica of the meta shard,
/// [`Unnamed::Meta`] when the registry cannot be read, and
/// [`Unnamed::Unknown`] when there is no such namespace.
pub async fn resolve(
    store: &Store,
    ask: &Quic,
    name: &NamespaceName,
) -> Result<NamespaceId, Unnamed> {
    if *name == NamespaceName::default_name() {
        return Ok(NamespaceId::DEFAULT);
    }
    let meta = store
        .inbox()
        .replica(ShardId::META)
        .ok_or(Unnamed::NoMeta)?;
    let registry = Meta::new(&meta, ask).registry().await?;
    registry
        .namespace(name)
        .map(Namespace::id)
        .ok_or_else(|| Unnamed::Unknown(name.clone()))
}

/// Why a namespace's name did not resolve.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Unnamed {
    /// The node holds no replica of the meta shard to ask.
    #[error("this node holds no replica of the meta shard")]
    NoMeta,
    /// The registry could not be read.
    #[error(transparent)]
    Meta(#[from] MetaError),
    /// There is no namespace of that name.
    #[error("there is no namespace {0}")]
    Unknown(NamespaceName),
}
