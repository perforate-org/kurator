//! Placement reports: every shard a node leads, told to the meta shard's
//! leader, wherever it is, as a heartbeat.
//!
//! The registry keeps a report only when it is newer than what it records,
//! so reporting the same placement again changes nothing, and a report
//! that a leader change cut off is made again on the next beat.

use std::time::Duration;

use kurator_meta::{Meta, MetaError, Placement};
use kurator_net::{Quic, Store};
use kurator_raft::Role;
use kurator_shard::ShardId;

/// How often a node reports the shards it leads.
pub const EVERY: Duration = Duration::from_secs(2);

/// Reports every shard but the meta shard that a replica of `store` leads,
/// through the store's replica of the meta shard and `ask`. The first
/// failure ends the beat.
///
/// # Errors
///
/// When the store holds no replica of the meta shard, or a report fails.
pub async fn heartbeat(store: &Store, ask: &Quic) -> Result<(), Unreported> {
    let meta = store
        .inbox()
        .replica(ShardId::META)
        .ok_or(Unreported::NoMeta)?;
    let meta = Meta::new(&meta, ask);
    // A replica that holds no state yet has no membership to lead.
    let leading = store.replicas().into_iter().filter(|replica| {
        replica.shard_id() != ShardId::META && replica.status().role() == Role::Leader
    });
    for replica in leading {
        let _reported = meta.report(&Placement::of(&replica)).await?;
    }
    Ok(())
}

/// Why a beat reported nothing, or not everything.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Unreported {
    /// The node holds no replica of the meta shard to find its leader by.
    #[error("this node holds no replica of the meta shard")]
    NoMeta,
    /// A report failed.
    #[error(transparent)]
    Meta(#[from] MetaError),
}
