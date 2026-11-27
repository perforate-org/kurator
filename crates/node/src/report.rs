//! Placement reports: every shard whose chores are a node's [`Duty`] (the
//! shards it leads, under Raft), told to the meta shard wherever writes to
//! it go, as a heartbeat.
//!
//! The registry keeps a report only when it is newer than what it records,
//! so reporting the same placement again changes nothing, and a report
//! that a leader change cut off is made again on the next beat.

use std::time::Duration;

use kurator_consensus::{Duty, Replicated as _};
use kurator_meta::{Meta, MetaError, Placement};
use kurator_net::{Quic, Store};
use kurator_shard::ShardId;

/// How often a node reports the shards whose chores are its own.
pub const EVERY: Duration = Duration::from_secs(2);

/// Reports every shard but the meta shard whose chores are the [`Duty`] of
/// a replica of `store`,
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
    let leading = store
        .replicas()
        .into_iter()
        .filter(|replica| replica.shard_id() != ShardId::META && replica.duty() == Duty::Mine);
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
