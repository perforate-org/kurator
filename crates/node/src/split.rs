//! Splits: by hand from the console, or on their own when a shard this
//! node leads grows past its [`Ceiling`].
//!
//! A split takes its new shard's id from the meta shard, splits through
//! the shard's leader on this node, and reports both halves to the meta
//! shard, so that clients route by them at once. A split that fails after
//! it took an id leaves the id unused; the meta shard never hands it out
//! again.
//!
//! Only the leader of a shard checks its size, as the leader of a
//! region in `TiKV` does, so one node checks each shard. Its halves are
//! checked again on the next round, once they have leaders of their own.

use std::time::Duration;

use kurator_consensus::Replicated as _;
use kurator_meta::{Meta, MetaError, Placement};
use kurator_net::{Quic, Store};
use kurator_shard::{Ceiling, Halves, ShardId};

/// How often a node checks the shards it leads.
pub const EVERY: Duration = Duration::from_secs(10);

/// Why a split did not happen, or was not recorded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Unsplit {
    /// The node holds no replica of the meta shard to find its leader by.
    #[error("this node holds no replica of the meta shard")]
    NoMeta,
    /// No id could be had for the new shard.
    #[error(transparent)]
    Allocate(MetaError),
    /// The shard refused the split, or this node does not lead it.
    #[error(transparent)]
    Split(#[from] kurator_raft::Error),
    /// The shard split, but the meta shard did not record a half; the next
    /// heartbeat reports it.
    #[error("split, but not recorded: {0}")]
    Unrecorded(MetaError),
}

/// Splits `shard` at `at` under an id the meta shard allocates, and
/// reports both halves to it.
///
/// # Errors
///
/// As [`Unsplit`] says.
pub async fn split(
    store: &Store,
    ask: &Quic,
    shard: ShardId,
    at: &[u8],
) -> Result<Halves, Unsplit> {
    let meta = store
        .inbox()
        .replica(ShardId::META)
        .ok_or(Unsplit::NoMeta)?;
    let meta = Meta::new(&meta, ask);
    let right = meta.allocate().await.map_err(Unsplit::Allocate)?;
    let halves = store.split(shard, at, right).await?;
    for half in [halves.left(), halves.right()] {
        // A half whose replica is not open yet is reported by its leader.
        if let Ok(Some(replica)) = store.replica(half.id()).await {
            let _reported = meta
                .report(&Placement::of(&replica))
                .await
                .map_err(Unsplit::Unrecorded)?;
        }
    }
    Ok(halves)
}

/// Splits every shard but the meta shard that a replica of `store` leads
/// and whose state is above `ceiling`, each at the key that halves it.
/// Returns the splits made; the first failure ends the round.
///
/// # Errors
///
/// As [`split`].
pub async fn grow(store: &Store, ask: &Quic, ceiling: Ceiling) -> Result<Vec<Halves>, Unsplit> {
    let shards = store
        .replicas()
        .into_iter()
        .filter(|replica| replica.shard_id() != ShardId::META);
    let mut made = Vec::new();
    for replica in shards {
        // A replica that does not lead refuses the read: its leader checks.
        let Ok(Some(at)) = replica.read(|kv| ceiling.split_point(kv)).await else {
            continue;
        };
        made.push(split(store, ask, replica.shard_id(), &at).await?);
    }
    Ok(made)
}
