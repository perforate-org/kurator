//! The boundary between Kurator's replicated shards and the consensus
//! protocol that replicates them.
//!
//! Each shard is replicated on a few nodes by a consensus protocol: Raft
//! today ([`kurator_raft`](https://docs.rs/kurator-raft)), which tolerates
//! crashes but trusts its leader; a Byzantine fault tolerant protocol
//! later, as ICP runs one per subnet, so that independent operators can
//! hold the replicas. Whatever runs underneath, a layer above it, the meta
//! shard among them, sees a shard's replica only as [`Replicated`]:
//!
//! - who it is, which shard it holds, and its [`Role`];
//! - the [`Members`] of the shard, each with the [`Peer`] it is reached
//!   at, and the position in the shard's history that set them;
//! - transactions ([`Replicated::begin`]) and linearizable reads
//!   ([`Replicated::read`]) on the shard's state, which fail with an
//!   [`Error`] that names the leader to ask instead.
//!
//! Wiring a node, its transport and its tests to one protocol stays with
//! that protocol's crate.

mod error;
mod node;
mod replicated;

pub use error::{BoxedError, Error, Result};
pub use node::{NodeId, Peer};
pub use replicated::{Members, Replicated, Role, Transaction};
