//! Raft replication for Kurator, on [`OpenRaft`](openraft).
//!
//! A cluster is one shard replicated on a few [`Replica`]s, usually three.
//! Each replica keeps:
//!
//! ```text
//! <dir>/log/        the Raft log: a signed WAL (kurator_wal::Integrity::Signed)
//! <dir>/snapshot/   the latest snapshot of the state machine
//! ```
//!
//! - **Durable quorum commit.** [`LogStore`] syncs every append before it
//!   reports it flushed, so a leader counts a replica, itself included,
//!   only once the entries are on its disk. A write that returned survives
//!   any minority of crashes.
//! - **Signed log.** Every record of the log carries the digest of the
//!   record before it and the replica's Ed25519 signature, so a changed,
//!   removed or reordered record is found when the log is opened.
//! - **Memory-first state.** The [`StateMachine`] holds the whole
//!   [`KvState`](kurator_storage::KvState) in memory. A restart loads the
//!   snapshot and replays the log after it; a follower that fell behind
//!   the purged log receives the leader's snapshot instead.
//!
//! The async runtime is the one thing left open: [`declare_config!`] binds
//! the types to Tokio for production or to a deterministic simulator for
//! tests, and the network is whatever [`openraft::RaftNetworkFactory`] the
//! caller passes, delivering into each replica's [`Inbox`].

mod codec;
mod error;
mod log_store;
mod replica;
mod snapshot;
mod state_machine;
mod types;

pub use error::{BoxedError, Error, Result, SnapshotDamage};
pub use log_store::LogStore;
pub use openraft;
pub use replica::{Inbox, Member, Replica, Role, Status};
pub use snapshot::SnapshotBytes;
pub use state_machine::StateMachine;
pub use types::{
    Config, Entry, LeaderId, LogId, Membership, NodeId, Payload, Proposal, Snapshot, SnapshotMeta,
    StoredMembership, Vote,
};
