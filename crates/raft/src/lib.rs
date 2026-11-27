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
//!   record before it and the replica's signature, so a changed,
//!   removed or reordered record is found when the log is opened.
//! - **Memory-first state.** The [`StateMachine`] holds the whole
//!   [`KvState`](kurator_storage::KvState) in memory. A restart loads the
//!   snapshot and replays the log after it; a follower that fell behind
//!   the purged log receives the leader's snapshot instead.
//!
//! - **Transactions.** [`Replicated::begin`](kurator_consensus::Replicated::begin) starts a [`Txn`] that reads one
//!   version of the state and commits its writes as one log entry. Each
//!   replica checks, as it applies that entry, whether a write after the
//!   read version touched what the transaction read, so every replica
//!   commits or aborts it alike and the committed ones are serializable.
//!   A version is the index of the entry that made it.
//!
//! - **Membership changes.** A new node joins as a learner, catches up
//!   from the snapshot and the log, and only then becomes a voter
//!   ([`Replica::add_learner`], [`Replica::progress`],
//!   [`Replica::promote`]); [`Replica::remove`] takes a node out. Each
//!   member's [`Peer`], its address and transport key, travels in the
//!   membership itself.
//!
//! Two things are left open. [`declare_config!`] binds the types to an
//! async runtime: Tokio in production, a deterministic simulator in tests.
//! And a [`Transport`] carries [`Request`]s and [`Response`]s between
//! replicas, delivering into each replica's [`Inbox`]: QUIC in production,
//! a simulated network in tests. Both speak the same binary encoding.

mod backup;
mod codec;
mod config;
mod error;
mod layout;
mod log_store;
mod network;
mod proposal;
mod replica;
mod rpc;
mod snapshot;
mod state_machine;
mod store;
mod types;

pub use backup::Backup;
pub use config::{Config, Entry, MembershipEdit, Payload};
pub use error::{BoxedError, Error, Result, SnapshotDamage};
pub use kurator_consensus::Role;
pub use kurator_shard::Applied;
pub use log_store::LogStore;
pub use network::{Connection, Failure, Link, Transport};
pub use openraft;
pub use proposal::Proposal;
pub use replica::{Member, Replica, Status, Txn};
pub use rpc::{Message, Refusal, Request, Response};
pub use snapshot::{Snapshot, SnapshotBytes};
pub use state_machine::StateMachine;
pub use store::{Inbox, Memberships, Store};
pub use types::{
    LeaderId, LogId, Membership, NodeId, Peer, Progress, SnapshotMeta, StoredMembership, Vote,
};
