//! Kurator's client, as a node sees it: the [`Request`]s a client sends a
//! node and the [`Reply`] the node answers with.
//!
//! A client is not a replica. It reads at a version it chose, so its reads
//! need no lock, and it writes by handing the shard's leader a
//! [`Command`](kurator_storage::Command): a write batch, a transaction
//! that commits only if what it read did not change, or a step of a
//! transaction across shards. The leader replicates the command and
//! answers with its [`Outcome`](kurator_storage::Outcome). A node that
//! does not lead names the leader it knows of.

mod remote;

pub use remote::{Call, Reach, Reply, Request, answer};
