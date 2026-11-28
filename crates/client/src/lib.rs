//! OleaphDB's client.
//!
//! A client is not a replica. It reads at a version it chose, so its reads
//! need no lock, and it writes by handing the shard's leader a
//! [`Command`](oleaph_storage::Command): a write batch, a transaction
//! that commits only if what it read did not change, or a step of a
//! transaction across shards. The leader replicates the command and
//! answers with its [`Outcome`](oleaph_storage::Outcome). A node that
//! does not lead names the leader it knows of.
//!
//! - [`Request`] and [`Reply`] are what a client and a node say, and
//!   [`answer`] is how a node answers from its replica.
//! - [`Client`] finds the shards and their leaders over the network.
//! - [`Coordinator`] runs transactions across shards in two phases, and
//!   [`Collector`] forgets their decisions once no one can ask about
//!   them. Both reach the shards through [`Shards`], so a simulation
//!   runs the same code on its replicas.

mod across;
mod client;
mod collect;
mod remote;
mod resolve;
mod shards;

pub use across::{Across, Coordinator, LOCK_TTL, Mixed, Transacted};
pub use client::{Client, Time};
pub use collect::{Collected, Collector};
pub use remote::{Call, Reach, Reply, Request, answer};
pub use resolve::{Lock, resolve_expired};
pub use shards::{Error, Shards};
