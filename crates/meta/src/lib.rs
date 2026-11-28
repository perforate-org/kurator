//! The meta shard: the registry of every other shard, as the placement
//! driver of `TiKV` and the registry of the Internet Computer keep theirs.
//!
//! The meta shard, [`ShardId::META`](oleaph_shard::ShardId::META), is a
//! Raft group like any other, founded with the cluster on the same voters
//! and never split. Its key space is its own and holds two kinds of
//! record:
//!
//! ```text
//! next              the id the next split names its new shard, u64
//! shard/<id u64 BE> a Placement: the shard, its replicas, and the log
//!                   entry of the membership they come from
//! next-namespace    the id the next namespace gets, u32
//! namespace/<name>  a Namespace: its id and its Assurance
//! mix/<low u32 BE><high u32 BE>
//!                   leave for one transaction to span the two namespaces
//! ```
//!
//! - **Namespaces.** A [`Namespace`] is a key space of its own, split into
//!   shards of its own, and its [`Assurance`] says how they agree. The
//!   default namespace, which holds the keys a client names without one,
//!   is there from the start and has no record. [`create`] makes a crash
//!   fault tolerant one, with its first shard; [`charter`] records a
//!   Byzantine fault tolerant one as its committee's charter names it.
//!
//! - **Ids.** [`allocate`] takes the next id in a transaction, so two
//!   splits never get the same id, even when one of them then fails.
//! - **Reports.** Whoever learns where a shard stands [`report`]s it: the
//!   client after a split, a shard's leader as a heartbeat. A report
//!   replaces the record only when it is newer, by epoch and then by
//!   membership, so reports may arrive late, twice or out of order.
//! - **Routing.** [`Registry::map`] is the shard map clients route by in
//!   a namespace. It is none while the records do not yet tile the
//!   namespace's keys, between the two reports of a split.
//!
//! Every operation runs on the meta shard's leader, as a transaction or a
//! linearizable read of its replica.

mod client;
mod error;
mod mix;
mod namespace;
mod placement;
mod registry;
mod remote;

pub use client::{Created, Mixing, allocate, charter, create, mix, registry, report};
pub use error::{MetaError, Result};
pub use mix::Mix;
pub use namespace::{Assurance, BadAssurance, BadName, Namespace, NamespaceName};
pub use placement::{Placement, Reported};
pub use registry::Registry;
pub use remote::{Answer, Ask, Call, Chartered, Creation, Meta, answer};
