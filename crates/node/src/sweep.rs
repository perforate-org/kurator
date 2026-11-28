//! Forgetting old decisions on transactions across shards that no one can
//! ask about any more.
//!
//! The node whose [`Duty`] the meta shard's chores are (its leader, under
//! Raft) collects them now and then.

use std::time::Duration;

use oleaph_client::{Client, Collected, Collector};
use oleaph_consensus::{Duty, Replicated as _};
use oleaph_net::{Quic, Store, Wall};
use oleaph_shard::ShardId;

/// How often the node checks whether to collect.
pub const EVERY: Duration = Duration::from_secs(60);

/// How long a decision is kept past the deadline of its transaction. Past
/// its deadline, whoever runs into a lock of the transaction resolves it
/// at the primary; this leaves them that long to.
pub const GRACE: Duration = Duration::from_secs(600);

/// Collects the decisions past [`GRACE`] when the meta shard's chores are
/// this node's [`Duty`], so that one node collects at a time: how it came out, or `None`
/// when another node collects.
pub async fn sweep(store: &Store, quic: &Quic) -> Option<Collected> {
    let meta = store.inbox().replica(ShardId::META)?;
    if meta.duty() != Duty::Mine {
        return None;
    }
    Some(collect(store, quic).await)
}

/// Collects the decisions past [`GRACE`] from every shard the cluster
/// has, reaching them as a client through `quic`.
pub async fn collect(store: &Store, quic: &Quic) -> Collected {
    let peers = store.inbox().memberships().nodes();
    // A collector starts no transaction, so its ids need no origin.
    let client = Client::new(quic.clone(), Wall::default(), peers, 0);
    Collector::new(&client).collect(GRACE).await
}
