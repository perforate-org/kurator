//! What the coordinator of a transaction needs of the shards: where a key
//! is, and a way to the leader of each.

use std::future::Future;

use oleaph_meta::{Assurance, Mix};
use oleaph_shard::{NamespaceId, Shard, ShardId};
use oleaph_storage::{Abort, Deadline, TxnId};

use crate::remote::{Call, Reply};

/// The shards, as a client reaches them. [`Client`](crate::Client)
/// reaches them over the network; a simulation reaches its replicas
/// directly.
pub trait Shards: Sync {
    /// The shard that holds `key` of `namespace`, as far as this client
    /// knows, or `None` when it knows no shards of the namespace.
    fn locate(&self, namespace: NamespaceId, key: &[u8]) -> Option<ShardId>;

    /// How `namespace` is kept, as far as this client knows, or `None`
    /// when it knows no such namespace.
    fn assurance(&self, namespace: NamespaceId) -> Option<Assurance>;

    /// Every shard of every namespace, learned afresh.
    fn refresh(&self) -> impl Future<Output = Vec<Shard>> + Send;

    /// Makes `call` on the leader of `shard`, following the leaders that
    /// replicas name: the leader's reply, or `None` when no leader
    /// answered in time.
    fn call(&self, shard: ShardId, call: Call) -> impl Future<Output = Option<Reply>> + Send;

    /// Now, on the clock that coordinators share.
    fn now(&self) -> Deadline;

    /// An id that no other transaction has.
    fn next_txn(&self) -> TxnId;
}

/// Why a step of a transaction failed. The transaction should be dropped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// No leader of the shard answered in time.
    #[error("no leader answered")]
    Unanswered,
    /// The client knows no shards of the namespace: there is no such
    /// namespace, or not yet.
    #[error("no shards of {0} are known")]
    Unmapped(NamespaceId),
    /// The transaction would span two namespaces of different assurances
    /// that have no leave to mix.
    #[error("{0} are not given leave to mix in one transaction")]
    Unmixed(Mix),
    /// The shard does not hold the key: it split.
    #[error("shard {} does not hold the key", .0.id())]
    Misrouted(Box<Shard>),
    /// The shard could not read: the version is forgotten or ahead of
    /// it, or a lock is in the way.
    #[error("refused: {0}")]
    Refused(Abort),
    /// The shard failed, or answered what the call does not expect.
    #[error("failed: {0}")]
    Failed(String),
}

impl Error {
    /// The error a reply stands for, when it is not the one expected.
    pub(crate) fn of(reply: Reply) -> Self {
        match reply {
            Reply::Misrouted(shard) => Self::Misrouted(Box::new(shard)),
            Reply::Refused(abort) => Self::Refused(abort),
            Reply::Failed(failed) => Self::Failed(failed),
            other @ (Reply::Map { .. }
            | Reply::Begun { .. }
            | Reply::Value(_)
            | Reply::Entries(_)
            | Reply::Applied(_)
            | Reply::NotLeader(_)
            | Reply::Discarded(_)
            | Reply::NoReplica) => Self::Failed(format!("unexpected reply {other:?}")),
        }
    }
}
