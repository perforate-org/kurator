//! What can go wrong when a replica is asked to read or write.

use oleaph_shard::Shard;

use crate::node::NodeId;

/// Result of an operation on a replica.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why an operation on a replica failed, as a layer above the consensus
/// protocol tells the cases apart.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// This replica does not lead the shard; `leader` is the one it knows
    /// of.
    #[error("not the leader; the leader is {}", leader.map_or_else(|| "unknown".to_owned(), |id| id.to_string()))]
    NotLeader {
        /// The leader, if this replica knows it.
        leader: Option<NodeId>,
    },
    /// The write was proposed but leadership changed before it committed:
    /// it may or may not take effect. `leader` is the new leader, if this
    /// replica knows it.
    #[error("the write was discarded on a leader change; it may still commit")]
    Discarded {
        /// The leader, if this replica knows it.
        leader: Option<NodeId>,
    },
    /// The command could not be encoded, or a transaction could not read
    /// or commit: [`oleaph_storage::Error::Aborted`] when it aborted.
    #[error(transparent)]
    Storage(#[from] oleaph_storage::Error),
    /// The shard does not hold a key the request touches: it split
    /// meanwhile. `shard` is the shard as the replica holds it now.
    #[error("misrouted: the shard is {shard}")]
    Misrouted {
        /// The shard as it is.
        shard: Box<Shard>,
    },
    /// The protocol failed otherwise: the replica stopped, or its storage
    /// failed.
    #[error(transparent)]
    Failed(BoxedError),
}

/// An error from a layer whose error type is generic.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

impl Error {
    /// The leader the error names, when it says this replica does not
    /// lead: `Some(None)` when it does not know who does.
    #[must_use]
    pub const fn redirect(&self) -> Option<Option<NodeId>> {
        match self {
            Self::NotLeader { leader } | Self::Discarded { leader } => Some(*leader),
            Self::Storage(_) | Self::Misrouted { .. } | Self::Failed(_) => None,
        }
    }

    /// Whether the transaction aborted, so that running it again may
    /// succeed.
    #[must_use]
    pub const fn is_aborted(&self) -> bool {
        matches!(self, Self::Storage(oleaph_storage::Error::Aborted(_)))
    }
}
