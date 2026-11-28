//! Errors.

use std::io;
use std::path::PathBuf;

use oleaph_codec::DecodeError;
use oleaph_shard::{Shard, ShardId};
use oleaph_wal::Lsn;

use crate::types::{NodeId, Progress};

/// Result of a replica operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a replica operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The file system failed.
    #[error("replica I/O failed: {0}")]
    Io(#[from] io::Error),
    /// The Raft log failed.
    #[error(transparent)]
    Wal(#[from] oleaph_wal::Error),
    /// The command could not be encoded, or a transaction could not read
    /// or commit.
    #[error(transparent)]
    Storage(#[from] oleaph_storage::Error),
    /// A Raft log record does not decode.
    #[error("Raft log record {lsn} does not decode: {source}")]
    Record {
        /// The record.
        lsn: Lsn,
        /// Why.
        source: DecodeError,
    },
    /// The snapshot is damaged.
    #[error("snapshot {} is damaged: {damage}", path.display())]
    Snapshot {
        /// The snapshot file.
        path: PathBuf,
        /// What is wrong.
        damage: SnapshotDamage,
    },
    /// The encoded command does not fit one Raft log record.
    #[error("a proposal of {len} bytes does not fit one log record")]
    ProposalTooLarge {
        /// Size of the encoded command.
        len: usize,
    },
    /// This replica is not the leader; `leader` is the one it knows of.
    #[error("not the leader; the leader is {}", leader.map_or_else(|| "unknown".to_owned(), |id| id.to_string()))]
    NotLeader {
        /// The leader, if this replica knows it.
        leader: Option<NodeId>,
    },
    /// This replica logged the write but lost leadership before it
    /// committed: the write may or may not take effect. `leader` is the new
    /// leader, if this replica knows it.
    #[error("the write was discarded on a leader change; it may still commit")]
    Discarded {
        /// The leader, if this replica knows it.
        leader: Option<NodeId>,
    },
    /// The node cannot become a voter: it is not a learner that holds
    /// everything the leader committed.
    #[error("{node} cannot vote yet: {progress:?}")]
    NotCaughtUp {
        /// The node.
        node: NodeId,
        /// Where it stands.
        progress: Progress,
    },
    /// The shard does not hold a key the request touches, or a split does
    /// not apply to it: it split meanwhile. `shard` is the shard as the
    /// replica holds it now.
    #[error("misrouted: the shard is {shard}")]
    Misrouted {
        /// The shard as it is.
        shard: Box<Shard>,
    },
    /// A split found a transaction across shards prepared on the shard and
    /// not decided yet; it may apply once that is decided.
    #[error("a transaction across shards holds locks on the shard")]
    Locked,
    /// The node holds no replica of the shard.
    #[error("no replica of {shard} here")]
    NoReplica {
        /// The shard.
        shard: ShardId,
    },
    /// The cluster is already initialized.
    #[error("the cluster is already initialized")]
    Initialized,
    /// Raft refused the request.
    #[error("Raft refused the request: {0}")]
    Raft(#[source] BoxedError),
    /// The Raft core stopped, after a storage failure or a shutdown.
    #[error("the replica has stopped: {0}")]
    Stopped(#[source] BoxedError),
}

/// An error from a layer whose error type is generic.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// What is wrong with a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SnapshotDamage {
    /// The contents do not decode.
    #[error("{0}")]
    Format(DecodeError),
    /// The metadata sent with a snapshot is not the metadata inside it.
    #[error("snapshot metadata does not match its contents")]
    MetaMismatch,
}

impl Error {
    /// The error as the storage layer of `OpenRaft` reports it.
    pub(crate) fn into_io(self) -> io::Error {
        if let Self::Io(error) = self {
            error
        } else {
            io::Error::other(self)
        }
    }
}

/// A Raft error, as the layers above consensus tell the cases apart.
impl From<Error> for oleaph_consensus::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::NotLeader { leader } => Self::NotLeader { leader },
            Error::Discarded { leader } => Self::Discarded { leader },
            Error::Storage(error) => Self::Storage(error),
            Error::Misrouted { shard } => Self::Misrouted { shard },
            other @ (Error::Io(_)
            | Error::Wal(_)
            | Error::Record { .. }
            | Error::Snapshot { .. }
            | Error::ProposalTooLarge { .. }
            | Error::NotCaughtUp { .. }
            | Error::Locked
            | Error::NoReplica { .. }
            | Error::Initialized
            | Error::Raft(_)
            | Error::Stopped(_)) => Self::Failed(Box::new(other)),
        }
    }
}
