//! Why a meta operation failed.

use kurator_codec::DecodeError;

/// Why a meta operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetaError {
    /// The meta shard's replica failed or refused: it is not the leader,
    /// say.
    #[error(transparent)]
    Raft(#[from] kurator_raft::Error),
    /// A record does not decode.
    #[error("a meta record does not decode: {0}")]
    Record(#[from] DecodeError),
    /// A record is too long to encode.
    #[error("a meta record does not encode: {0}")]
    Encode(#[from] std::io::Error),
    /// Every shard id is taken.
    #[error("every shard id is taken")]
    Exhausted,
    /// Other transactions kept changing the records this one read.
    #[error("the meta shard stayed too busy to commit")]
    Contended,
}

/// The result of a meta operation.
pub type Result<T> = std::result::Result<T, MetaError>;
