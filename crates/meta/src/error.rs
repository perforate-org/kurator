//! Why a meta operation failed.

use oleaph_codec::DecodeError;

/// Why a meta operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetaError {
    /// The meta shard's replica failed or refused: it is not the leader,
    /// say.
    #[error(transparent)]
    Consensus(#[from] oleaph_consensus::Error),
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
    /// No node the call reached knew a leader of the meta shard.
    #[error("the meta shard has no leader this node knows of")]
    NoLeader,
    /// A node could not be asked.
    #[error("cannot ask {node}: {source}")]
    Unreachable {
        /// The node.
        node: oleaph_consensus::NodeId,
        /// Why.
        #[source]
        source: std::io::Error,
    },
    /// The leader failed to run the call, for this reason.
    #[error("the meta shard's leader failed: {0}")]
    Remote(String),
    /// The leader answered another call than the one asked.
    #[error("the meta shard's leader answered another call")]
    Mismatch,
}

/// The result of a meta operation.
pub type Result<T> = std::result::Result<T, MetaError>;
