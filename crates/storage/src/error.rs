//! Errors.

use std::path::PathBuf;
use std::sync::Arc;

use oleaph_wal::Lsn;

use oleaph_codec::DecodeError;

use crate::outcome::{Abort, Unreadable};

/// Result of a storage operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a storage operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The file system failed.
    #[error("storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The write-ahead log failed while opening or checkpointing.
    #[error(transparent)]
    Wal(#[from] oleaph_wal::Error),
    /// The log failed to make a commit durable. Every write in the same
    /// group commit gets this error; the database takes no more writes and
    /// must be reopened.
    #[error("commit failed: {0}")]
    Commit(Arc<oleaph_wal::Error>),
    /// A log record does not decode.
    #[error("log record {lsn} does not decode: {source}")]
    Record {
        /// The record.
        lsn: Lsn,
        /// Why.
        source: DecodeError,
    },
    /// The latest snapshot is damaged.
    #[error("snapshot {} is damaged: {damage}", path.display())]
    Snapshot {
        /// The snapshot file.
        path: PathBuf,
        /// What is wrong.
        damage: SnapshotDamage,
    },
    /// Another [`Db`](crate::Db) holds the directory.
    #[error("database {} is already open", path.display())]
    Locked {
        /// The database directory.
        path: PathBuf,
    },
    /// The batch has no changes.
    #[error("write batch is empty")]
    EmptyBatch,
    /// The encoded batch exceeds [`oleaph_wal::MAX_PAYLOAD_LEN`].
    #[error("write batch is too large for one log record")]
    BatchTooLarge,
    /// The transaction aborted and wrote nothing.
    #[error("transaction aborted: {0}")]
    Aborted(#[from] Abort),
    /// A transaction's read version is not readable.
    #[error(transparent)]
    Unreadable(#[from] Unreadable),
    /// The commit thread is gone.
    #[error("the commit thread has stopped")]
    Closed,
}

impl Error {
    /// A key or value too long for its length prefix.
    pub(crate) fn too_large(_: std::io::Error) -> Self {
        Self::BatchTooLarge
    }
}

/// What is wrong with a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SnapshotDamage {
    /// The contents do not decode.
    #[error("{0}")]
    Format(DecodeError),
    /// The file name and the contents name different log positions.
    #[error("file name says {named}, contents say {stored}")]
    LsnMismatch {
        /// From the file name.
        named: Lsn,
        /// From the contents.
        stored: Lsn,
    },
}
