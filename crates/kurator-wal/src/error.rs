//! Errors.

use std::path::PathBuf;

use crate::lsn::Lsn;

/// Result of a log operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a log operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The file system failed.
    #[error("WAL I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// A segment holds damage that a crash cannot explain.
    #[error("WAL segment {} is corrupt at byte {offset}: {reason}", path.display())]
    Corrupt {
        /// The damaged segment.
        path: PathBuf,
        /// Byte offset of the damage in the segment.
        offset: u64,
        /// What is wrong.
        reason: Corruption,
    },
    /// The oldest segment starts after the first record the caller needs.
    #[error("records after {after} were requested, but the WAL starts at {first}")]
    Gap {
        /// The caller already holds every record up to this one.
        after: Lsn,
        /// The first record in the log.
        first: Lsn,
    },
    /// The log ends before the first record the caller needs.
    #[error("records after {after} were requested, but the WAL ends before {end}")]
    Behind {
        /// The caller already holds every record up to this one.
        after: Lsn,
        /// The position the next record would get.
        end: Lsn,
    },
    /// A payload is larger than [`MAX_PAYLOAD_LEN`](crate::MAX_PAYLOAD_LEN).
    #[error("WAL payload of {len} bytes exceeds the limit")]
    PayloadTooLarge {
        /// Size of the payload.
        len: usize,
    },
    /// An earlier write or sync failed, so the file may hold a partial
    /// batch; the log takes no more writes until it is opened again.
    #[error("an earlier WAL write failed; reopen the WAL to recover")]
    Poisoned,
    /// The replay callback rejected a record.
    #[error("replaying WAL record {lsn} failed: {source}")]
    Replay {
        /// The record.
        lsn: Lsn,
        /// The callback's error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// What is wrong in a corrupt segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Corruption {
    /// The segment header is missing or malformed.
    #[error("bad segment header")]
    BadHeader,
    /// The header and the file name name different first records.
    #[error("file name says the segment starts at {named}, header says {header}")]
    NameMismatch {
        /// From the file name.
        named: Lsn,
        /// From the header.
        header: Lsn,
    },
    /// A record is not one past the record before it.
    #[error("expected record {expected}, found {found}")]
    LsnMismatch {
        /// The position that should come next.
        expected: Lsn,
        /// The position found.
        found: Lsn,
    },
    /// A record frame is damaged before the end of the log.
    #[error("{0}")]
    Frame(InvalidFrame),
}

/// Why the bytes at some offset are not a valid frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidFrame {
    /// Fewer bytes remain than a frame header needs.
    #[error("record header is cut short")]
    TruncatedHeader,
    /// The length field is above [`MAX_PAYLOAD_LEN`](crate::MAX_PAYLOAD_LEN).
    #[error("record length is above the limit")]
    Oversized,
    /// Fewer bytes remain than the length field promises.
    #[error("record payload is cut short")]
    TruncatedPayload,
    /// The checksum does not match the frame.
    #[error("record checksum does not match")]
    ChecksumMismatch,
}
