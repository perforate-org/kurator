//! Append-only write-ahead log for Kurator.
//!
//! A log is a directory of segment files. Every record gets a [`Lsn`], one
//! higher than the record before it, and [`Wal::append_batch`] makes a whole
//! batch durable with a single `fdatasync`: that is what group commit builds
//! on.
//!
//! [`Wal::open`] replays the records after a given [`Lsn`] and repairs the
//! one kind of damage a crash can leave: a partly written record at the end
//! of the newest segment. That tail is cut off and reported in
//! [`RecoveryReport`]. Damage anywhere else is [`Error::Corrupt`], because a
//! crash cannot explain it.
//!
//! # On-disk format
//!
//! Segment file `<first lsn as 20 digits>.wal`:
//!
//! ```text
//! header: magic "KRWL" | version u16 | flags u16 | first lsn u64
//! frame:  payload len u32 | crc32c u32 | lsn u64 | payload
//! ```
//!
//! Integers are little endian. The checksum covers the length, the lsn and
//! the payload.
//!
//! # Signed logs
//!
//! A log opened with [`Integrity::Signed`] chains its records: each one ends
//! with the [`Digest`] of the record before it and an Ed25519 signature
//! over its own digest, `BLAKE3(lsn | prev | payload)`. Flag bit 0 of the
//! segment header marks such a segment, and its header ends with the digest
//! its chain starts from, so purging old segments keeps the rest checkable.
//! [`Wal::open`] verifies every record and reports a mismatch as
//! [`Error::Corrupt`] with [`Corruption::Seal`]: a crash cannot explain it.
//!
//! ```text
//! signed header: magic | version | flags = 1 | first lsn | anchor [32]
//! signed frame:  len | crc32c | lsn | payload | prev [32] | signature [64]
//! ```

mod bytes;
mod checksum;
mod error;
mod frame;
mod log;
mod lsn;
mod seal;
mod segment;

pub use error::{Corruption, Error, InvalidFrame, Result};
pub use frame::MAX_PAYLOAD_LEN;
pub use log::{Options, RecoveryReport, Start, TailDamage, TruncatedTail, Wal};
pub use lsn::{Lsn, LsnFiles, LsnRange};
pub use seal::{Digest, Integrity, Protection, SealDamage, SigningKey, VerifyingKey};
pub use segment::sync_dir;
