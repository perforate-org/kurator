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
//! with the [`Digest`] of the record before it, and the last record of
//! every batch that [`Wal::append_batch`] syncs also carries a signature
//! over its own digest, `BLAKE3(lsn | prev | payload)`. Through
//! the chain, that signature covers the whole batch, so signing costs one
//! signature per sync rather than one per record. Flag bit 0 of the segment
//! header marks such a segment, and its header ends with the digest its
//! chain starts from, so purging old segments keeps the rest checkable.
//!
//! [`Wal::open`] replays a batch only once its signature checks out. A
//! newest segment that ends inside a batch was cut short by a crash before
//! the batch was synced: that batch is cut off and reported as
//! [`TailDamage::Unsigned`]. Any other mismatch is [`Error::Corrupt`] with
//! [`Corruption::Seal`], because a crash cannot explain it.
//!
//! ```text
//! signed header: magic | version | flags = 1 | first lsn | anchor [32]
//! linked frame:  len | crc32c | lsn | payload | prev [32] | 0
//! signed frame:  len | crc32c | lsn | payload | prev [32] | signature | algorithm u8 | 1
//! ```
//!
//! The signature is as long as the signatures of the algorithm whose
//! [`kurator_crypto::Algorithm::id`] follows it: 64 bytes for Ed25519.

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
pub use seal::{Digest, Integrity, Protection, SealDamage};
pub use segment::sync_dir;
