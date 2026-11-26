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
//! the payload. `flags` is reserved for signed records.

mod bytes;
mod checksum;
mod error;
mod frame;
mod log;
mod lsn;
mod segment;

pub use error::{Corruption, Error, InvalidFrame, Result};
pub use frame::MAX_PAYLOAD_LEN;
pub use log::{Options, RecoveryReport, TailDamage, TruncatedTail, Wal};
pub use lsn::{Lsn, LsnFiles, LsnRange};
pub use segment::sync_dir;
