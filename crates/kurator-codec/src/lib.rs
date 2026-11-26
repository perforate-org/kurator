//! Little-endian, length-prefixed binary encoding shared by Kurator's log
//! records, snapshots and messages.
//!
//! Writers append fixed-width little-endian integers and [`put_bytes`]
//! values; a [`Reader`] takes them back off the front of a buffer and fails
//! with a [`DecodeError`] instead of reading past its end. A [`Format`]
//! frames a whole file or message: magic bytes and a version up front, a
//! CRC-32C at the end.
//!
//! ```
//! use kurator_codec::{Reader, put_bytes};
//!
//! let mut out = vec![7];
//! put_bytes(&mut out, b"abc")?;
//! let mut reader = Reader::new(&out);
//! assert_eq!(reader.read::<u8>()?, 7);
//! assert_eq!(reader.bytes()?, b"abc");
//! reader.finish()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod format;
mod reader;

use std::io::{self, Write};

pub use format::Format;
pub use reader::{Fixed, Reader};

/// Why bytes could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The input ends in the middle of a value.
    #[error("input ends early")]
    Truncated,
    /// Bytes are left over after the last value.
    #[error("unexpected bytes after the end")]
    Trailing,
    /// The checksum does not match the contents.
    #[error("checksum does not match")]
    Checksum,
    /// The input does not start with the expected magic bytes.
    #[error("not a Kurator file")]
    Magic,
    /// The format version is not one this build reads.
    #[error("unknown format version {0}")]
    Version(u8),
    /// A tag byte names nothing this build knows.
    #[error("unknown tag {0}")]
    Tag(u8),
}

/// A length that does not fit the `u32` prefix.
#[derive(Debug, thiserror::Error)]
#[error("value of {0} bytes is too long to encode")]
struct TooLong(usize);

/// Writes `bytes` with a `u32` length prefix.
///
/// # Errors
///
/// When `bytes` is longer than `u32::MAX`, or when `out` fails.
pub fn put_bytes(out: &mut (impl Write + ?Sized), bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| io::Error::other(TooLong(bytes.len())))?;
    out.write_all(&len.to_le_bytes())?;
    out.write_all(bytes)
}
