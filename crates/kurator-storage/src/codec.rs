//! Little-endian, length-prefixed encoding shared by log records and
//! snapshots.

use std::io::{self, Write};

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
pub(crate) fn put_bytes(out: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| io::Error::other(TooLong(bytes.len())))?;
    out.write_all(&len.to_le_bytes())?;
    out.write_all(bytes)
}

/// Reads values from the front of a buffer.
#[derive(Debug)]
pub(crate) struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(buf: &'a [u8]) -> Self {
        Self { rest: buf }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let (head, rest) = self
            .rest
            .split_at_checked(len)
            .ok_or(DecodeError::Truncated)?;
        self.rest = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        self.take(N)?.try_into().map_err(|_| DecodeError::Truncated)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        self.array().map(u8::from_le_bytes)
    }

    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        self.array().map(u16::from_le_bytes)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        self.array().map(u32::from_le_bytes)
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        self.array().map(u64::from_le_bytes)
    }

    /// Reads `N` raw bytes.
    pub(crate) fn exact<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        self.array()
    }

    /// Reads bytes written by [`put_bytes`].
    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.u32()?;
        self.take(usize::try_from(len).map_err(|_| DecodeError::Truncated)?)
    }

    /// Succeeds when every byte was read.
    pub(crate) const fn finish(self) -> Result<(), DecodeError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::Trailing)
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{DecodeError, Reader, put_bytes};

    #[test]
    fn reads_what_was_written() {
        let mut out = vec![7];
        put_bytes(&mut out, b"abc").unwrap();
        out.extend_from_slice(&9_u64.to_le_bytes());
        let mut reader = Reader::new(&out);
        assert_eq!(reader.u8(), Ok(7), "u8");
        assert_eq!(reader.bytes(), Ok(b"abc".as_slice()), "bytes");
        assert_eq!(reader.u64(), Ok(9), "u64");
        assert_eq!(reader.finish(), Ok(()), "finished");
    }

    #[test]
    fn short_and_long_inputs_are_errors() {
        assert_eq!(
            Reader::new(&[1]).u16(),
            Err(DecodeError::Truncated),
            "short"
        );
        assert_eq!(
            Reader::new(&[5, 0, 0, 0, 1]).bytes(),
            Err(DecodeError::Truncated),
            "short bytes"
        );
        assert_eq!(
            Reader::new(&[1]).finish(),
            Err(DecodeError::Trailing),
            "left over"
        );
    }

    proptest! {
        #[test]
        fn arbitrary_input_never_panics(buf in proptest::collection::vec(any::<u8>(), 0..32)) {
            // Each value consumes at least its 4-byte prefix, so the input
            // runs out well within one read per byte.
            let mut reader = Reader::new(&buf);
            let reads = buf.len().saturating_add(1);
            prop_assert!((0..reads).any(|_| reader.bytes().is_err()), "input exhausted");
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::Reader;

    /// Reading length-prefixed values never panics and never yields more
    /// bytes than the input holds.
    #[kani::proof]
    #[kani::unwind(10)]
    fn bytes_is_total() {
        let buf: [u8; 8] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let mut reader = Reader::new(&buf[..len]);
        if let Ok(value) = reader.bytes() {
            assert!(value.len() + 4 <= len, "value fits the input");
        }
    }
}
