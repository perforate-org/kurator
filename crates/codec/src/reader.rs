//! Reading values back off the front of a buffer.

use crate::DecodeError;

/// A fixed-width value a [`Reader`] can read: a little-endian integer, or
/// an array of raw bytes.
pub trait Fixed: sealed::Width {}

mod sealed {
    /// How a fixed-width value is laid out. Private, so that only this
    /// crate decides what is fixed-width.
    pub trait Width: Sized {
        /// Its width in bytes.
        const LEN: usize;

        /// The value in exactly [`Self::LEN`] bytes.
        fn from_bytes(bytes: &[u8]) -> Option<Self>;
    }
}

macro_rules! little_endian {
    ($($int:ty),*) => {$(
        impl sealed::Width for $int {
            const LEN: usize = size_of::<$int>();

            fn from_bytes(bytes: &[u8]) -> Option<Self> {
                bytes.try_into().ok().map(<$int>::from_le_bytes)
            }
        }

        impl Fixed for $int {}
    )*};
}

little_endian!(u8, u16, u32, u64);

impl<const N: usize> sealed::Width for [u8; N] {
    const LEN: usize = N;

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok()
    }
}

impl<const N: usize> Fixed for [u8; N] {}

/// Reads values from the front of a buffer.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    /// A reader over all of `buf`.
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
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

    /// Reads a fixed-width value: a little-endian integer or raw bytes.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] when the input ends before the value.
    pub fn read<T: Fixed>(&mut self) -> Result<T, DecodeError> {
        let bytes = self.take(T::LEN)?;
        T::from_bytes(bytes).ok_or(DecodeError::Truncated)
    }

    /// Reads bytes written by [`put_bytes`](crate::put_bytes).
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] when the input ends before the value does.
    pub fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len: u32 = self.read()?;
        self.take(usize::try_from(len).map_err(|_| DecodeError::Truncated)?)
    }

    /// Takes every byte that is left.
    pub const fn rest(&mut self) -> &'a [u8] {
        let rest = self.rest;
        self.rest = &[];
        rest
    }

    /// Succeeds when every byte was read.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Trailing`] when bytes are left.
    pub const fn finish(self) -> Result<(), DecodeError> {
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

    use super::Reader;
    use crate::{DecodeError, put_bytes};

    #[test]
    fn reads_what_was_written() {
        let mut out = vec![7];
        put_bytes(&mut out, b"abc").unwrap();
        out.extend_from_slice(&9_u64.to_le_bytes());
        let mut reader = Reader::new(&out);
        assert_eq!(reader.read::<u8>(), Ok(7), "u8");
        assert_eq!(reader.bytes(), Ok(b"abc".as_slice()), "bytes");
        assert_eq!(reader.read::<u64>(), Ok(9), "u64");
        assert_eq!(reader.finish(), Ok(()), "finished");
    }

    #[test]
    fn short_and_long_inputs_are_errors() {
        assert_eq!(
            Reader::new(&[1]).read::<u16>(),
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
