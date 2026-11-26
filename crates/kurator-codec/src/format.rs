//! Framing a whole file or message: magic bytes, a version and flags up
//! front, a CRC-32C over everything at the end.
//!
//! ```text
//! magic [u8; 4] | version u16 | flags u16 | body | crc32c u32
//! ```

use std::io::{self, Write};

use crate::{DecodeError, Reader};

/// Flags this build writes and reads: none yet.
const FLAGS: u16 = 0;

/// A framed binary format, told apart by its magic bytes and version.
///
/// ```
/// use std::io::Write as _;
///
/// use kurator_codec::Format;
///
/// let format = Format::new(*b"DEMO", 1);
/// let framed = format.write(Vec::new(), |body| body.write_all(&[42]))?;
/// let mut body = format.read(&framed)?;
/// assert_eq!(body.read::<u8>()?, 42);
/// body.finish()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Format {
    magic: [u8; 4],
    version: u16,
}

impl Format {
    /// The format called `magic`, at `version`.
    #[must_use]
    pub const fn new(magic: [u8; 4], version: u16) -> Self {
        Self { magic, version }
    }

    /// Writes the header to `out`, then what `body` writes, then the
    /// checksum, and hands `out` back.
    ///
    /// # Errors
    ///
    /// When `out` or `body` fails.
    pub fn write<W: Write>(
        &self,
        out: W,
        body: impl FnOnce(&mut dyn Write) -> io::Result<()>,
    ) -> io::Result<W> {
        let mut out = Checksummed { inner: out, crc: 0 };
        for part in [
            &self.magic[..],
            &self.version.to_le_bytes(),
            &FLAGS.to_le_bytes(),
        ] {
            out.write_all(part)?;
        }
        body(&mut out)?;
        let Checksummed { mut inner, crc } = out;
        inner.write_all(&crc.to_le_bytes())?;
        Ok(inner)
    }

    /// Checks `framed` and returns a reader over its body.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Checksum`] when the checksum does not match,
    /// [`DecodeError::Magic`] when it is another format, and
    /// [`DecodeError::Version`] when it is a version or has flags this
    /// build does not read.
    pub fn read<'a>(&self, framed: &'a [u8]) -> Result<Reader<'a>, DecodeError> {
        let (covered, crc) = framed
            .split_last_chunk::<4>()
            .ok_or(DecodeError::Checksum)?;
        if crc32c::crc32c(covered) != u32::from_le_bytes(*crc) {
            return Err(DecodeError::Checksum);
        }
        let mut reader = Reader::new(covered);
        if reader.read::<[u8; 4]>()? != self.magic {
            return Err(DecodeError::Magic);
        }
        let version: u16 = reader.read()?;
        if version != self.version || reader.read::<u16>()? != FLAGS {
            let [low, _] = version.to_le_bytes();
            return Err(DecodeError::Version(low));
        }
        Ok(reader)
    }
}

/// A writer that keeps a CRC-32C of everything written through it.
struct Checksummed<W> {
    inner: W,
    crc: u32,
}

impl<W: Write> Write for Checksummed<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.crc = crc32c::crc32c_append(self.crc, buf.get(..written).unwrap_or_default());
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {

    use super::Format;
    use crate::DecodeError;

    const FORMAT: Format = Format::new(*b"TEST", 3);

    fn framed(body: &[u8]) -> Vec<u8> {
        FORMAT.write(Vec::new(), |out| out.write_all(body)).unwrap()
    }

    #[test]
    fn frames_roundtrip() {
        let bytes = framed(b"body");
        assert_eq!(bytes.len(), 4 + 2 + 2 + 4 + 4, "header, body, checksum");
        let mut reader = FORMAT.read(&bytes).unwrap();
        assert_eq!(reader.read::<[u8; 4]>(), Ok(*b"body"), "body");
        assert_eq!(reader.finish(), Ok(()), "nothing else");
    }

    #[test]
    fn damage_and_foreign_formats_are_told_apart() {
        let mut flipped = framed(b"body");
        flipped[9] ^= 1;
        assert_eq!(
            FORMAT.read(&flipped).err(),
            Some(DecodeError::Checksum),
            "bit flip"
        );
        assert_eq!(
            FORMAT.read(&[1, 2]).err(),
            Some(DecodeError::Checksum),
            "too short"
        );
        let other = Format::new(*b"ELSE", 3)
            .write(Vec::new(), |_| Ok(()))
            .unwrap();
        assert_eq!(
            FORMAT.read(&other).err(),
            Some(DecodeError::Magic),
            "other magic"
        );
        let newer = Format::new(*b"TEST", 4)
            .write(Vec::new(), |_| Ok(()))
            .unwrap();
        assert_eq!(
            FORMAT.read(&newer).err(),
            Some(DecodeError::Version(4)),
            "newer"
        );
    }
}
