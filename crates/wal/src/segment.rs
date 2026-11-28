//! Segment files: `<first lsn as 20 digits>.wal`, a header, then frames.
//!
//! ```text
//! header: magic "OLWL" | version u16 | flags u16 | first lsn u64 | anchor?
//! ```
//!
//! Flag bit 0 marks a signed segment, whose header ends with the 32-byte
//! digest of the record before its first one: the anchor its chain starts
//! from.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use crate::bytes;
use crate::lsn::{Lsn, LsnFiles};
use crate::seal::{Digest, Protection};

/// Size of the fixed part of the header in bytes.
const BASE_LEN: usize = 16;
const ANCHOR_LEN: usize = 32;

const MAGIC: [u8; 4] = *b"OLWL";
const VERSION: u16 = 1;
const FLAG_SIGNED: u16 = 1;
const FILES: LsnFiles = LsnFiles::new(".wal");

/// How the records of a segment are protected, as its header says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Checksummed,
    /// Signed records whose chain starts from `anchor`.
    Signed {
        anchor: Digest,
    },
}

impl Format {
    /// Size of the header of a segment in this format.
    pub(crate) const fn header_len(self) -> usize {
        match self {
            Self::Checksummed => BASE_LEN,
            Self::Signed { .. } => BASE_LEN + ANCHOR_LEN,
        }
    }

    pub(crate) const fn protection(self) -> Protection {
        match self {
            Self::Checksummed => Protection::Checksum,
            Self::Signed { .. } => Protection::Signature,
        }
    }
}

/// A parsed segment header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    first: Lsn,
    format: Format,
}

impl Header {
    pub(crate) const fn first(self) -> Lsn {
        self.first
    }

    pub(crate) const fn format(self) -> Format {
        self.format
    }
}

/// One segment file, named by the first record it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Segment {
    first: Lsn,
    path: PathBuf,
}

impl Segment {
    fn in_dir(dir: &Path, first: Lsn) -> Self {
        Self {
            first,
            path: dir.join(FILES.name(first)),
        }
    }

    /// The first record the file name promises.
    pub(crate) const fn first(&self) -> Lsn {
        self.first
    }

    /// Where the file is.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The segments in `dir`, oldest first. Other files are ignored.
    pub(crate) fn list(dir: &Path) -> io::Result<Vec<Self>> {
        let mut segments = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if let Some(first) = entry
                .file_name()
                .to_str()
                .and_then(|name| FILES.parse(name))
            {
                segments.push(Self {
                    first,
                    path: entry.path(),
                });
            }
        }
        segments.sort_unstable_by_key(Self::first);
        Ok(segments)
    }

    /// Creates the segment for `first` in `dir`, writes and syncs its header,
    /// and syncs `dir` so that the file survives a crash.
    pub(crate) fn create(dir: &Path, first: Lsn, format: Format) -> io::Result<(Self, File)> {
        let segment = Self::in_dir(dir, first);
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&segment.path)?;
        file.write_all(&header(first, format))?;
        file.sync_all()?;
        sync_dir(dir)?;
        Ok((segment, file))
    }

    /// Reopens the segment to append after its last valid frame at `len`,
    /// cutting off anything beyond it.
    pub(crate) fn reopen(&self, len: u64) -> io::Result<File> {
        let file = OpenOptions::new().append(true).open(&self.path)?;
        if file.metadata()?.len() != len {
            file.set_len(len)?;
            file.sync_all()?;
        }
        Ok(file)
    }

    /// Deletes the file. A file that is already gone counts as deleted.
    pub(crate) fn remove(&self) -> io::Result<()> {
        match fs::remove_file(&self.path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}

/// Flushes a directory's entries, so that files created, renamed or removed
/// in it survive a crash.
///
/// # Errors
///
/// When the directory cannot be opened or synced.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn header(first: Lsn, format: Format) -> Vec<u8> {
    let flags = match format {
        Format::Checksummed => 0,
        Format::Signed { .. } => FLAG_SIGNED,
    };
    let mut header = Vec::with_capacity(format.header_len());
    for part in [&MAGIC[..], &VERSION.to_le_bytes(), &flags.to_le_bytes()] {
        header.extend_from_slice(part);
    }
    header.extend_from_slice(&first.get().to_le_bytes());
    if let Format::Signed { anchor } = format {
        header.extend_from_slice(&anchor.to_bytes());
    }
    header
}

/// The header at the start of `buf`, or `None` when `buf` does not start
/// with a whole header of this version.
pub(crate) fn parse_header(buf: &[u8]) -> Option<Header> {
    if bytes::array_at::<4>(buf, 0) != Some(MAGIC) || bytes::u16_at(buf, 4) != Some(VERSION) {
        return None;
    }
    let format = match bytes::u16_at(buf, 6)? {
        0 => Format::Checksummed,
        FLAG_SIGNED => Format::Signed {
            anchor: Digest::from_bytes(bytes::array_at(buf, BASE_LEN)?),
        },
        _ => return None,
    };
    let first = Lsn::new(bytes::u64_at(buf, 8)?);
    Some(Header { first, format })
}

#[cfg(test)]
mod tests {
    use super::{FILES, Format, Header, Segment, header, parse_header};
    use crate::lsn::Lsn;
    use crate::seal::{Digest, Protection};

    const SIGNED: Format = Format::Signed {
        anchor: Digest::from_bytes([9; 32]),
    };

    #[test]
    fn headers_roundtrip() {
        let first = Lsn::new(0x0102_0304_0506_0708);
        for format in [Format::Checksummed, SIGNED] {
            let bytes = header(first, format);
            assert_eq!(bytes.len(), format.header_len(), "{format:?} length");
            assert_eq!(&bytes[..4], b"OLWL", "magic");
            assert_eq!(
                parse_header(&bytes),
                Some(Header { first, format }),
                "{format:?} roundtrip"
            );
        }
        assert_eq!(Format::Checksummed.header_len(), 16, "checksummed length");
        assert_eq!(SIGNED.header_len(), 48, "signed length");
        assert_eq!(SIGNED.protection(), Protection::Signature, "signed");
        assert_eq!(
            Format::Checksummed.protection(),
            Protection::Checksum,
            "checksummed"
        );
    }

    #[test]
    fn foreign_headers_are_rejected() {
        for format in [Format::Checksummed, SIGNED] {
            let good = header(Lsn::new(1), format);
            for at in 0..8 {
                let mut bad = good.clone();
                bad[at] ^= 2;
                assert_eq!(parse_header(&bad), None, "byte {at} changed");
            }
            assert_eq!(
                parse_header(&good[..good.len() - 1]),
                None,
                "{format:?} short"
            );
        }
    }

    #[test]
    fn file_names_roundtrip() {
        let dir = std::path::Path::new("/wal");
        let segment = Segment::in_dir(dir, Lsn::new(42));
        let name = segment.path().file_name().unwrap().to_str().unwrap();
        assert_eq!(name, "00000000000000000042.wal", "name");
        assert_eq!(FILES.parse(name), Some(Lsn::new(42)), "parse");
    }
}
