//! Segment files: `<first lsn as 20 digits>.wal`, a header, then frames.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use crate::bytes;
use crate::lsn::{Lsn, LsnFiles};

/// Size of the segment header in bytes.
pub(crate) const HEADER_LEN: usize = 16;

const MAGIC: [u8; 4] = *b"KRWL";
const VERSION: u16 = 1;
const FLAGS: u16 = 0;
const FILES: LsnFiles = LsnFiles::new(".wal");

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
    pub(crate) fn create(dir: &Path, first: Lsn) -> io::Result<(Self, File)> {
        let segment = Self::in_dir(dir, first);
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&segment.path)?;
        file.write_all(&header(first))?;
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

fn header(first: Lsn) -> [u8; HEADER_LEN] {
    let mut header = [0; HEADER_LEN];
    let fields: [&[u8]; 4] = [
        &MAGIC,
        &VERSION.to_le_bytes(),
        &FLAGS.to_le_bytes(),
        &first.get().to_le_bytes(),
    ];
    for (slot, byte) in header.iter_mut().zip(fields.into_iter().flatten()) {
        *slot = *byte;
    }
    header
}

/// The first record a segment header names, or `None` when `buf` does not
/// start with a header of this version.
pub(crate) fn parse_header(buf: &[u8]) -> Option<Lsn> {
    let known = bytes::array_at::<4>(buf, 0) == Some(MAGIC)
        && bytes::u16_at(buf, 4) == Some(VERSION)
        && bytes::u16_at(buf, 6) == Some(FLAGS);
    known.then(|| bytes::u64_at(buf, 8).map(Lsn::new)).flatten()
}

#[cfg(test)]
mod tests {
    use super::{FILES, HEADER_LEN, Segment, header, parse_header};
    use crate::lsn::Lsn;

    #[test]
    fn header_roundtrips() {
        let lsn = Lsn::new(0x0102_0304_0506_0708);
        assert_eq!(parse_header(&header(lsn)), Some(lsn), "roundtrip");
        assert_eq!(&header(lsn)[..4], b"KRWL", "magic");
    }

    #[test]
    fn foreign_headers_are_rejected() {
        let good = header(Lsn::new(1));
        for at in 0..8 {
            let mut bad = good;
            bad[at] ^= 1;
            assert_eq!(parse_header(&bad), None, "byte {at} changed");
        }
        assert_eq!(parse_header(&good[..HEADER_LEN - 1]), None, "short");
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
