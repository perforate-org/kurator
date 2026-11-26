//! Snapshots: the whole state at one log position, in one file.
//!
//! File `<applied lsn as 20 digits>.snap`:
//!
//! ```text
//! magic "KRSN" | version u16 | flags u16 | applied lsn u64 | entries u64
//! (key, value)*   each length prefixed, in key order
//! crc32c u32      over everything before it
//! ```
//!
//! A snapshot is written to a temporary file, synced, renamed into place,
//! and the directory synced, so a crash leaves either the old snapshot or
//! the new one. Leftover temporary files are removed on open.

use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use kurator_wal::{Lsn, LsnFiles, sync_dir};

use crate::codec::{DecodeError, Reader, put_bytes};
use crate::error::{Error, Result, SnapshotDamage};
use crate::state::KvState;

const MAGIC: [u8; 4] = *b"KRSN";
const VERSION: u16 = 1;
const FLAGS: u16 = 0;
const FILES: LsnFiles = LsnFiles::new(".snap");
const TEMP_PREFIX: &str = ".snapshot-";

/// The snapshot directory.
#[derive(Debug)]
pub(crate) struct Snapshots {
    dir: PathBuf,
}

impl Snapshots {
    /// Opens `dir`, creating it when needed and removing temporary files
    /// that a crash left behind.
    pub(crate) fn open(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(TEMP_PREFIX))
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(Self { dir })
    }

    /// Loads the newest snapshot, if there is one.
    pub(crate) fn load_latest(&self) -> Result<Option<KvState>> {
        let Some((named, path)) = self.list()?.pop() else {
            return Ok(None);
        };
        let damaged = |damage| Error::Snapshot {
            path: path.clone(),
            damage,
        };
        let data = fs::read(&path)?;
        let Some((body, crc)) = data.split_last_chunk::<4>() else {
            return Err(damaged(SnapshotDamage::Checksum));
        };
        if crc32c::crc32c(body) != u32::from_le_bytes(*crc) {
            return Err(damaged(SnapshotDamage::Checksum));
        }
        let state = decode(body).map_err(|error| damaged(SnapshotDamage::Format(error)))?;
        if state.applied() != named {
            return Err(damaged(SnapshotDamage::LsnMismatch {
                named,
                stored: state.applied(),
            }));
        }
        Ok(Some(state))
    }

    /// Writes `state` as the newest snapshot and deletes the older ones.
    pub(crate) fn write(&self, state: &KvState) -> Result<Lsn> {
        let applied = state.applied();
        let mut file = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile_in(&self.dir)?;
        {
            let mut out = Checksummed::new(BufWriter::new(file.as_file_mut()));
            encode(state, &mut out)?;
            let (mut out, crc) = out.finish();
            out.write_all(&crc.to_le_bytes())?;
            out.flush()?;
        }
        file.as_file().sync_all()?;
        let path = self.path(applied);
        drop(file.persist(&path).map_err(|error| error.error)?);
        sync_dir(&self.dir)?;
        for (lsn, older) in self.list()? {
            if lsn < applied {
                fs::remove_file(older)?;
            }
        }
        Ok(applied)
    }

    fn path(&self, lsn: Lsn) -> PathBuf {
        self.dir.join(FILES.name(lsn))
    }

    /// Snapshots, oldest first.
    fn list(&self) -> io::Result<Vec<(Lsn, PathBuf)>> {
        let mut found = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if let Some(lsn) = entry
                .file_name()
                .to_str()
                .and_then(|name| FILES.parse(name))
            {
                found.push((lsn, entry.path()));
            }
        }
        found.sort_unstable();
        Ok(found)
    }
}

fn encode(state: &KvState, out: &mut impl Write) -> io::Result<()> {
    let count = u64::try_from(state.len()).map_err(io::Error::other)?;
    for part in [&MAGIC[..], &VERSION.to_le_bytes(), &FLAGS.to_le_bytes()] {
        out.write_all(part)?;
    }
    out.write_all(&state.applied().get().to_le_bytes())?;
    out.write_all(&count.to_le_bytes())?;
    for (key, value) in state.iter() {
        put_bytes(out, key)?;
        put_bytes(out, value)?;
    }
    Ok(())
}

fn decode(body: &[u8]) -> Result<KvState, DecodeError> {
    let mut reader = Reader::new(body);
    if reader.exact::<4>()? != MAGIC {
        return Err(DecodeError::Magic);
    }
    let version = reader.u16()?;
    if version != VERSION || reader.u16()? != FLAGS {
        return Err(DecodeError::Version(version.to_le_bytes()[0]));
    }
    let applied = Lsn::new(reader.u64()?);
    let count = reader.u64()?;
    let mut entries = Vec::new();
    for _ in 0..count {
        entries.push((reader.bytes()?, reader.bytes()?));
    }
    reader.finish()?;
    Ok(KvState::restore(applied, entries))
}

/// A writer that keeps a CRC-32C of everything written through it.
struct Checksummed<W> {
    inner: W,
    crc: u32,
}

impl<W: Write> Checksummed<W> {
    const fn new(inner: W) -> Self {
        Self { inner, crc: 0 }
    }

    fn finish(self) -> (W, u32) {
        (self.inner, self.crc)
    }
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
    use std::fs;

    use super::Snapshots;
    use crate::batch::{Command, WriteBatch};
    use crate::error::{Error, SnapshotDamage};
    use crate::state::KvState;
    use kurator_wal::Lsn;

    fn state(lsn: u64) -> KvState {
        let mut state = KvState::default();
        state.apply(
            Lsn::new(lsn),
            Command::Kv(WriteBatch::new().put("k", "v").put("", "")),
        );
        state
    }

    #[test]
    fn writes_load_back_and_replace_older_ones() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(snapshots.load_latest().unwrap(), None, "none yet");
        assert_eq!(snapshots.write(&state(3)).unwrap(), Lsn::new(3), "written");
        assert_eq!(snapshots.write(&state(5)).unwrap(), Lsn::new(5), "written");
        assert_eq!(snapshots.load_latest().unwrap(), Some(state(5)), "newest");
        assert_eq!(
            fs::read_dir(dir.path()).unwrap().count(),
            1,
            "older removed"
        );
    }

    #[test]
    fn leftover_temporary_files_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(".snapshot-crashed"), b"partial").unwrap();
        drop(Snapshots::open(dir.path().to_path_buf()).unwrap());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0, "cleaned");
    }

    #[test]
    fn damage_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(dir.path().to_path_buf()).unwrap();
        let _: Lsn = snapshots.write(&state(1)).unwrap();
        let path = snapshots.path(Lsn::new(1));
        let mut bytes = fs::read(&path).unwrap();
        bytes[20] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(
            matches!(
                snapshots.load_latest(),
                Err(Error::Snapshot {
                    damage: SnapshotDamage::Checksum,
                    ..
                })
            ),
            "bit flip"
        );
        fs::write(&path, &bytes[..2]).unwrap();
        assert!(
            matches!(
                snapshots.load_latest(),
                Err(Error::Snapshot {
                    damage: SnapshotDamage::Checksum,
                    ..
                })
            ),
            "too short"
        );
    }

    #[test]
    fn a_renamed_snapshot_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots = Snapshots::open(dir.path().to_path_buf()).unwrap();
        let _: Lsn = snapshots.write(&state(1)).unwrap();
        fs::rename(snapshots.path(Lsn::new(1)), snapshots.path(Lsn::new(2))).unwrap();
        assert!(
            matches!(
                snapshots.load_latest(),
                Err(Error::Snapshot {
                    damage: SnapshotDamage::LsnMismatch { .. },
                    ..
                })
            ),
            "name and contents disagree"
        );
    }
}
