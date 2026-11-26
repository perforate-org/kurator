//! Snapshots of the replicated state: the bytes that go to disk and over
//! the network.
//!
//! ```text
//! magic "KRRS" | version u16 | flags u16
//! last log id      option(log id)
//! membership id    option(log id)
//! membership       as in the Raft log
//! state            kurator_storage::KvState::encode
//! crc32c u32       over everything before it
//! ```
//!
//! A snapshot is written to a temporary file, synced, renamed into place
//! and the directory synced, so a crash leaves the old snapshot or the new
//! one.

use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use kurator_codec::{DecodeError, Reader};
use kurator_storage::KvState;
use kurator_wal::sync_dir;

use crate::codec::{membership, option, put_membership, put_option};
use crate::error::{Error, Result, SnapshotDamage};
use crate::types::{SnapshotMeta, StoredMembership};

const MAGIC: [u8; 4] = *b"KRRS";
const VERSION: u16 = 1;
const FLAGS: u16 = 0;
const FILE: &str = "snapshot";
const TEMP_PREFIX: &str = ".snapshot-";

/// An encoded snapshot, as it is stored and sent to other replicas.
///
/// Its contents are checked when it is installed, so bytes from anywhere
/// can be wrapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBytes(Vec<u8>);

impl SnapshotBytes {
    /// Wraps bytes received from another replica.
    #[must_use]
    pub const fn from_vec(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The encoded snapshot.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Encodes `state` with `meta`.
    pub(crate) fn encode(meta: &SnapshotMeta, state: &KvState) -> io::Result<Self> {
        let mut out = Vec::new();
        for part in [&MAGIC[..], &VERSION.to_le_bytes(), &FLAGS.to_le_bytes()] {
            out.extend_from_slice(part);
        }
        put_option(&mut out, meta.last_log_id.as_ref());
        put_option(&mut out, meta.last_membership.log_id().as_ref());
        put_membership(&mut out, meta.last_membership.membership());
        state.encode(&mut out)?;
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        Ok(Self(out))
    }

    /// The metadata and the state.
    pub(crate) fn decode(&self) -> Result<(SnapshotMeta, KvState), SnapshotDamage> {
        let (body, crc) = self
            .0
            .split_last_chunk::<4>()
            .ok_or(SnapshotDamage::Checksum)?;
        if crc32c::crc32c(body) != u32::from_le_bytes(*crc) {
            return Err(SnapshotDamage::Checksum);
        }
        decode(body).map_err(SnapshotDamage::Format)
    }
}

fn decode(body: &[u8]) -> Result<(SnapshotMeta, KvState), DecodeError> {
    let mut reader = Reader::new(body);
    if reader.exact::<4>()? != MAGIC {
        return Err(DecodeError::Magic);
    }
    let version = reader.u16()?;
    if version != VERSION || reader.u16()? != FLAGS {
        return Err(DecodeError::Version(version.to_le_bytes()[0]));
    }
    let last_log_id = option(&mut reader)?;
    let membership_log_id = option(&mut reader)?;
    let membership = membership(&mut reader)?;
    let state = KvState::decode(&mut reader)?;
    reader.finish()?;
    let meta = SnapshotMeta {
        last_log_id,
        last_membership: StoredMembership::new(membership_log_id, membership),
    };
    Ok((meta, state))
}

/// The directory that holds a replica's snapshot.
#[derive(Debug)]
pub(crate) struct SnapshotDir {
    dir: PathBuf,
}

impl SnapshotDir {
    /// Opens `dir`, creating it when needed and removing temporary files
    /// that a crash left behind.
    pub(crate) fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(TEMP_PREFIX))
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }

    /// The stored snapshot, if there is one.
    pub(crate) fn load(&self) -> Result<Option<SnapshotBytes>> {
        match fs::read(self.path()) {
            Ok(bytes) => Ok(Some(SnapshotBytes(bytes))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Replaces the stored snapshot with `snapshot`, durably.
    pub(crate) fn store(&self, snapshot: &SnapshotBytes) -> Result<()> {
        let mut file = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile_in(&self.dir)?;
        file.write_all(&snapshot.0)?;
        file.as_file().sync_all()?;
        drop(file.persist(self.path()).map_err(|error| error.error)?);
        sync_dir(&self.dir)?;
        Ok(())
    }

    /// The error for a damaged stored snapshot.
    pub(crate) fn damaged(&self, damage: SnapshotDamage) -> Error {
        Error::Snapshot {
            path: self.path(),
            damage,
        }
    }

    fn path(&self) -> PathBuf {
        self.dir.join(FILE)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use kurator_codec::DecodeError;
    use kurator_storage::{Command, KvState, WriteBatch};

    use super::{SnapshotBytes, SnapshotDir};
    use crate::error::SnapshotDamage;
    use crate::types::{LeaderId, LogId, Membership, NodeId, SnapshotMeta, StoredMembership};

    fn meta() -> SnapshotMeta {
        let voters: BTreeSet<_> = [1, 2, 3].map(NodeId::new).into_iter().collect();
        let log_id = |index| {
            LogId::new(
                LeaderId {
                    term: 2,
                    node_id: NodeId::new(1),
                },
                index,
            )
        };
        SnapshotMeta {
            last_log_id: Some(log_id(9)),
            last_membership: StoredMembership::new(
                Some(log_id(1)),
                Membership::new_with_defaults(vec![voters], []),
            ),
        }
    }

    fn state() -> KvState {
        let mut state = KvState::default();
        state.apply(Command::Kv(WriteBatch::new().put("a", "1").put("b", "")));
        state
    }

    #[test]
    fn snapshots_roundtrip() {
        let bytes = SnapshotBytes::encode(&meta(), &state()).unwrap();
        assert_eq!(bytes.decode(), Ok((meta(), state())), "roundtrip");
        let empty = SnapshotMeta::default();
        let empty_bytes = SnapshotBytes::encode(&empty, &KvState::default()).unwrap();
        assert_eq!(
            empty_bytes.decode(),
            Ok((empty, KvState::default())),
            "empty roundtrip"
        );
    }

    #[test]
    fn damage_is_detected() {
        let bytes = SnapshotBytes::encode(&meta(), &state()).unwrap();
        let mut flipped = bytes.as_slice().to_vec();
        flipped[10] ^= 1;
        assert_eq!(
            SnapshotBytes::from_vec(flipped).decode(),
            Err(SnapshotDamage::Checksum),
            "bit flip"
        );
        assert_eq!(
            SnapshotBytes::from_vec(vec![1, 2]).decode(),
            Err(SnapshotDamage::Checksum),
            "too short"
        );
        let mut foreign = b"KRSN".to_vec();
        let crc = crc32c::crc32c(&foreign);
        foreign.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(
            SnapshotBytes::from_vec(foreign).decode(),
            Err(SnapshotDamage::Format(DecodeError::Magic)),
            "other magic"
        );
    }

    #[test]
    fn the_stored_snapshot_is_replaced_and_temporaries_removed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".snapshot-crashed"), b"partial").unwrap();
        let snapshots = SnapshotDir::open(dir.path()).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0, "cleaned");
        assert_eq!(snapshots.load().unwrap(), None, "none yet");
        let first = SnapshotBytes::encode(&SnapshotMeta::default(), &KvState::default()).unwrap();
        let second = SnapshotBytes::encode(&meta(), &state()).unwrap();
        snapshots.store(&first).unwrap();
        snapshots.store(&second).unwrap();
        assert_eq!(snapshots.load().unwrap(), Some(second), "newest");
    }
}
