//! Snapshots of the replicated state: the bytes that go to disk and over
//! the network.
//!
//! ```text
//! kurator_codec::Format "KRRS", version 1, around:
//! last log id      option(log id)
//! membership id    option(log id)
//! membership       as in the Raft log
//! state            kurator_storage::KvState::encode
//! ```
//!
//! A snapshot is written to a temporary file, synced, renamed into place
//! and the directory synced, so a crash leaves the old snapshot or the new
//! one.

use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use kurator_codec::{DecodeError, Format};
use kurator_storage::KvState;
use kurator_wal::sync_dir;

use crate::codec::{membership, option, put_membership, put_option};
use crate::error::{Error, Result, SnapshotDamage};
use crate::types::{LeaderId, Membership, SnapshotMeta, StoredMembership};

const FORMAT: Format = Format::new(*b"KRRS", 1);
const FILE: &str = "snapshot";
const TEMP_PREFIX: &str = ".snapshot-";

/// A snapshot with its metadata, as Raft hands it around.
pub type Snapshot = openraft::Snapshot<LeaderId, Membership, SnapshotBytes>;

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
        let mut head = Vec::new();
        put_option(&mut head, meta.last_log_id.as_ref());
        put_option(&mut head, meta.last_membership.log_id().as_ref());
        put_membership(&mut head, meta.last_membership.membership());
        let framed = FORMAT.write(Vec::new(), |out| {
            out.write_all(&head)?;
            state.encode(out)
        })?;
        Ok(Self(framed))
    }

    /// The metadata and the state.
    pub(crate) fn decode(&self) -> Result<(SnapshotMeta, KvState), SnapshotDamage> {
        decode(&self.0).map_err(SnapshotDamage::Format)
    }
}

fn decode(framed: &[u8]) -> Result<(SnapshotMeta, KvState), DecodeError> {
    let mut reader = FORMAT.read(framed)?;
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

    use kurator_codec::{DecodeError, Format};
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
            Err(SnapshotDamage::Format(DecodeError::Checksum)),
            "bit flip"
        );
        assert_eq!(
            SnapshotBytes::from_vec(vec![1, 2]).decode(),
            Err(SnapshotDamage::Format(DecodeError::Checksum)),
            "too short"
        );
        let foreign = Format::new(*b"KRSN", 1)
            .write(Vec::new(), |_| Ok(()))
            .unwrap();
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
