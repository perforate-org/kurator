//! The states a replica held at its checkpoints, one file each, so that it
//! can forget the blocks that made them.
//!
//! File `<height as 20 digits>.state` holds the state as
//! [`ShardState::encode`] writes it, and nothing else: the root of a
//! certified checkpoint names its bytes, so loading checks them against
//! the certificate. A state is written to a temporary file, synced,
//! renamed into place and the directory synced, and leftover temporary
//! files are removed on open.

use std::fs;
use std::io::{self, Write as _};
use std::path::PathBuf;

use oleaph_codec::Reader;
use oleaph_shard::ShardState;
use oleaph_wal::sync_dir;

use super::manifest::Manifest;
use crate::checkpoint::{Checkpoint, CheckpointCertificate, Height};
use crate::vote::BlockId;

const SUFFIX: &str = ".state";
const TEMP_PREFIX: &str = ".state-";

/// A replica's state at one of its checkpoints, encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Snapshot {
    checkpoint: Checkpoint,
    encoded: Vec<u8>,
}

impl Snapshot {
    /// The state `state`, which block `block` left at `height`.
    ///
    /// # Errors
    ///
    /// When the state does not encode, as [`Manifest::of`].
    pub(crate) fn take(height: Height, block: BlockId, state: &ShardState) -> io::Result<Self> {
        let mut encoded = Vec::new();
        state.encode(&mut encoded)?;
        let root = Manifest::over(&encoded).root();
        Ok(Self {
            checkpoint: Checkpoint::new(height, block, root),
            encoded,
        })
    }

    /// The checkpoint that names it.
    #[cfg(test)]
    pub(crate) const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}

/// The directory of a replica's states.
#[derive(Debug)]
pub(crate) struct States {
    dir: PathBuf,
}

impl States {
    /// Opens `dir`, creating it when needed and removing temporary files
    /// that a crash left behind.
    pub(crate) fn open(dir: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let temporary = entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(TEMP_PREFIX));
            if temporary {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(Self { dir })
    }

    /// Writes `snapshot` durably, and returns the checkpoint that names
    /// it.
    pub(crate) fn keep(&self, snapshot: &Snapshot) -> io::Result<Checkpoint> {
        let mut file = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile_in(&self.dir)?;
        file.as_file_mut().write_all(&snapshot.encoded)?;
        file.as_file().sync_all()?;
        let path = self.path(snapshot.checkpoint.height());
        drop(file.persist(&path).map_err(|error| error.error)?);
        sync_dir(&self.dir)?;
        Ok(snapshot.checkpoint)
    }

    /// The state `certificate` certifies.
    ///
    /// # Errors
    ///
    /// When it was not kept, or the file holds another state.
    pub(crate) fn load(&self, certificate: &CheckpointCertificate) -> io::Result<ShardState> {
        let path = self.path(certificate.height());
        let encoded = fs::read(&path)?;
        let damaged = |what: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: {what}", path.display()),
            )
        };
        if Manifest::over(&encoded).root() != certificate.root() {
            return Err(damaged("not the state the committee certified"));
        }
        let mut reader = Reader::new(&encoded);
        let state = ShardState::decode(&mut reader).map_err(|_| damaged("does not decode"))?;
        reader.finish().map_err(|_| damaged("does not decode"))?;
        Ok(state)
    }

    /// Deletes the states below `height`.
    pub(crate) fn forget_below(&self, height: Height) -> io::Result<()> {
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let below = entry
                .file_name()
                .to_str()
                .and_then(parse)
                .is_some_and(|kept| kept < height);
            if below {
                fs::remove_file(entry.path())?;
            }
        }
        sync_dir(&self.dir)
    }

    fn path(&self, height: Height) -> PathBuf {
        self.dir.join(format!("{:020}{SUFFIX}", height.get()))
    }
}

/// The height a state file is named after.
fn parse(name: &str) -> Option<Height> {
    let digits = name.strip_suffix(SUFFIX)?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().map(Height::new)
}

#[cfg(test)]
mod tests {
    use super::{Height, parse};

    #[test]
    fn only_state_files_name_a_height() {
        assert_eq!(
            parse("00000000000000000042.state"),
            Some(Height::new(42)),
            "a state"
        );
        let others = [
            "42.state",
            "000000000000000000042.state",
            "0000000000000000004x.state",
            "00000000000000000042.snap",
            ".state-tmp",
        ];
        for name in others {
            assert_eq!(parse(name), None, "{name}");
        }
    }
}
