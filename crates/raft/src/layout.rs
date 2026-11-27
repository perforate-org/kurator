//! Where a node keeps its shards, and the seeds of the shards its splits
//! make.
//!
//! ```text
//! <root>/shards/<id>/log/        a replica's Raft log
//! <root>/shards/<id>/snapshot/   its latest snapshot
//! <root>/seeds/<id>/snapshot     the first snapshot of a shard a split made,
//!                                until the store plants it
//! ```
//!
//! A split writes its seed, durably, before it finishes applying, so a
//! crash either replays the split or finds the seed. The store plants each
//! seed as the new shard's snapshot, unless the shard already holds one,
//! and only then removes it.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kurator_shard::ShardId;
use kurator_wal::sync_dir;
use parking_lot::Mutex;

use crate::error::Result;
use crate::snapshot::{SnapshotBytes, SnapshotDir};

const SHARDS: &str = "shards";
const SEEDS: &str = "seeds";
const LOG: &str = "log";
const SNAPSHOT: &str = "snapshot";

/// A node's directory, and the seeds written since the store last looked.
/// Clones share the same seeds.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    root: PathBuf,
    sown: Arc<Mutex<BTreeSet<ShardId>>>,
}

/// The directory of one shard's replica.
#[derive(Debug, Clone)]
pub(crate) struct Home {
    dir: PathBuf,
    layout: Layout,
}

impl Layout {
    /// The node directory `root`, creating what is missing. The seeds left
    /// from before are waiting to be planted.
    pub(crate) fn open(root: &Path) -> Result<Self> {
        let layout = Self {
            root: root.to_path_buf(),
            sown: Arc::default(),
        };
        for dir in [layout.root.join(SHARDS), layout.root.join(SEEDS)] {
            fs::create_dir_all(&dir)?;
        }
        sync_dir(&layout.root)?;
        let left = ids(&layout.root.join(SEEDS))?;
        layout.sown.lock().extend(left);
        Ok(layout)
    }

    /// The home of shard `id`.
    pub(crate) fn home(&self, id: ShardId) -> Home {
        Home {
            dir: self.root.join(SHARDS).join(id.get().to_string()),
            layout: self.clone(),
        }
    }

    /// The shards that have a home.
    pub(crate) fn shards(&self) -> Result<BTreeSet<ShardId>> {
        Ok(ids(&self.root.join(SHARDS))?)
    }

    /// Whether the node holds nothing yet: no shard has a home, and no
    /// seed waits to be planted.
    pub(crate) fn is_empty(&self) -> Result<bool> {
        Ok(self.shards()?.is_empty() && ids(&self.root.join(SEEDS))?.is_empty())
    }

    /// Writes `seed` as the first snapshot of shard `id`, durably, for the
    /// store to plant.
    pub(crate) fn sow(&self, id: ShardId, seed: &SnapshotBytes) -> Result<()> {
        let dir = self.seed_dir(id);
        SnapshotDir::open(&dir)?.store(seed)?;
        sync_dir(&self.root.join(SEEDS))?;
        let _new: bool = self.sown.lock().insert(id);
        Ok(())
    }

    /// The seeds written since the last call, by shard, each with the
    /// snapshot it holds. A seed discarded since it was written is left
    /// out.
    pub(crate) fn seeds(&self) -> Result<Vec<(ShardId, SnapshotBytes)>> {
        let sown = std::mem::take(&mut *self.sown.lock());
        let mut seeds = Vec::new();
        for id in sown {
            if let Some(seed) = self.seed(id)? {
                seeds.push((id, seed));
            }
        }
        Ok(seeds)
    }

    /// The seed of shard `id`, if there is one.
    fn seed(&self, id: ShardId) -> Result<Option<SnapshotBytes>> {
        let seed = self.seed_dir(id);
        if !seed.exists() {
            return Ok(None);
        }
        SnapshotDir::open(&seed)?.load()
    }

    /// Removes the seed of shard `id`.
    pub(crate) fn discard(&self, id: ShardId) -> Result<()> {
        match fs::remove_dir_all(self.seed_dir(id)) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error.into()),
            Ok(()) | Err(_) => {}
        }
        sync_dir(&self.root.join(SEEDS))?;
        Ok(())
    }

    fn seed_dir(&self, id: ShardId) -> PathBuf {
        self.root.join(SEEDS).join(id.get().to_string())
    }
}

impl Home {
    /// Where the replica's log is.
    pub(crate) fn log(&self) -> PathBuf {
        self.dir.join(LOG)
    }

    /// Where the replica's snapshot is.
    pub(crate) fn snapshot(&self) -> PathBuf {
        self.dir.join(SNAPSHOT)
    }

    /// Writes `seed` as the first snapshot of the new shard `id`, durably,
    /// for the store to plant.
    pub(crate) fn sow(&self, id: ShardId, seed: &SnapshotBytes) -> Result<()> {
        self.layout.sow(id, seed)
    }
}

/// The shard ids named by the entries of `dir`. Entries that do not name
/// one are left alone.
fn ids(dir: &Path) -> io::Result<BTreeSet<ShardId>> {
    let mut ids = BTreeSet::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(id) = name.to_str().and_then(|name| name.parse().ok()) {
            let _new: bool = ids.insert(ShardId::new(id));
        }
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use kurator_shard::{ShardId, ShardState};

    use super::Layout;
    use crate::snapshot::SnapshotBytes;
    use crate::types::SnapshotMeta;

    #[test]
    fn seeds_stay_until_they_are_discarded() {
        let root = tempfile::tempdir().unwrap();
        let layout = Layout::open(root.path()).unwrap();
        assert!(layout.is_empty().unwrap(), "new");
        let seed = SnapshotBytes::encode(&SnapshotMeta::default(), &ShardState::default()).unwrap();
        let home = layout.home(ShardId::FIRST);
        assert!(home.log().ends_with("shards/1/log"), "log");
        assert!(home.snapshot().ends_with("shards/1/snapshot"), "snapshot");
        home.sow(ShardId::new(2), &seed).unwrap();
        assert_eq!(
            layout.seeds().unwrap(),
            [(ShardId::new(2), seed.clone())],
            "sown"
        );
        assert!(layout.seeds().unwrap().is_empty(), "taken");
        std::fs::create_dir_all(home.log()).unwrap();
        std::fs::create_dir_all(root.path().join("shards/not-a-shard")).unwrap();
        assert_eq!(layout.shards().unwrap(), [ShardId::FIRST].into(), "homes");
        assert!(!layout.is_empty().unwrap(), "holds a shard");

        let reopened = Layout::open(root.path()).unwrap();
        assert_eq!(
            reopened.seeds().unwrap(),
            [(ShardId::new(2), seed.clone())],
            "left from before"
        );
        reopened.discard(ShardId::new(2)).unwrap();
        reopened.discard(ShardId::new(2)).unwrap();
        let again = Layout::open(root.path()).unwrap();
        assert!(again.seeds().unwrap().is_empty(), "gone");

        home.sow(ShardId::new(3), &seed).unwrap();
        layout.discard(ShardId::new(3)).unwrap();
        assert!(
            layout.seeds().unwrap().is_empty(),
            "a seed discarded since it was written is left out"
        );
    }
}
