//! The public database handle.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use kurator_wal::{Lsn, RecoveryReport};

use crate::batch::{Command, WriteBatch};
use crate::commit::GroupCommit;
use crate::core::Core;
use crate::error::Result;
use crate::lock::DirLock;
use crate::state::KvState;

/// How a database is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// Layout of the write-ahead log.
    pub wal: kurator_wal::Options,
    /// The most writes one group commit takes.
    pub max_batch: NonZeroUsize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            wal: kurator_wal::Options::default(),
            max_batch: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// An open single-node database.
///
/// `Db` is `Send + Sync`: share it between threads with `Arc` or scoped
/// threads, and their writes are committed together. Dropping it finishes
/// the queued writes and releases the directory.
#[derive(Debug)]
pub struct Db {
    // Field order is drop order: stop the commit thread before the lock.
    commits: GroupCommit,
    core: Arc<Core>,
    recovery: RecoveryReport,
    _lock: DirLock,
}

impl Db {
    /// Opens the database in `dir`, creating it when it does not exist, and
    /// recovers the state from the latest snapshot and the log.
    ///
    /// # Errors
    ///
    /// [`Error::Locked`](crate::Error::Locked) when the database is already
    /// open, and any error from reading the snapshot or the log.
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let lock = DirLock::acquire(dir)?;
        let (core, recovery) = Core::recover(dir, options.wal)?;
        let core = Arc::new(core);
        Ok(Self {
            commits: GroupCommit::spawn(Arc::clone(&core), options.max_batch)?,
            core,
            recovery,
            _lock: lock,
        })
    }

    /// Applies `batch` atomically. Returns once it is durable and visible.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyBatch`](crate::Error::EmptyBatch),
    /// [`Error::BatchTooLarge`](crate::Error::BatchTooLarge), and
    /// [`Error::Commit`](crate::Error::Commit) when the log fails.
    pub fn write(&self, batch: WriteBatch) -> Result<Lsn> {
        self.commits.commit(Command::Kv(batch).encode()?)
    }

    /// Sets `key` to `value`; see [`Db::write`].
    ///
    /// # Errors
    ///
    /// As [`Db::write`].
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<Lsn> {
        self.write(WriteBatch::new().put(key, value))
    }

    /// Removes `key`; see [`Db::write`].
    ///
    /// # Errors
    ///
    /// As [`Db::write`].
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<Lsn> {
        self.write(WriteBatch::new().delete(key))
    }

    /// A copy of the value of `key`.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.core.get(key)
    }

    /// Runs `read` against a consistent view of the state. Commits wait
    /// until it returns, so keep it short.
    pub fn read<R>(&self, read: impl FnOnce(&KvState) -> R) -> R {
        self.core.read(read)
    }

    /// Writes a snapshot and deletes the log segments it makes unneeded.
    /// Returns the log position the snapshot holds.
    ///
    /// # Errors
    ///
    /// When the snapshot cannot be written or segments cannot be deleted.
    pub fn checkpoint(&self) -> Result<Lsn> {
        self.core.checkpoint()
    }

    /// What recovery found when the database was opened.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }
}
