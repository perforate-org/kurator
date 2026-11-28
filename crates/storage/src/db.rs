//! The public database handle, and the engine behind it: the directory
//! lock, the group commit thread, the in-memory core and its snapshots.

mod commit;
mod core;
mod lock;
mod snapshot;

use std::num::NonZeroUsize;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use oleaph_wal::{Lsn, RecoveryReport};

use self::commit::GroupCommit;
use self::core::Core;
use self::lock::DirLock;
use crate::batch::WriteBatch;
use crate::command::Command;
use crate::draft::{Draft, Finished, KeyValue};
use crate::error::Result;
use crate::state::KvState;
use crate::version::Version;

/// How a database is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// Layout of the write-ahead log.
    pub wal: oleaph_wal::Options,
    /// The most writes one group commit takes.
    pub max_batch: NonZeroUsize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            wal: oleaph_wal::Options::default(),
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

    /// Applies `batch` atomically, whatever the state holds. Returns the
    /// version it made, once it is durable and visible.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyBatch`](crate::Error::EmptyBatch), [`Error::BatchTooLarge`](crate::Error::BatchTooLarge), and
    /// [`Error::Commit`](crate::Error::Commit) when the log fails.
    pub fn write(&self, batch: WriteBatch) -> Result<Version> {
        self.commit(Command::Kv(batch))
    }

    fn commit(&self, command: Command) -> Result<Version> {
        Ok(self.commits.commit(command.encode()?)?.into_result()?)
    }

    /// Starts a transaction that reads the newest version.
    ///
    /// ```
    /// # fn main() -> Result<(), oleaph_storage::Error> {
    /// use oleaph_storage::{Db, Error, Options};
    ///
    /// let dir = tempfile::tempdir()?;
    /// let db = Db::open(dir.path(), Options::default())?;
    /// db.put("stock", "1")?;
    ///
    /// let mut first = db.begin();
    /// let mut second = db.begin();
    /// for txn in [&mut first, &mut second] {
    ///     assert_eq!(txn.get(b"stock")?.as_deref(), Some(b"1".as_slice()));
    ///     txn.put("stock", "0");
    /// }
    /// first.commit()?;
    /// assert!(matches!(second.commit(), Err(Error::Aborted(_))), "it read a stale stock");
    /// # Ok(())
    /// # }
    /// ```
    pub fn begin(&self) -> Txn<'_> {
        Txn {
            db: self,
            draft: Draft::new(self.core.read(KvState::version)),
        }
    }

    /// Sets `key` to `value`; see [`Db::write`].
    ///
    /// # Errors
    ///
    /// As [`Db::write`].
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<Version> {
        self.write(WriteBatch::new().put(key, value))
    }

    /// Removes `key`; see [`Db::write`].
    ///
    /// # Errors
    ///
    /// As [`Db::write`].
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<Version> {
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

    /// The log position of the last write applied to the state.
    #[must_use]
    pub fn applied(&self) -> Lsn {
        self.core.applied()
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

/// A transaction on a [`Db`]: it reads one version, holds its writes back,
/// and commits them only if nothing it read changed in the meantime.
///
/// Dropping it without committing writes nothing.
#[derive(Debug)]
#[must_use = "a transaction writes nothing until it commits"]
pub struct Txn<'db> {
    db: &'db Db,
    draft: Draft,
}

impl Txn<'_> {
    /// The version its reads see.
    #[must_use]
    pub const fn read_at(&self) -> Version {
        self.draft.read_at()
    }

    /// The value of `key`, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Unreadable`](crate::Error::Unreadable) when so many writes committed since the
    /// transaction began that its version is forgotten.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.core.read(|state| self.draft.get(state, key))?)
    }

    /// The entries between `start` and `end` in key order, with the
    /// transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// As [`Txn::get`].
    pub fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<KeyValue>> {
        Ok(self
            .db
            .core
            .read(|state| self.draft.range(state, start, end))?)
    }

    /// Sets `key` to `value` when the transaction commits.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.draft.put(key, value);
    }

    /// Removes `key` when the transaction commits.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.draft.delete(key);
    }

    /// Commits the writes, unless a write committed after the read version
    /// touched a key this transaction read or a range it scanned. Returns
    /// the version the writes made; a transaction that wrote nothing
    /// returns its read version and logs nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Aborted`](crate::Error::Aborted) when it conflicts or its read version is
    /// forgotten: then it wrote nothing, and running it again may succeed.
    /// Otherwise as [`Db::write`].
    pub fn commit(self) -> Result<Version> {
        match self.draft.finish() {
            Finished::Writes(transaction) => self.db.commit(Command::Txn(transaction)),
            // A database prepares no part of a transaction across shards,
            // so a read of one version is serializable at that version.
            Finished::Reads(read) => Ok(read.read_at()),
        }
    }
}
