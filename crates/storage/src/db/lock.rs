//! Exclusive ownership of a database directory.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use crate::error::{Error, Result};

/// Holds an exclusive lock on `<dir>/LOCK`; dropping it releases the lock.
#[derive(Debug)]
pub(crate) struct DirLock {
    _file: File,
}

impl DirLock {
    /// Locks `dir`, failing with [`Error::Locked`] when another process or
    /// another [`Db`](crate::Db) in this one holds it.
    pub(crate) fn acquire(dir: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("LOCK"))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(Error::Locked {
                path: dir.to_path_buf(),
            }),
            Err(TryLockError::Error(error)) => Err(error.into()),
        }
    }
}
