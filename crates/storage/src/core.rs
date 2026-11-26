//! The state, the log and the snapshots, and the rules that tie them
//! together.

use std::path::Path;
use std::sync::Arc;

use kurator_wal::{Lsn, LsnRange, RecoveryReport, Start, Wal};
use parking_lot::{Mutex, RwLock};

use crate::command::{Command, Encoded};
use crate::error::{Error, Result};
use crate::outcome::Outcome;
use crate::snapshot::Snapshots;
use crate::state::{KvState, LoggedState};

/// Everything a database shares between the commit thread and callers.
///
/// Lock order: `checkpointing`, then `state`, then `wal`. The commit path
/// takes `wal` and `state` one after the other, never both.
#[derive(Debug)]
pub(crate) struct Core {
    state: RwLock<LoggedState>,
    wal: Mutex<Wal>,
    snapshots: Snapshots,
    checkpointing: Mutex<()>,
}

impl Core {
    /// Loads the latest snapshot from `dir` and replays the log after it.
    pub(crate) fn recover(dir: &Path, wal: kurator_wal::Options) -> Result<(Self, RecoveryReport)> {
        let snapshots = Snapshots::open(dir.join("snapshots"))?;
        let mut state = snapshots.load_latest()?.unwrap_or_default();
        let (wal, report) = Wal::open(
            dir.join("wal"),
            wal,
            Start::After(state.applied()),
            |lsn, payload| {
                let command =
                    Command::decode(payload).map_err(|source| Error::Record { lsn, source })?;
                let _replayed = state.apply(lsn, command);
                Ok::<(), Error>(())
            },
        )?;
        let core = Self {
            state: RwLock::new(state),
            wal: Mutex::new(wal),
            snapshots,
            checkpointing: Mutex::new(()),
        };
        Ok((core, report))
    }

    /// Makes `entries` durable as consecutive log records, then applies
    /// them to the state in the same order. Returns what applying each
    /// one did, in order.
    pub(crate) fn commit(
        &self,
        entries: Vec<Encoded>,
    ) -> Result<Vec<Outcome>, Arc<kurator_wal::Error>> {
        let range = self
            .wal
            .lock()
            .append_batch(entries.iter().map(Encoded::payload))?;
        let mut state = self.state.write();
        Ok(range
            .into_iter()
            .flat_map(LsnRange::iter)
            .zip(entries)
            .map(|(lsn, entry)| state.apply(lsn, entry.into_command()))
            .collect())
    }

    /// A copy of the current value of `key`.
    pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.state.read().state().get(key).map(<[u8]>::to_vec)
    }

    /// Runs `read` against the current state.
    pub(crate) fn read<R>(&self, read: impl FnOnce(&KvState) -> R) -> R {
        read(self.state.read().state())
    }

    /// The last record applied to the state.
    pub(crate) fn applied(&self) -> Lsn {
        self.state.read().applied()
    }

    /// Writes a snapshot of the current state and purges the log segments
    /// it covers. Commits wait while the snapshot is written.
    pub(crate) fn checkpoint(&self) -> Result<Lsn> {
        let _checkpointing = self.checkpointing.lock();
        let applied = self.snapshots.write(&self.state.read())?;
        let _purged: usize = self.wal.lock().purge_through(applied)?;
        Ok(applied)
    }
}
