//! The replicated state machine: the key-value state in memory, made
//! durable by the Raft log and by snapshots.

use std::io;
use std::path::Path;
use std::sync::Arc;

use futures_util::{Stream, TryStreamExt as _};
use kurator_storage::KvState;
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use parking_lot::Mutex;

use crate::error::{Error, Result, SnapshotDamage};
use crate::snapshot::{SnapshotBytes, SnapshotDir};
use crate::types::{Config, Entry, LogId, Snapshot, SnapshotMeta, StoredMembership};

/// The state machine of one replica. Clones share the same state.
#[derive(Debug, Clone)]
pub struct StateMachine {
    machine: Arc<Mutex<Machine>>,
}

#[derive(Debug)]
struct Machine {
    state: KvState,
    applied: Option<LogId>,
    membership: StoredMembership,
    snapshots: SnapshotDir,
    /// The stored snapshot, if there is one.
    current: Option<(SnapshotMeta, SnapshotBytes)>,
}

impl StateMachine {
    /// Opens the state machine in `dir`, starting from its snapshot. The
    /// entries after the snapshot come back from the Raft log.
    ///
    /// # Errors
    ///
    /// When the snapshot is damaged or cannot be read.
    pub fn open(dir: &Path) -> Result<Self> {
        let snapshots = SnapshotDir::open(dir)?;
        let current = snapshots
            .load()?
            .map(|bytes| {
                bytes
                    .decode()
                    .map(|(meta, state)| (meta, state, bytes))
                    .map_err(|damage| snapshots.damaged(damage))
            })
            .transpose()?;
        let machine = match current {
            Some((meta, state, bytes)) => Machine {
                state,
                applied: meta.last_log_id,
                membership: meta.last_membership.clone(),
                snapshots,
                current: Some((meta, bytes)),
            },
            None => Machine {
                state: KvState::default(),
                applied: None,
                membership: StoredMembership::default(),
                snapshots,
                current: None,
            },
        };
        Ok(Self {
            machine: Arc::new(Mutex::new(machine)),
        })
    }

    /// Runs `read` against the state as this replica has applied it.
    pub fn read<R>(&self, read: impl FnOnce(&KvState) -> R) -> R {
        read(&self.machine.lock().state)
    }

    /// The last entry applied to the state.
    #[must_use]
    pub fn applied(&self) -> Option<LogId> {
        self.machine.lock().applied
    }
}

impl Machine {
    fn apply(&mut self, entry: Entry) {
        match entry.payload {
            EntryPayload::Blank => {}
            EntryPayload::Normal(proposal) => self.state.apply(proposal.into_command()),
            EntryPayload::Membership(membership) => {
                self.membership = StoredMembership::new(Some(entry.log_id), membership);
            }
        }
        self.applied = Some(entry.log_id);
    }

    fn snapshot(&mut self) -> Result<(SnapshotMeta, SnapshotBytes)> {
        let meta = SnapshotMeta {
            last_log_id: self.applied,
            last_membership: self.membership.clone(),
        };
        let bytes = SnapshotBytes::encode(&meta, &self.state)?;
        self.snapshots.store(&bytes)?;
        self.current = Some((meta.clone(), bytes.clone()));
        Ok((meta, bytes))
    }

    fn install(&mut self, meta: &SnapshotMeta, bytes: SnapshotBytes) -> Result<()> {
        let (stored, state) = bytes
            .decode()
            .map_err(|damage| self.snapshots.damaged(damage))?;
        if &stored != meta {
            return Err(self.snapshots.damaged(SnapshotDamage::MetaMismatch));
        }
        self.snapshots.store(&bytes)?;
        self.state = state;
        self.applied = stored.last_log_id;
        self.membership = stored.last_membership.clone();
        self.current = Some((stored, bytes));
        Ok(())
    }
}

impl<C: Config> RaftSnapshotBuilder<C> for StateMachine {
    type SnapshotData = SnapshotBytes;

    async fn build_snapshot(&mut self) -> io::Result<Snapshot> {
        let (meta, snapshot) = self.machine.lock().snapshot().map_err(Error::into_io)?;
        Ok(Snapshot { meta, snapshot })
    }
}

impl<C: Config> RaftStateMachine<C> for StateMachine {
    type SnapshotData = SnapshotBytes;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> io::Result<(Option<LogId>, StoredMembership)> {
        let machine = self.machine.lock();
        Ok((machine.applied, machine.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<C>>> + Unpin + OptionalSend,
    {
        while let Some((entry, responder)) = entries.try_next().await? {
            self.machine.lock().apply(entry);
            if let Some(responder) = responder {
                responder.send(());
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: SnapshotBytes,
    ) -> io::Result<()> {
        self.machine
            .lock()
            .install(meta, snapshot)
            .map_err(Error::into_io)
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
        Ok(self
            .machine
            .lock()
            .current
            .clone()
            .map(|(meta, snapshot)| Snapshot { meta, snapshot }))
    }
}
