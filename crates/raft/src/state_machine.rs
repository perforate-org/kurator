//! The replicated state machine: one shard's state in memory, made
//! durable by the Raft log and by snapshots.

use std::io;
use std::sync::Arc;

use futures_util::{Stream, TryStreamExt as _};
use kurator_shard::{Applied, Shard, ShardId, ShardState, Split};
use kurator_storage::{KvState, Outcome, Version};
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use parking_lot::Mutex;

use crate::config::{Config, Entry};
use crate::error::{Error, Result, SnapshotDamage};
use crate::layout::Home;
use crate::proposal::Body;
use crate::snapshot::{Snapshot, SnapshotBytes, SnapshotDir};
use crate::types::{LeaderId, LogId, NodeId, SnapshotMeta, StoredMembership};

/// The state machine of one replica. Clones share the same state.
#[derive(Debug, Clone)]
pub struct StateMachine {
    machine: Arc<Mutex<Machine>>,
}

#[derive(Debug)]
struct Machine {
    state: ShardState,
    applied: Option<LogId>,
    membership: StoredMembership,
    snapshots: SnapshotDir,
    /// The stored snapshot, if there is one.
    current: Option<(SnapshotMeta, SnapshotBytes)>,
    /// Where the replica lives, and where its splits sow.
    home: Home,
}

impl StateMachine {
    /// Opens the state machine of the replica of `shard` at `home`,
    /// starting from its snapshot, or from the whole, empty shard when it
    /// has none. The entries after the snapshot come back from the Raft
    /// log.
    ///
    /// # Errors
    ///
    /// When the snapshot is damaged or cannot be read.
    pub(crate) fn open(home: Home, shard: ShardId) -> Result<Self> {
        let snapshots = SnapshotDir::open(&home.snapshot())?;
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
                home,
            },
            None => Machine {
                state: ShardState::new(Shard::whole(shard), KvState::default()),
                applied: None,
                membership: StoredMembership::default(),
                snapshots,
                current: None,
                home,
            },
        };
        Ok(Self {
            machine: Arc::new(Mutex::new(machine)),
        })
    }

    /// Runs `read` against the state as this replica has applied it.
    pub fn read<R>(&self, read: impl FnOnce(&ShardState) -> R) -> R {
        read(&self.machine.lock().state)
    }

    /// The last entry applied to the state.
    #[must_use]
    pub fn applied(&self) -> Option<LogId> {
        self.machine.lock().applied
    }

    /// Whether the replica holds a state at all: one it was seeded with, a
    /// snapshot it received, or entries it applied. A replica that a
    /// message created holds none until the leader sends it a snapshot.
    pub(crate) fn is_initialized(&self) -> bool {
        let machine = self.machine.lock();
        machine.applied.is_some() || machine.current.is_some()
    }
}

impl Machine {
    /// Applies `entry`. A command applies at the version of its index; an
    /// entry without one leaves the state as it is.
    ///
    /// A split writes the seed of its new shard before it returns, so the
    /// seed is on disk before this entry counts as applied.
    fn apply(&mut self, entry: Entry) -> Result<Applied> {
        let log_id = entry.log_id;
        let unchanged = Applied::Data(Outcome::Committed(self.state.kv().version()));
        let applied = match entry.payload {
            EntryPayload::Normal(proposal) => match proposal.into_body() {
                Body::Command(encoded) => self
                    .state
                    .apply(Version::new(log_id.index), encoded.into_command()),
                Body::Split(split, _) => self.split(log_id, &split)?,
            },
            EntryPayload::Blank => unchanged,
            EntryPayload::Membership(membership) => {
                self.membership = StoredMembership::new(Some(log_id), membership);
                unchanged
            }
        };
        self.applied = Some(log_id);
        Ok(applied)
    }

    /// Splits the shard at the entry `log_id` and sows the right half: a
    /// snapshot of it as of that entry, with this shard's membership. The
    /// new shard's log starts after the entry, so its versions go on from
    /// this shard's.
    fn split(&mut self, log_id: LogId, split: &Split) -> Result<Applied> {
        let (halves, right) = match self.state.split(split) {
            Ok(split) => split,
            Err(unsplit) => return Ok(unsplit.into()),
        };
        let seeded = LogId::new(SEED_LEADER, log_id.index);
        let meta = SnapshotMeta {
            last_log_id: Some(seeded),
            last_membership: StoredMembership::new(
                Some(seeded),
                self.membership.membership().clone(),
            ),
        };
        let seed = SnapshotBytes::encode(&meta, &right)?;
        self.home.sow(halves.right().id(), &seed)?;
        Ok(Applied::Split(halves))
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

/// The leader a seed's entry is written by: nobody, before the first term.
/// Any leader of the new shard is in a later term, so its entries come
/// after the seed's.
const SEED_LEADER: LeaderId = LeaderId {
    term: 0,
    node_id: NodeId::new(0),
};

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
            let applied = self.machine.lock().apply(entry).map_err(Error::into_io)?;
            if let Some(responder) = responder {
                responder.send(applied);
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
