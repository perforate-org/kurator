//! A replica: one Raft node with its log store and state machine.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use kurator_storage::{Command, KvState, WriteBatch};
use kurator_wal::SigningKey;
use openraft::error::{ClientWriteError, Fatal, InitializeError, LinearizableReadError, RaftError};
use openraft::network::NetSnapshot;
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::async_runtime::WatchReceiver as _;
use openraft::{Raft, RaftNetworkFactory, ReadPolicy, ServerState};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::log_store::LogStore;
use crate::proposal::Proposal;
use crate::snapshot::{Snapshot, SnapshotBytes};
use crate::state_machine::StateMachine;
use crate::types::{LogId, NodeId, Vote};

/// Who a replica is: its id in the cluster and the key it signs its log
/// with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    id: NodeId,
    key: SigningKey,
}

impl Member {
    /// The member `id`, signing with `key`.
    #[must_use]
    pub const fn new(id: NodeId, key: SigningKey) -> Self {
        Self { id, key }
    }

    /// Its id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }
}

/// One Raft node: a [`LogStore`] in `<dir>/log` and a [`StateMachine`]
/// whose snapshot is in `<dir>/snapshot`.
///
/// A write returns once a quorum holds it durably: every replica syncs its
/// log before it acknowledges entries, and the leader syncs its own before
/// it counts itself.
#[derive(Debug)]
pub struct Replica<C: Config> {
    id: NodeId,
    raft: Raft<C, StateMachine>,
    state: StateMachine,
}

/// Where a replica stands, from its own point of view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    role: Role,
    term: u64,
    leader: Option<NodeId>,
    last_log: Option<u64>,
    applied: Option<LogId>,
    snapshot: Option<LogId>,
    purged: Option<LogId>,
}

/// The role a replica plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// Receives entries but does not vote.
    Learner,
    /// Votes and follows a leader.
    Follower,
    /// Asks for votes.
    Candidate,
    /// Accepts writes and replicates them.
    Leader,
    /// Shut down.
    Stopped,
}

impl Status {
    /// The role.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// The current term.
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.term
    }

    /// The leader this replica knows of.
    #[must_use]
    pub const fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// The index of the last entry in the log.
    #[must_use]
    pub const fn last_log(&self) -> Option<u64> {
        self.last_log
    }

    /// The last entry applied to the state machine.
    #[must_use]
    pub const fn applied(&self) -> Option<LogId> {
        self.applied
    }

    /// The last entry the stored snapshot holds.
    #[must_use]
    pub const fn snapshot(&self) -> Option<LogId> {
        self.snapshot
    }

    /// The last entry purged from the log.
    #[must_use]
    pub const fn purged(&self) -> Option<LogId> {
        self.purged
    }
}

impl<C: Config> Replica<C> {
    /// Opens the replica `member` in `dir`, recovering its log and state,
    /// and starts it. Other replicas are reached through `network`.
    ///
    /// # Errors
    ///
    /// When the log or the snapshot is damaged, and when Raft cannot start.
    pub async fn open<N>(
        member: Member,
        dir: &Path,
        config: Arc<openraft::Config>,
        network: N,
    ) -> Result<Self>
    where
        N: RaftNetworkFactory<C>,
        N::Network: NetSnapshot<C, SnapshotData = SnapshotBytes>,
    {
        let log = LogStore::open(&dir.join("log"), member.key)?;
        let state = StateMachine::open(&dir.join("snapshot"))?;
        let raft = Raft::new(member.id, config, network, log, state.clone())
            .await
            .map_err(stopped)?;
        Ok(Self {
            id: member.id,
            raft,
            state,
        })
    }

    /// Makes `voters` the first membership of a new cluster. Call it on one
    /// replica, once.
    ///
    /// # Errors
    ///
    /// [`Error::Initialized`] when this replica already has a log.
    pub async fn initialize(&self, voters: impl IntoIterator<Item = NodeId>) -> Result<()> {
        let members: BTreeSet<NodeId> = voters.into_iter().collect();
        self.raft
            .initialize(members)
            .await
            .map_err(|error| match error {
                RaftError::APIError(InitializeError::NotAllowed(_)) => Error::Initialized,
                RaftError::APIError(other) => Error::Raft(Box::new(other)),
                RaftError::Fatal(fatal) => stopped(fatal),
            })
    }

    /// Replicates `batch` and applies it. Returns once a quorum holds it
    /// durably and this replica applied it.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] when this replica cannot take writes, and
    /// [`Error::Discarded`] when it lost leadership after logging the
    /// write: then nobody knows whether the write commits.
    pub async fn write(&self, batch: WriteBatch) -> Result<LogId> {
        let proposal = Proposal::new(Command::Kv(batch))?;
        match self.raft.client_write(proposal).await {
            Ok(response) => Ok(response.log_id),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                Err(Error::NotLeader {
                    leader: forward.leader_id,
                })
            }
            Err(RaftError::APIError(ClientWriteError::LogEntryDiscarded(forward))) => {
                Err(Error::Discarded {
                    leader: forward.leader_id,
                })
            }
            Err(RaftError::APIError(other)) => Err(Error::Raft(Box::new(other))),
            Err(RaftError::Fatal(fatal)) => Err(stopped(fatal)),
        }
    }

    /// Runs `read` against a state that reflects every write that returned
    /// before this call: the leader confirms with a quorum that it still
    /// leads, and waits until it applied everything committed by then.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] when this replica is not the leader.
    pub async fn read<R>(&self, read: impl FnOnce(&KvState) -> R) -> Result<R> {
        let _read_from = self
            .raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|error| match error {
                RaftError::APIError(LinearizableReadError::ForwardToLeader(forward)) => {
                    Error::NotLeader {
                        leader: forward.leader_id,
                    }
                }
                RaftError::APIError(other) => Error::Raft(Box::new(other)),
                RaftError::Fatal(fatal) => stopped(fatal),
            })?;
        Ok(self.state.read(read))
    }

    /// Runs `read` against the state as this replica applied it, which may
    /// lag the cluster.
    pub fn read_local<R>(&self, read: impl FnOnce(&KvState) -> R) -> R {
        self.state.read(read)
    }

    /// Where this replica stands.
    #[must_use]
    pub fn status(&self) -> Status {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let role = match metrics.state {
            ServerState::Learner => Role::Learner,
            ServerState::Follower => Role::Follower,
            ServerState::Candidate => Role::Candidate,
            ServerState::Leader => Role::Leader,
            ServerState::Shutdown => Role::Stopped,
        };
        Status {
            role,
            term: metrics.current_term,
            leader: metrics.current_leader,
            last_log: metrics.last_log_index,
            applied: self.state.applied(),
            snapshot: metrics.snapshot,
            purged: metrics.purged,
        }
    }

    /// The id of this replica.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Where messages from other replicas are delivered.
    #[must_use]
    pub fn inbox(&self) -> Inbox<C> {
        Inbox {
            raft: self.raft.clone(),
        }
    }

    /// Stops the replica. Everything it acknowledged is already durable, so
    /// stopping and crashing leave the same files behind.
    ///
    /// # Errors
    ///
    /// When the Raft core panicked.
    pub async fn shutdown(self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| Error::Stopped(error.to_string().into()))
    }
}

/// Delivers messages from other replicas to one replica. Clones deliver to
/// the same replica.
#[derive(Debug)]
pub struct Inbox<C: Config> {
    raft: Raft<C, StateMachine>,
}

impl<C: Config> Clone for Inbox<C> {
    fn clone(&self) -> Self {
        Self {
            raft: self.raft.clone(),
        }
    }
}

impl<C: Config> Inbox<C> {
    /// Handles an append-entries request from the leader.
    ///
    /// # Errors
    ///
    /// When the replica has stopped.
    pub async fn append_entries(
        &self,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendResult<C>, RaftError<C>> {
        self.raft.append_entries(request).await
    }

    /// Handles a vote request from a candidate.
    ///
    /// # Errors
    ///
    /// When the replica has stopped.
    pub async fn vote(&self, request: VoteRequest<C>) -> Result<VoteResponse<C>, RaftError<C>> {
        self.raft.vote(request).await
    }

    /// Handles a pre-vote request: would this replica grant the vote?
    ///
    /// # Errors
    ///
    /// When the replica has stopped.
    pub async fn pre_vote(&self, request: VoteRequest<C>) -> Result<VoteResponse<C>, RaftError<C>> {
        self.raft.pre_vote(request).await
    }

    /// Installs a snapshot sent by the leader.
    ///
    /// # Errors
    ///
    /// When the replica has stopped.
    pub async fn install_snapshot(
        &self,
        vote: Vote,
        snapshot: Snapshot,
    ) -> Result<SnapshotResponse<C>, Fatal<C>> {
        self.raft.install_full_snapshot(vote, snapshot).await
    }
}

fn stopped<C: Config>(fatal: Fatal<C>) -> Error {
    Error::Stopped(Box::new(fatal))
}
