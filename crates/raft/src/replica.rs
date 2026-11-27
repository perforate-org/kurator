//! A replica: one Raft node with its log store and state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::ops::Bound;
use std::sync::Arc;

use kurator_shard::{Applied, Halves, Shard, ShardId, ShardState, Split};
use kurator_storage::{
    Abort, Command, Decide, Decided, Draft, KeyValue, KvState, Outcome, Prepare, Version,
    WriteBatch,
};
use openraft::error::{ClientWriteError, Fatal, InitializeError, LinearizableReadError, RaftError};
use openraft::network::NetSnapshot;
use openraft::raft::ClientWriteResponse;
use openraft::type_config::async_runtime::WatchReceiver as _;
use openraft::{Raft, RaftNetworkFactory, ReadPolicy, ServerState};

use crate::config::{Config, MembershipEdit};
use crate::error::{Error, Result};
use crate::identity::{KeyPurpose, NodeSecret};
use crate::layout::Home;
use crate::log_store::LogStore;
use crate::proposal::Proposal;
use crate::rpc::{Refusal, Request, Response};
use crate::snapshot::SnapshotBytes;
use crate::state_machine::StateMachine;
use crate::types::{LogId, Membership, NodeId, Peer, Progress};

/// Who a replica is: its id in the cluster and the secret its keys come
/// from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    id: NodeId,
    secret: NodeSecret,
}

impl Member {
    /// The member `id`, holding `secret`.
    #[must_use]
    pub const fn new(id: NodeId, secret: NodeSecret) -> Self {
        Self { id, secret }
    }

    /// Its id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Its secret.
    #[must_use]
    pub const fn secret(&self) -> &NodeSecret {
        &self.secret
    }

    /// How others reach it when it listens on `addr`.
    #[must_use]
    pub fn peer(&self, addr: SocketAddr) -> Peer {
        Peer::new(addr, self.secret.public_key(KeyPurpose::Transport))
    }
}

/// One node's replica of one shard: a [`LogStore`] and a
/// [`StateMachine`], in the shard's directory of a
/// [`Store`](crate::Store).
///
/// A write returns once a quorum holds it durably: every replica syncs its
/// log before it acknowledges entries, and the leader syncs its own before
/// it counts itself.
#[derive(Debug)]
pub struct Replica<C: Config> {
    id: NodeId,
    shard: ShardId,
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
    /// Opens `member`'s replica of `shard` at `home`, recovering its log
    /// and state, and starts it. Other replicas are reached through
    /// `network`.
    pub(crate) async fn open<N>(
        member: &Member,
        shard: ShardId,
        home: Home,
        config: Arc<openraft::Config>,
        network: N,
    ) -> Result<Self>
    where
        N: RaftNetworkFactory<C>,
        N::Network: NetSnapshot<C, SnapshotData = SnapshotBytes>,
    {
        let log = LogStore::open(
            &home.log(),
            member.secret.signing_key(KeyPurpose::WalSigning),
        )?;
        let state = StateMachine::open(home, shard)?;
        let raft = Raft::new(member.id, config, network, log, state.clone())
            .await
            .map_err(stopped)?;
        Ok(Self {
            id: member.id,
            shard,
            raft,
            state,
        })
    }

    /// Makes `voters` the first membership of a new cluster. Call it on one
    /// replica, once.
    pub(crate) async fn initialize(
        &self,
        voters: impl IntoIterator<Item = (NodeId, Peer)>,
    ) -> Result<()> {
        let members: BTreeMap<NodeId, Peer> = voters.into_iter().collect();
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
    ///
    /// [`Error::Misrouted`] when a key is not in the shard: it split
    /// meanwhile. Nothing is written.
    pub async fn write(&self, batch: WriteBatch) -> Result<LogId> {
        let proposal = Proposal::new(Command::Kv(batch))?;
        let response = written(self.raft.client_write(proposal).await)?;
        let _version: Version = version(response.data)?;
        Ok(response.log_id)
    }

    /// Splits the shard at `at`: once the split applies, the keys from `at`
    /// on belong to the new shard `right`, replicated by the same members.
    /// Returns the two halves.
    ///
    /// The split is for the shard as this replica last applied it; if the
    /// shard split since, or `at` is not strictly inside its span, nothing
    /// changes.
    ///
    /// # Errors
    ///
    /// [`Error::Misrouted`] when the split does not apply, and otherwise
    /// as [`Replica::write`].
    pub(crate) async fn split(&self, at: &[u8], right: ShardId) -> Result<Halves> {
        let epoch = self.shard().epoch();
        let proposal = Proposal::split(Split::new(at, right, epoch))?;
        let response = written(self.raft.client_write(proposal).await)?;
        match response.data {
            Applied::Split(halves) => Ok(halves),
            Applied::Misrouted(shard) => Err(misrouted(shard)),
            Applied::Locked => Err(Error::Locked),
            Applied::Data(_) | _ => Err(Error::Raft("a split applied as a command".into())),
        }
    }

    /// The shard as this replica last applied it.
    #[must_use]
    pub fn shard(&self) -> Shard {
        self.state.read(|state| state.shard().clone())
    }

    /// The id of the shard it replicates.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.shard
    }

    /// Whether the replica holds a state yet: one it was seeded with, a
    /// snapshot it received, or entries it applied. A replica that a
    /// message opened holds none until the leader sends it a snapshot, and
    /// until then [`Replica::shard`] does not describe its shard.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.state.is_initialized()
    }

    /// Starts a transaction that reads the newest version this cluster
    /// committed: the leader confirms it still leads, as for
    /// [`Replica::read`], and the transaction reads that version from this
    /// replica's state.
    ///
    /// ```text
    /// let mut txn = replica.begin().await?;
    /// let balance = txn.get(b"alice")?;
    /// txn.put("alice", debit(balance));
    /// txn.commit().await?;   // Err(Aborted) when alice changed meanwhile
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] when this replica is not the leader.
    pub async fn begin(&self) -> Result<Txn<'_, C>> {
        let read_at = self.read(KvState::version).await?;
        Ok(Txn {
            replica: self,
            shard: self.shard(),
            draft: Draft::new(read_at),
        })
    }

    /// Prepares this shard's part of a transaction across shards: once it
    /// applies, the shard holds the part's writes back and locks what it
    /// read and writes, until [`Replica::decide`] tells it how the
    /// transaction came out. Returns the version it applied at.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] with [`kurator_storage::Error::Aborted`] when the
    /// shard refuses: a read changed since its read version, another
    /// prepared transaction holds a key it touches, or the transaction was
    /// decided already. Otherwise as [`Replica::write`].
    pub async fn prepare(&self, prepare: Prepare) -> Result<Version> {
        let proposal = Proposal::new(Command::Prepare(prepare))?;
        let response = written(self.raft.client_write(proposal).await)?;
        version(response.data)
    }

    /// Tells this shard how a transaction across shards came out, and
    /// returns how the shard decided it: as told when it prepared its
    /// part, as it was decided before when it was, and aborted otherwise.
    /// A commit told to the primary is the transaction's commit point.
    ///
    /// # Errors
    ///
    /// As [`Replica::write`]; [`Error::Discarded`] leaves the decision
    /// unknown, and telling it again is safe.
    pub async fn decide(&self, decide: Decide) -> Result<Decided> {
        let proposal = Proposal::new(Command::Decide(decide))?;
        let response = written(self.raft.client_write(proposal).await)?;
        let rolled_back = matches!(
            response.data,
            Applied::Data(Outcome::Aborted(Abort::RolledBack(_)))
        );
        if rolled_back {
            return Ok(Decided::Aborted);
        }
        version(response.data).map(Decided::Committed)
    }

    /// Adds `id`, reached as `peer`, as a learner: the leader starts sending
    /// it the log, or its snapshot when the log is purged, but it does not
    /// vote. Returns once the membership with the learner committed;
    /// [`Replica::progress`] then tells when it caught up.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] when this replica cannot change the membership.
    pub async fn add_learner(&self, id: NodeId, peer: Peer) -> Result<LogId> {
        written(self.raft.add_learner(id, peer, false).await).map(|response| response.log_id)
    }

    /// How far `id` is from holding everything the leader committed.
    #[must_use]
    pub fn progress(&self, id: NodeId) -> Progress {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let membership = metrics.membership_config.membership();
        if membership.voter_ids().any(|voter| voter == id) {
            return Progress::Voter;
        }
        if membership.get_node(&id).is_none() {
            return Progress::Stranger;
        }
        let Some(replication) = metrics.replication.as_ref() else {
            return Progress::Unknown;
        };
        let matched = replication.get(&id).copied().flatten();
        let committed = metrics.cluster_committed;
        if matched.map(|log_id| log_id.index) >= committed.map(|log_id| log_id.index) {
            Progress::CaughtUp
        } else {
            Progress::Behind { matched, committed }
        }
    }

    /// Makes the learner `id` a voter, once it caught up: a voter that is
    /// behind would hold back every commit until it caught up.
    ///
    /// # Errors
    ///
    /// [`Error::NotCaughtUp`] when `id` is not a learner that caught up, and
    /// [`Error::NotLeader`] when this replica cannot change the membership.
    pub async fn promote(&self, id: NodeId) -> Result<LogId> {
        match self.progress(id) {
            Progress::CaughtUp => {}
            Progress::Unknown => return Err(self.not_leader()),
            progress @ (Progress::Stranger | Progress::Voter | Progress::Behind { .. }) => {
                return Err(Error::NotCaughtUp { node: id, progress });
            }
        }
        let edit = MembershipEdit::AddVoterIds(BTreeSet::from([id]));
        written(self.raft.change_membership(edit, false).await).map(|response| response.log_id)
    }

    /// Removes `id` from the cluster, voter or learner. Once the change
    /// commits, `id` gets no more entries and its votes no longer count; it
    /// should be shut down.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] when this replica cannot change the membership,
    /// and [`Error::Raft`] when `id` is the last voter.
    pub async fn remove(&self, id: NodeId) -> Result<LogId> {
        let ids = BTreeSet::from([id]);
        let edit = match self.progress(id) {
            Progress::Voter => MembershipEdit::RemoveVoters(ids),
            Progress::Unknown => return Err(self.not_leader()),
            Progress::Stranger | Progress::Behind { .. } | Progress::CaughtUp => {
                MembershipEdit::RemoveNodes(ids)
            }
        };
        written(self.raft.change_membership(edit, false).await).map(|response| response.log_id)
    }

    /// The membership this replica knows of, committed or not: who may
    /// send to it.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .clone()
    }

    /// The log entry the membership of [`Replica::membership`] comes from,
    /// if any: a later change has a later entry.
    #[must_use]
    pub fn membership_log(&self) -> Option<LogId> {
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .log_id()
            .to_owned()
    }

    fn not_leader(&self) -> Error {
        Error::NotLeader {
            leader: self.status().leader(),
        }
    }

    /// Runs `read` against a state that reflects every write that returned
    /// before this call: the leader confirms with a quorum that it still
    /// leads, and waits until it applied everything committed by then. The
    /// state holds the keys of the shard's span only.
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
        Ok(self.state.read(|state| read(state.kv())))
    }

    /// Runs `read` against the shard's state as this replica applied it,
    /// which may lag the cluster.
    pub fn read_local<R>(&self, read: impl FnOnce(&ShardState) -> R) -> R {
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

    /// Stops the replica. Everything it acknowledged is already durable, so
    /// stopping and crashing leave the same files behind.
    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| Error::Stopped(error.to_string().into()))
    }

    /// Handles `request` from another replica of the shard.
    pub(crate) async fn handle(&self, request: Request<C>) -> Response<C> {
        let response = match request {
            Request::AppendEntries(request) => self
                .raft
                .append_entries(request)
                .await
                .map(Response::AppendEntries),
            Request::Vote(request) => self.raft.vote(request).await.map(Response::Vote),
            Request::PreVote(request) => self.raft.pre_vote(request).await.map(Response::PreVote),
            Request::InstallSnapshot { vote, snapshot } => self
                .raft
                .install_full_snapshot(vote, snapshot)
                .await
                .map(Response::InstallSnapshot)
                .map_err(RaftError::Fatal),
        };
        response.unwrap_or(Response::Refused(Refusal::Stopped))
    }
}

/// A transaction on a replicated state: it reads one version from its
/// replica, holds its writes back, and commits them as one log entry that
/// every replica checks for conflicts as it applies it.
///
/// Dropping it without committing writes nothing.
#[derive(Debug)]
#[must_use = "a transaction writes nothing until it commits"]
pub struct Txn<'replica, C: Config> {
    replica: &'replica Replica<C>,
    /// The shard as the transaction began: it reads only keys in its span.
    shard: Shard,
    draft: Draft,
}

impl<C: Config> Txn<'_, C> {
    /// The version its reads see.
    #[must_use]
    pub const fn read_at(&self) -> Version {
        self.draft.read_at()
    }

    /// The value of `key`, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Misrouted`] when `key` is not in the shard, and
    /// [`Error::Storage`] when so many writes committed since the
    /// transaction began that its version is forgotten.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.shard.span().contains(key) {
            return Err(misrouted(self.shard.clone()));
        }
        let value = self
            .replica
            .state
            .read(|state| self.draft.get(state.kv(), key));
        Ok(value.map_err(kurator_storage::Error::from)?)
    }

    /// The entries between `start` and `end` that the shard holds, in key
    /// order, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] when the version is forgotten, as for
    /// [`Txn::get`].
    pub fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<KeyValue>> {
        let (start, end) = self.shard.span().clip(start, end);
        let entries = self
            .replica
            .state
            .read(|state| self.draft.range(state.kv(), start, end));
        Ok(entries.map_err(kurator_storage::Error::from)?)
    }

    /// Sets `key` to `value` when the transaction commits.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.draft.put(key, value);
    }

    /// Removes `key` when the transaction commits.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.draft.delete(key);
    }

    /// Replicates the transaction and returns the version its writes made,
    /// once a quorum holds it and this replica applied it. A transaction
    /// that wrote nothing returns its read version and logs nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] with [`kurator_storage::Error::Aborted`] when a
    /// write after the read version touched what it read: it wrote
    /// nothing, and running it again may succeed. Otherwise as
    /// [`Replica::write`]; [`Error::Misrouted`] means the shard split
    /// before the transaction applied, and a key it touched moved.
    pub async fn commit(self) -> Result<Version> {
        let read_at = self.draft.read_at();
        let Some(transaction) = self.draft.into_transaction() else {
            return Ok(read_at);
        };
        let proposal = Proposal::new(Command::Txn(transaction))?;
        let response = written(self.replica.raft.client_write(proposal).await)?;
        version(response.data)
    }
}

/// The version a command's writes made, or why it made none.
fn version(applied: Applied) -> Result<Version> {
    match applied {
        Applied::Data(Outcome::Committed(version) | Outcome::Prepared(version)) => Ok(version),
        Applied::Data(Outcome::Aborted(abort)) => Err(kurator_storage::Error::from(abort).into()),
        Applied::Misrouted(shard) => Err(misrouted(shard)),
        Applied::Split(_) | Applied::Data(_) | _ => {
            Err(Error::Raft("a command applied as a split".into()))
        }
    }
}

fn misrouted(shard: Shard) -> Error {
    Error::Misrouted {
        shard: Box::new(shard),
    }
}

/// What applying a write did, or why it failed.
fn written<C: Config>(
    result: Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>>,
) -> Result<ClientWriteResponse<C>> {
    match result {
        Ok(response) => Ok(response),
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

fn stopped<C: Config>(fatal: Fatal<C>) -> Error {
    Error::Stopped(Box::new(fatal))
}
