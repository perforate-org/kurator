//! A replica: one Raft node with its log store and state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Instant;

use oleaph_consensus::{Duty, Members, Reads, Replicated, Role, Route, Transaction};
use oleaph_metrics::{Exposition, Histogram, Histograms, Label, Values};
use oleaph_shard::{Applied, Halves, Shard, ShardId, ShardState, Split};
use oleaph_storage::{Command, Draft, Finished, KeyValue, KvState, Outcome, Version, WriteBatch};
use openraft::error::{ClientWriteError, Fatal, InitializeError, LinearizableReadError, RaftError};
use openraft::network::NetSnapshot;
use openraft::raft::ClientWriteResponse;
use openraft::type_config::async_runtime::WatchReceiver as _;
use openraft::{Raft, RaftNetworkFactory, ReadPolicy, ServerState};

use crate::config::{Config, MembershipEdit};
use crate::error::{Error, Result};
use crate::layout::Home;
use crate::log_store::LogStore;
use crate::network::chunk::{Assembly, Received, SnapshotChunk};
use crate::network::rpc::{Refusal, Request, Response};
use crate::proposal::Proposal;
use crate::snapshot::SnapshotBytes;
use crate::state_machine::StateMachine;
use crate::types::{LogId, Membership, NodeId, Peer, Progress, Vote};
use oleaph_crypto::{KeyPurpose, NodeSecret};

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
    /// How long each `fdatasync` of the log took.
    syncs: Arc<Histogram>,
    /// How long each proposal took from this replica, as the leader, to
    /// being committed and applied. Proposals that failed are not counted.
    commits: Histogram,
    /// The snapshot the leader is sending, as far as it has come.
    incoming: parking_lot::Mutex<Assembly>,
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
        let syncs = log.syncs();
        let state = StateMachine::open(home, shard)?;
        let raft = Raft::new(member.id, config, network, log, state.clone())
            .await
            .map_err(stopped)?;
        Ok(Self {
            id: member.id,
            shard,
            raft,
            state,
            syncs,
            commits: Histogram::default(),
            incoming: parking_lot::Mutex::default(),
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
        let response = self.replicate(proposal).await?;
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
        let response = self.replicate(proposal).await?;
        match response.data {
            Applied::Split(halves) => Ok(halves),
            Applied::Misrouted(shard) => Err(misrouted(shard)),
            Applied::Locked => Err(Error::Locked),
            Applied::Data(_) | _ => Err(Error::Raft("a split applied as a command".into())),
        }
    }

    /// The id of the shard it replicates.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.shard
    }

    /// Whether the replica holds a state yet: one it was seeded with, a
    /// snapshot it received, or entries it applied. A replica that a
    /// message opened holds none until the leader sends it a snapshot, and
    /// until then [`Replicated::shard`] does not describe its shard.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.state.is_initialized()
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

    /// Runs `read` against the shard's state as this replica applied it,
    /// which may lag the cluster.
    pub fn read_local<R>(&self, read: impl FnOnce(&ShardState) -> R) -> R {
        self.state.read(read)
    }

    /// Replicates `proposal` through Raft and waits until it applied here,
    /// counting how long it took in [`Replica::commits`].
    async fn replicate(&self, proposal: Proposal) -> Result<ClientWriteResponse<C>> {
        let started = Instant::now();
        let response = written(self.raft.client_write(proposal).await)?;
        self.commits.record(started.elapsed());
        Ok(response)
    }

    /// Adds what this replica is doing to `out`, labelled by its shard:
    /// whether it leads, its term, its log positions, the shard's size,
    /// which walks its keys, and how long syncs of its log and its commits
    /// took.
    pub(crate) fn expose(&self, out: &mut Exposition) {
        let shard = [Label::new("shard", &self.shard.get())];
        let status = self.status();
        out.value(&LEADER, &shard, u64::from(status.role == Role::Leader));
        out.value(&TERM, &shard, status.term);
        out.value(&LAST_LOG, &shard, status.last_log.unwrap_or_default());
        out.value(
            &APPLIED,
            &shard,
            status.applied.map_or(0, |log_id| log_id.index),
        );
        let (footprint, keys) = self.state.read(|state| {
            let kv = state.kv();
            (kv.footprint().bytes(), kv.keys().count())
        });
        out.value(&FOOTPRINT, &shard, footprint);
        out.value(&KEYS, &shard, u64::try_from(keys).unwrap_or(u64::MAX));
        out.histogram(&SYNCS, &shard, &self.syncs);
        out.histogram(&COMMITS, &shard, &self.commits);
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
            Request::InstallSnapshot { vote, chunk } => return self.receive(vote, chunk).await,
        };
        response.unwrap_or(Response::Refused(Refusal::Stopped))
    }

    /// Takes `chunk` of the snapshot the leader of `vote` sends, and
    /// installs the snapshot once its last chunk is in. A chunk from a
    /// leader of a lower vote than this replica holds is answered with
    /// that vote, so the sender stops.
    async fn receive(&self, vote: Vote, chunk: SnapshotChunk) -> Response<C> {
        let held = self.raft.metrics().borrow_watched().vote;
        if held > vote {
            return Response::InstallSnapshot(openraft::raft::SnapshotResponse::new(held));
        }
        let received = self.incoming.lock().add(vote, chunk);
        match received {
            Received::Stray => Response::Refused(Refusal::OutOfOrder),
            Received::Damaged => Response::Refused(Refusal::Malformed),
            Received::Part => Response::Received,
            Received::Whole(snapshot) => self
                .raft
                .install_full_snapshot(vote, snapshot)
                .await
                .map_or(
                    Response::Refused(Refusal::Stopped),
                    Response::InstallSnapshot,
                ),
        }
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
}

/// The version a command's writes made, or why it made none.
fn version(applied: Applied) -> Result<Version> {
    match applied {
        Applied::Data(Outcome::Committed(version) | Outcome::Prepared(version)) => Ok(version),
        Applied::Data(Outcome::Aborted(abort)) => Err(oleaph_storage::Error::from(abort).into()),
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

impl<C: Config> Replicated for Replica<C> {
    type Txn<'replica> = Txn<'replica, C>;

    fn node(&self) -> NodeId {
        self.id
    }

    /// The shard as this replica last applied it.
    fn shard(&self) -> Shard {
        self.state.read(|state| state.shard().clone())
    }

    const READS: Reads = Reads::Linearizable;

    /// Here when this replica leads, else the leader it knows of.
    fn route(&self) -> Route {
        let status = self.status();
        match (status.role(), status.leader()) {
            (Role::Leader, _) => Route::Here,
            (_, Some(leader)) => Route::Leader(leader),
            (_, None) => Route::Unknown,
        }
    }

    /// Mine while this replica leads.
    fn duty(&self) -> Duty {
        if self.status().role() == Role::Leader {
            Duty::Mine
        } else {
            Duty::Others
        }
    }

    fn members(&self) -> Members {
        let membership = self.membership();
        let voters = membership.voter_ids().collect();
        let peers = membership
            .nodes()
            .map(|(id, peer)| (*id, peer.clone()))
            .collect();
        let position = self.membership_log().map_or(0, |log_id| log_id.index);
        Members::new(voters, peers, position)
    }

    /// Starts a transaction that reads the newest version this cluster
    /// committed: the leader confirms it still leads, as for
    /// [`Replicated::read`], and the transaction reads that version from this
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
    async fn begin(&self) -> oleaph_consensus::Result<Self::Txn<'_>> {
        let read_at = self.read(KvState::version).await?;
        Ok(Txn {
            replica: self,
            shard: self.shard(),
            draft: Draft::new(read_at),
        })
    }

    /// Runs `read` against a state that reflects every write that returned
    /// before this call: the leader confirms with a quorum that it still
    /// leads, and waits until it applied everything committed by then. The
    /// state holds the keys of the shard's span only.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] when this replica is not the leader.
    async fn read<R: Send>(
        &self,
        read: impl FnOnce(&KvState) -> R + Send,
    ) -> oleaph_consensus::Result<R> {
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

    async fn propose(&self, command: Command) -> oleaph_consensus::Result<Outcome> {
        let response = self.replicate(Proposal::new(command)?).await?;
        match response.data {
            Applied::Data(outcome) => Ok(outcome),
            Applied::Misrouted(shard) => Err(misrouted(shard).into()),
            Applied::Split(_) | _ => Err(Error::Raft("a command applied as a split".into()).into()),
        }
    }
}

impl<C: Config> Transaction for Txn<'_, C> {
    /// The value of `key`, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Misrouted`] when `key` is not in the shard, and
    /// [`Error::Storage`] when so many writes committed since the
    /// transaction began that its version is forgotten.
    fn get(&mut self, key: &[u8]) -> oleaph_consensus::Result<Option<Vec<u8>>> {
        if !self.shard.span().contains(key) {
            return Err(misrouted(self.shard.clone()).into());
        }
        let value = self
            .replica
            .state
            .read(|state| self.draft.get(state.kv(), key));
        Ok(value.map_err(oleaph_storage::Error::from)?)
    }

    /// The entries between `start` and `end` that the shard holds, in key
    /// order, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] when the version is forgotten, as for
    /// [`Transaction::get`].
    fn range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> oleaph_consensus::Result<Vec<KeyValue>> {
        let (start, end) = self.shard.span().clip(start, end);
        let entries = self
            .replica
            .state
            .read(|state| self.draft.range(state.kv(), start, end));
        Ok(entries.map_err(oleaph_storage::Error::from)?)
    }

    /// Sets `key` to `value` when the transaction commits.
    fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.draft.put(key, value);
    }

    /// Removes `key` when the transaction commits.
    fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.draft.delete(key);
    }

    /// Replicates the transaction and returns the version its writes made,
    /// once a quorum holds it and this replica applied it. A transaction
    /// that wrote nothing logs nothing: it is confirmed on this replica's
    /// state ([`KvState::confirm`]) and
    /// returns its read version.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] with [`oleaph_storage::Error::Aborted`] when a
    /// write after the read version touched what it read: it wrote
    /// nothing, and running it again may succeed. Otherwise as
    /// [`Replica::write`]; [`Error::Misrouted`] means the shard split
    /// before the transaction applied, and a key it touched moved.
    async fn commit(self) -> oleaph_consensus::Result<Version> {
        let transaction = match self.draft.finish() {
            Finished::Writes(transaction) => transaction,
            Finished::Reads(read) => {
                let confirmed = self.replica.state.read(|state| state.kv().confirm(&read));
                return Ok(confirmed.map_err(oleaph_storage::Error::from)?);
            }
        };
        let proposal = Proposal::new(Command::Txn(transaction))?;
        let response = self.replica.replicate(proposal).await?;
        Ok(version(response.data)?)
    }
}
const LEADER: Values = Values::gauge(
    "oleaph_shard_leader",
    "1 while this node's replica of the shard leads it, 0 otherwise.",
);
const TERM: Values = Values::gauge(
    "oleaph_raft_term",
    "The Raft term the replica is in; it grows by one an election.",
);
const LAST_LOG: Values = Values::gauge(
    "oleaph_raft_last_log_index",
    "The index of the last entry in the replica's log.",
);
const APPLIED: Values = Values::gauge(
    "oleaph_raft_applied_index",
    "The index of the last entry the replica applied.",
);
const FOOTPRINT: Values = Values::gauge(
    "oleaph_shard_footprint_bytes",
    "The bytes the shard's keys take with every version they keep: about its snapshot's size.",
);
const KEYS: Values = Values::gauge(
    "oleaph_shard_keys",
    "The keys the shard keeps versions of, removed ones included.",
);
const SYNCS: Histograms = Histograms::new(
    "oleaph_wal_sync_seconds",
    "How long each fdatasync of the replica's log took.",
);
const COMMITS: Histograms = Histograms::new(
    "oleaph_commit_seconds",
    "How long each proposal this replica made as the leader took to commit and apply.",
);
