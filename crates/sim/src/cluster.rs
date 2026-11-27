//! A simulated cluster: nodes in temporary directories, a simulated
//! network, and what a test drives: writes, splits, crashes, restarts,
//! partitions, and nodes joining and leaving.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kurator_meta::{MetaError, Placement, Registry, Reported, allocate};
use kurator_raft::openraft::{self, AsyncRuntime as _, Instant as _, SnapshotPolicy};
use kurator_raft::{
    Error, Member, Membership, NodeId, NodeSecret, Peer, Progress, Replica, Role, Status, Store,
    Txn,
};
use kurator_shard::{Halves, Shard, ShardId, ShardMap, ShardState};
use kurator_storage::{
    Abort, Deadline, Decide, Decided, Decision, KvOp, KvState, Prepare, TxnId, Version, WriteBatch,
};
use openraft_rt_sim::{SimInstant, SimRuntime};
use parking_lot::Mutex;

use crate::Sim;
use crate::holdings::Holdings;
use crate::network::{Endpoint, Faults, Garbled, Network};

pub use self::across::{Across, Coordinator};
pub use self::collect::Collector;

mod across;
mod collect;

/// Why the simulation could not go on.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SimError {
    /// A replica failed in a way the simulation does not inject.
    #[error("replica {node}: {source}")]
    Replica {
        /// The replica.
        node: NodeId,
        /// What failed.
        source: Error,
    },
    /// The file system failed.
    #[error("simulation I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The Raft configuration is invalid.
    #[error("invalid Raft configuration: {0}")]
    Config(#[from] openraft::ConfigError),
    /// A message did not decode: the encoding is broken.
    #[error(transparent)]
    Garbled(#[from] Garbled),
    /// The cluster did not settle in time.
    #[error("the cluster did not settle: {0:?}")]
    Unsettled(BTreeMap<ShardId, Vec<(NodeId, Status)>>),
}

/// The outcome of a client request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// A quorum holds the request durably and it applied.
    Acked,
    /// The request was never logged, or applied and changed nothing: no
    /// leader took it, it did not fit the shard, or a transaction across
    /// shards held a lock it needed.
    Rejected,
    /// The request may or may not take effect: it timed out, or its leader
    /// lost leadership after logging it.
    Unknown,
}

/// The outcome of a client transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Transacted {
    /// A quorum holds the writes, made at this version; or the
    /// transaction wrote nothing and read this version.
    Committed(Version),
    /// A write after its read version touched what it read: it wrote
    /// nothing.
    Aborted,
    /// It never reached the log: no leader took it, or a read failed.
    Rejected,
    /// It may or may not take effect: it timed out, or its leader lost
    /// leadership after logging it.
    Unknown,
}

/// What a client asks a shard's leader to do.
#[derive(Debug, Clone)]
enum Op {
    Write(WriteBatch),
    Split { at: Vec<u8>, right: ShardId },
    AddLearner(ShardId, NodeId, Peer),
    Promote(ShardId, NodeId),
    Remove(ShardId, NodeId),
}

/// Why an attempt did not settle a request.
#[derive(Debug, Clone, Copy)]
enum Retry {
    /// Try this node next, or the leader if there is none.
    To(Option<NodeId>),
    /// The shard split: look the shard up again.
    Reroute,
}

/// An error that may name the node to try instead.
trait Redirect {
    /// The leader to try, when the error names one.
    fn leader(&self) -> Option<NodeId>;
}

impl Redirect for Error {
    fn leader(&self) -> Option<NodeId> {
        let Self::NotLeader { leader } = self else {
            return None;
        };
        *leader
    }
}

impl Redirect for MetaError {
    fn leader(&self) -> Option<NodeId> {
        let Self::Raft(error) = self else {
            return None;
        };
        error.leader()
    }
}

/// A store on the simulated network.
type SimStore = Store<Sim, Endpoint>;

/// A cluster of nodes on one simulated network. Every node holds a
/// replica of every shard, unless a join or a leave stopped half way.
#[derive(Debug)]
pub struct Cluster {
    root: tempfile::TempDir,
    config: Arc<openraft::Config>,
    network: Network,
    /// The nodes in the cluster, and the nodes on their way in. A node
    /// that left is forgotten.
    members: BTreeMap<NodeId, Member>,
    /// The stores that are up. A crashed node keeps its directory.
    up: BTreeMap<NodeId, SimStore>,
    /// Where the client thinks each key is.
    map: Mutex<ShardMap>,
    /// The id of the last transaction across shards it coordinated.
    txns: Mutex<u128>,
}

/// How long a joining node may take to catch up before it gives up.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a joining node checks whether it caught up.
const CATCH_UP_POLL: Duration = Duration::from_millis(100);

/// How long a client waits for a request.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a coordinator means to take over a transaction across shards,
/// from when it starts to prepare it: past that, whoever runs into its
/// locks resolves it.
const LOCK_TTL: Duration = Duration::from_secs(5);

/// How many replicas a client tries before it gives up on a request.
/// With [`ROUTE_BACKOFF`] this spans a few elections and stays inside
/// [`CLIENT_TIMEOUT`].
const ROUTE_TRIES: u32 = 30;

/// How long a client waits before trying the next replica.
const ROUTE_BACKOFF: Duration = Duration::from_millis(50);

impl Cluster {
    /// Starts `size` nodes, numbered from 1, and founds the cluster with
    /// all of them as voters of its first shard. Network faults are drawn
    /// from `seed`.
    ///
    /// # Errors
    ///
    /// When a node cannot start.
    pub async fn start(seed: u64, size: u64, faults: Faults) -> Result<Self, SimError> {
        // Snapshots and purges every few dozen entries, so that runs exercise
        // catching up from a snapshot.
        let config = openraft::Config {
            snapshot_policy: SnapshotPolicy::LogsSinceLast(40),
            max_in_snapshot_log_to_keep: 10,
            ..openraft::Config::default()
        }
        .validate()?;
        let members = (1..=size)
            .map(|id| (NodeId::new(id), member(NodeId::new(id))))
            .collect();
        let mut cluster = Self {
            root: tempfile::tempdir()?,
            config: Arc::new(config),
            network: Network::new(seed, faults),
            members,
            up: BTreeMap::new(),
            map: Mutex::new(ShardMap::new()),
            txns: Mutex::new(0),
        };
        for id in cluster.ids() {
            cluster.restart(id).await?;
        }
        let voters: Vec<(NodeId, Peer)> = cluster
            .members
            .iter()
            .map(|(id, member)| (*id, peer(member)))
            .collect();
        if let Some((id, first)) = cluster.up.iter().next() {
            first
                .found(voters)
                .await
                .map_err(|source| SimError::Replica { node: *id, source })?;
        }
        Ok(cluster)
    }

    /// Every node, up or down.
    #[must_use]
    pub fn ids(&self) -> Vec<NodeId> {
        self.members.keys().copied().collect()
    }

    /// The nodes that are up.
    #[must_use]
    pub fn up(&self) -> Vec<NodeId> {
        self.up.keys().copied().collect()
    }

    /// The nodes that are down.
    #[must_use]
    pub fn down(&self) -> Vec<NodeId> {
        self.members
            .keys()
            .filter(|id| !self.up.contains_key(id))
            .copied()
            .collect()
    }

    /// The shards as the client last saw them.
    #[must_use]
    pub fn shards(&self) -> Vec<Shard> {
        self.map.lock().iter().cloned().collect()
    }

    /// Stops `id`: it answers nothing until it is restarted, and comes back
    /// from its directory.
    ///
    /// Writes that reached the file system but were not synced survive, so
    /// this does not test lost page caches; the WAL's recovery tests cover
    /// torn and damaged tails.
    ///
    /// # Errors
    ///
    /// When the node fails to stop.
    pub async fn crash(&mut self, id: NodeId) -> Result<(), SimError> {
        self.network.disconnect(id);
        if let Some(store) = self.up.remove(&id) {
            store
                .shutdown()
                .await
                .map_err(|source| SimError::Replica { node: id, source })?;
        }
        Ok(())
    }

    /// Starts `id` again from its directory. Starting a node that is up
    /// does nothing.
    ///
    /// # Errors
    ///
    /// When the node cannot recover.
    pub async fn restart(&mut self, id: NodeId) -> Result<(), SimError> {
        let Some(member) = self.members.get(&id).cloned() else {
            return Ok(());
        };
        if self.up.contains_key(&id) {
            return Ok(());
        }
        let endpoint = Endpoint::new(id, self.network.clone());
        let store = Store::open(member, &self.dir(id), Arc::clone(&self.config), endpoint)
            .await
            .map_err(|source| SimError::Replica { node: id, source })?;
        self.network.connect(id, store.inbox());
        drop(self.up.insert(id, store));
        Ok(())
    }

    /// Cuts `id` off from every other node.
    pub fn isolate(&self, id: NodeId) {
        self.network.isolate(id, self.ids());
    }

    /// Restores every link.
    pub fn heal(&self) {
        self.network.heal();
    }

    /// Writes `batch` through whichever replica leads the shard of its
    /// first key, following leader hints and splits. Gives up after three
    /// seconds of simulated time. A batch whose keys lie in different
    /// shards is rejected.
    pub async fn write(&self, batch: WriteBatch) -> Outcome {
        self.submit(Op::Write(batch)).await
    }

    /// Writes `batch` through `id` alone, without following leader hints.
    /// Gives up after three seconds of simulated time.
    pub async fn write_to(&self, id: NodeId, batch: WriteBatch) -> Outcome {
        self.submit_to(id, Op::Write(batch)).await
    }

    /// Splits the shard that holds `at`, so that the keys from `at` on
    /// move to a new shard, whose id the meta shard allocates. `Rejected`
    /// when `at` is where a shard starts, or no id could be had; an id
    /// allocated for a split that then fails is not handed out again.
    pub async fn split(&self, at: &[u8]) -> Outcome {
        let Some(right) = self
            .on_leader(ShardId::META, |meta| async move { allocate(&meta).await })
            .await
        else {
            return Outcome::Rejected;
        };
        self.submit(Op::Split {
            at: at.to_vec(),
            right,
        })
        .await
    }

    /// Brings a new node into every shard the way an operator would:
    /// [`Cluster::add_learner`], then [`Cluster::promote`] until the
    /// learner caught up, shard by shard.
    ///
    /// `Acked` means it votes in every shard. Otherwise it may stay a
    /// learner, or not be in a membership at all; [`Cluster::settle`]
    /// sorts that out.
    ///
    /// # Errors
    ///
    /// When the new node cannot start.
    pub async fn join(&mut self, id: NodeId) -> Result<Outcome, SimError> {
        match self.add_learner(id).await? {
            Outcome::Acked => {}
            other @ (Outcome::Rejected | Outcome::Unknown) => return Ok(other),
        }
        let promoted = SimRuntime::timeout(CATCH_UP_TIMEOUT, async {
            loop {
                match self.promote(id).await {
                    Outcome::Rejected => SimRuntime::sleep(CATCH_UP_POLL).await,
                    other @ (Outcome::Acked | Outcome::Unknown) => return other,
                }
            }
        });
        Ok(promoted.await.unwrap_or(Outcome::Rejected))
    }

    /// Starts the new node `id`, empty, and has the leader of every shard
    /// add it as a learner.
    ///
    /// # Errors
    ///
    /// When the new node cannot start.
    pub async fn add_learner(&mut self, id: NodeId) -> Result<Outcome, SimError> {
        self.add_node(id).await?;
        let peer = peer(&member(id));
        Ok(self
            .every_shard(|shard| Op::AddLearner(shard, id, peer.clone()))
            .await)
    }

    /// Starts the new node `id`, empty and in no shard, for replicas to
    /// move to. [`Cluster::settle`] forgets it if none did.
    ///
    /// # Errors
    ///
    /// When the new node cannot start.
    pub async fn add_node(&mut self, id: NodeId) -> Result<(), SimError> {
        let _: Option<Member> = self.members.insert(id, member(id));
        self.restart(id).await
    }

    /// Asks the leader of every shard to make the learner `id` a voter.
    /// `Rejected` while the learner has not caught up somewhere.
    pub async fn promote(&self, id: NodeId) -> Outcome {
        self.every_shard(|shard| Op::Promote(shard, id)).await
    }

    /// Moves the replica of `shard` on `from` to `to`, a node already in
    /// the cluster, the way an operator would: `to` joins the shard as a
    /// learner, is promoted once it caught up, and `from` is taken out.
    /// `from`'s replica stays behind, unused. `Rejected` when `to` is in
    /// the shard's membership already, or the shard has no leader.
    pub async fn move_replica(&self, shard: ShardId, from: NodeId, to: NodeId) -> Outcome {
        let held = self
            .leader_replica(shard)
            .map(|leader| leader.membership().get_node(&to).is_some());
        if held != Some(false) {
            return Outcome::Rejected;
        }
        let added = self
            .submit(Op::AddLearner(shard, to, peer(&member(to))))
            .await;
        if added != Outcome::Acked {
            return added;
        }
        let promoted = SimRuntime::timeout(CATCH_UP_TIMEOUT, async {
            loop {
                match self.submit(Op::Promote(shard, to)).await {
                    Outcome::Rejected => SimRuntime::sleep(CATCH_UP_POLL).await,
                    other @ (Outcome::Acked | Outcome::Unknown) => return other,
                }
            }
        });
        match promoted.await.unwrap_or(Outcome::Rejected) {
            Outcome::Acked => self.submit(Op::Remove(shard, from)).await,
            other @ (Outcome::Rejected | Outcome::Unknown) => other,
        }
    }

    /// Takes `id` out of every shard that holds it, the meta shard among
    /// them. Once every leader acknowledges the change, `id` is shut down
    /// and forgotten.
    ///
    /// # Errors
    ///
    /// When the removed node fails to stop.
    pub async fn leave(&mut self, id: NodeId) -> Result<Outcome, SimError> {
        let outcome = self.holding(id, |shard| Op::Remove(shard, id)).await;
        self.left(id, outcome).await
    }

    /// Takes `id` out of the first shard through `via` alone, without
    /// following leader hints, as [`Cluster::leave`] does.
    ///
    /// # Errors
    ///
    /// When the removed node fails to stop.
    pub async fn leave_via(&mut self, via: NodeId, id: NodeId) -> Result<Outcome, SimError> {
        let outcome = self.submit_to(via, Op::Remove(ShardId::FIRST, id)).await;
        self.left(id, outcome).await
    }

    async fn left(&mut self, id: NodeId, outcome: Outcome) -> Result<Outcome, SimError> {
        if outcome == Outcome::Acked {
            self.forget(id).await?;
        }
        Ok(outcome)
    }

    /// The voters of the first shard, as its leader knows them, if it has
    /// a leader.
    #[must_use]
    pub fn voters(&self) -> Option<Vec<NodeId>> {
        self.voters_of(ShardId::FIRST)
    }

    /// The voters of `shard`, as [`Cluster::voters`] finds them.
    #[must_use]
    pub fn voters_of(&self, shard: ShardId) -> Option<Vec<NodeId>> {
        let leader = self.leader_replica(shard)?;
        Some(leader.membership().voter_ids().collect())
    }

    /// Where a learner stands in the first shard, as its leader sees it.
    #[must_use]
    pub fn progress(&self, id: NodeId) -> Option<Progress> {
        Some(self.leader_replica(ShardId::FIRST)?.progress(id))
    }

    /// Reads the first shard through its leader with `read`,
    /// linearizably; `None` when no leader answers in time.
    pub async fn read<R: Send>(&self, read: impl FnOnce(&KvState) -> R + Send) -> Option<R> {
        let replica = self.leader_replica(ShardId::FIRST)?;
        SimRuntime::timeout(CLIENT_TIMEOUT, replica.read(read))
            .await
            .ok()?
            .ok()
    }

    /// Runs `body` as one transaction on the leader of the first shard:
    /// begins it there, lets `body` read and write, and commits. Gives up
    /// after three seconds of simulated time. A failing `body` writes
    /// nothing.
    pub async fn transact<F>(&self, body: F) -> Transacted
    where
        F: FnOnce(&mut Txn<'_, Sim>) -> Result<(), Error> + Send,
    {
        let Some(replica) = self.leader_replica(ShardId::FIRST) else {
            return Transacted::Rejected;
        };
        let attempt = async {
            let mut txn = replica.begin().await?;
            body(&mut txn)?;
            txn.commit().await
        };
        match SimRuntime::timeout(CLIENT_TIMEOUT, attempt).await {
            Ok(Ok(version)) => Transacted::Committed(version),
            Ok(Err(Error::Storage(kurator_storage::Error::Aborted(abort)))) => {
                let _resolved: bool = self.resolve_expired(ShardId::FIRST, &abort).await;
                Transacted::Aborted
            }
            Ok(Err(Error::Discarded { .. })) | Err(_) => Transacted::Unknown,
            Ok(Err(_)) => Transacted::Rejected,
        }
    }

    /// The leader of the first shard: of the newest term among the
    /// replicas that are up. A cut-off leader of an older term may still
    /// think it leads.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader_of(ShardId::FIRST)
    }

    /// The leader of `shard`, as [`Cluster::leader`] finds it.
    #[must_use]
    pub fn leader_of(&self, shard: ShardId) -> Option<NodeId> {
        self.replicas(shard)
            .map(|(id, replica)| (id, replica.status()))
            .filter(|(_, status)| status.role() == Role::Leader)
            .max_by_key(|(_, status)| status.term())
            .map(|(id, _)| id)
    }

    /// Heals the network, restarts every node, and waits until every node
    /// in each shard's membership applied the same log as its leader. Nodes
    /// that no shard's membership holds, because they left or never
    /// finished joining, are shut down and forgotten, and nodes forgotten
    /// already are taken out of the shards that still hold them.
    ///
    /// # Errors
    ///
    /// [`SimError::Unsettled`] when that does not happen within a minute of
    /// simulated time, and [`SimError::Garbled`] at once when a message
    /// did not decode.
    pub async fn settle(&mut self) -> Result<(), SimError> {
        self.heal();
        for id in self.down() {
            self.restart(id).await?;
        }
        // A linearizable read waits until the leader committed an entry of
        // its own term, and with it everything before: membership changes
        // and splits included.
        for _ in 0..60 {
            self.refresh().await;
            let mut committed = true;
            for shard in self.groups() {
                committed &= self.confirm(shard).await;
            }
            if committed {
                break;
            }
            SimRuntime::sleep(Duration::from_millis(500)).await;
        }
        self.remove_strays().await;
        for _ in 0..600 {
            if let Some(garbled) = self.network.garbled() {
                return Err(garbled.into());
            }
            // A split whose outcome was unknown may apply only now.
            self.refresh().await;
            self.forget_outsiders().await?;
            self.resolve().await;
            if self.converged() && self.unlocked() {
                return Ok(());
            }
            SimRuntime::sleep(Duration::from_millis(100)).await;
        }
        let statuses = self
            .groups()
            .into_iter()
            .map(|shard| {
                let statuses = self
                    .replicas(shard)
                    .map(|(id, replica)| (id, replica.status()))
                    .collect();
                (shard, statuses)
            })
            .collect();
        Err(SimError::Unsettled(statuses))
    }

    /// What each node that is up holds: its replica of every shard whose
    /// membership, as the shard's leader knows it, includes the node. The
    /// meta shard holds no keys of the cluster's and is left out.
    #[must_use]
    pub fn states(&self) -> BTreeMap<NodeId, Holdings> {
        let memberships = self.memberships();
        self.up
            .iter()
            .map(|(id, store)| {
                let held = store.replicas().into_iter().filter(|replica| {
                    replica.is_initialized()
                        && replica.shard_id() != ShardId::META
                        && memberships
                            .get(&replica.shard_id())
                            .is_some_and(|membership| membership.get_node(id).is_some())
                });
                let states: Vec<ShardState> = held
                    .map(|replica| replica.read_local(ShardState::clone))
                    .collect();
                (*id, Holdings::new(states))
            })
            .collect()
    }

    /// Where each replica of the first shard that is up stands.
    #[must_use]
    pub fn statuses(&self) -> Vec<(NodeId, Status)> {
        self.replicas(ShardId::FIRST)
            .map(|(id, replica)| (id, replica.status()))
            .collect()
    }

    /// Stops every node.
    ///
    /// # Errors
    ///
    /// When a node fails to stop.
    pub async fn shutdown(mut self) -> Result<(), SimError> {
        for id in self.up() {
            self.crash(id).await?;
        }
        Ok(())
    }

    /// The replicas of `shard` on the nodes that are up.
    fn replicas(&self, shard: ShardId) -> impl Iterator<Item = (NodeId, Arc<Replica<Sim>>)> + '_ {
        self.up.iter().filter_map(move |(id, store)| {
            let replica = store
                .replicas()
                .into_iter()
                .find(|replica| replica.shard_id() == shard)?;
            Some((*id, replica))
        })
    }

    /// Resolves every transaction across shards that a shard's leader
    /// holds prepared past its deadline.
    async fn resolve(&self) {
        let now = now();
        let expired: Vec<(ShardId, Lock)> = self
            .shards()
            .iter()
            .filter_map(|shard| Some((shard.id(), self.leader_replica(shard.id())?)))
            .flat_map(|(shard, replica)| {
                replica.read_local(|state| {
                    state
                        .kv()
                        .pending()
                        .filter(|prepared| prepared.deadline().has_passed(now))
                        .map(|prepared| (shard, Lock::of(prepared)))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for (shard, lock) in expired {
            self.resolve_lock(shard, lock).await;
        }
    }

    /// Resolves the transaction whose lock `abort` ran into on `shard`,
    /// when its deadline passed; whether it did.
    pub(super) async fn resolve_expired(&self, shard: ShardId, abort: &Abort) -> bool {
        let Abort::Locked {
            by,
            primary,
            deadline,
            ..
        } = abort
        else {
            return false;
        };
        if !deadline.has_passed(now()) {
            return false;
        }
        self.resolve_lock(shard, Lock::new(*by, primary.clone(), *deadline))
            .await;
        true
    }

    /// Decides the transaction that holds `lock` on `shard` as its primary
    /// decided it, or, when the primary never did, aborted there first.
    /// The primary's log orders that abort and its coordinator's commit,
    /// so whichever came first stands.
    async fn resolve_lock(&self, shard: ShardId, lock: Lock) {
        let Lock {
            id,
            primary,
            deadline,
        } = lock;
        let primary_shard = self.locate(&primary);
        let abort = Decide::new(id, primary.clone(), deadline, Decision::Abort);
        let Some(decided) = self.decide(primary_shard, abort).await else {
            return;
        };
        let outcome = Decide::new(id, primary, deadline, decision(decided));
        let _decided: Option<Decided> = self.decide(shard, outcome).await;
    }

    /// Whether every shard has a leader, and none holds a transaction
    /// across shards prepared and undecided.
    #[must_use]
    pub fn unlocked(&self) -> bool {
        self.shards().iter().all(|shard| {
            self.leader_replica(shard.id())
                .is_some_and(|replica| !replica.read_local(|state| state.kv().is_locked()))
        })
    }

    /// Tells `shard`'s leader `decide`, until it answers how it decided.
    pub(super) async fn decide(&self, shard: ShardId, decide: Decide) -> Option<Decided> {
        self.on_leader(shard, |replica| {
            let decide = decide.clone();
            async move { replica.decide(decide).await }
        })
        .await
    }

    /// When a coordinator that starts now means to have decided.
    fn lock_deadline() -> Deadline {
        let ttl = u64::try_from(LOCK_TTL.as_millis()).unwrap_or(u64::MAX);
        Deadline::new(now().millis().saturating_add(ttl))
    }

    /// An id no other transaction of this cluster has.
    fn next_txn(&self) -> TxnId {
        let mut next = self.txns.lock();
        *next = next.saturating_add(1);
        TxnId::new(*next)
    }

    fn leader_replica(&self, shard: ShardId) -> Option<Arc<Replica<Sim>>> {
        let leader = self.leader_of(shard)?;
        self.replicas(shard)
            .find(|(id, _)| *id == leader)
            .map(|(_, replica)| replica)
    }

    /// The meta shard, then every shard the client knows of.
    fn groups(&self) -> Vec<ShardId> {
        std::iter::once(ShardId::META)
            .chain(self.map.lock().iter().map(Shard::id))
            .collect()
    }

    /// The membership of each shard's leader, the meta shard's among them,
    /// once it committed it.
    fn memberships(&self) -> BTreeMap<ShardId, Membership> {
        self.groups()
            .into_iter()
            .filter_map(|shard| {
                let membership = self.leader_replica(shard)?.membership();
                // Joint configs exist only while a change is in flight.
                (membership.get_joint_config().len() == 1).then_some((shard, membership))
            })
            .collect()
    }

    /// Forgets the nodes that no shard's membership holds, once every
    /// shard has a leader to tell.
    async fn forget_outsiders(&mut self) -> Result<(), SimError> {
        let memberships = self.memberships();
        if memberships.len() != self.groups().len() {
            return Ok(());
        }
        let outsiders: Vec<NodeId> = self
            .ids()
            .into_iter()
            .filter(|id| {
                memberships
                    .values()
                    .all(|membership| membership.get_node(id).is_none())
            })
            .collect();
        for id in outsiders {
            self.forget(id).await?;
        }
        Ok(())
    }

    /// Shuts `id` down and forgets it.
    async fn forget(&mut self, id: NodeId) -> Result<(), SimError> {
        self.crash(id).await?;
        let _: Option<Member> = self.members.remove(&id);
        Ok(())
    }

    /// Whether every node is up, the client knows every shard as the
    /// replicas hold it, every shard has a leader, and every node in a
    /// shard's membership applied the same last entry of it as the leader.
    fn converged(&self) -> bool {
        let memberships = self.memberships();
        self.up.len() == self.members.len()
            && self.mapped(&memberships)
            && memberships.len() == self.groups().len()
            && memberships.iter().all(|(shard, membership)| {
                let applied: Vec<_> = self
                    .replicas(*shard)
                    .filter(|(id, _)| membership.get_node(id).is_some())
                    .map(|(_, replica)| replica.status().applied())
                    .collect();
                applied.len() == membership.nodes().count()
                    && applied.windows(2).all(|pair| pair.first() == pair.last())
            })
    }

    /// Whether the client's map holds every shard a replica that is up
    /// holds, as that replica holds it. Replicas a membership change left
    /// behind, which no leader updates any more, do not count.
    fn mapped(&self, memberships: &BTreeMap<ShardId, Membership>) -> bool {
        let map = self.map.lock();
        self.up.iter().all(|(id, store)| {
            store
                .replicas()
                .iter()
                .filter(|replica| {
                    replica.is_initialized()
                        && replica.shard_id() != ShardId::META
                        && memberships
                            .get(&replica.shard_id())
                            .is_none_or(|membership| membership.get_node(id).is_some())
                })
                .all(|replica| map.get(replica.shard_id()) == Some(&replica.shard()))
        })
    }

    /// Learns where every shard is from the meta shard, after every
    /// replica reported where its shard is, as a heartbeat would.
    async fn refresh(&self) {
        for store in self.up.values() {
            let _planted: Result<(), Error> = store.plant().await;
        }
        for placement in self.heartbeats() {
            self.report(&placement).await;
        }
        self.adopt().await;
    }

    /// Routes by the registry from now on, once it tiles every key. The
    /// client knows where shards are only from the registry.
    async fn adopt(&self) {
        if let Some(map) = self.registry().await.and_then(|registry| registry.map()) {
            *self.map.lock() = map;
        }
    }

    /// Where each shard is, as each replica of it knows from what it
    /// applied. Only committed changes apply, so no replica knows of a
    /// newer placement than the shard's leader, and the registry keeps the
    /// newest report.
    fn heartbeats(&self) -> Vec<Placement> {
        self.up
            .values()
            .flat_map(SimStore::replicas)
            .filter(|replica| replica.is_initialized() && replica.shard_id() != ShardId::META)
            .map(|replica| Placement::of(&replica))
            .collect()
    }

    /// Reports `placement` to the meta shard, which keeps it if it is
    /// newer than what it records.
    async fn report(&self, placement: &Placement) {
        let _reported: Option<Reported> = self
            .on_leader(ShardId::META, |meta| async move {
                kurator_meta::report(&meta, placement).await
            })
            .await;
    }

    /// Reports both halves of a split that just applied on `store`.
    async fn report_split(&self, store: &SimStore, halves: &Halves) {
        for shard in [halves.left(), halves.right()] {
            if let Ok(Some(replica)) = store.replica(shard.id()).await {
                self.report(&Placement::of(&replica)).await;
            }
        }
    }

    /// The registry as the meta shard's leader holds it, if one answers
    /// in time.
    pub async fn registry(&self) -> Option<Registry> {
        self.on_leader(ShardId::META, |meta| async move {
            kurator_meta::registry(&meta).await
        })
        .await
    }

    /// Runs `run` on the leader of `shard`, following leader hints and
    /// waiting [`ROUTE_BACKOFF`] between tries; `None` when no leader
    /// answered within [`ROUTE_TRIES`] or [`CLIENT_TIMEOUT`].
    async fn on_leader<T, E, F, Fut>(&self, shard: ShardId, run: F) -> Option<T>
    where
        F: Fn(Arc<Replica<Sim>>) -> Fut + Sync,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Redirect,
    {
        if self.network.garbled().is_some() {
            return None;
        }
        let tries = async {
            let mut hint = None;
            for _ in 0..ROUTE_TRIES {
                let id = hint
                    .or_else(|| self.leader_of(shard))
                    .or_else(|| self.up.keys().next().copied())?;
                match self.leader_attempt(shard, id, &run).await {
                    Ok(value) => return Some(value),
                    Err(next) => hint = next,
                }
                SimRuntime::sleep(ROUTE_BACKOFF).await;
            }
            None
        };
        // Boxed: the refreshes inside routing nest these futures deeper
        // than the compiler lays out.
        SimRuntime::timeout(CLIENT_TIMEOUT, Box::pin(tries))
            .await
            .ok()
            .flatten()
    }

    /// Runs `run` on `id`'s replica of `shard`: what it returned, or the
    /// node to try next.
    async fn leader_attempt<T, E, F, Fut>(
        &self,
        shard: ShardId,
        id: NodeId,
        run: &F,
    ) -> Result<T, Option<NodeId>>
    where
        F: Fn(Arc<Replica<Sim>>) -> Fut + Sync,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Redirect,
    {
        let next = self.next_up(id);
        let Some(store) = self.up.get(&id) else {
            return Err(next);
        };
        let Ok(Some(replica)) = store.replica(shard).await else {
            return Err(next);
        };
        run(replica).await.map_err(|error| error.leader().or(next))
    }

    /// Whether the leader of `shard` confirms it leads and has committed
    /// everything before.
    async fn confirm(&self, shard: ShardId) -> bool {
        let Some(replica) = self.leader_replica(shard) else {
            return false;
        };
        let read = SimRuntime::timeout(CLIENT_TIMEOUT, replica.read(KvState::version)).await;
        matches!(read, Ok(Ok(_)))
    }

    /// Takes the nodes the cluster forgot out of every shard that still
    /// holds them: a leave that ran before the client learned of a split
    /// missed the new shard.
    async fn remove_strays(&self) {
        let strays: Vec<(ShardId, NodeId)> = self
            .memberships()
            .into_iter()
            .flat_map(|(shard, membership)| {
                membership
                    .nodes()
                    .map(|(id, _)| (shard, *id))
                    .collect::<Vec<_>>()
            })
            .filter(|(_, id)| !self.members.contains_key(id))
            .collect();
        for (shard, id) in strays {
            let _outcome: Outcome = self.submit(Op::Remove(shard, id)).await;
        }
    }

    /// Submits `op` to every shard in turn, the meta shard first: `Acked`
    /// when every shard acknowledged it, otherwise the first other
    /// outcome.
    async fn every_shard(&self, op: impl Fn(ShardId) -> Op) -> Outcome {
        // A split whose outcome the client did not learn made a shard it
        // does not know of yet.
        self.refresh().await;
        self.each(self.groups(), op).await
    }

    /// Submits `op` to every shard whose membership holds `id`, or whose
    /// membership is not known.
    async fn holding(&self, id: NodeId, op: impl Fn(ShardId) -> Op) -> Outcome {
        self.refresh().await;
        let memberships = self.memberships();
        let held = self.groups().into_iter().filter(|shard| {
            memberships
                .get(shard)
                .is_none_or(|membership| membership.get_node(&id).is_some())
        });
        self.each(held.collect(), op).await
    }

    /// Submits `op` to each of `shards` in turn: `Acked` when each
    /// acknowledged it, otherwise the first other outcome.
    async fn each(&self, shards: Vec<ShardId>, op: impl Fn(ShardId) -> Op) -> Outcome {
        for shard in shards {
            let outcome = self.submit(op(shard)).await;
            if outcome != Outcome::Acked {
                return outcome;
            }
        }
        Outcome::Acked
    }

    /// The shard `op` is for.
    fn shard_of(&self, op: &Op) -> ShardId {
        let key = match op {
            Op::AddLearner(shard, ..) | Op::Promote(shard, _) | Op::Remove(shard, _) => {
                return *shard;
            }
            Op::Write(batch) => batch.ops().first().map_or(&[][..], KvOp::key),
            Op::Split { at, .. } => at,
        };
        self.locate(key)
    }

    /// The shard that holds `key`, as the client's map has it.
    fn locate(&self, key: &[u8]) -> ShardId {
        self.map.lock().locate(key).id()
    }

    /// Routes `op` to its shard's leader. Gives up after three seconds of
    /// simulated time.
    async fn submit(&self, op: Op) -> Outcome {
        if self.network.garbled().is_some() {
            return Outcome::Rejected;
        }
        SimRuntime::timeout(CLIENT_TIMEOUT, self.route(op))
            .await
            .unwrap_or(Outcome::Unknown)
    }

    /// Submits `op` to `id` alone. Gives up after three seconds of
    /// simulated time.
    async fn submit_to(&self, id: NodeId, op: Op) -> Outcome {
        if self.network.garbled().is_some() {
            return Outcome::Rejected;
        }
        let attempt = async {
            match self.attempt(id, op).await {
                Ok(outcome) => outcome,
                Err(Retry::To(_) | Retry::Reroute) => Outcome::Rejected,
            }
        };
        SimRuntime::timeout(CLIENT_TIMEOUT, attempt)
            .await
            .unwrap_or(Outcome::Unknown)
    }

    /// Tries the shard's newest leader first, then replicas in turn,
    /// following leader hints and splits and waiting [`ROUTE_BACKOFF`]
    /// between tries, until one takes `op` or [`ROUTE_TRIES`] are spent.
    async fn route(&self, op: Op) -> Outcome {
        let mut hint = None;
        for _ in 0..ROUTE_TRIES {
            let shard = self.shard_of(&op);
            let target = hint
                .or_else(|| self.leader_of(shard))
                .or_else(|| self.up.keys().next().copied());
            let Some(id) = target else {
                break;
            };
            match self.attempt(id, op.clone()).await {
                Ok(outcome) => return outcome,
                Err(Retry::To(next)) => hint = next,
                Err(Retry::Reroute) => {
                    self.refresh().await;
                    hint = None;
                }
            }
            SimRuntime::sleep(ROUTE_BACKOFF).await;
        }
        Outcome::Rejected
    }

    /// Submits `op` to `id`: the outcome when it took, logged or refused
    /// `op` for good, otherwise what to try next.
    async fn attempt(&self, id: NodeId, op: Op) -> Result<Outcome, Retry> {
        let shard = self.shard_of(&op);
        let store = self
            .up
            .get(&id)
            .ok_or_else(|| Retry::To(self.next_up(id)))?;
        let Ok(Some(replica)) = store.replica(shard).await else {
            return Err(Retry::To(self.next_up(id)));
        };
        let splits = matches!(op, Op::Split { .. });
        let result = match op {
            Op::Write(batch) => replica.write(batch).await.map(drop),
            Op::Split { at, right } => match store.split(shard, &at, right).await {
                Ok(halves) => {
                    self.report_split(store, &halves).await;
                    self.adopt().await;
                    Ok(())
                }
                Err(error) => Err(error),
            },
            Op::AddLearner(_, node, peer) => replica.add_learner(node, peer).await.map(drop),
            Op::Promote(_, node) => replica.promote(node).await.map(drop),
            Op::Remove(_, node) => replica.remove(node).await.map(drop),
        };
        match result {
            // Promoting a voter again is done already.
            Ok(())
            | Err(Error::NotCaughtUp {
                progress: Progress::Voter,
                ..
            }) => Ok(Outcome::Acked),
            Err(Error::Discarded { .. }) => Ok(Outcome::Unknown),
            // A lock whose coordinator gave up is resolved, and the write
            // tried again.
            Err(Error::Storage(kurator_storage::Error::Aborted(abort)))
                if self.resolve_expired(shard, &abort).await =>
            {
                Err(Retry::To(Some(id)))
            }
            // A write that conflicts with a transaction across shards, or
            // a split while one holds locks, is refused, not lost.
            Err(Error::NotCaughtUp { .. } | Error::Storage(_) | Error::Locked) => {
                Ok(Outcome::Rejected)
            }
            Err(Error::Misrouted { .. }) if splits => {
                self.refresh().await;
                Ok(Outcome::Rejected)
            }
            Err(Error::Misrouted { .. }) => Err(Retry::Reroute),
            Err(Error::NotLeader {
                leader: Some(leader),
            }) => Err(Retry::To(Some(leader))),
            Err(_) => Err(Retry::To(self.next_up(id))),
        }
    }

    fn next_up(&self, after: NodeId) -> Option<NodeId> {
        self.up
            .keys()
            .find(|id| **id > after)
            .or_else(|| self.up.keys().next())
            .copied()
    }

    fn dir(&self, id: NodeId) -> PathBuf {
        self.root.path().join(id.to_string())
    }
}

/// The member `id`, with a secret derived from its id.
fn member(id: NodeId) -> Member {
    let seed = [u8::try_from(id.get()).unwrap_or(u8::MAX); 32];
    Member::new(id, NodeSecret::from_seed(seed))
}

/// The peer of `member`. The simulated network routes by id, so the
/// address only has to be unique.
fn peer(member: &Member) -> Peer {
    let port = 7000_u16.saturating_add(u16::try_from(member.id().get()).unwrap_or(u16::MAX));
    member.peer(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port))
}

/// The decision to tell the other shards once the primary `decided`.
/// A lock a transaction across shards holds: what to resolve it by.
#[derive(Debug)]
struct Lock {
    id: TxnId,
    primary: Vec<u8>,
    deadline: Deadline,
}

impl Lock {
    const fn new(id: TxnId, primary: Vec<u8>, deadline: Deadline) -> Self {
        Self {
            id,
            primary,
            deadline,
        }
    }

    /// The lock `prepared` holds.
    fn of(prepared: &Prepare) -> Self {
        Self::new(
            prepared.id(),
            prepared.primary().to_vec(),
            prepared.deadline(),
        )
    }
}

const fn decision(decided: Decided) -> Decision {
    match decided {
        Decided::Committed(_) => Decision::Commit,
        Decided::Aborted | _ => Decision::Abort,
    }
}

/// Now on the clock the clients share: simulated time since the start.
fn now() -> Deadline {
    let elapsed = SimInstant::now().elapsed_since_start();
    Deadline::new(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}
