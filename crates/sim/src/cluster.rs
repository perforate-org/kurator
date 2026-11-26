//! A simulated cluster: nodes in temporary directories, a simulated
//! network, and what a test drives: writes, splits, crashes, restarts,
//! partitions, and nodes joining and leaving.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kurator_raft::openraft::{self, AsyncRuntime as _, SnapshotPolicy};
use kurator_raft::{
    Error, Member, Membership, NodeId, NodeSecret, Peer, Progress, Replica, Role, Status, Store,
    Txn,
};
use kurator_shard::{Halves, Shard, ShardId, ShardMap, ShardState};
use kurator_storage::{KvOp, KvState, Version, WriteBatch};
use openraft_rt_sim::SimRuntime;
use parking_lot::Mutex;

use crate::Sim;
use crate::holdings::Holdings;
use crate::network::{Endpoint, Faults, Garbled, Network};

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
    /// leader took it, or it did not fit the shard.
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
    /// The id the next split names its new shard. Ids are never reused,
    /// even when a split's outcome is unknown.
    next_shard: u64,
}

/// How long a joining node may take to catch up before it gives up.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a joining node checks whether it caught up.
const CATCH_UP_POLL: Duration = Duration::from_millis(100);

/// How long a client waits for a request.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(3);

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
            next_shard: ShardId::FIRST.get().saturating_add(1),
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
    /// move to a new shard. `Rejected` when `at` is where a shard starts.
    pub async fn split(&mut self, at: &[u8]) -> Outcome {
        let right = ShardId::new(self.next_shard);
        self.next_shard = self.next_shard.saturating_add(1);
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
        let joining = member(id);
        let peer = peer(&joining);
        let _: Option<Member> = self.members.insert(id, joining);
        self.restart(id).await?;
        Ok(self
            .every_shard(|shard| Op::AddLearner(shard, id, peer.clone()))
            .await)
    }

    /// Asks the leader of every shard to make the learner `id` a voter.
    /// `Rejected` while the learner has not caught up somewhere.
    pub async fn promote(&self, id: NodeId) -> Outcome {
        self.every_shard(|shard| Op::Promote(shard, id)).await
    }

    /// Takes `id` out of every shard. Once every leader acknowledges the
    /// change, `id` is shut down and forgotten.
    ///
    /// # Errors
    ///
    /// When the removed node fails to stop.
    pub async fn leave(&mut self, id: NodeId) -> Result<Outcome, SimError> {
        let outcome = self.every_shard(|shard| Op::Remove(shard, id)).await;
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
        let leader = self.leader_replica(ShardId::FIRST)?;
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
            Ok(Err(Error::Storage(kurator_storage::Error::Aborted(_)))) => Transacted::Aborted,
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
            for shard in self.shards() {
                committed &= self.confirm(shard.id()).await;
            }
            if committed {
                break;
            }
            SimRuntime::sleep(Duration::from_millis(500)).await;
        }
        self.remove_strays().await;
        let memberships = self.memberships();
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
        for _ in 0..600 {
            if let Some(garbled) = self.network.garbled() {
                return Err(garbled.into());
            }
            if self.converged() {
                return Ok(());
            }
            SimRuntime::sleep(Duration::from_millis(100)).await;
        }
        let statuses = self
            .shards()
            .into_iter()
            .map(|shard| {
                let statuses = self
                    .replicas(shard.id())
                    .map(|(id, replica)| (id, replica.status()))
                    .collect();
                (shard.id(), statuses)
            })
            .collect();
        Err(SimError::Unsettled(statuses))
    }

    /// What each node that is up holds: its replica of every shard whose
    /// membership, as the shard's leader knows it, includes the node.
    #[must_use]
    pub fn states(&self) -> BTreeMap<NodeId, Holdings> {
        let memberships = self.memberships();
        self.up
            .iter()
            .map(|(id, store)| {
                let held = store.replicas().into_iter().filter(|replica| {
                    replica.is_initialized()
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

    fn leader_replica(&self, shard: ShardId) -> Option<Arc<Replica<Sim>>> {
        let leader = self.leader_of(shard)?;
        self.replicas(shard)
            .find(|(id, _)| *id == leader)
            .map(|(_, replica)| replica)
    }

    /// The membership of each shard's leader, once it committed it.
    fn memberships(&self) -> BTreeMap<ShardId, Membership> {
        self.shards()
            .into_iter()
            .filter_map(|shard| {
                let membership = self.leader_replica(shard.id())?.membership();
                // Joint configs exist only while a change is in flight.
                (membership.get_joint_config().len() == 1).then_some((shard.id(), membership))
            })
            .collect()
    }

    /// Shuts `id` down and forgets it.
    async fn forget(&mut self, id: NodeId) -> Result<(), SimError> {
        self.crash(id).await?;
        let _: Option<Member> = self.members.remove(&id);
        Ok(())
    }

    /// Whether every node is up, every shard has a leader, and every node
    /// in a shard's membership applied the same last entry of it as the
    /// leader.
    fn converged(&self) -> bool {
        let memberships = self.memberships();
        let shards = self.shards();
        self.up.len() == self.members.len()
            && memberships.len() == shards.len()
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

    /// Learns where every shard is from the replicas that are up: the
    /// newest epoch of each shard wins. The map stays as it was while the
    /// replicas do not yet tile every key.
    async fn refresh(&self) {
        let mut seen = Vec::new();
        for store in self.up.values() {
            if store.plant().await.is_ok() {
                seen.extend(
                    store
                        .replicas()
                        .into_iter()
                        .filter(|replica| replica.is_initialized())
                        .map(|replica| replica.shard()),
                );
            }
        }
        let mut newest: BTreeMap<ShardId, Shard> = BTreeMap::new();
        for shard in seen {
            let known = newest.get(&shard.id());
            if known.is_none_or(|known| known.epoch() < shard.epoch()) {
                drop(newest.insert(shard.id(), shard));
            }
        }
        if let Some(map) = ShardMap::from_shards(newest.into_values()) {
            *self.map.lock() = map;
        }
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

    /// Records a split the client saw succeed.
    fn learn(&self, halves: &Halves) {
        let _stale = self.map.lock().split(halves);
    }

    /// Submits `op` to every shard in turn: `Acked` when every shard
    /// acknowledged it, otherwise the first other outcome.
    async fn every_shard(&self, op: impl Fn(ShardId) -> Op) -> Outcome {
        // A split whose outcome the client did not learn made a shard it
        // does not know of yet.
        self.refresh().await;
        for shard in self.shards() {
            let outcome = self.submit(op(shard.id())).await;
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
            Op::Split { at, right } => store
                .split(shard, &at, right)
                .await
                .map(|halves| self.learn(&halves)),
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
            Err(Error::NotCaughtUp { .. }) => Ok(Outcome::Rejected),
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
