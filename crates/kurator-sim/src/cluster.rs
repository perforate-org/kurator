//! A simulated cluster: replicas in temporary directories, a simulated
//! network, and what a test drives: crashes, restarts, partitions, and
//! nodes joining and leaving.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kurator_raft::openraft::{self, AsyncRuntime as _, SnapshotPolicy};
use kurator_raft::{
    Error, LogId, Member, Membership, Network as RaftNetwork, NodeId, NodeSecret, Peer, Replica,
    Role, Status,
};
use kurator_storage::{KvState, WriteBatch};
use openraft_rt_sim::SimRuntime;

use crate::Sim;
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
    Unsettled(Vec<(NodeId, Status)>),
}

/// The outcome of a client write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// A quorum holds the write durably.
    Acked(LogId),
    /// The write was never logged: no leader took it.
    Rejected,
    /// The write may or may not take effect: it timed out, or its leader
    /// lost leadership after logging it.
    Unknown,
}

/// What a client asks the leader to do.
#[derive(Debug, Clone)]
enum Op {
    Write(WriteBatch),
    AddLearner(NodeId, Peer),
    Promote(NodeId),
    Remove(NodeId),
}

/// A cluster of replicas on one simulated network.
#[derive(Debug)]
pub struct Cluster {
    root: tempfile::TempDir,
    config: Arc<openraft::Config>,
    network: Network,
    /// The nodes in the cluster, voters and learners, and the nodes on
    /// their way in. A node that left is forgotten.
    members: BTreeMap<NodeId, Member>,
    /// The replicas that are up. A crashed replica keeps its directory.
    up: BTreeMap<NodeId, Replica<Sim>>,
}

/// How long a joining node may take to catch up before it gives up.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a joining node checks whether it caught up.
const CATCH_UP_POLL: Duration = Duration::from_millis(100);

/// How long a client waits for a write or a read.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(3);

/// How many replicas a client tries before it gives up on a write. With
/// [`ROUTE_BACKOFF`] this spans a few elections and stays inside
/// [`CLIENT_TIMEOUT`].
const ROUTE_TRIES: u32 = 30;

/// How long a client waits before trying the next replica.
const ROUTE_BACKOFF: Duration = Duration::from_millis(50);

impl Cluster {
    /// Starts `size` replicas, numbered from 1, and initializes the cluster
    /// with all of them as voters. Network faults are drawn from `seed`.
    ///
    /// # Errors
    ///
    /// When a replica cannot start.
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
        };
        let ids = cluster.ids();
        for id in &ids {
            cluster.restart(*id).await?;
        }
        let voters: Vec<(NodeId, Peer)> = cluster
            .members
            .iter()
            .map(|(id, member)| (*id, peer(member)))
            .collect();
        if let Some(first) = cluster.up.values().next() {
            first
                .initialize(voters)
                .await
                .map_err(|source| SimError::Replica {
                    node: first.id(),
                    source,
                })?;
        }
        Ok(cluster)
    }

    /// Every replica, up or down.
    #[must_use]
    pub fn ids(&self) -> Vec<NodeId> {
        self.members.keys().copied().collect()
    }

    /// The replicas that are up.
    #[must_use]
    pub fn up(&self) -> Vec<NodeId> {
        self.up.keys().copied().collect()
    }

    /// The replicas that are down.
    #[must_use]
    pub fn down(&self) -> Vec<NodeId> {
        self.members
            .keys()
            .filter(|id| !self.up.contains_key(id))
            .copied()
            .collect()
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
    /// When the replica fails to stop.
    pub async fn crash(&mut self, id: NodeId) -> Result<(), SimError> {
        self.network.disconnect(id);
        if let Some(replica) = self.up.remove(&id) {
            replica
                .shutdown()
                .await
                .map_err(|source| SimError::Replica { node: id, source })?;
        }
        Ok(())
    }

    /// Starts `id` again from its directory. Starting a replica that is up
    /// does nothing.
    ///
    /// # Errors
    ///
    /// When the replica cannot recover.
    pub async fn restart(&mut self, id: NodeId) -> Result<(), SimError> {
        let Some(member) = self.members.get(&id).cloned() else {
            return Ok(());
        };
        if self.up.contains_key(&id) {
            return Ok(());
        }
        let network = RaftNetwork::new(Endpoint::new(id, self.network.clone()));
        let replica = Replica::open(member, &self.dir(id), Arc::clone(&self.config), network)
            .await
            .map_err(|source| SimError::Replica { node: id, source })?;
        self.network.connect(id, replica.inbox());
        drop(self.up.insert(id, replica));
        Ok(())
    }

    /// Cuts `id` off from every other replica.
    pub fn isolate(&self, id: NodeId) {
        self.network.isolate(id, self.ids());
    }

    /// Restores every link.
    pub fn heal(&self) {
        self.network.heal();
    }

    /// Writes `batch` through whichever replica leads, following leader
    /// hints. Gives up after three seconds of simulated time.
    pub async fn write(&self, batch: WriteBatch) -> Outcome {
        self.submit(Op::Write(batch)).await
    }

    /// Writes `batch` through `id` alone, without following leader hints.
    /// Gives up after three seconds of simulated time.
    pub async fn write_to(&self, id: NodeId, batch: WriteBatch) -> Outcome {
        self.submit_to(id, Op::Write(batch)).await
    }

    /// Brings a new node into the cluster the way an operator would:
    /// [`Cluster::add_learner`], then [`Cluster::promote`] until the
    /// learner caught up.
    ///
    /// `Acked` means it votes. Otherwise it may stay a learner, or not be
    /// in the membership at all; [`Cluster::settle`] sorts that out.
    ///
    /// # Errors
    ///
    /// When the new replica cannot start.
    pub async fn join(&mut self, id: NodeId) -> Result<Outcome, SimError> {
        match self.add_learner(id).await? {
            Outcome::Acked(_) => {}
            other @ (Outcome::Rejected | Outcome::Unknown) => return Ok(other),
        }
        let promoted = SimRuntime::timeout(CATCH_UP_TIMEOUT, async {
            loop {
                match self.promote(id).await {
                    Outcome::Rejected => SimRuntime::sleep(CATCH_UP_POLL).await,
                    other @ (Outcome::Acked(_) | Outcome::Unknown) => return other,
                }
            }
        });
        Ok(promoted.await.unwrap_or(Outcome::Rejected))
    }

    /// Starts the new node `id`, empty, and has the leader add it as a
    /// learner.
    ///
    /// # Errors
    ///
    /// When the new replica cannot start.
    pub async fn add_learner(&mut self, id: NodeId) -> Result<Outcome, SimError> {
        let joining = member(id);
        let peer = peer(&joining);
        let _: Option<Member> = self.members.insert(id, joining);
        self.restart(id).await?;
        Ok(self.submit(Op::AddLearner(id, peer)).await)
    }

    /// Asks the leader to make the learner `id` a voter. `Rejected` while
    /// the learner has not caught up.
    pub async fn promote(&self, id: NodeId) -> Outcome {
        self.submit(Op::Promote(id)).await
    }

    /// Takes `id` out of the cluster. Once the leader acknowledges the
    /// change, `id` is shut down and forgotten.
    ///
    /// # Errors
    ///
    /// When the removed replica fails to stop.
    pub async fn leave(&mut self, id: NodeId) -> Result<Outcome, SimError> {
        let outcome = self.submit(Op::Remove(id)).await;
        self.left(id, outcome).await
    }

    /// Takes `id` out of the cluster through `via` alone, without following
    /// leader hints, as [`Cluster::leave`] does.
    ///
    /// # Errors
    ///
    /// When the removed replica fails to stop.
    pub async fn leave_via(&mut self, via: NodeId, id: NodeId) -> Result<Outcome, SimError> {
        let outcome = self.submit_to(via, Op::Remove(id)).await;
        self.left(id, outcome).await
    }

    async fn left(&mut self, id: NodeId, outcome: Outcome) -> Result<Outcome, SimError> {
        if let Outcome::Acked(_) = outcome {
            self.forget(id).await?;
        }
        Ok(outcome)
    }

    /// The voters of the membership the leader knows of, if there is a
    /// leader.
    #[must_use]
    pub fn voters(&self) -> Option<Vec<NodeId>> {
        let leader = self.up.get(&self.leader()?)?;
        Some(leader.membership().voter_ids().collect())
    }

    /// Where a learner stands, as the leader sees it.
    #[must_use]
    pub fn progress(&self, id: NodeId) -> Option<kurator_raft::Progress> {
        Some(self.up.get(&self.leader()?)?.progress(id))
    }

    /// Reads through the leader with `read`, linearizably; `None` when no
    /// leader answers in time.
    pub async fn read<R: Send>(&self, read: impl FnOnce(&KvState) -> R + Send) -> Option<R> {
        let leader = self.leader()?;
        let replica = self.up.get(&leader)?;
        SimRuntime::timeout(CLIENT_TIMEOUT, replica.read(read))
            .await
            .ok()?
            .ok()
    }

    /// The leader of the newest term among the replicas that are up. A
    /// cut-off leader of an older term may still think it leads.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.up
            .values()
            .map(|replica| (replica.id(), replica.status()))
            .filter(|(_, status)| status.role() == Role::Leader)
            .max_by_key(|(_, status)| status.term())
            .map(|(id, _)| id)
    }

    /// Heals the network, restarts every replica, and waits until every
    /// node in the leader's membership applied the same log. Nodes that the
    /// membership does not hold, because they left or never finished
    /// joining, are shut down and forgotten.
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
        // A write in the leader's term commits everything before it,
        // membership changes included.
        for _ in 0..60 {
            if let Outcome::Acked(_) = self
                .write(WriteBatch::new().delete(b"settle".to_vec()))
                .await
            {
                break;
            }
            SimRuntime::sleep(Duration::from_millis(500)).await;
        }
        if let Some(membership) = self.committed_membership() {
            let outsiders: Vec<NodeId> = self
                .ids()
                .into_iter()
                .filter(|id| membership.get_node(id).is_none())
                .collect();
            for id in outsiders {
                self.forget(id).await?;
            }
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
        Err(SimError::Unsettled(self.statuses()))
    }

    /// The state each replica that is up applied.
    #[must_use]
    pub fn states(&self) -> BTreeMap<NodeId, KvState> {
        self.up
            .iter()
            .map(|(id, replica)| (*id, replica.read_local(KvState::clone)))
            .collect()
    }

    /// Where each replica that is up stands.
    #[must_use]
    pub fn statuses(&self) -> Vec<(NodeId, Status)> {
        self.up
            .iter()
            .map(|(id, replica)| (*id, replica.status()))
            .collect()
    }

    /// Stops every replica.
    ///
    /// # Errors
    ///
    /// When a replica fails to stop.
    pub async fn shutdown(mut self) -> Result<(), SimError> {
        for id in self.up() {
            self.crash(id).await?;
        }
        Ok(())
    }

    /// The membership of the leader, once it committed it.
    fn committed_membership(&self) -> Option<Membership> {
        let leader = self.up.get(&self.leader()?)?;
        let membership = leader.membership();
        // Joint configs exist only while a change is in flight.
        (membership.get_joint_config().len() == 1).then_some(membership)
    }

    /// Shuts `id` down and forgets it.
    async fn forget(&mut self, id: NodeId) -> Result<(), SimError> {
        self.crash(id).await?;
        let _: Option<Member> = self.members.remove(&id);
        Ok(())
    }

    /// Whether every replica is up and applied the same last entry.
    fn converged(&self) -> bool {
        let applied: Vec<Option<LogId>> = self
            .up
            .values()
            .map(|replica| replica.status().applied())
            .collect();
        self.up.len() == self.members.len()
            && applied.windows(2).all(|pair| pair.first() == pair.last())
    }

    /// Routes `op` to the leader. Gives up after three seconds of
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
        SimRuntime::timeout(CLIENT_TIMEOUT, self.attempt(id, op))
            .await
            .map_or(Outcome::Unknown, |attempt| {
                attempt.unwrap_or(Outcome::Rejected)
            })
    }

    /// Tries the newest leader first, then replicas in turn, following
    /// leader hints and waiting
    /// [`ROUTE_BACKOFF`] between tries, until one takes `op` or
    /// [`ROUTE_TRIES`] are spent.
    async fn route(&self, op: Op) -> Outcome {
        let mut target = self.leader().or_else(|| self.up.keys().next().copied());
        for _ in 0..ROUTE_TRIES {
            let Some(id) = target else {
                break;
            };
            match self.attempt(id, op.clone()).await {
                Ok(outcome) => return outcome,
                Err(next) => target = next,
            }
            SimRuntime::sleep(ROUTE_BACKOFF).await;
        }
        Outcome::Rejected
    }

    /// Submits `op` to `id`: the outcome when it took, logged or refused
    /// `op` for good, otherwise the replica to try next.
    async fn attempt(&self, id: NodeId, op: Op) -> Result<Outcome, Option<NodeId>> {
        let replica = self.up.get(&id).ok_or_else(|| self.next_up(id))?;
        let result = match op {
            Op::Write(batch) => replica.write(batch).await,
            Op::AddLearner(node, peer) => replica.add_learner(node, peer).await,
            Op::Promote(node) => replica.promote(node).await,
            Op::Remove(node) => replica.remove(node).await,
        };
        match result {
            Ok(log_id) => Ok(Outcome::Acked(log_id)),
            Err(Error::Discarded { .. }) => Ok(Outcome::Unknown),
            Err(Error::NotCaughtUp { .. }) => Ok(Outcome::Rejected),
            Err(Error::NotLeader {
                leader: Some(leader),
            }) => Err(Some(leader)),
            Err(_) => Err(self.next_up(id)),
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
