//! A simulated cluster: replicas in temporary directories, a simulated
//! network, and the faults a test drives: crashes, restarts and
//! partitions.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kurator_raft::openraft::{self, AsyncRuntime as _, SnapshotPolicy};
use kurator_raft::{Error, LogId, Member, NodeId, Replica, Role, Status};
use kurator_storage::{KvState, WriteBatch};
use kurator_wal::SigningKey;
use openraft_rt_sim::SimRuntime;

use crate::Sim;
use crate::network::{Faults, Links, Network};

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

/// A cluster of replicas on one simulated network.
#[derive(Debug)]
pub struct Cluster {
    root: tempfile::TempDir,
    config: Arc<openraft::Config>,
    network: Network,
    members: BTreeMap<NodeId, Member>,
    /// The replicas that are up. A crashed replica keeps its directory.
    up: BTreeMap<NodeId, Replica<Sim>>,
}

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
        let config = openraft::Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(40),
            max_in_snapshot_log_to_keep: 10,
            purge_batch_size: 1,
            ..openraft::Config::default()
        }
        .validate()?;
        let members = (1..=size)
            .map(|id| {
                let id = NodeId::new(id);
                let key_seed: [u8; 32] = std::array::from_fn(|byte| {
                    u8::try_from(byte).unwrap_or(0) ^ u8::try_from(id.get()).unwrap_or(0)
                });
                (id, Member::new(id, SigningKey::from_seed(key_seed)))
            })
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
        if let Some(first) = cluster.up.values().next() {
            first
                .initialize(ids.iter().copied())
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

    /// The network, to change its faults.
    #[must_use]
    pub const fn network(&self) -> &Network {
        &self.network
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
        let links = Links::new(id, self.network.clone());
        let replica = Replica::open(member, &self.dir(id), Arc::clone(&self.config), links)
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
        SimRuntime::timeout(CLIENT_TIMEOUT, self.route(batch))
            .await
            .unwrap_or(Outcome::Unknown)
    }

    /// Writes `batch` through `id` alone, without following leader hints.
    /// Gives up after three seconds of simulated time.
    pub async fn write_to(&self, id: NodeId, batch: WriteBatch) -> Outcome {
        SimRuntime::timeout(CLIENT_TIMEOUT, self.attempt(id, batch))
            .await
            .map_or(Outcome::Unknown, |attempt| {
                attempt.unwrap_or(Outcome::Rejected)
            })
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

    /// The replica every replica that is up agrees leads, if there is one.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.up
            .values()
            .find(|replica| replica.status().role() == Role::Leader)
            .map(Replica::id)
    }

    /// Restarts every replica, heals the network, makes it reliable, and
    /// waits until every replica applied the same log.
    ///
    /// # Errors
    ///
    /// [`SimError::Unsettled`] when that does not happen within a minute of
    /// simulated time.
    pub async fn settle(&mut self) -> Result<(), SimError> {
        self.heal();
        self.network.set_faults(Faults {
            loss_per_mille: 0,
            ..Faults::default()
        });
        for id in self.down() {
            self.restart(id).await?;
        }
        // A write in the leader's term commits everything before it.
        for _ in 0..60 {
            if let Outcome::Acked(_) = self
                .write(WriteBatch::new().delete(b"settle".to_vec()))
                .await
            {
                break;
            }
            SimRuntime::sleep(Duration::from_millis(500)).await;
        }
        for _ in 0..600 {
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

    /// Tries replicas in turn, following leader hints and waiting
    /// [`ROUTE_BACKOFF`] between tries, until one takes `batch` or
    /// [`ROUTE_TRIES`] are spent.
    async fn route(&self, batch: WriteBatch) -> Outcome {
        let mut target = self.up.keys().next().copied();
        for _ in 0..ROUTE_TRIES {
            let Some(id) = target else {
                break;
            };
            match self.attempt(id, batch.clone()).await {
                Ok(outcome) => return outcome,
                Err(next) => target = next,
            }
            SimRuntime::sleep(ROUTE_BACKOFF).await;
        }
        Outcome::Rejected
    }

    /// Writes `batch` through `id`: the outcome when it took or logged the
    /// write, otherwise the replica to try next.
    async fn attempt(&self, id: NodeId, batch: WriteBatch) -> Result<Outcome, Option<NodeId>> {
        let replica = self.up.get(&id).ok_or_else(|| self.next_up(id))?;
        match replica.write(batch).await {
            Ok(log_id) => Ok(Outcome::Acked(log_id)),
            Err(Error::Discarded { .. }) => Ok(Outcome::Unknown),
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
