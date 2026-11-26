//! A node's store: its replica of every shard it holds, one Raft group
//! each, behind one transport.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use kurator_shard::{Halves, ShardId};
use openraft::async_runtime::Mutex as _;
use openraft::{AsyncRuntime, RaftTypeConfig};
use parking_lot::Mutex;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::layout::Layout;
use crate::network::{Network, Transport};
use crate::replica::{Member, Replica};
use crate::rpc::{Message, Refusal, Response};
use crate::snapshot::{SnapshotBytes, SnapshotDir};
use crate::types::{NodeId, Peer};

/// The async mutex of `C`'s runtime.
type AsyncMutex<C, T> = <<C as RaftTypeConfig>::AsyncRuntime as AsyncRuntime>::Mutex<T>;

/// The replicas a store holds, by shard.
type Replicas<C> = Mutex<BTreeMap<ShardId, Arc<Replica<C>>>>;

/// The replicas of one node, one per shard, in one directory.
///
/// Every replica sends through the same transport, and every message
/// that arrives names its shard, so one [`Inbox`] serves them all.
///
/// - **Splits.** When a shard splits, each of its replicas sows the new
///   shard's seed, the right half as of the split, and the store plants it
///   as the new shard's first snapshot and opens the new shard's replica.
///   The new shard has the same members, so its replicas elect a leader
///   among themselves and go on from the seed.
/// - **Shards it hears of.** A message for a shard the node does not hold
///   yet opens an empty replica for it, which the leader then sends a
///   snapshot. That is how a node that missed a split, because it received
///   a snapshot taken after it, comes to hold the new shard. If the node
///   then applies the split itself after all, its seed replaces the empty
///   replica, unless the replica received a snapshot first.
#[derive(Debug)]
pub struct Store<C: Config, T> {
    shared: Arc<Shared<C, T>>,
}

struct Shared<C: Config, T> {
    member: Member,
    layout: Layout,
    config: Arc<openraft::Config>,
    transport: T,
    replicas: Arc<Replicas<C>>,
    /// Held while a replica opens or a seed is planted, so that a shard
    /// opens once.
    opening: AsyncMutex<C, ()>,
}

impl<C: Config, T: Transport<C>> Store<C, T> {
    /// Opens the store of `member` in `root`: every replica it holds, and
    /// every seed a split left to plant. Replicas reach their peers through
    /// `transport`.
    ///
    /// # Errors
    ///
    /// When a replica's log or snapshot is damaged, and when Raft cannot
    /// start.
    pub async fn open(
        member: Member,
        root: &Path,
        config: Arc<openraft::Config>,
        transport: T,
    ) -> Result<Self> {
        let layout = Layout::open(root)?;
        let shards = layout.shards()?;
        let store = Self {
            shared: Arc::new(Shared {
                member,
                layout,
                config,
                transport,
                replicas: Arc::default(),
                opening: AsyncMutex::<C, ()>::new(()),
            }),
        };
        {
            let _opening = store.shared.opening.lock().await;
            for shard in shards {
                let _replica: Arc<Replica<C>> = store.shared.open(shard).await?;
            }
        }
        store.shared.plant().await?;
        Ok(store)
    }

    /// Starts a new cluster: opens the replica of [`ShardId::FIRST`],
    /// which holds every key, with `voters` as its first membership. Call
    /// it on one node, once.
    ///
    /// # Errors
    ///
    /// [`Error::Initialized`] when the node already holds a shard.
    pub async fn found(&self, voters: impl IntoIterator<Item = (NodeId, Peer)>) -> Result<()> {
        let first = {
            let _opening = self.shared.opening.lock().await;
            if !self.shared.replicas.lock().is_empty() {
                return Err(Error::Initialized);
            }
            self.shared.open(ShardId::FIRST).await?
        };
        first.initialize(voters).await
    }

    /// The node's replica of `shard`, if it holds one.
    ///
    /// # Errors
    ///
    /// When a seed cannot be planted.
    pub async fn replica(&self, shard: ShardId) -> Result<Option<Arc<Replica<C>>>> {
        self.shared.plant().await?;
        Ok(self.shared.replicas.lock().get(&shard).cloned())
    }

    /// Plants every seed the node's splits sowed, opening the replicas of
    /// the shards they start. Delivering a message, looking a replica up
    /// and splitting plant too.
    ///
    /// # Errors
    ///
    /// When a seed cannot be planted.
    pub async fn plant(&self) -> Result<()> {
        self.shared.plant().await
    }

    /// Every replica the node holds now, by shard.
    #[must_use]
    pub fn replicas(&self) -> Vec<Arc<Replica<C>>> {
        self.shared.replicas.lock().values().cloned().collect()
    }

    /// Splits `shard` at `at` through this node's replica, which must
    /// lead: the keys from `at` on go to the new shard `right`. Returns the
    /// two halves once the split applied here and the new shard's replica
    /// opened.
    ///
    /// `right` must name no shard that ever existed.
    ///
    /// # Errors
    ///
    /// [`Error::Misrouted`] when the node holds no replica of `shard`, or
    /// the split does not apply to it as it is now; otherwise as
    /// [`Replica::write`].
    pub async fn split(&self, shard: ShardId, at: &[u8], right: ShardId) -> Result<Halves> {
        let replica = self
            .replica(shard)
            .await?
            .ok_or(Error::NoReplica { shard })?;
        let halves = replica.split(at, right).await?;
        self.shared.plant().await?;
        Ok(halves)
    }

    /// The id of the node.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.shared.member.id()
    }

    /// Where messages from other nodes are delivered.
    #[must_use]
    pub fn inbox(&self) -> Inbox<C, T> {
        Inbox {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Stops every replica. Everything they acknowledged is already
    /// durable, so stopping and crashing leave the same files behind.
    ///
    /// # Errors
    ///
    /// When a Raft core panicked.
    pub async fn shutdown(self) -> Result<()> {
        let replicas: Vec<Arc<Replica<C>>> = {
            let _opening = self.shared.opening.lock().await;
            std::mem::take(&mut *self.shared.replicas.lock())
                .into_values()
                .collect()
        };
        for replica in replicas {
            replica.shutdown().await?;
        }
        Ok(())
    }
}

impl<C: Config, T: fmt::Debug> fmt::Debug for Shared<C, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("member", &self.member.id())
            .field("layout", &self.layout)
            .field("transport", &self.transport)
            .field("replicas", &self.replicas)
            .finish_non_exhaustive()
    }
}

impl<C: Config, T: Transport<C>> Shared<C, T> {
    /// Opens the replica of `shard` from its home, empty if it has none,
    /// and adds it to the store. The caller holds `opening`.
    async fn open(&self, shard: ShardId) -> Result<Arc<Replica<C>>> {
        let network = Network::new(self.transport.clone(), shard);
        let home = self.layout.home(shard);
        let replica =
            Replica::open(&self.member, shard, home, Arc::clone(&self.config), network).await?;
        let replica = Arc::new(replica);
        drop(self.replicas.lock().insert(shard, Arc::clone(&replica)));
        Ok(replica)
    }

    /// Plants every seed sown since the last call.
    async fn plant(&self) -> Result<()> {
        let seeds = self.layout.seeds()?;
        if seeds.is_empty() {
            return Ok(());
        }
        let _opening = self.opening.lock().await;
        for (shard, seed) in seeds {
            self.plant_one(shard, &seed).await?;
        }
        Ok(())
    }

    /// Plants `seed` as the first snapshot of `shard` and opens its
    /// replica, unless the replica already holds a state. An empty replica
    /// that a message opened is stopped first, and opened again on the
    /// seed. The caller holds `opening`.
    async fn plant_one(&self, shard: ShardId, seed: &SnapshotBytes) -> Result<()> {
        let open = self.replicas.lock().get(&shard).cloned();
        if let Some(replica) = open {
            if replica.is_initialized() {
                return self.layout.discard(shard);
            }
            drop(self.replicas.lock().remove(&shard));
            replica.shutdown().await?;
        }
        let snapshots = SnapshotDir::open(&self.layout.home(shard).snapshot())?;
        if snapshots.load()?.is_none() {
            snapshots.store(seed)?;
        }
        self.layout.discard(shard)?;
        let _replica: Arc<Replica<C>> = self.open(shard).await?;
        Ok(())
    }

    /// The replica `message` is for, opened empty if the node does not
    /// hold it yet.
    async fn recipient(&self, message: &Message<C>) -> Result<Arc<Replica<C>>> {
        self.plant().await?;
        let shard = message.shard();
        let held = self.replicas.lock().get(&shard).cloned();
        if let Some(replica) = held {
            return Ok(replica);
        }
        let _opening = self.opening.lock().await;
        let open = self.replicas.lock().get(&shard).cloned();
        match open {
            Some(replica) => Ok(replica),
            None => self.open(shard).await,
        }
    }
}

/// Delivers messages from other nodes to the replicas of one store. Clones
/// deliver to the same store.
#[derive(Debug)]
pub struct Inbox<C: Config, T> {
    shared: Arc<Shared<C, T>>,
}

impl<C: Config, T> Clone for Inbox<C, T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<C: Config, T: Transport<C>> Inbox<C, T> {
    /// Handles `message` from another node.
    pub async fn handle(&self, message: Message<C>) -> Response<C> {
        match self.shared.recipient(&message).await {
            Ok(replica) => replica.handle(message.into_request()).await,
            Err(_) => Response::Refused(Refusal::Stopped),
        }
    }

    /// The memberships of the store's replicas, as they change.
    #[must_use]
    pub fn memberships(&self) -> Memberships<C> {
        Memberships {
            replicas: Arc::clone(&self.shared.replicas),
        }
    }
}

/// The memberships of one store's replicas: who may send to the store.
/// It knows nothing of the transport, so the transport can consult it.
/// Clones read the same store.
#[derive(Debug)]
pub struct Memberships<C: Config> {
    replicas: Arc<Replicas<C>>,
}

impl<C: Config> Clone for Memberships<C> {
    fn clone(&self) -> Self {
        Self {
            replicas: Arc::clone(&self.replicas),
        }
    }
}

impl<C: Config> Memberships<C> {
    /// Every node in the membership of some replica, committed or not.
    #[must_use]
    pub fn nodes(&self) -> BTreeMap<NodeId, Peer> {
        let replicas: Vec<Arc<Replica<C>>> = self.replicas.lock().values().cloned().collect();
        let mut nodes = BTreeMap::new();
        for replica in replicas {
            for (id, peer) in replica.membership().nodes() {
                let _known: Option<Peer> = nodes.insert(*id, peer.clone());
            }
        }
        nodes
    }
}

#[cfg(test)]
mod tests;
