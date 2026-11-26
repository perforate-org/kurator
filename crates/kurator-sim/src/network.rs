//! A simulated network between replicas: messages are delayed, lost, or
//! stopped by partitions, all decided by one seeded [`Rng`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use kurator_raft::openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use kurator_raft::openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use kurator_raft::openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use kurator_raft::openraft::{AsyncRuntime as _, impls::EmptyNode};
use kurator_raft::{Inbox, NodeId, Snapshot, SnapshotBytes, Vote};
use openraft_rt_sim::SimRuntime;
use parking_lot::Mutex;

use crate::Sim;
use crate::rng::Rng;

/// How unreliable the network is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Faults {
    /// Shortest one-way delay.
    pub min_delay: Duration,
    /// Longest one-way delay.
    pub max_delay: Duration,
    /// Messages lost per thousand, each way.
    pub loss_per_mille: u64,
}

impl Default for Faults {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(20),
            loss_per_mille: 20,
        }
    }
}

/// The network every replica of a simulated cluster talks through. Clones
/// share it.
#[derive(Debug, Clone)]
pub struct Network {
    shared: Arc<Mutex<Shared>>,
}

#[derive(Debug)]
struct Shared {
    rng: Rng,
    faults: Faults,
    /// The replicas that are up, by id.
    inboxes: BTreeMap<NodeId, Inbox<Sim>>,
    /// Links that are cut, from sender to receiver.
    cut: BTreeSet<(NodeId, NodeId)>,
}

/// What happens to one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    Lost,
    Delivered(Duration),
}

impl Network {
    /// A network whose faults are drawn from `seed`.
    #[must_use]
    pub fn new(seed: u64, faults: Faults) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Shared {
                rng: Rng::new(seed),
                faults,
                inboxes: BTreeMap::new(),
                cut: BTreeSet::new(),
            })),
        }
    }

    /// Connects a replica that is up.
    pub fn connect(&self, id: NodeId, inbox: Inbox<Sim>) {
        drop(self.shared.lock().inboxes.insert(id, inbox));
    }

    /// Disconnects a replica that went down.
    pub fn disconnect(&self, id: NodeId) {
        drop(self.shared.lock().inboxes.remove(&id));
    }

    /// Cuts `node` off from every other replica.
    pub fn isolate(&self, node: NodeId, others: impl IntoIterator<Item = NodeId>) {
        let mut shared = self.shared.lock();
        for other in others {
            let _: bool = shared.cut.insert((node, other));
            let _: bool = shared.cut.insert((other, node));
        }
    }

    /// Restores every link.
    pub fn heal(&self) {
        self.shared.lock().cut.clear();
    }

    /// The fate of the next message, either way.
    fn fate(&self) -> Fate {
        let mut shared = self.shared.lock();
        let faults = shared.faults;
        let delay = shared
            .rng
            .between(nanos(faults.min_delay), nanos(faults.max_delay));
        if shared.rng.chance(faults.loss_per_mille) {
            Fate::Lost
        } else {
            Fate::Delivered(Duration::from_nanos(delay))
        }
    }

    /// Whether the link from `from` to `to` is cut.
    fn is_cut(&self, from: NodeId, to: NodeId) -> bool {
        self.shared.lock().cut.contains(&(from, to))
    }

    /// The inbox of `to` when `from` can reach it now.
    fn reachable(&self, from: NodeId, to: NodeId) -> Option<Inbox<Sim>> {
        if self.is_cut(from, to) {
            return None;
        }
        self.shared.lock().inboxes.get(&to).cloned()
    }

    /// Carries one message from `from` to `to` and back: waits out the
    /// delay, checks the link and the target, runs `deliver`, waits out
    /// the delay of the reply and checks the link back.
    async fn carry<T, F, Fut>(
        &self,
        from: NodeId,
        to: NodeId,
        deliver: F,
    ) -> Result<T, Unreachable<Sim>>
    where
        F: FnOnce(Inbox<Sim>) -> Fut,
        Fut: Future<Output = Result<T, Unreachable<Sim>>>,
    {
        let Fate::Delivered(delay) = self.fate() else {
            return Err(unreachable("message lost"));
        };
        SimRuntime::sleep(delay).await;
        let inbox = self
            .reachable(from, to)
            .ok_or_else(|| unreachable("target is down or cut off"))?;
        let reply = deliver(inbox).await?;
        let Fate::Delivered(back) = self.fate() else {
            return Err(unreachable("reply lost"));
        };
        SimRuntime::sleep(back).await;
        if self.is_cut(to, from) {
            return Err(unreachable("reply cut off"));
        }
        Ok(reply)
    }
}

/// The link from one replica to another.
#[derive(Debug, Clone)]
pub struct Link {
    from: NodeId,
    to: NodeId,
    network: Network,
}

/// Opens the links of one replica.
#[derive(Debug, Clone)]
pub struct Links {
    from: NodeId,
    network: Network,
}

impl Links {
    /// The links of `from` over `network`.
    #[must_use]
    pub const fn new(from: NodeId, network: Network) -> Self {
        Self { from, network }
    }
}

impl RaftNetworkFactory<Sim> for Links {
    type Network = Link;

    async fn new_client(&mut self, target: NodeId, _node: &EmptyNode) -> Link {
        Link {
            from: self.from,
            to: target,
            network: self.network.clone(),
        }
    }
}

impl RaftNetworkV2<Sim> for Link {
    type SnapshotData = SnapshotBytes;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<Sim>,
        _option: RPCOption,
    ) -> Result<AppendResult<Sim>, RPCError<Sim>> {
        let reply = self
            .network
            .carry(self.from, self.to, |inbox| async move {
                inbox
                    .append_entries(rpc)
                    .await
                    .map_err(|error| unreachable(&error.to_string()))
            })
            .await?;
        Ok(reply)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<Sim>,
        _option: RPCOption,
    ) -> Result<VoteResponse<Sim>, RPCError<Sim>> {
        let reply = self
            .network
            .carry(self.from, self.to, |inbox| async move {
                inbox
                    .vote(rpc)
                    .await
                    .map_err(|error| unreachable(&error.to_string()))
            })
            .await?;
        Ok(reply)
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<Sim>,
        _option: RPCOption,
    ) -> Result<VoteResponse<Sim>, RPCError<Sim>> {
        let reply = self
            .network
            .carry(self.from, self.to, |inbox| async move {
                inbox
                    .pre_vote(rpc)
                    .await
                    .map_err(|error| unreachable(&error.to_string()))
            })
            .await?;
        Ok(reply)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<Sim>, StreamingError<Sim>> {
        let reply = self
            .network
            .carry(self.from, self.to, |inbox| async move {
                inbox
                    .install_snapshot(vote, snapshot)
                    .await
                    .map_err(|error| unreachable(&error.to_string()))
            })
            .await?;
        Ok(reply)
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn unreachable(why: &str) -> Unreachable<Sim> {
    Unreachable::new(&io::Error::other(why.to_owned()))
}
