//! A simulated network between replicas: messages are delayed, lost, or
//! stopped by partitions, all decided by one seeded [`Rng`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use kurator_codec::DecodeError;
use kurator_raft::openraft::AsyncRuntime as _;
use kurator_raft::{
    Connection, Failure, Inbox, Message, NodeId, Peer, Refusal, Response, Transport,
};
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
    inboxes: BTreeMap<NodeId, Inbox<Sim, Endpoint>>,
    /// Links that are cut, from sender to receiver.
    cut: BTreeSet<(NodeId, NodeId)>,
    /// The first message that did not decode: a bug, never a fault.
    garbled: Option<Garbled>,
}

/// A message that did not decode on the other side of a [`Wire`]. The
/// simulator never damages messages, so this is an encoding bug, and the
/// cluster stops making progress instead of retrying for minutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a message from {from} to {to} did not decode: {error}")]
pub struct Garbled {
    from: NodeId,
    to: NodeId,
    error: DecodeError,
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
                garbled: None,
            })),
        }
    }

    /// Connects a replica that is up.
    pub fn connect(&self, id: NodeId, inbox: Inbox<Sim, Endpoint>) {
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

    /// The first message that did not decode, if one did not.
    #[must_use]
    pub fn garbled(&self) -> Option<Garbled> {
        self.shared.lock().garbled
    }

    fn garble(&self, from: NodeId, to: NodeId, error: DecodeError) {
        let mut shared = self.shared.lock();
        let _: &mut Garbled = shared.garbled.get_or_insert(Garbled { from, to, error });
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
    fn reachable(&self, from: NodeId, to: NodeId) -> Option<Inbox<Sim, Endpoint>> {
        if self.is_cut(from, to) {
            return None;
        }
        self.shared.lock().inboxes.get(&to).cloned()
    }

    /// Carries one message from `from` to `to` and back: waits out the
    /// delay, checks the link and the target, runs `deliver`, waits out
    /// the delay of the reply and checks the link back.
    async fn carry<T, F, Fut>(&self, from: NodeId, to: NodeId, deliver: F) -> Result<T, Failure>
    where
        F: FnOnce(Inbox<Sim, Endpoint>) -> Fut,
        Fut: Future<Output = T>,
    {
        let Fate::Delivered(delay) = self.fate() else {
            return Err(lost("message lost"));
        };
        SimRuntime::sleep(delay).await;
        let inbox = self
            .reachable(from, to)
            .ok_or_else(|| lost("target is down or cut off"))?;
        let reply = deliver(inbox).await;
        let Fate::Delivered(back) = self.fate() else {
            return Err(lost("reply lost"));
        };
        SimRuntime::sleep(back).await;
        if self.is_cut(to, from) {
            return Err(lost("reply cut off"));
        }
        Ok(reply)
    }
}

/// One replica's end of the network: it opens a [`Wire`] to each peer.
#[derive(Debug, Clone)]
pub struct Endpoint {
    from: NodeId,
    network: Network,
}

impl Endpoint {
    /// The end of `from` on `network`.
    #[must_use]
    pub const fn new(from: NodeId, network: Network) -> Self {
        Self { from, network }
    }
}

/// The wire from one replica to another. Messages cross it encoded, as
/// they cross a real network.
#[derive(Debug, Clone)]
pub struct Wire {
    from: NodeId,
    to: NodeId,
    network: Network,
}

impl Transport<Sim> for Endpoint {
    type Connection = Wire;

    fn connect(&self, target: NodeId, _peer: &Peer) -> Wire {
        Wire {
            from: self.from,
            to: target,
            network: self.network.clone(),
        }
    }
}

impl Connection<Sim> for Wire {
    async fn call(
        &mut self,
        message: Message<Sim>,
        deadline: Duration,
    ) -> Result<Response<Sim>, Failure> {
        let bytes = message.encode().map_err(Failure::Unreachable)?;
        let (from, to, network) = (self.from, self.to, self.network.clone());
        let carried = self.network.carry(from, to, |inbox| async move {
            let response = match Message::decode(&bytes) {
                Ok(decoded) => inbox.handle(decoded).await,
                Err(error) => {
                    network.garble(from, to, error);
                    Response::Refused(Refusal::Malformed)
                }
            };
            response.encode()
        });
        let reply = SimRuntime::timeout(deadline, carried)
            .await
            .map_err(|_| Failure::Timeout)??;
        Response::decode(&reply).map_err(|error| {
            self.network.garble(to, from, error);
            Failure::Unreachable(io::Error::other(error))
        })
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn lost(why: &str) -> Failure {
    Failure::Unreachable(io::Error::other(why.to_owned()))
}
