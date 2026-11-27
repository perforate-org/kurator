//! BFT replicas on QUIC.
//!
//! A replica's [`Wire`] is its [`Host`]: it frames each message with the
//! shard's id and queues it for the peer it is for, and a task per peer
//! sends the queue on that peer's persistent connection, one message at a
//! time, on the BFT channel. A message that finds its queue full, or its
//! peer unreachable, is lost: the protocol asks again for what it needs.
//!
//! The server hands what a committee's member sends to the shard's
//! replica, and answers once the replica took it in. Taking a message in
//! writes the replica's journal, so it runs on a blocking thread, as a
//! timer that expires does.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Weak};
use std::time::Duration;

use kurator_bft::{Core, Host, Journal, Ledger, Message, View};
use kurator_client::{Call, Reply};
use kurator_consensus::{NodeId, Peer, Replicated as _};
use kurator_crypto::VerifyingKey;
use kurator_raft::Transport as _;
use kurator_shard::ShardId;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::connection::{Channel, Quic, QuicLink};

/// A BFT replica whose host is QUIC.
pub type BftReplica = kurator_bft::Replica<Wire>;

/// How many messages wait for one peer before more are dropped.
const QUEUE: usize = 1024;

/// How long one message may take to reach a peer and be taken in.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Starts BFT replicas that reach their peers through one [`Quic`].
#[derive(Debug)]
pub struct Wiring {
    quic: Quic,
    shard: ShardId,
    runtime: Handle,
}

impl Wiring {
    /// Wires the replica of `shard` to `quic`, on the Tokio runtime this
    /// is called on.
    ///
    /// # Errors
    ///
    /// When it is not called on a Tokio runtime.
    pub fn new(quic: Quic, shard: ShardId) -> io::Result<Self> {
        let runtime = Handle::try_current().map_err(io::Error::other)?;
        Ok(Self {
            quic,
            shard,
            runtime,
        })
    }

    /// Starts `core`, with the `journal` that recovered it and the
    /// `ledger` of its decided state, reaching the committee's members at
    /// `peers`.
    #[must_use]
    pub fn start(
        self,
        core: Core,
        journal: Journal,
        ledger: Ledger,
        peers: BTreeMap<NodeId, Peer>,
    ) -> Arc<BftReplica> {
        let queues = peers
            .iter()
            .filter(|(node, _)| **node != core.node())
            .map(|(node, peer)| {
                let (queue, waiting) = mpsc::channel(QUEUE);
                let link = self.quic.connect(*node, peer);
                drop(self.runtime.spawn(carry(link, waiting)));
                (*node, queue)
            })
            .collect();
        Arc::new_cyclic(|replica| {
            let wire = Wire {
                shard: self.shard,
                queues,
                timer: Mutex::new(None),
                replica: Weak::clone(replica),
                runtime: self.runtime,
            };
            kurator_bft::Replica::start(core, journal, ledger, peers, wire)
        })
    }
}

/// Sends what `waiting` holds to one peer, in order, until the queue
/// closes with its replica.
async fn carry(mut link: QuicLink, mut waiting: mpsc::Receiver<Vec<u8>>) {
    while let Some(frame) = waiting.recv().await {
        // A message lost is asked for again.
        let _sent = link.call_on(Channel::Bft, &frame, SEND_TIMEOUT).await;
    }
}

/// The QUIC [`Host`] of one BFT replica.
#[derive(Debug)]
pub struct Wire {
    shard: ShardId,
    /// The queue to each other member.
    queues: BTreeMap<NodeId, mpsc::Sender<Vec<u8>>>,
    /// The timer armed last.
    timer: Mutex<Option<AbortHandle>>,
    /// The replica the timer expires, once it runs.
    replica: Weak<BftReplica>,
    runtime: Handle,
}

impl Host for Wire {
    fn send(&self, to: NodeId, message: &Message) {
        let (Some(queue), Ok(frame)) = (self.queues.get(&to), frame(self.shard, message)) else {
            return;
        };
        // A full queue drops the message: the peer is behind or gone.
        let _queued = queue.try_send(frame);
    }

    fn wake(&self, view: View, after: Duration) {
        let replica = Weak::clone(&self.replica);
        let timer = self.runtime.spawn(async move {
            tokio::time::sleep(after).await;
            if let Some(replica) = replica.upgrade() {
                // A replica that panicked expires nothing more.
                let _expired = tokio::task::spawn_blocking(move || replica.expire(view)).await;
            }
        });
        let earlier = self.timer.lock().replace(timer.abort_handle());
        if let Some(earlier) = earlier {
            earlier.abort();
        }
    }
}

/// `message` for the replica of `shard`: the shard's id, then the message.
fn frame(shard: ShardId, message: &Message) -> io::Result<Vec<u8>> {
    let mut frame = shard.get().to_le_bytes().to_vec();
    frame.extend(message.encode()?);
    Ok(frame)
}

/// The shard a frame is for, and its message.
fn unframe(frame: &[u8]) -> Option<(ShardId, Message)> {
    let (shard, message) = frame.split_first_chunk::<8>()?;
    let message = Message::decode(message).ok()?;
    Some((ShardId::new(u64::from_le_bytes(*shard)), message))
}

/// The BFT replicas a node serves, by shard.
#[derive(Debug, Clone, Default)]
pub(crate) struct Committees {
    replicas: BTreeMap<ShardId, Arc<BftReplica>>,
}

impl Committees {
    pub(crate) const fn new(replicas: BTreeMap<ShardId, Arc<BftReplica>>) -> Self {
        Self { replicas }
    }

    /// The transport key of every member of every committee.
    pub(crate) fn keys(&self) -> BTreeMap<NodeId, VerifyingKey> {
        self.replicas
            .values()
            .flat_map(|replica| {
                replica
                    .members()
                    .peers()
                    .map(|(node, peer)| (node, peer.key().clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Hands the message `frame` holds to its shard's replica, when
    /// `sender` is a member of that shard's committee. Frames that are not
    /// are dropped, as a lost message would be.
    pub(crate) async fn deliver(&self, sender: NodeId, frame: &[u8]) {
        let Some((shard, message)) = unframe(frame) else {
            return;
        };
        let Some(replica) = self
            .replicas
            .get(&shard)
            .filter(|replica| replica.members().votes(sender))
        else {
            return;
        };
        let replica = Arc::clone(replica);
        // A replica that panicked takes nothing more in.
        let _received = tokio::task::spawn_blocking(move || replica.receive(sender, message)).await;
    }

    /// Answers `call` from the replica of `shard`, if this node holds one.
    pub(crate) async fn answer(&self, shard: ShardId, call: Call) -> Result<Reply, Call> {
        match self.replicas.get(&shard) {
            Some(replica) => Ok(kurator_client::answer(&**replica, call).await),
            None => Err(call),
        }
    }
}

#[cfg(test)]
mod tests {
    use kurator_bft::{BlockId, Message};
    use kurator_shard::ShardId;

    use super::{frame, unframe};

    #[test]
    fn a_frame_names_its_shard() {
        let message = Message::Fetch(BlockId::new([7; 32]));
        let framed = frame(ShardId::new(9), &message).unwrap();
        assert_eq!(
            unframe(&framed),
            Some((ShardId::new(9), message)),
            "round trip"
        );
        assert_eq!(unframe(&framed[..7]), None, "no shard");
        assert_eq!(unframe(&framed[..8]), None, "no message");
    }
}
