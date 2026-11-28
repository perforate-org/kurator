//! BFT replicas on QUIC.
//!
//! A replica's [`Wire`] is its [`Host`]: it frames each message with the
//! shard's id and queues it for the peer it is for, and a task per peer
//! sends the queue on that peer's persistent connection, one message at a
//! time, on the BFT channel. A message that finds its queue full, or its
//! peer unreachable, is lost: the protocol asks again for what it needs.
//! The peers are the charter's committee's at first; the replica tells
//! its wire of each member a later committee brings, as the change that
//! starts it names them.
//!
//! The server hands what a committee's member sends to the shard's
//! replica, and answers once the replica took it in. Taking a message in
//! writes the replica's journal, so it runs on a blocking thread, as a
//! timer that expires does.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Weak};
use std::time::Duration;

use oleaph_bft::{Core, Host, Journal, Ledger, Message, View};
use oleaph_client::{Call, Reply};
use oleaph_consensus::{NodeId, Peer, Replicated as _};
use oleaph_crypto::VerifyingKey;
use oleaph_raft::Transport as _;
use oleaph_shard::ShardId;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::connection::{Channel, Quic, QuicLink};
use crate::roster::Members;

/// A BFT replica whose host is QUIC.
pub type BftReplica = oleaph_bft::Replica<Wire>;

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
        Arc::new_cyclic(|replica| {
            let wire = Wire {
                node: core.node(),
                shard: self.shard,
                quic: self.quic,
                queues: Mutex::new(BTreeMap::new()),
                timer: Mutex::new(None),
                replica: Weak::clone(replica),
                runtime: self.runtime,
            };
            for (node, peer) in &peers {
                wire.meet(*node, peer);
            }
            oleaph_bft::Replica::start(core, journal, ledger, peers, wire)
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
    /// The node it runs on, which sends itself nothing.
    node: NodeId,
    shard: ShardId,
    quic: Quic,
    /// The queue to each other member of every committee it knows.
    queues: Mutex<BTreeMap<NodeId, mpsc::Sender<Vec<u8>>>>,
    /// The timer armed last.
    timer: Mutex<Option<AbortHandle>>,
    /// The replica the timer expires, once it runs.
    replica: Weak<BftReplica>,
    runtime: Handle,
}

impl Host for Wire {
    fn send(&self, to: NodeId, message: &Message) {
        let Ok(frame) = frame(self.shard, message) else {
            return;
        };
        // A full queue drops the message: the peer is behind or gone.
        if let Some(queue) = self.queues.lock().get(&to) {
            let _queued = queue.try_send(frame);
        }
    }

    /// Opens a queue to `node` at `peer`, in place of any it had: the
    /// queue it replaces closes, and its task ends.
    fn meet(&self, node: NodeId, peer: &Peer) {
        if node == self.node {
            return;
        }
        let (queue, waiting) = mpsc::channel(QUEUE);
        let link = self.quic.connect(node, peer);
        drop(self.runtime.spawn(carry(link, waiting)));
        drop(self.queues.lock().insert(node, queue));
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

    /// Hands the message `frame` holds to its shard's replica, when
    /// `sender` is a member of that shard's committee. Frames that are not
    /// are dropped, as a lost message would be.
    pub(crate) async fn deliver(&self, sender: NodeId, frame: &[u8]) {
        let Some((shard, message)) = unframe(frame) else {
            return;
        };
        // A member of any committee it knows, which may be behind or
        // ahead of this one's.
        let Some(replica) = self
            .replicas
            .get(&shard)
            .filter(|replica| replica.members().peer(sender).is_some())
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
            Some(replica) => Ok(oleaph_client::answer(&**replica, call).await),
            None => Err(call),
        }
    }
}

impl Members for Committees {
    /// The transport key of every member of every committee each replica
    /// knows.
    fn keys(&self) -> BTreeMap<NodeId, VerifyingKey> {
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
}

#[cfg(test)]
mod tests {
    use oleaph_bft::{BlockId, Message};
    use oleaph_shard::ShardId;

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
