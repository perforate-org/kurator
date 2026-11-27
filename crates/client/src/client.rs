//! The client over the network: it learns where the shards are from any
//! node, and finds each shard's leader by following the leaders replicas
//! name.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kurator_consensus::{NodeId, Peer};
use kurator_meta::Registry;
use kurator_shard::{Shard, ShardId, ShardMap};
use kurator_storage::{Deadline, TxnId};
use parking_lot::Mutex;

use crate::remote::{Call, Reach, Reply, Request};
use crate::shards::Shards;

/// How long a client waits for a call on a shard, retries included.
const CALL_TIMEOUT: Duration = Duration::from_secs(3);

/// How many nodes a client tries before it gives up on a call.
const ROUTE_TRIES: usize = 30;

/// How long a client waits before it tries a node no one named.
const ROUTE_BACKOFF: Duration = Duration::from_millis(50);

/// A clock and a timer: the runtime a client runs on.
pub trait Time: Sync {
    /// Now, on the clock that coordinators share.
    fn now(&self) -> Deadline;

    /// Waits `length`.
    fn pause(&self, length: Duration) -> impl Future<Output = ()> + Send;

    /// Runs `future` for at most `limit`: its output, or `None` when it
    /// took longer.
    fn within<F>(
        &self,
        limit: Duration,
        future: F,
    ) -> impl Future<Output = Option<F::Output>> + Send
    where
        F: Future + Send,
        F::Output: Send;
}

/// A client of a cluster, reached through `R` on the runtime `T`.
#[derive(Debug)]
pub struct Client<R, T> {
    reach: R,
    time: T,
    known: Mutex<Known>,
    /// The high half of every transaction id: no other client's.
    origin: u64,
    /// The low half of the last transaction id.
    txns: AtomicU64,
}

/// What a client knows of the cluster.
#[derive(Debug)]
struct Known {
    map: ShardMap,
    peers: BTreeMap<NodeId, Peer>,
    /// The node that last answered for each shard.
    leaders: BTreeMap<ShardId, NodeId>,
}

impl<R: Reach, T: Time> Client<R, T> {
    /// A client that first asks `entry` where the shards are. `origin`
    /// is a number no other client of the cluster uses: it tells this
    /// client's transactions from theirs.
    #[must_use]
    pub const fn new(reach: R, time: T, entry: BTreeMap<NodeId, Peer>, origin: u64) -> Self {
        Self {
            reach,
            time,
            known: Mutex::new(Known {
                map: ShardMap::new(),
                peers: entry,
                leaders: BTreeMap::new(),
            }),
            origin,
            txns: AtomicU64::new(0),
        }
    }

    /// Every shard as this client knows it.
    #[must_use]
    pub fn shards(&self) -> Vec<Shard> {
        self.known.lock().map.iter().cloned().collect()
    }

    /// Asks one node after another where the shards are, until one
    /// answers; whether one did.
    pub async fn learn(&self) -> bool {
        let peers: Vec<(NodeId, Peer)> = self
            .known
            .lock()
            .peers
            .iter()
            .map(|(id, peer)| (*id, peer.clone()))
            .collect();
        for (id, peer) in peers {
            let answer = self
                .time
                .within(CALL_TIMEOUT, self.reach.reach(id, &peer, &Request::Map))
                .await;
            if let Some(Ok(Reply::Map {
                registry,
                peers: all,
            })) = answer
            {
                self.adopt(&registry, all);
                return true;
            }
        }
        false
    }

    /// Routes by `registry` from now on, once it holds every key, and
    /// reaches the nodes at `peers`.
    fn adopt(&self, registry: &Registry, peers: BTreeMap<NodeId, Peer>) {
        let mut known = self.known.lock();
        if let Some(map) = registry.map() {
            known.map = map;
        }
        if !peers.is_empty() {
            known.peers = peers;
        }
    }

    /// The `attempt`th node to try for `shard`: the one that answered
    /// for it last, then each node in turn.
    fn guess(&self, shard: ShardId, attempt: usize) -> Option<(NodeId, Peer)> {
        let known = self.known.lock();
        let last = known.leaders.get(&shard).filter(|_| attempt == 0);
        let (id, peer) = if let Some(id) = last {
            (id, known.peers.get(id)?)
        } else {
            let turn = attempt.checked_rem(known.peers.len())?;
            known.peers.iter().nth(turn)?
        };
        Some((*id, peer.clone()))
    }

    /// `id` and its peer, if this client knows it.
    fn reached(&self, id: NodeId) -> Option<(NodeId, Peer)> {
        Some((id, self.known.lock().peers.get(&id)?.clone()))
    }

    /// Makes `request` on `shard`'s leader, following the leaders that
    /// replicas name, and pausing before a node no one named. A leader
    /// named that this client cannot reach is passed over.
    async fn route(&self, shard: ShardId, request: &Request) -> Option<Reply> {
        let mut named = None;
        for attempt in 0..ROUTE_TRIES {
            let target = named
                .take()
                .and_then(|id| self.reached(id))
                .or_else(|| self.guess(shard, attempt));
            if let Some((id, peer)) = target {
                match self.ask(shard, id, &peer, request).await {
                    Ok(reply) => return Some(reply),
                    Err(leader) => named = leader,
                }
            }
            if named.is_none() {
                self.time.pause(ROUTE_BACKOFF).await;
            }
        }
        None
    }

    /// Makes `request` on `id`, reached at `peer`: its answer for `shard`,
    /// or the leader it named instead, if any.
    async fn ask(
        &self,
        shard: ShardId,
        id: NodeId,
        peer: &Peer,
        request: &Request,
    ) -> Result<Reply, Option<NodeId>> {
        let reply = self
            .reach
            .reach(id, peer, request)
            .await
            .map_err(|_| None)?;
        if let Some(leader) = reply.redirect() {
            return Err(leader);
        }
        let _previous: Option<NodeId> = self.known.lock().leaders.insert(shard, id);
        Ok(reply)
    }
}

impl<R: Reach, T: Time> Shards for Client<R, T> {
    fn locate(&self, key: &[u8]) -> ShardId {
        self.known.lock().map.locate(key).id()
    }

    async fn refresh(&self) -> Vec<ShardId> {
        let _learned: bool = self.learn().await;
        self.shards().iter().map(Shard::id).collect()
    }

    /// A reply that the shard does not hold the key makes the client
    /// learn the shards afresh.
    async fn call(&self, shard: ShardId, call: Call) -> Option<Reply> {
        let request = Request::Shard(shard, call);
        let reply = self
            .time
            .within(CALL_TIMEOUT, self.route(shard, &request))
            .await
            .flatten()?;
        if matches!(reply, Reply::Misrouted(_)) {
            let _learned: bool = self.learn().await;
        }
        Some(reply)
    }

    fn now(&self) -> Deadline {
        self.time.now()
    }

    fn next_txn(&self) -> TxnId {
        let low = self.txns.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let high = u128::from(self.origin).checked_shl(u64::BITS).unwrap_or(0);
        // The low half of `high` is zero, so the sum is exact.
        TxnId::new(high.saturating_add(u128::from(low)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::Future;
    use std::io;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use futures_util::FutureExt as _;
    use kurator_codec::{Reader, put_bytes};
    use kurator_consensus::{NodeId, Peer};
    use kurator_crypto::{Algorithm, SigningKey};
    use kurator_meta::{Placement, Registry};
    use kurator_shard::{Epoch, Shard, ShardId};
    use kurator_storage::{Deadline, End, Span, TxnId, Version};
    use parking_lot::Mutex;

    use super::{Client, ROUTE_TRIES, Time};
    use crate::remote::{Call, Reach, Reply, Request};
    use crate::shards::Shards as _;

    fn peer(id: u8) -> Peer {
        let key = SigningKey::from_seed(Algorithm::Ed25519, [id; 32]).verifying_key();
        Peer::new(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 7000), key)
    }

    fn node(id: u8) -> NodeId {
        NodeId::new(u64::from(id))
    }

    fn peers(ids: &[u8]) -> BTreeMap<NodeId, Peer> {
        ids.iter().map(|id| (node(*id), peer(*id))).collect()
    }

    /// Keys before `m`, and from `m` on.
    fn halves() -> [Shard; 2] {
        let left = Span::new("", End::Before(b"m".to_vec())).unwrap();
        let right = Span::new("m", End::Unbounded).unwrap();
        [
            Shard::new(ShardId::FIRST, left, Epoch::new(1)),
            Shard::new(ShardId::new(2), right, Epoch::new(1)),
        ]
    }

    /// A registry that places each of `shards` on nodes 1, 2 and 3.
    fn registry(shards: &[Shard]) -> Registry {
        let mut out = 3_u64.to_le_bytes().to_vec();
        out.extend_from_slice(&u32::try_from(shards.len()).unwrap().to_le_bytes());
        for shard in shards {
            let replicas = BTreeSet::from([node(1), node(2), node(3)]);
            let mut placement = Vec::new();
            Placement::new(shard.clone(), replicas, 1)
                .encode(&mut placement)
                .unwrap();
            put_bytes(&mut out, &placement).unwrap();
        }
        Registry::decode(&mut Reader::new(&out)).unwrap()
    }

    /// What a node does with a request.
    #[derive(Debug, Clone)]
    enum Node {
        /// It cannot be reached.
        Down,
        /// It does not lead a shard, and names the leader it knows of. It
        /// does tell where the shards are.
        Follows(Option<NodeId>),
        /// It answers: the map, and a begin at version 4 on any shard.
        Leads,
        /// It answers that the shard does not hold the key.
        Moved,
    }

    /// Nodes that do as they are told, and remember who was asked what.
    #[derive(Debug)]
    struct Nodes {
        behaviour: Mutex<BTreeMap<NodeId, Node>>,
        registry: Registry,
        asked: Mutex<Vec<(NodeId, Request)>>,
    }

    impl Nodes {
        fn new(nodes: &[(u8, Node)], registry: Registry) -> Self {
            Self {
                behaviour: Mutex::new(
                    nodes
                        .iter()
                        .map(|(id, node)| (NodeId::new(u64::from(*id)), node.clone()))
                        .collect(),
                ),
                registry,
                asked: Mutex::new(Vec::new()),
            }
        }

        /// The nodes asked, in order, that a request on a shard went to.
        fn calls(&self) -> Vec<u64> {
            self.asked
                .lock()
                .iter()
                .filter(|(_, request)| matches!(request, Request::Shard(..)))
                .map(|(id, _)| id.get())
                .collect()
        }

        /// How many maps were asked for.
        fn maps(&self) -> usize {
            self.asked
                .lock()
                .iter()
                .filter(|(_, request)| matches!(request, Request::Map))
                .count()
        }
    }

    impl Reach for Nodes {
        async fn reach(&self, node: NodeId, _peer: &Peer, request: &Request) -> io::Result<Reply> {
            self.asked.lock().push((node, request.clone()));
            let behaviour = self.behaviour.lock().get(&node).cloned();
            match (behaviour.unwrap_or(Node::Down), request) {
                (Node::Down, _) => Err(io::Error::other("down")),
                (_, Request::Map) => Ok(Reply::Map {
                    registry: self.registry.clone(),
                    peers: peers(&[1, 2, 3]),
                }),
                (Node::Follows(leader), _) => Ok(Reply::NotLeader(leader)),
                (Node::Leads, _) => Ok(Reply::Begun {
                    shard: Shard::first(),
                    at: Version::new(4),
                }),
                (Node::Moved, _) => Ok(Reply::Misrouted(Shard::first())),
            }
        }
    }

    /// A clock stopped at 10 ms that counts its pauses.
    #[derive(Debug, Default)]
    struct Stopped {
        pauses: Mutex<u32>,
    }

    impl Time for Stopped {
        fn now(&self) -> Deadline {
            Deadline::new(10)
        }

        async fn pause(&self, _length: Duration) {
            let mut pauses = self.pauses.lock();
            *pauses = pauses.checked_add(1).unwrap();
        }

        async fn within<F>(&self, _limit: Duration, future: F) -> Option<F::Output>
        where
            F: Future + Send,
            F::Output: Send,
        {
            Some(future.await)
        }
    }

    fn client(nodes: Nodes, entry: &[u8]) -> Client<Nodes, Stopped> {
        Client::new(nodes, Stopped::default(), peers(entry), 7)
    }

    fn begin(client: &Client<Nodes, Stopped>, shard: ShardId) -> Option<Reply> {
        client.call(shard, Call::Begin).now_or_never().flatten()
    }

    #[test]
    fn a_client_learns_the_shards_from_any_node() {
        let nodes = Nodes::new(&[(1, Node::Down), (2, Node::Leads)], registry(&halves()));
        let client = client(nodes, &[1, 2]);
        assert_eq!(client.locate(b"z"), ShardId::FIRST, "one shard at first");
        assert_eq!(client.shards(), [Shard::first()], "the founding map");
        let shards = client.refresh().now_or_never().unwrap();
        assert_eq!(shards, [ShardId::FIRST, ShardId::new(2)], "both halves");
        assert_eq!(client.locate(b"a"), ShardId::FIRST, "left");
        assert_eq!(client.locate(b"z"), ShardId::new(2), "right");
        assert_eq!(
            client.reach.maps(),
            2,
            "asked the first node, then the next"
        );
    }

    #[test]
    fn a_client_keeps_its_map_when_no_node_answers() {
        let nodes = Nodes::new(&[(1, Node::Down)], registry(&halves()));
        let client = client(nodes, &[1]);
        assert!(!client.learn().now_or_never().unwrap(), "no map");
        assert_eq!(client.shards(), [Shard::first()], "the founding map");
    }

    #[test]
    fn a_registry_that_misses_keys_is_not_routed_by() {
        let [left, _] = halves();
        let nodes = Nodes::new(&[(1, Node::Leads)], registry(&[left]));
        let client = client(nodes, &[1]);
        assert!(client.learn().now_or_never().unwrap(), "a map");
        assert_eq!(client.shards(), [Shard::first()], "still the founding map");
    }

    #[test]
    fn a_call_follows_the_leader_named_and_goes_there_next() {
        let nodes = Nodes::new(
            &[
                (1, Node::Follows(Some(node(3)))),
                (2, Node::Follows(Some(node(3)))),
                (3, Node::Leads),
            ],
            registry(&[Shard::first()]),
        );
        let client = client(nodes, &[1]);
        assert!(client.learn().now_or_never().unwrap(), "learned");
        let begun = Reply::Begun {
            shard: Shard::first(),
            at: Version::new(4),
        };
        assert_eq!(begin(&client, ShardId::FIRST), Some(begun.clone()), "begun");
        assert_eq!(client.reach.calls(), [1, 3], "redirected once");
        assert_eq!(
            *client.time.pauses.lock(),
            0,
            "a named leader is asked at once"
        );
        assert_eq!(begin(&client, ShardId::FIRST), Some(begun), "again");
        assert_eq!(client.reach.calls(), [1, 3, 3], "straight to the leader");
    }

    #[test]
    fn a_call_tries_each_replica_and_pauses_before_unnamed_ones() {
        let nodes = Nodes::new(
            &[(1, Node::Down), (2, Node::Follows(None)), (3, Node::Leads)],
            registry(&[Shard::first()]),
        );
        let client = client(nodes, &[3]);
        assert!(client.learn().now_or_never().unwrap(), "learned");
        assert!(begin(&client, ShardId::FIRST).is_some(), "answered");
        assert_eq!(client.reach.calls(), [1, 2, 3], "every replica in turn");
        assert_eq!(*client.time.pauses.lock(), 2, "a pause after each miss");
    }

    #[test]
    fn a_call_on_unknown_nodes_tries_every_peer() {
        let nodes = Nodes::new(
            &[(1, Node::Down), (2, Node::Leads)],
            registry(&[Shard::first()]),
        );
        let client = client(nodes, &[1, 2]);
        assert!(begin(&client, ShardId::new(9)).is_some(), "answered");
        assert_eq!(client.reach.calls(), [1, 2], "the entry nodes in turn");
    }

    #[test]
    fn a_call_no_leader_answers_gives_up() {
        let nodes = Nodes::new(&[(1, Node::Follows(None))], registry(&[Shard::first()]));
        let client = client(nodes, &[1]);
        assert_eq!(begin(&client, ShardId::FIRST), None, "no answer");
        let tries = u32::try_from(ROUTE_TRIES).unwrap();
        assert_eq!(*client.time.pauses.lock(), tries, "every try");
        assert_eq!(client.reach.calls().len(), ROUTE_TRIES, "every try asked");
    }

    #[test]
    fn a_leader_named_that_the_client_cannot_reach_is_skipped() {
        let nodes = Nodes::new(
            &[(1, Node::Follows(Some(node(8)))), (2, Node::Leads)],
            registry(&[Shard::first()]),
        );
        let client = client(nodes, &[1, 2]);
        assert!(begin(&client, ShardId::FIRST).is_some(), "answered");
        assert_eq!(client.reach.calls(), [1, 2], "node 8 has no peer");
    }

    #[test]
    fn a_misrouted_reply_makes_the_client_learn_the_shards() {
        let nodes = Nodes::new(&[(1, Node::Moved)], registry(&halves()));
        let client = client(nodes, &[1]);
        let reply = begin(&client, ShardId::FIRST);
        assert_eq!(reply, Some(Reply::Misrouted(Shard::first())), "misrouted");
        assert_eq!(client.reach.maps(), 1, "learned afresh");
        assert_eq!(client.locate(b"z"), ShardId::new(2), "the split");
    }

    #[test]
    fn transaction_ids_carry_the_origin() {
        let client = client(Nodes::new(&[], registry(&[])), &[]);
        let origin = 7_u128 << 64;
        assert_eq!(client.next_txn(), TxnId::new(origin | 1), "first");
        assert_eq!(client.next_txn(), TxnId::new(origin | 2), "second");
        assert_eq!(client.now(), Deadline::new(10), "the runtime's clock");
    }
}
