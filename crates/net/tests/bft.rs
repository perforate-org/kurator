//! BFT replicas over real QUIC connections on the loopback interface.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use oleaph_bft::{
    Change, ChangeCertificate, Committee, Config, Core, Domain, Endorsement, Epoch, Journal, Ledger,
};
use oleaph_client::{Call, Reach as _, Reply, Request};
use oleaph_consensus::{NodeId, Peer, Replicated as _};
use oleaph_crypto::{KeyPurpose, NodeSecret};
use oleaph_net::{BftReplica, Endpoint, Quic, Server, Store, Wiring};
use oleaph_shard::{NamespaceId, Shard, ShardId, ShardState};
use oleaph_storage::{Command, KvState, Outcome, WriteBatch};
use tempfile::TempDir;

const LOOPBACK: &str = "127.0.0.1:0";

/// The shard the committee decides.
const SHARD: ShardId = ShardId::new(40);

/// The client's seed.
const CLIENT: u64 = 9;

fn secret(id: u64) -> NodeSecret {
    NodeSecret::from_seed([u8::try_from(id).unwrap(); 32])
}

/// One node: its BFT replica, the Raft store it serves beside it, and its
/// server.
struct Node {
    replica: Arc<BftReplica>,
    _server: Server,
    _store: Store,
}

/// Nodes 1 to `last`, bound to the loopback interface, and where each
/// listens.
fn bind(last: u64) -> (BTreeMap<u64, Endpoint>, BTreeMap<NodeId, Peer>) {
    let endpoints: BTreeMap<u64, Endpoint> = (1..=last)
        .map(|id| {
            let endpoint =
                Endpoint::bind(LOOPBACK.parse().unwrap(), &secret(id), BTreeMap::new()).unwrap();
            (id, endpoint)
        })
        .collect();
    let peers = endpoints
        .iter()
        .map(|(id, endpoint)| {
            let key = secret(*id).public_key(KeyPurpose::Transport);
            (
                NodeId::new(*id),
                Peer::new(endpoint.local_addr().unwrap(), key),
            )
        })
        .collect();
    (endpoints, peers)
}

/// The committee of nodes `ids`.
fn committee_of(ids: &[u64]) -> Committee {
    let voters = ids
        .iter()
        .map(|id| {
            let key = secret(*id).public_key(KeyPurpose::Consensus);
            (NodeId::new(*id), key)
        })
        .collect();
    Committee::new(voters).unwrap()
}

/// The first committee: nodes 1 to 4.
const FIRST: [u64; 4] = [1, 2, 3, 4];

/// Starts node `id` on `endpoint`, with the charter's committee
/// [`FIRST`] at `peers`.
async fn start(dir: &TempDir, id: u64, endpoint: Endpoint, peers: &BTreeMap<NodeId, Peer>) -> Node {
    let committee = committee_of(&FIRST);
    let domain = Domain::of(SHARD, &committee);
    let charter: BTreeMap<NodeId, Peer> = peers
        .iter()
        .filter(|(node, _)| FIRST.contains(&node.get()))
        .map(|(node, peer)| (*node, peer.clone()))
        .collect();
    let node = NodeId::new(id);
    let home = dir.path().join(id.to_string());
    let (journal, recovered) =
        Journal::open(home.join("bft"), oleaph_wal::Options::default()).unwrap();
    let state = ShardState::new(
        Shard::whole(NamespaceId::DEFAULT, SHARD),
        KvState::default(),
    );
    let ledger = Ledger::replay(state, &recovered);
    let key = secret(id).signing_key(KeyPurpose::Consensus);
    let config = Config::new(node, key, committee, domain);
    let core = Core::recover(config, recovered);
    let replica = Wiring::new(endpoint.transport(), SHARD)
        .unwrap()
        .start(core, journal, ledger, charter);
    let member = oleaph_raft::Member::new(node, secret(id));
    let raft = Arc::new(oleaph_raft::openraft::Config::default().validate().unwrap());
    let store = Store::open(member, &home, raft, endpoint.transport())
        .await
        .unwrap();
    let client = secret(CLIENT).public_key(KeyPurpose::Transport);
    let server = endpoint
        .with_clients(vec![client])
        .with_bft(BTreeMap::from([(SHARD, Arc::clone(&replica))]))
        .serve(store.inbox())
        .unwrap();
    Node {
        replica,
        _server: server,
        _store: store,
    }
}

/// Starts four nodes whose replicas of [`SHARD`] make one committee.
async fn committee(dir: &TempDir) -> BTreeMap<u64, Node> {
    let (endpoints, peers) = bind(4);
    let mut nodes = BTreeMap::new();
    for (id, endpoint) in endpoints {
        drop(nodes.insert(id, start(dir, id, endpoint, &peers).await));
    }
    nodes
}

fn client() -> Quic {
    Endpoint::bind(LOOPBACK.parse().unwrap(), &secret(CLIENT), BTreeMap::new())
        .unwrap()
        .transport()
}

/// Writes `key` through node `id`, as a client.
async fn put(client: &Quic, nodes: &BTreeMap<u64, Node>, id: u64, key: &str) {
    let peer = nodes[&id]
        .replica
        .members()
        .peer(NodeId::new(id))
        .cloned()
        .unwrap();
    let put = Call::Propose(Command::Kv(WriteBatch::new().put(key, "v")));
    let reply = client
        .reach(NodeId::new(id), &peer, &Request::Shard(SHARD, put))
        .await
        .unwrap();
    assert!(
        matches!(reply, Reply::Applied(Outcome::Committed(_))),
        "{reply:?}"
    );
}

/// Polls until every replica of `nodes` holds `key`.
async fn everywhere(nodes: &BTreeMap<u64, Node>, key: &str) {
    for _ in 0..500 {
        let mut held = 0_usize;
        for node in nodes.values() {
            let value = node
                .replica
                .read(|kv| kv.get(key.as_bytes()).map(<[u8]>::to_vec))
                .await
                .unwrap();
            held = held.saturating_add(usize::from(value.is_some()));
        }
        if held == nodes.len() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{key} never reached every replica");
}

/// How long the test may take: a broken replica hangs instead of
/// failing. A view whose leader is down costs a timeout of a second or
/// two.
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committee_decides_over_quic_with_a_member_down() {
    tokio::time::timeout(TEST_TIMEOUT, decides())
        .await
        .unwrap_or_else(|_| panic!("the test did not finish within {TEST_TIMEOUT:?}"));
}

async fn decides() {
    let dir = tempfile::tempdir().unwrap();
    let mut nodes = committee(&dir).await;
    let client = client();
    put(&client, &nodes, 2, "a").await;
    everywhere(&nodes, "a").await;

    // One of four may fail: the other three go on deciding.
    drop(nodes.remove(&4));
    put(&client, &nodes, 1, "b").await;
    everywhere(&nodes, "b").await;

    // A shard no replica here holds has none to answer.
    let peer = nodes[&1]
        .replica
        .members()
        .peer(NodeId::new(1))
        .cloned()
        .unwrap();
    let elsewhere = Request::Shard(ShardId::new(41), Call::Begin);
    let reply = client
        .reach(NodeId::new(1), &peer, &elsewhere)
        .await
        .unwrap();
    assert!(matches!(reply, Reply::NoReplica), "{reply:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_newcomer_joins_the_next_committee_over_quic() {
    tokio::time::timeout(TEST_TIMEOUT, joins())
        .await
        .unwrap_or_else(|_| panic!("the test did not finish within {TEST_TIMEOUT:?}"));
}

async fn joins() {
    let dir = tempfile::tempdir().unwrap();
    let (mut endpoints, peers) = bind(5);
    let newcomer = endpoints.remove(&5).unwrap();
    let mut nodes = BTreeMap::new();
    for (id, endpoint) in endpoints {
        drop(nodes.insert(id, start(&dir, id, endpoint, &peers).await));
    }
    let client = client();
    put(&client, &nodes, 2, "a").await;

    // The committee lets 4 go and takes 5 in, which it names with where
    // 5 listens.
    let next = [1, 2, 3, 5];
    let change = Change::new(
        Epoch::new(1),
        committee_of(&next),
        peers
            .iter()
            .filter(|(node, _)| next.contains(&node.get()))
            .map(|(node, peer)| (*node, peer.clone()))
            .collect(),
    )
    .unwrap();
    let domain = Domain::of(SHARD, &committee_of(&FIRST));
    let endorsements: Vec<_> = (1..=3)
        .map(|id| {
            let key = secret(id).signing_key(KeyPurpose::Consensus);
            Endorsement::sign(&key, domain, NodeId::new(id), &change)
        })
        .collect();
    let certificate = ChangeCertificate::new(change, endorsements);
    let epoch = nodes[&1].replica.change(certificate).await.unwrap();
    assert_eq!(epoch, Epoch::new(1), "the next epoch");
    drop(nodes.remove(&4));

    // Started with the charter alone, 5 is shown the change, reaches the
    // new committee, and decides with it.
    drop(nodes.insert(5, start(&dir, 5, newcomer, &peers).await));
    for _ in 0..500 {
        if nodes[&5].replica.epoch() == Epoch::new(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(nodes[&5].replica.epoch(), Epoch::new(1), "joined");
    everywhere(&nodes, "a").await;
    put(&client, &nodes, 5, "c").await;
    everywhere(&nodes, "c").await;
}
