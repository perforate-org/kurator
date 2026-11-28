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

use oleaph_bft::{Committee, Config, Core, Domain, Journal, Ledger};
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

/// Starts four nodes whose replicas of [`SHARD`] make one committee.
async fn committee(dir: &TempDir) -> BTreeMap<u64, Node> {
    let ids = 1..=4_u64;
    let endpoints: BTreeMap<u64, Endpoint> = ids
        .clone()
        .map(|id| {
            let endpoint =
                Endpoint::bind(LOOPBACK.parse().unwrap(), &secret(id), BTreeMap::new()).unwrap();
            (id, endpoint)
        })
        .collect();
    let peers: BTreeMap<NodeId, Peer> = endpoints
        .iter()
        .map(|(id, endpoint)| {
            let key = secret(*id).public_key(KeyPurpose::Transport);
            (
                NodeId::new(*id),
                Peer::new(endpoint.local_addr().unwrap(), key),
            )
        })
        .collect();
    let voters = ids
        .clone()
        .map(|id| {
            let key = secret(id).public_key(KeyPurpose::Consensus);
            (NodeId::new(id), key)
        })
        .collect();
    let committee = Committee::new(voters).unwrap();
    let domain = Domain::of(SHARD, &committee);
    let mut nodes = BTreeMap::new();
    for (id, endpoint) in endpoints {
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
        let config = Config::new(node, key, committee.clone(), domain);
        let core = Core::recover(config, recovered);
        let replica = Wiring::new(endpoint.transport(), SHARD).unwrap().start(
            core,
            journal,
            ledger,
            peers.clone(),
        );
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
        drop(nodes.insert(
            id,
            Node {
                replica,
                _server: server,
                _store: store,
            },
        ));
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
