//! Replicas over real QUIC connections on the loopback interface.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kurator_net::{Endpoint, Replica, Server, Store, Tokio};
use kurator_raft::openraft::raft::VoteRequest;
use kurator_raft::openraft::{self, SnapshotPolicy};
use kurator_raft::{
    Connection as _, Failure, KeyPurpose, Member, Message, NodeId, NodeSecret, Peer, Progress,
    Refusal, Request, Response, Role, Transport as _, Vote,
};
use kurator_shard::ShardId;
use kurator_storage::WriteBatch;
use kurator_wal::VerifyingKey;

const LOOPBACK: &str = "127.0.0.1:0";

/// One node's store and its server.
struct Node {
    store: Store,
    peer: Peer,
    server: Server,
    _dir: tempfile::TempDir,
}

fn secret(id: u64) -> NodeSecret {
    NodeSecret::from_seed([u8::try_from(id).unwrap(); 32])
}

fn key(id: u64) -> VerifyingKey {
    secret(id).public_key(KeyPurpose::Transport)
}

fn config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            snapshot_policy: SnapshotPolicy::LogsSinceLast(20),
            max_in_snapshot_log_to_keep: 0,
            ..openraft::Config::default()
        }
        .validate()
        .unwrap(),
    )
}

impl Node {
    /// The node's replica of the first shard, once it holds one.
    fn first(&self) -> Option<Arc<Replica>> {
        self.store
            .replicas()
            .into_iter()
            .find(|replica| replica.shard_id() == ShardId::FIRST)
    }

    fn replica(&self) -> Arc<Replica> {
        self.first().unwrap()
    }
}

/// Starts node `id`, trusting `bootstrap` until it has a membership.
async fn start(id: u64, bootstrap: &[u64]) -> Node {
    let member = Member::new(NodeId::new(id), secret(id));
    let trusted = bootstrap
        .iter()
        .map(|other| (NodeId::new(*other), key(*other)))
        .collect();
    let endpoint = Endpoint::bind(LOOPBACK.parse().unwrap(), member.secret(), trusted).unwrap();
    let peer = member.peer(endpoint.local_addr().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(member, dir.path(), config(), endpoint.transport())
        .await
        .unwrap();
    let server = endpoint.serve(store.inbox()).unwrap();
    Node {
        store,
        peer,
        server,
        _dir: dir,
    }
}

/// Polls `check` every 20 ms for up to ten seconds.
async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..500 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

fn leader(nodes: &BTreeMap<u64, Node>) -> Option<Arc<Replica>> {
    nodes
        .values()
        .filter_map(Node::first)
        .filter(|replica| replica.status().role() == Role::Leader)
        .max_by_key(|replica| replica.status().term())
}

async fn three_nodes() -> BTreeMap<u64, Node> {
    let ids = [1, 2, 3];
    let mut nodes = BTreeMap::new();
    for id in ids {
        drop(nodes.insert(id, start(id, &ids).await));
    }
    let voters: Vec<(NodeId, Peer)> = nodes
        .iter()
        .map(|(id, node)| (NodeId::new(*id), node.peer.clone()))
        .collect();
    nodes[&1].store.found(voters).await.unwrap();
    eventually("a leader", || leader(&nodes).is_some()).await;
    nodes
}

async fn write(nodes: &BTreeMap<u64, Node>, key: &str) {
    for _ in 0..100 {
        if let Some(replica) = leader(nodes)
            && replica.write(WriteBatch::new().put(key, "v")).await.is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader took {key}");
}

/// How long a whole test may take: a broken replica hangs instead of
/// failing, and should fail.
/// A passing run takes well under a second.
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

async fn within(test: impl Future<Output = ()>) {
    tokio::time::timeout(TEST_TIMEOUT, test)
        .await
        .unwrap_or_else(|_| panic!("the test did not finish within {TEST_TIMEOUT:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicas_elect_replicate_and_change_membership_over_quic() {
    within(membership_changes()).await;
}

async fn membership_changes() {
    let mut nodes = three_nodes().await;
    for index in 0..40 {
        write(&nodes, &format!("key-{index}")).await;
    }
    let read = leader(&nodes)
        .unwrap()
        .read(|state| state.get(b"key-39").is_some())
        .await
        .unwrap();
    assert!(read, "a linearizable read sees the write");
    eventually("every replica applies every write", || {
        nodes.values().all(|node| {
            node.replica()
                .read_local(|state| state.kv().get(b"key-39").is_some())
        })
    })
    .await;

    // Node 4 trusts the founders until it learns the membership.
    let newcomer = start(4, &[1, 2, 3]).await;
    let peer = newcomer.peer.clone();
    drop(nodes.insert(4, newcomer));
    let four = NodeId::new(4);
    let _: kurator_raft::LogId = leader(&nodes)
        .unwrap()
        .add_learner(four, peer)
        .await
        .unwrap();
    eventually("node 4 catches up", || {
        leader(&nodes).is_some_and(|replica| replica.progress(four) == Progress::CaughtUp)
    })
    .await;
    let _: kurator_raft::LogId = leader(&nodes).unwrap().promote(four).await.unwrap();
    let status = nodes[&4].replica().status();
    assert!(
        status.snapshot().is_some(),
        "node 4 received a snapshot over QUIC: {status:?}"
    );
    assert!(
        nodes[&4]
            .replica()
            .read_local(|state| state.kv().get(b"key-0").is_some()),
        "node 4 holds the early writes"
    );

    // A follower leaves; the rest, node 4 among them, carry on.
    let leaving = nodes
        .iter()
        .find(|(id, node)| **id != 4 && node.replica().status().role() != Role::Leader)
        .map(|(id, _)| *id)
        .unwrap();
    let _: kurator_raft::LogId = leader(&nodes)
        .unwrap()
        .remove(NodeId::new(leaving))
        .await
        .unwrap();
    let gone = nodes.remove(&leaving).unwrap();
    gone.store.shutdown().await.unwrap();

    // Its key no longer opens a connection to the rest.
    let former =
        Endpoint::bind(LOOPBACK.parse().unwrap(), &secret(leaving), BTreeMap::new()).unwrap();
    let mut link = former.transport().connect(four, &nodes[&4].peer);
    let mut refused = false;
    for _ in 0..100 {
        if link
            .call(vote_from(leaving), Duration::from_secs(2))
            .await
            .is_err()
        {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(refused, "a node that left is cut off");
    write(&nodes, "after").await;
    eventually("node 4 applies the write after the change", || {
        nodes[&4]
            .replica()
            .read_local(|state| state.kv().get(b"after").is_some())
    })
    .await;
    let voters: Vec<NodeId> = leader(&nodes).unwrap().membership().voter_ids().collect();
    assert_eq!(voters.len(), 3, "three voters: {voters:?}");
    assert!(voters.contains(&four), "node 4 votes");
    for node in nodes.into_values() {
        node.store.shutdown().await.unwrap();
    }
}

fn vote_from(sender: u64) -> Message<Tokio> {
    let vote = VoteRequest::new(Vote::new(1, NodeId::new(sender)), None);
    Message::new(ShardId::FIRST, Request::Vote(vote))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_trusted_keys_get_through_and_only_as_themselves() {
    within(authentication()).await;
}

async fn authentication() {
    let target = start(1, &[1, 2]).await;
    let deadline = Duration::from_secs(5);

    // A trusted peer is answered.
    let trusted = Endpoint::bind(LOOPBACK.parse().unwrap(), &secret(2), BTreeMap::new()).unwrap();
    let mut member = trusted.transport().connect(NodeId::new(1), &target.peer);
    let answer = member.call(vote_from(2), deadline).await.unwrap();
    assert!(matches!(answer, Response::Vote(_)), "answered: {answer:?}");

    // The same peer claiming to be someone else is refused.
    let forged = member.call(vote_from(3), deadline).await.unwrap();
    assert!(
        matches!(forged, Response::Refused(Refusal::Forged)),
        "forged: {forged:?}"
    );

    // A key the target does not trust cannot connect.
    let stranger = Endpoint::bind(LOOPBACK.parse().unwrap(), &secret(9), BTreeMap::new()).unwrap();
    let mut unknown = stranger.transport().connect(NodeId::new(1), &target.peer);
    let rejected = unknown.call(vote_from(9), deadline).await;
    assert!(
        matches!(rejected, Err(Failure::Unreachable(_))),
        "stranger: {rejected:?}"
    );

    // A server that does not hold the expected key is not trusted either.
    let impostor = Peer::new(target.peer.addr(), key(3));
    let mut misled = trusted.transport().connect(NodeId::new(1), &impostor);
    let unverified = misled.call(vote_from(2), deadline).await;
    assert!(
        matches!(unverified, Err(Failure::Unreachable(_))),
        "impostor: {unverified:?}"
    );

    // Nothing listens: unreachable, not a hang.
    let closed: SocketAddr = {
        let socket = std::net::UdpSocket::bind(LOOPBACK).unwrap();
        socket.local_addr().unwrap()
    };
    let mut nowhere = trusted
        .transport()
        .connect(NodeId::new(1), &Peer::new(closed, key(1)));
    let silent = nowhere.call(vote_from(2), Duration::from_millis(300)).await;
    assert!(silent.is_err(), "nobody there: {silent:?}");

    // Dropping the server closes the endpoint.
    let Node { store, server, .. } = target;
    drop(server);
    let gone = member.call(vote_from(2), deadline).await;
    assert!(gone.is_err(), "the server is gone: {gone:?}");
    store.shutdown().await.unwrap();
}
