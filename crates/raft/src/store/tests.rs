use std::io;
use std::sync::Arc;
use std::time::Duration;

use kurator_shard::{Epoch, Shard, ShardId, ShardState, Split};
use openraft::AsyncRuntime as _;
use openraft::raft::VoteRequest;
use openraft_rt_sim::SimRuntime;

use super::Store;
use crate::identity::NodeSecret;
use crate::network::{Connection, Failure, Transport};
use crate::replica::Member;
use crate::rpc::{Message, Request, Response};
use crate::snapshot::SnapshotBytes;
use crate::types::testing::membership;
use crate::types::{LeaderId, LogId, NodeId, Peer, SnapshotMeta, StoredMembership, Vote};

crate::declare_config!(Test, SimRuntime);

/// Reaches no one.
#[derive(Debug, Clone, Copy)]
struct Nowhere;

impl Transport<Test> for Nowhere {
    type Connection = Self;

    fn connect(&self, _target: NodeId, _peer: &Peer) -> Self {
        *self
    }
}

impl Connection<Test> for Nowhere {
    async fn call(
        &mut self,
        _message: Message<Test>,
        _deadline: Duration,
    ) -> Result<Response<Test>, Failure> {
        Err(Failure::Unreachable(io::Error::other("nowhere")))
    }
}

async fn open(root: &std::path::Path) -> Store<Test, Nowhere> {
    let member = Member::new(NodeId::new(1), NodeSecret::from_seed([1; 32]));
    let config = Arc::new(openraft::Config::default().validate().unwrap());
    Store::open(member, root, config, Nowhere).await.unwrap()
}

/// The seed a split of the first shard at `m` sows for shard 2.
fn seed() -> SnapshotBytes {
    let (_, right) = ShardState::default()
        .split(&Split::new("m", ShardId::new(2), Epoch::ZERO))
        .unwrap();
    let seeded = LogId::new(
        LeaderId {
            term: 0,
            node_id: NodeId::new(0),
        },
        3,
    );
    let meta = SnapshotMeta {
        last_log_id: Some(seeded),
        last_membership: StoredMembership::new(Some(seeded), membership(&[1, 2], &[])),
    };
    SnapshotBytes::encode(&meta, &right).unwrap()
}

fn vote(shard: ShardId) -> Message<Test> {
    let request = Request::Vote(VoteRequest::new(Vote::new(1, NodeId::new(2)), None));
    Message::new(shard, request)
}

#[test]
fn a_message_opens_an_empty_replica_that_a_seed_then_replaces() {
    SimRuntime::with_seed(1).block_on(async {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path()).await;
        assert_eq!(store.id(), NodeId::new(1), "id");
        assert!(
            format!("{store:?}").contains("member: NodeId(1)"),
            "debug: {store:?}"
        );
        let shard = ShardId::new(2);
        let _response = store.inbox().handle(vote(shard)).await;
        let empty = store.replica(shard).await.unwrap().unwrap();
        assert!(!empty.is_initialized(), "the message opened it empty");

        store
            .shared
            .layout
            .home(ShardId::FIRST)
            .sow(shard, &seed())
            .unwrap();
        assert!(
            !store
                .replicas()
                .iter()
                .any(|replica| replica.is_initialized()),
            "not planted yet"
        );
        store.plant().await.unwrap();
        let planted = store.replica(shard).await.unwrap().unwrap();
        assert!(planted.is_initialized(), "the seed replaced it");
        assert_eq!(
            planted.shard().span().start(),
            b"m",
            "the right half: {}",
            planted.shard()
        );
        store.shutdown().await.unwrap();
    });
}

#[test]
fn a_seed_left_from_before_is_planted_on_open_and_a_held_shard_keeps_its_state() {
    SimRuntime::with_seed(2).block_on(async {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path()).await;
        let shard = ShardId::new(2);
        store
            .shared
            .layout
            .home(ShardId::FIRST)
            .sow(shard, &seed())
            .unwrap();
        store.shutdown().await.unwrap();

        let reopened = open(root.path()).await;
        let replicas = reopened.replicas();
        assert_eq!(replicas.len(), 1, "planted on open");
        let held = replicas.first().unwrap();
        assert!(held.is_initialized(), "from the seed");

        // A second seed for a shard that holds a state already is dropped.
        let other = ShardState::new(
            Shard::new(shard, kurator_storage::Span::FULL, Epoch::ZERO),
            kurator_storage::KvState::default(),
        );
        let meta = SnapshotMeta::default();
        let stale = SnapshotBytes::encode(&meta, &other).unwrap();
        reopened
            .shared
            .layout
            .home(ShardId::FIRST)
            .sow(shard, &stale)
            .unwrap();
        reopened.plant().await.unwrap();
        let kept = reopened.replica(shard).await.unwrap().unwrap();
        assert_eq!(kept.shard().span().start(), b"m", "kept: {}", kept.shard());
        reopened.shutdown().await.unwrap();
    });
}
