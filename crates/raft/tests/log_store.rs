//! The log store against real files: whatever Raft did to the log before a
//! restart, the reopened store reports the same entries, vote and
//! committed position.
#![expect(
    clippy::unwrap_used,
    reason = "test code: a failure should panic with context"
)]

use std::path::Path;

use oleaph_crypto::{Algorithm, SigningKey};
use oleaph_raft::openraft::storage::{IOFlushed, RaftLogStorage};
use oleaph_raft::openraft::{AsyncRuntime as _, EntryPayload, RaftLogReader};
use oleaph_raft::{Entry, LeaderId, LogId, LogStore, NodeId, Vote};
use openraft_rt_sim::SimRuntime;
use proptest as _;

oleaph_raft::declare_config!(
    /// Replicas on the simulator, for driving the log store.
    Test, SimRuntime
);

fn key() -> SigningKey {
    SigningKey::from_seed(Algorithm::Ed25519, [7; 32])
}

fn log_id(term: u64, index: u64) -> LogId {
    LogId::new(
        LeaderId {
            term,
            node_id: NodeId::new(1),
        },
        index,
    )
}

fn blank(term: u64, index: u64) -> Entry {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Blank,
    }
}

/// What a reopened store reports.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    purged: Option<LogId>,
    last: Option<LogId>,
    ids: Vec<LogId>,
    vote: Option<Vote>,
    committed: Option<LogId>,
}

async fn seen(store: &mut LogStore) -> Seen {
    let state = RaftLogStorage::<Test>::get_log_state(store).await.unwrap();
    let entries = RaftLogReader::<Test>::try_get_log_entries(store, ..)
        .await
        .unwrap();
    Seen {
        purged: state.last_purged_log_id,
        last: state.last_log_id,
        ids: entries.iter().map(|entry| entry.log_id).collect(),
        vote: RaftLogReader::<Test>::read_vote(store).await.unwrap(),
        committed: RaftLogStorage::<Test>::read_committed(store).await.unwrap(),
    }
}

async fn append(store: &mut LogStore, entries: Vec<Entry>) {
    RaftLogStorage::<Test>::append(store, entries, IOFlushed::noop())
        .await
        .unwrap();
}

async fn reopened(dir: &Path) -> Seen {
    seen(&mut LogStore::open(dir, key()).unwrap()).await
}

#[test]
fn truncations_purges_votes_and_the_committed_position_survive_a_restart() {
    SimRuntime::with_seed(0).block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::open(dir.path(), key()).unwrap();
        append(&mut store, (0..10).map(|index| blank(1, index)).collect()).await;
        let vote = Vote::new(2, NodeId::new(1));
        RaftLogStorage::<Test>::save_vote(&mut store, &vote)
            .await
            .unwrap();
        // The committed position goes to disk with the next write.
        RaftLogStorage::<Test>::save_committed(&mut store, Some(log_id(1, 5)))
            .await
            .unwrap();
        RaftLogStorage::<Test>::truncate_after(&mut store, Some(log_id(1, 7)))
            .await
            .unwrap();
        append(&mut store, vec![blank(2, 8), blank(2, 9)]).await;
        RaftLogStorage::<Test>::purge(&mut store, log_id(1, 3))
            .await
            .unwrap();
        let expected = Seen {
            purged: Some(log_id(1, 3)),
            last: Some(log_id(2, 9)),
            ids: [(1, 4), (1, 5), (1, 6), (1, 7), (2, 8), (2, 9)]
                .map(|(term, index)| log_id(term, index))
                .to_vec(),
            vote: Some(vote),
            committed: Some(log_id(1, 5)),
        };
        assert_eq!(seen(&mut store).await, expected, "before the restart");
        drop(store);
        assert_eq!(reopened(dir.path()).await, expected, "after the restart");
    });
}

#[test]
fn truncating_everything_leaves_the_purged_position_last() {
    SimRuntime::with_seed(0).block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::open(dir.path(), key()).unwrap();
        append(&mut store, (0..4).map(|index| blank(1, index)).collect()).await;
        RaftLogStorage::<Test>::purge(&mut store, log_id(1, 1))
            .await
            .unwrap();
        RaftLogStorage::<Test>::truncate_after(&mut store, None)
            .await
            .unwrap();
        drop(store);
        let seen = reopened(dir.path()).await;
        assert_eq!(seen.ids, Vec::new(), "no entries");
        assert_eq!(seen.last, Some(log_id(1, 1)), "the purged position is last");
        assert_eq!(seen.committed, None, "nothing committed");
    });
}

#[test]
fn the_committed_position_is_durable_after_the_next_write() {
    SimRuntime::with_seed(0).block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::open(dir.path(), key()).unwrap();
        append(&mut store, vec![blank(1, 0)]).await;
        RaftLogStorage::<Test>::save_committed(&mut store, Some(log_id(1, 0)))
            .await
            .unwrap();
        let vote = Vote::new(1, NodeId::new(1));
        RaftLogStorage::<Test>::save_vote(&mut store, &vote)
            .await
            .unwrap();
        drop(store);
        assert_eq!(
            reopened(dir.path()).await.committed,
            Some(log_id(1, 0)),
            "committed"
        );
    });
}
