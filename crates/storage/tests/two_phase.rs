//! The two phases of a transaction across shards on one shard's state:
//! what a prepared transaction locks, how a decision applies, and that a
//! decision is made once however often it is told.
#![expect(
    clippy::unwrap_used,
    reason = "test code: a failure should panic with context"
)]

use std::ops::Bound::{Excluded, Included};

use criterion as _;
use kurator_codec::Reader;
use kurator_storage::{
    Abort, Command, Decide, Decided, Decision, Draft, KvState, Outcome, Prepare, TxnId, Version,
    WriteBatch,
};
use proptest as _;
use tempfile as _;

/// A state that holds `a = 1` and `b = 2`, both written at version 1.
fn state() -> KvState {
    let mut state = KvState::default();
    let batch = WriteBatch::new().put("a", "1").put("b", "2");
    let _outcome = state.apply(Version::new(1), Command::Kv(batch));
    state
}

/// The part of the transaction `id` that reads `a` and writes `b`, with
/// `a` as its primary key.
fn part(state: &KvState, id: u128) -> Prepare {
    let mut draft = Draft::new(state.version());
    let _a = draft.get(state, b"a").unwrap();
    draft.put("b", "3");
    draft.into_prepare(TxnId::new(id), b"a".to_vec())
}

fn decide(id: u128, decision: Decision) -> Command {
    Command::Decide(Decide::new(TxnId::new(id), b"a".to_vec(), decision))
}

fn locked(key: &str, id: u128) -> Outcome {
    Outcome::Aborted(Abort::Locked {
        key: key.as_bytes().to_vec(),
        by: TxnId::new(id),
        primary: b"a".to_vec(),
    })
}

#[test]
fn a_prepared_transaction_locks_what_it_read_and_writes() {
    let mut state = state();
    let prepare = Command::Prepare(part(&state, 7));
    assert_eq!(
        state.apply(Version::new(2), prepare),
        Outcome::Prepared(Version::new(2)),
        "prepared"
    );
    assert!(state.is_locked(), "locked");
    let pending: Vec<_> = state
        .pending()
        .map(|(id, key)| (id, key.to_vec()))
        .collect();
    assert_eq!(pending, [(TxnId::new(7), b"a".to_vec())], "pending");
    assert_eq!(state.get(b"b"), Some(b"2".as_slice()), "held back");

    let write_read = Command::Kv(WriteBatch::new().put("a", "9"));
    assert_eq!(
        state.apply(Version::new(3), write_read),
        locked("a", 7),
        "read"
    );
    let write_written = Command::Kv(WriteBatch::new().delete("b"));
    assert_eq!(
        state.apply(Version::new(4), write_written),
        locked("b", 7),
        "written"
    );
    let elsewhere = Command::Kv(WriteBatch::new().put("c", "3"));
    assert_eq!(
        state.apply(Version::new(5), elsewhere),
        Outcome::Committed(Version::new(5)),
        "an unlocked key"
    );

    let mut reader = Draft::new(state.version());
    let _b = reader.get(&state, b"b").unwrap();
    reader.put("c", "4");
    let reads_written = Command::Txn(reader.into_transaction().unwrap());
    assert_eq!(
        state.apply(Version::new(6), reads_written),
        locked("b", 7),
        "get"
    );
    let mut scanner = Draft::new(state.version());
    let _entries = scanner
        .range(&state, Excluded(b"a"), Included(b"b"))
        .unwrap();
    scanner.put("c", "4");
    let scans_written = Command::Txn(scanner.into_transaction().unwrap());
    assert_eq!(
        state.apply(Version::new(7), scans_written),
        locked("b", 7),
        "range"
    );
    let mut sharer = Draft::new(state.version());
    let _a = sharer.get(&state, b"a").unwrap();
    sharer.put("c", "5");
    let shares_a_read = Command::Txn(sharer.into_transaction().unwrap());
    assert_eq!(
        state.apply(Version::new(8), shares_a_read),
        Outcome::Committed(Version::new(8)),
        "reads do not conflict with reads"
    );
    let mut splitter = state.clone();
    let right = splitter.split_off(b"a");
    assert!(right.is_locked() && !splitter.is_locked(), "by primary key");
}

#[test]
fn a_commit_applies_the_writes_held_back_once() {
    let mut state = state();
    let _prepared = state.apply(Version::new(2), Command::Prepare(part(&state, 7)));
    assert_eq!(
        state.apply(Version::new(3), decide(7, Decision::Commit)),
        Outcome::Committed(Version::new(3)),
        "committed"
    );
    assert_eq!(state.get(b"b"), Some(b"3".as_slice()), "written");
    assert!(!state.is_locked(), "unlocked");
    assert_eq!(
        state.decided(TxnId::new(7)),
        Some(Decided::Committed(Version::new(3))),
        "recorded"
    );
    for (version, command) in [
        (4, decide(7, Decision::Commit)),
        (5, decide(7, Decision::Abort)),
        (6, Command::Prepare(part(&state, 7))),
    ] {
        assert_eq!(
            state.apply(Version::new(version), command),
            Outcome::Committed(Version::new(3)),
            "told again at {version}"
        );
    }
    assert!(!state.is_locked(), "a late prepare locks nothing");
    assert_eq!(state.version(), Version::new(3), "nothing more written");
}

#[test]
fn an_abort_drops_the_writes_and_outlasts_a_late_prepare() {
    let mut state = state();
    let rolled_back = Outcome::Aborted(Abort::RolledBack(TxnId::new(7)));
    let _prepared = state.apply(Version::new(2), Command::Prepare(part(&state, 7)));
    assert_eq!(
        state.apply(Version::new(3), decide(7, Decision::Abort)),
        rolled_back,
        "aborted"
    );
    assert_eq!(state.get(b"b"), Some(b"2".as_slice()), "not written");
    assert!(!state.is_locked(), "unlocked");
    let late = Command::Prepare(part(&state, 7));
    assert_eq!(state.apply(Version::new(4), late), rolled_back, "late");
    assert!(!state.is_locked(), "still unlocked");
    assert_eq!(
        state.apply(Version::new(5), decide(8, Decision::Commit)),
        Outcome::Aborted(Abort::RolledBack(TxnId::new(8))),
        "a commit of a part never prepared"
    );
    assert_eq!(
        state.decided(TxnId::new(8)),
        Some(Decided::Aborted),
        "aborted"
    );
}

#[test]
fn a_prepare_checks_its_reads_and_the_locks() {
    let mut state = state();
    let outdated = part(&state, 7);
    let _changed = state.apply(
        Version::new(2),
        Command::Kv(WriteBatch::new().put("a", "5")),
    );
    assert!(
        matches!(
            state.apply(Version::new(3), Command::Prepare(outdated)),
            Outcome::Aborted(Abort::Conflict { .. })
        ),
        "a read changed"
    );
    let _prepared = state.apply(Version::new(4), Command::Prepare(part(&state, 7)));
    assert_eq!(
        state.apply(Version::new(5), Command::Prepare(part(&state, 7))),
        Outcome::Prepared(Version::new(5)),
        "the same part again"
    );
    assert_eq!(
        state.apply(Version::new(6), Command::Prepare(part(&state, 8))),
        locked("b", 7),
        "another transaction's lock"
    );
}

#[test]
fn locks_and_decisions_survive_encoding() {
    let mut state = state();
    let _prepared = state.apply(Version::new(2), Command::Prepare(part(&state, 7)));
    let _decided = state.apply(Version::new(3), decide(8, Decision::Abort));
    let mut out = Vec::new();
    state.encode(&mut out).unwrap();
    let decoded = KvState::decode(&mut Reader::new(&out)).unwrap();
    assert_eq!(decoded, state, "roundtrip");
    assert!(decoded.is_locked(), "still locked");
    assert_eq!(
        decoded.decided(TxnId::new(8)),
        Some(Decided::Aborted),
        "kept"
    );
}

#[test]
fn commands_of_both_phases_roundtrip() {
    let state = state();
    for command in [
        Command::Prepare(part(&state, u128::MAX)),
        Command::Prepare(Draft::new(state.version()).into_prepare(TxnId::new(1), Vec::new())),
        decide(1, Decision::Commit),
        decide(2, Decision::Abort),
    ] {
        let encoded = command.clone().encode().unwrap();
        assert_eq!(Command::decode(encoded.payload()), Ok(command), "roundtrip");
    }
    assert_eq!(TxnId::new(255).to_string(), "tff", "display");
}
