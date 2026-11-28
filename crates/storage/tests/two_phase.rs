//! The two phases of a transaction across shards on one shard's state:
//! what a prepared transaction locks, how a decision applies, and that a
//! decision is made once however often it is told.
#![expect(
    clippy::unwrap_used,
    reason = "test code: a failure should panic with context"
)]

use std::ops::Bound::{Excluded, Included};

use criterion as _;
use oleaph_codec::Reader;
use oleaph_storage::{
    Abort, Collect, Command, Deadline, Decide, Decided, Decision, Draft, KvState, NamespaceId,
    Outcome, Prepare, Primary, TxnId, Version, WriteBatch,
};

/// `key` of the default namespace, as a primary.
fn primary(key: &str) -> Primary {
    Primary::new(NamespaceId::DEFAULT, key)
}
use proptest as _;
use tempfile as _;

/// A state that holds `a = 1` and `b = 2`, both written at version 1.
fn state() -> KvState {
    let mut state = KvState::default();
    let batch = WriteBatch::new().put("a", "1").put("b", "2");
    let _outcome = state.apply(Version::new(1), Command::Kv(batch));
    state
}

/// When the coordinator of every transaction here means to decide it.
const DEADLINE: Deadline = Deadline::new(1_000);

/// The part of the transaction `id` that reads `a` and writes `b`, with
/// `a` as its primary key.
fn part(state: &KvState, id: u128) -> Prepare {
    let mut draft = Draft::new(state.version());
    let _a = draft.get(state, b"a").unwrap();
    draft.put("b", "3");
    draft.into_prepare(TxnId::new(id), primary("a"), DEADLINE)
}

fn decide(id: u128, decision: Decision) -> Command {
    Command::Decide(Decide::new(
        TxnId::new(id),
        primary("a"),
        DEADLINE,
        decision,
    ))
}

fn locked(key: &str, id: u128) -> Outcome {
    Outcome::Aborted(Abort::Locked {
        key: key.as_bytes().to_vec(),
        by: TxnId::new(id),
        primary: primary("a"),
        deadline: DEADLINE,
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
        .map(|prepared| {
            (
                prepared.id(),
                prepared.primary().clone(),
                prepared.deadline(),
            )
        })
        .collect();
    assert_eq!(
        pending,
        [(TxnId::new(7), primary("a"), DEADLINE)],
        "pending"
    );
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
fn a_split_hands_each_decision_to_the_half_of_its_primary_key() {
    let mut state = state();
    let elsewhere = Decide::new(TxnId::new(9), primary("z"), DEADLINE, Decision::Abort);
    let _decided = state.apply(Version::new(2), Command::Decide(elsewhere));
    let _here = state.apply(Version::new(3), decide(8, Decision::Abort));
    let right = state.split_off(b"m");
    assert_eq!(
        right.decided(TxnId::new(9)),
        Some(Decided::Aborted),
        "right"
    );
    assert_eq!(state.decided(TxnId::new(9)), None, "not left");
    assert_eq!(state.decided(TxnId::new(8)), Some(Decided::Aborted), "left");
    assert_eq!(right.decided(TxnId::new(8)), None, "not right");
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
        Command::Prepare(Draft::new(state.version()).into_prepare(
            TxnId::new(1),
            primary(""),
            Deadline::new(u64::MAX),
        )),
        decide(1, Decision::Commit),
        decide(2, Decision::Abort),
    ] {
        let encoded = command.clone().encode().unwrap();
        assert_eq!(Command::decode(encoded.payload()), Ok(command), "roundtrip");
    }
    assert_eq!(TxnId::new(255).to_string(), "tff", "display");
}

/// A step of collecting at `horizon` milliseconds.
const fn fence(horizon: u64) -> Command {
    Command::Collect(Collect::Fence(Deadline::new(horizon)))
}

const fn forget(horizon: u64) -> Command {
    Command::Collect(Collect::Forget(Deadline::new(horizon)))
}

#[test]
fn a_fence_waits_for_the_transactions_prepared_before_it() {
    let mut state = state();
    let _prepared = state.apply(Version::new(2), Command::Prepare(part(&state, 7)));
    let held = Outcome::Aborted(Abort::Held {
        by: TxnId::new(7),
        primary: primary("a"),
        deadline: DEADLINE,
    });
    assert_eq!(state.apply(Version::new(3), fence(1_001)), held, "held");
    assert_eq!(
        state.apply(Version::new(4), fence(1_000)),
        Outcome::Committed(Version::new(4)),
        "a fence at the deadline passes"
    );
    let _aborted = state.apply(Version::new(5), decide(7, Decision::Abort));
    assert_eq!(
        state.apply(Version::new(6), fence(1_001)),
        Outcome::Committed(Version::new(6)),
        "once it is decided"
    );
    assert_eq!(state.version(), Version::new(1), "nothing written");
}

#[test]
fn behind_the_fence_nothing_is_prepared_or_decided_and_decisions_are_forgotten() {
    let mut state = state();
    let _aborted = state.apply(Version::new(2), decide(7, Decision::Abort));
    let _prepared = state.apply(Version::new(3), Command::Prepare(part(&state, 8)));
    let _committed = state.apply(Version::new(4), decide(8, Decision::Commit));
    assert_eq!(
        state.apply(Version::new(5), forget(2_000)),
        Outcome::Committed(Version::new(5)),
        "forgets"
    );
    assert_eq!(
        state.decided(TxnId::new(7)),
        Some(Decided::Aborted),
        "nothing before the fence"
    );
    assert_eq!(state.decisions().count(), 2, "both kept");
    let _fenced = state.apply(Version::new(6), fence(2_000));
    let _lower = state.apply(Version::new(7), fence(500));
    let _forgotten = state.apply(Version::new(8), forget(3_000));
    assert_eq!(state.decided(TxnId::new(7)), None, "forgotten");
    assert_eq!(state.decisions().count(), 0, "none kept");
    assert_eq!(state.decided(TxnId::new(8)), None, "forgotten too");
    let fenced = |id| Outcome::Aborted(Abort::Fenced(TxnId::new(id)));
    assert_eq!(
        state.apply(Version::new(9), Command::Prepare(part(&state, 7))),
        fenced(7),
        "a late prepare"
    );
    assert_eq!(
        state.apply(Version::new(10), decide(8, Decision::Commit)),
        fenced(8),
        "a late decision"
    );
    assert!(!state.is_locked(), "nothing locked");
    assert_eq!(state.decided(TxnId::new(8)), None, "nor recorded");
    let later = |id| {
        let mut draft = Draft::new(state.version());
        draft.put("c", "1");
        draft.into_prepare(TxnId::new(id), primary("c"), Deadline::new(2_000))
    };
    assert_eq!(
        state.apply(Version::new(11), Command::Prepare(later(9))),
        Outcome::Prepared(Version::new(11)),
        "a transaction at the fence"
    );
    let right = state.split_off(b"b");
    assert!(right.is_locked(), "by its primary key");
    let mut right = right;
    assert_eq!(
        right.apply(Version::new(12), Command::Prepare(part(&state, 10))),
        fenced(10),
        "the other half keeps the fence"
    );
}

#[test]
fn the_fence_and_what_it_kept_survive_encoding() {
    let mut state = state();
    let _aborted = state.apply(Version::new(2), decide(7, Decision::Abort));
    let _fenced = state.apply(Version::new(3), fence(2_000));
    let mut out = Vec::new();
    state.encode(&mut out).unwrap();
    let mut decoded = KvState::decode(&mut Reader::new(&out)).unwrap();
    assert_eq!(decoded, state, "roundtrip");
    assert_eq!(
        decoded.apply(Version::new(4), Command::Prepare(part(&state, 8))),
        Outcome::Aborted(Abort::Fenced(TxnId::new(8))),
        "still fenced"
    );
}

#[test]
fn a_deadline_passes_after_its_moment() {
    assert!(!DEADLINE.has_passed(Deadline::new(999)), "before");
    assert!(!DEADLINE.has_passed(DEADLINE), "at");
    assert!(DEADLINE.has_passed(Deadline::new(1_001)), "after");
    assert_eq!(DEADLINE.millis(), 1_000, "millis");
    assert_eq!(DEADLINE.to_string(), "1000ms", "display");
}

#[test]
fn the_primary_key_is_the_first_key_written() {
    let state = state();
    let mut draft = Draft::new(state.version());
    assert_eq!(draft.first_write(), None, "nothing written");
    let _a = draft.get(&state, b"a").unwrap();
    assert_eq!(draft.first_write(), None, "only read");
    draft.put("c", "1");
    draft.delete("b");
    assert_eq!(draft.first_write(), Some(b"b".as_slice()), "in key order");
}
