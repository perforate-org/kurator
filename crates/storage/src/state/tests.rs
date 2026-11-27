#![expect(
    clippy::shadow_unrelated,
    reason = "test code: each step names its outcome"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use proptest::prelude::*;

use super::{KvState, LoggedState, View};
use crate::batch::WriteBatch;
use crate::command::Command;
use crate::outcome::{Abort, Outcome, Unreadable};
use crate::txn::{KeyRange, ReadSet, Transaction};
use crate::version::{Retention, Version};
use kurator_codec::{DecodeError, Reader};
use kurator_wal::Lsn;

fn logged() -> LoggedState {
    let mut state = LoggedState::default();
    let _first = state.apply(
        Lsn::new(1),
        Command::Kv(WriteBatch::new().put("a", "1").put("b", "2").put("c", "3")),
    );
    let _second = state.apply(
        Lsn::new(2),
        Command::Kv(WriteBatch::new().delete("b").put("a", "9")),
    );
    state
}

fn state() -> KvState {
    logged().state().clone()
}

fn keys<'a>(entries: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Vec<&'a [u8]> {
    entries.map(|(key, _)| key).collect()
}

fn retention(versions: u64) -> Retention {
    Retention::new(NonZeroU64::new(versions).unwrap())
}

fn reading(keys: &[&[u8]], ranges: Vec<KeyRange>) -> ReadSet {
    let keys = keys.iter().map(|key| key.to_vec()).collect();
    ReadSet::new(keys, ranges)
}

fn transaction(read_at: u64, reads: ReadSet, writes: WriteBatch) -> Command {
    Command::Txn(Transaction::new(Version::new(read_at), reads, writes))
}

fn roundtrip(state: &KvState) -> Option<KvState> {
    let mut out = Vec::new();
    state.encode(&mut out).ok()?;
    let mut reader = Reader::new(&out);
    let decoded = KvState::decode(&mut reader).ok()?;
    reader.finish().ok()?;
    Some(decoded)
}

/// A key's versions, oldest first, and the tag of each.
type Versions<'a> = &'a [(u64, u8)];

#[test]
fn batches_apply_in_order() {
    assert_eq!(logged().applied(), Lsn::new(2), "applied");
    let state = state();
    assert_eq!(state.version(), Version::new(2), "version");
    assert_eq!(state.get(b"a"), Some(b"9".as_slice()), "overwritten");
    assert_eq!(state.get(b"b"), None, "deleted");
    assert_eq!((state.len(), state.is_empty()), (2, false), "size");
    assert!(KvState::default().is_empty(), "empty");
}

#[test]
fn older_versions_read_as_they_were() {
    let state = state();
    let before = state.view(Version::new(1)).unwrap();
    assert_eq!(before.at(), Version::new(1), "at");
    assert_eq!(before.get(b"a"), Some(b"1".as_slice()), "old value");
    assert_eq!(before.get(b"b"), Some(b"2".as_slice()), "not yet deleted");
    assert_eq!(
        keys(before.range(Unbounded, Unbounded)).len(),
        3,
        "all three"
    );
    let empty = state.view(Version::ZERO).unwrap();
    assert_eq!(empty.get(b"a"), None, "before every write");
    assert_eq!(
        state.view(Version::new(3)).map(View::at),
        Err(Unreadable::Ahead {
            at: Version::new(3),
            newest: Version::new(2)
        }),
        "ahead"
    );
}

#[test]
fn a_batch_that_writes_a_key_twice_keeps_the_last_write() {
    let mut state = KvState::default();
    let outcome = state.apply(
        Version::new(1),
        Command::Kv(WriteBatch::new().put("k", "1").delete("k").put("j", "1")),
    );
    assert_eq!(outcome, Outcome::Committed(Version::new(1)), "committed");
    assert_eq!(state.get(b"k"), None, "last write");
    assert_eq!(state.get(b"j"), Some(b"1".as_slice()), "other key");
}

#[test]
fn ranges_follow_their_bounds() {
    let state = state();
    assert_eq!(keys(state.range(Unbounded, Unbounded)), [b"a", b"c"], "all");
    assert_eq!(
        keys(state.range(Excluded(b"a"), Unbounded)),
        [b"c"],
        "after a"
    );
    assert_eq!(
        keys(state.range(Included(b"a"), Excluded(b"c"))),
        [b"a"],
        "half open"
    );
    assert_eq!(
        keys(state.range(Included(b"c"), Included(b"a"))),
        Vec::<&[u8]>::new(),
        "reversed"
    );
    assert_eq!(
        keys(state.range(Excluded(b"a"), Excluded(b"a"))),
        Vec::<&[u8]>::new(),
        "empty"
    );
    assert_eq!(
        keys(state.range(Included(b"a"), Excluded(b"a"))),
        Vec::<&[u8]>::new(),
        "empty half open"
    );
}

#[test]
fn a_transaction_commits_when_nothing_it_read_changed() {
    let mut state = state();
    let reads = reading(&[b"c"], vec![KeyRange::new(Included(b"d"), Unbounded)]);
    let outcome = state.apply(
        Version::new(3),
        transaction(1, reads, WriteBatch::new().put("d", "4")),
    );
    assert_eq!(outcome, Outcome::Committed(Version::new(3)), "committed");
    assert_eq!(state.get(b"d"), Some(b"4".as_slice()), "written");
}

#[test]
fn a_transaction_aborts_when_a_key_it_read_changed() {
    let mut state = state();
    let before = state.clone();
    let outcome = state.apply(
        Version::new(3),
        transaction(
            1,
            reading(&[b"c", b"b"], Vec::new()),
            WriteBatch::new().put("d", "4"),
        ),
    );
    assert_eq!(
        outcome,
        Outcome::Aborted(Abort::Conflict {
            key: b"b".to_vec(),
            version: Version::new(2)
        }),
        "the removal of b conflicts"
    );
    assert_eq!(state, before, "nothing written, version unchanged");
}

#[test]
fn a_transaction_aborts_when_a_key_appears_in_a_range_it_scanned() {
    let mut state = state();
    let _insert = state.apply(
        Version::new(3),
        Command::Kv(WriteBatch::new().put("bb", "new")),
    );
    let scanned = vec![KeyRange::new(Excluded(b"a"), Excluded(b"c"))];
    let outcome = state.apply(
        Version::new(4),
        transaction(2, reading(&[], scanned), WriteBatch::new().put("z", "1")),
    );
    assert_eq!(
        outcome,
        Outcome::Aborted(Abort::Conflict {
            key: b"bb".to_vec(),
            version: Version::new(3)
        }),
        "phantom"
    );
    let reversed = vec![KeyRange::new(Included(b"c"), Included(b"a"))];
    let outcome = state.apply(
        Version::new(4),
        transaction(2, reading(&[], reversed), WriteBatch::new().put("z", "1")),
    );
    assert_eq!(outcome, Outcome::Committed(Version::new(4)), "empty range");
}

#[test]
fn a_transaction_aborts_when_its_read_version_is_unreadable() {
    let mut state = KvState::new(retention(2));
    for version in 1..=5 {
        let _write = state.apply(
            Version::new(version),
            Command::Kv(WriteBatch::new().put("k", version.to_le_bytes())),
        );
    }
    assert_eq!(state.horizon(), Version::new(3), "horizon");
    let outcome = state.apply(
        Version::new(6),
        transaction(2, ReadSet::default(), WriteBatch::new().put("j", "1")),
    );
    assert_eq!(
        outcome,
        Outcome::Aborted(Abort::Unreadable(Unreadable::Forgotten {
            at: Version::new(2),
            horizon: Version::new(3)
        })),
        "forgotten"
    );
    let outcome = state.apply(
        Version::new(6),
        transaction(9, ReadSet::default(), WriteBatch::new().put("j", "1")),
    );
    assert_eq!(
        outcome,
        Outcome::Aborted(Abort::Unreadable(Unreadable::Ahead {
            at: Version::new(9),
            newest: Version::new(5)
        })),
        "ahead"
    );
    assert!(state.view(Version::new(2)).is_err(), "forgotten view");
    assert_eq!(
        state.view(Version::new(3)).unwrap().get(b"k"),
        Some(3_u64.to_le_bytes().as_slice()),
        "the horizon reads as it was"
    );
}

#[test]
fn old_versions_and_removed_keys_are_forgotten() {
    let mut state = KvState::new(retention(2));
    let _put = state.apply(
        Version::new(1),
        Command::Kv(WriteBatch::new().put("k", "1")),
    );
    let _delete = state.apply(Version::new(2), Command::Kv(WriteBatch::new().delete("k")));
    for version in 3..=6 {
        let _other = state.apply(
            Version::new(version),
            Command::Kv(WriteBatch::new().put("other", "x")),
        );
    }
    assert!(
        !state.keys.contains_key(b"k".as_slice()),
        "removed key swept"
    );
    let history = &state.keys[b"other".as_slice()];
    assert_eq!(
        history.0.first().map(|(version, _)| *version),
        Some(Version::new(4)),
        "only what the horizon sees"
    );
}

#[test]
fn untouched_keys_are_swept_once_per_window() {
    let mut state = KvState::new(retention(3));
    let _put = state.apply(
        Version::new(1),
        Command::Kv(WriteBatch::new().put("k", "1")),
    );
    let _delete = state.apply(Version::new(2), Command::Kv(WriteBatch::new().delete("k")));
    let swept = |state: &KvState| !state.keys.contains_key(b"k".as_slice());
    for version in 3..=5 {
        let _other = state.apply(
            Version::new(version),
            Command::Kv(WriteBatch::new().put("other", "x")),
        );
    }
    assert_eq!(state.horizon(), Version::new(2), "the removal is behind");
    assert!(!swept(&state), "not swept inside the window");
    let _next = state.apply(
        Version::new(6),
        Command::Kv(WriteBatch::new().put("other", "y")),
    );
    assert!(swept(&state), "swept on entering the next window");
}

#[test]
fn encoding_roundtrips() {
    let state = state();
    assert_eq!(roundtrip(&state), Some(state), "roundtrip");
    let empty = KvState::new(retention(7));
    assert_eq!(roundtrip(&empty), Some(empty), "retention kept");
}

#[test]
fn malformed_states_are_rejected() {
    let encode = |retention: u64, version: u64, keys: &[(&[u8], Versions<'_>)]| {
        let mut out = Vec::new();
        out.extend_from_slice(&retention.to_le_bytes());
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&u64::try_from(keys.len()).unwrap().to_le_bytes());
        for (key, versions) in keys {
            kurator_codec::put_bytes(&mut out, key).unwrap();
            out.extend_from_slice(&u32::try_from(versions.len()).unwrap().to_le_bytes());
            for (version, tag) in *versions {
                out.extend_from_slice(&version.to_le_bytes());
                out.push(*tag);
            }
        }
        // No transactions across shards: no fence, none prepared, none
        // decided.
        out.extend_from_slice(&[0; 16]);
        KvState::decode(&mut Reader::new(&out))
    };
    assert!(encode(1, 2, &[(b"a", &[(1, 0), (2, 0)])]).is_ok(), "valid");
    assert_eq!(encode(0, 2, &[]), Err(DecodeError::Invalid), "no retention");
    assert_eq!(
        encode(1, 2, &[(b"a", &[])]),
        Err(DecodeError::Invalid),
        "empty"
    );
    let unordered: &[(u64, u8)] = &[(2, 0), (2, 0)];
    assert_eq!(
        encode(1, 2, &[(b"a", unordered)]),
        Err(DecodeError::Invalid),
        "versions out of order"
    );
    assert_eq!(
        encode(1, 2, &[(b"a", &[(3, 0)])]),
        Err(DecodeError::Invalid),
        "newer than the state"
    );
    assert_eq!(
        encode(1, 2, &[(b"b", &[(1, 0)]), (b"a", &[(1, 0)])]),
        Err(DecodeError::Invalid),
        "keys out of order"
    );
    assert_eq!(
        encode(1, 2, &[(b"a", &[(1, 7)])]),
        Err(DecodeError::Tag(7)),
        "unknown tag"
    );
}

/// What a test run does next.
#[derive(Debug, Clone)]
enum Step {
    Batch(Vec<(u8, Option<u8>)>),
    Txn {
        behind: u64,
        reads: Vec<u8>,
        scan: Option<(u8, u8)>,
        writes: Vec<(u8, Option<u8>)>,
    },
}

fn writes() -> impl Strategy<Value = Vec<(u8, Option<u8>)>> {
    proptest::collection::vec((0..5_u8, proptest::option::of(any::<u8>())), 1..4)
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        writes().prop_map(Step::Batch),
        (
            0..6_u64,
            proptest::collection::vec(0..5_u8, 0..3),
            proptest::option::of((0..5_u8, 0..5_u8)),
            writes()
        )
            .prop_map(|(behind, reads, scan, writes)| Step::Txn {
                behind,
                reads,
                scan,
                writes
            }),
    ]
}

fn batch(writes: &[(u8, Option<u8>)]) -> WriteBatch {
    writes
        .iter()
        .fold(WriteBatch::new(), |batch, (key, value)| match value {
            Some(value) => batch.put([*key], [*value]),
            None => batch.delete([*key]),
        })
}

/// Every committed version in full, and the keys it wrote.
#[derive(Debug, Default)]
struct Model {
    versions: Vec<(u64, BTreeMap<u8, u8>, BTreeSet<u8>)>,
}

impl Model {
    fn at(&self, at: u64) -> BTreeMap<u8, u8> {
        self.versions
            .iter()
            .rev()
            .find(|(version, _, _)| *version <= at)
            .map(|(_, state, _)| state.clone())
            .unwrap_or_default()
    }

    fn written_after(&self, at: u64, read: impl Fn(u8) -> bool) -> bool {
        self.versions
            .iter()
            .filter(|(version, _, _)| *version > at)
            .any(|(_, _, written)| written.iter().copied().any(&read))
    }

    fn commit(&mut self, version: u64, writes: &[(u8, Option<u8>)]) {
        let mut state = self.at(version);
        for (key, value) in writes {
            match value {
                Some(value) => drop(state.insert(*key, *value)),
                None => drop(state.remove(key)),
            }
        }
        let written = writes.iter().map(|(key, _)| *key).collect();
        self.versions.push((version, state, written));
    }
}

proptest! {
    #[test]
    fn decoding_arbitrary_bytes_never_panics(buf in proptest::collection::vec(any::<u8>(), 0..64)) {
        drop(KvState::decode(&mut Reader::new(&buf)));
    }

    #[test]
    fn range_never_panics(start in proptest::option::of(any::<(bool, u8)>()), end in proptest::option::of(any::<(bool, u8)>())) {
        let state = state();
        let bound = |bound: Option<(bool, u8)>, byte: &[u8; 1]| -> Bound<Vec<u8>> {
            match bound {
                None => Unbounded,
                Some((true, _)) => Included(byte.to_vec()),
                Some((false, _)) => Excluded(byte.to_vec()),
            }
        };
        let (start_key, end_key) = ([start.map_or(0, |(_, b)| b)], [end.map_or(0, |(_, b)| b)]);
        let (start, end) = (bound(start, &start_key), bound(end, &end_key));
        let found = keys(state.range(start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice))).len();
        prop_assert!(found <= state.len(), "subset");
    }

    /// The state against a model that keeps every version in full:
    /// transactions commit exactly when the model finds no write after
    /// their read version to what they read, and every readable version
    /// reads as the model says.
    #[test]
    fn the_state_matches_a_model(kept in 1..5_u64, gaps in proptest::collection::vec(1..3_u64, 24), steps in proptest::collection::vec(step(), 24)) {
        let mut state = KvState::new(retention(kept));
        let mut model = Model::default();
        let mut version = 0_u64;
        for (gap, step) in gaps.into_iter().zip(steps) {
            let newest = state.version().get();
            version = version.checked_add(gap).unwrap();
            let (command, writes, commits) = match step {
                Step::Batch(writes) => (Command::Kv(batch(&writes)), writes, true),
                Step::Txn { behind, reads, scan, writes } => {
                    let read_at = newest.saturating_sub(behind);
                    let range = scan.map(|(start, end)| KeyRange::new(Included(&[start]), Excluded(&[end])));
                    let scanned = |key: u8| scan.is_some_and(|(start, end)| start <= key && key < end);
                    let readable = read_at >= newest.saturating_sub(kept);
                    let commits = readable && !model.written_after(read_at, |key| reads.contains(&key) || scanned(key));
                    let read_set = ReadSet::new(reads.iter().map(|key| vec![*key]).collect(), range.into_iter().collect());
                    (transaction(read_at, read_set, batch(&writes)), writes, commits)
                }
            };
            let outcome = state.apply(Version::new(version), command);
            prop_assert_eq!(matches!(outcome, Outcome::Committed(_)), commits, "decision at {}", version);
            if commits {
                model.commit(version, &writes);
            }
            let newest = state.version();
            for at in state.horizon().get()..=newest.get() {
                let view = state.view(Version::new(at)).unwrap();
                let read: BTreeMap<u8, u8> = view
                    .range(Unbounded, Unbounded)
                    .map(|(key, value)| (key[0], value[0]))
                    .collect();
                prop_assert_eq!(read, model.at(at), "version {}", at);
            }
            if let Some(forgotten) = state.horizon().get().checked_sub(1) {
                prop_assert!(state.view(Version::new(forgotten)).is_err(), "forgotten");
            }
            prop_assert_eq!(roundtrip(&state), Some(state.clone()), "roundtrip");
        }
    }
}

proptest! {
    /// Splitting a state leaves each half reading, at every readable
    /// version, what the whole read for the keys on its side.
    #[test]
    fn splitting_keeps_every_readable_version(
        kept in 1..5_u64,
        batches in proptest::collection::vec(writes(), 1..12),
        at in any::<u8>(),
    ) {
        let mut whole = KvState::new(retention(kept));
        for (version, writes) in (1_u64..).zip(&batches) {
            let _outcome = whole.apply(Version::new(version), Command::Kv(batch(writes)));
        }
        let mut left = whole.clone();
        let right = left.split_off(&[at]);
        let keys = |state: &KvState| -> Vec<Vec<u8>> { state.keys().map(<[u8]>::to_vec).collect() };
        let (below, above): (Vec<_>, Vec<_>) =
            keys(&whole).into_iter().partition(|key| key.as_slice() < [at].as_slice());
        prop_assert_eq!(keys(&left), below, "the keys on the left");
        prop_assert_eq!(keys(&right), above, "the keys on the right");
        for half in [&left, &right] {
            prop_assert_eq!(half.version(), whole.version(), "same version");
            prop_assert_eq!(half.retention(), whole.retention(), "same retention");
        }
        for version in whole.horizon().get()..=whole.version().get() {
            let version = Version::new(version);
            let read = |state: &KvState| -> Vec<(Vec<u8>, Vec<u8>)> {
                state
                    .view(version)
                    .unwrap()
                    .range(Unbounded, Unbounded)
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .collect()
            };
            let (below, above): (Vec<_>, Vec<_>) =
                read(&whole).into_iter().partition(|(key, _)| key.as_slice() < [at].as_slice());
            prop_assert_eq!(read(&left), below, "left at {}", version);
            prop_assert_eq!(read(&right), above, "right at {}", version);
        }
    }
}

#[test]
fn keys_include_the_removed_ones() {
    let mut state = KvState::default();
    let write = WriteBatch::new().put("b", "1").put("a", "1");
    let _put = state.apply(Version::new(1), Command::Kv(write));
    let _removed = state.apply(Version::new(2), Command::Kv(WriteBatch::new().delete("b")));
    let keys: Vec<&[u8]> = state.keys().collect();
    assert_eq!(keys, [b"a".as_slice(), b"b".as_slice()], "in key order");
}

#[test]
fn commands_fit_a_span_by_every_key_they_touch() {
    use crate::span::{End, Span};

    let span = Span::new("b", End::Before(b"m".to_vec())).unwrap();
    assert!(
        Command::Kv(WriteBatch::new().put("b", "1").delete("l")).fits(&span),
        "inside"
    );
    assert!(
        !Command::Kv(WriteBatch::new().put("b", "1").delete("m")).fits(&span),
        "a write outside"
    );
    let txn = |keys: &[&[u8]], ranges: Vec<KeyRange>, writes: WriteBatch| {
        transaction(1, reading(keys, ranges), writes)
    };
    let inside = || KeyRange::new(Included(b"c"), Excluded(b"m"));
    assert!(
        txn(&[b"c"], vec![inside()], WriteBatch::new().put("d", "1")).fits(&span),
        "a transaction inside"
    );
    assert!(
        !txn(&[b"a"], vec![inside()], WriteBatch::new().put("d", "1")).fits(&span),
        "a read outside"
    );
    let beyond = KeyRange::new(Included(b"c"), Unbounded);
    assert!(
        !txn(&[b"c"], vec![beyond], WriteBatch::new().put("d", "1")).fits(&span),
        "a scan outside"
    );
    assert!(
        !txn(&[b"c"], vec![inside()], WriteBatch::new().put("z", "1")).fits(&span),
        "a write outside"
    );
}
