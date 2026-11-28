//! The checker against histories built to show each anomaly, so that a
//! check that passes is known to be able to fail, and against histories of
//! serial executions, which it must pass.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeMap;

use oleaph_history::{Anomaly, CycleKind, Dependency, Element, Fate, History, Key, Txn, check};
use proptest::prelude::*;

const X: Key = Key::new(0);
const Y: Key = Key::new(1);

fn list(elements: &[u64]) -> Vec<Element> {
    elements.iter().copied().map(Element::new).collect()
}

/// A transaction of `steps`: a key with `Some` list is a read of it, with
/// `None` and an element an append.
#[derive(Default)]
struct Steps(Txn);

impl Steps {
    fn read(mut self, key: Key, elements: &[u64]) -> Self {
        self.0.read(key, list(elements));
        self
    }

    fn append(mut self, key: Key, element: u64) -> Self {
        self.0.append(key, Element::new(element));
        self
    }
}

fn history(txns: Vec<(Steps, Fate)>) -> History {
    let mut history = History::new();
    for (steps, fate) in txns {
        let _index = history.record(steps.0, fate);
    }
    history
}

fn anomalies(txns: Vec<(Steps, Fate)>) -> Vec<Anomaly> {
    check(&history(txns))
        .expect_err("an anomaly")
        .anomalies()
        .to_vec()
}

fn cycle(txns: Vec<(Steps, Fate)>) -> CycleKind {
    let found = anomalies(txns);
    match found.as_slice() {
        [Anomaly::Cycle(cycle)] => cycle.kind(),
        other => panic!("one cycle: {other:?}"),
    }
}

const C: Fate = Fate::Committed;

#[test]
fn a_serial_history_passes_with_every_dependency() {
    let checked = check(&history(vec![
        (Steps::default().read(X, &[]).append(X, 1), C),
        (Steps::default().read(X, &[1]).append(X, 2), C),
        (Steps::default().read(X, &[1, 2]), C),
        (Steps::default().read(X, &[1]), C),
    ]))
    .unwrap();
    assert_eq!(checked.committed(), 4, "{checked}");
    assert_eq!(checked.dependencies(Dependency::Write), 1, "{checked}");
    assert_eq!(checked.dependencies(Dependency::Read), 3, "{checked}");
    assert_eq!(checked.dependencies(Dependency::Anti), 1, "{checked}");
    assert_eq!(checked.to_string(), "4 committed, 1 ww, 3 wr, 1 rw");
}

#[test]
fn a_write_cycle_is_g0() {
    let kind = cycle(vec![
        (Steps::default().append(X, 1).append(Y, 4), C),
        (Steps::default().append(X, 2).append(Y, 3), C),
        (Steps::default().read(X, &[1, 2]).read(Y, &[3, 4]), C),
    ]);
    assert_eq!(kind, CycleKind::G0);
}

#[test]
fn reading_each_others_writes_is_g1c() {
    let kind = cycle(vec![
        (Steps::default().append(X, 1).read(Y, &[2]), C),
        (Steps::default().append(Y, 2).read(X, &[1]), C),
    ]);
    assert_eq!(kind, CycleKind::G1c);
}

#[test]
fn seeing_half_a_transaction_is_read_skew() {
    let kind = cycle(vec![
        (
            Steps::default()
                .read(X, &[])
                .append(X, 1)
                .read(Y, &[])
                .append(Y, 2),
            C,
        ),
        (Steps::default().read(X, &[]).read(Y, &[2]), C),
    ]);
    assert_eq!(kind, CycleKind::GSingle);
}

#[test]
fn a_cycle_names_its_steps_in_order() {
    let found = anomalies(vec![
        (Steps::default().read(X, &[]).append(X, 1), C),
        (Steps::default().read(X, &[1]).read(Y, &[]).append(Y, 2), C),
        (Steps::default().read(Y, &[2]).read(X, &[]), C),
    ]);
    let [Anomaly::Cycle(cycle)] = found.as_slice() else {
        panic!("one cycle: {found:?}");
    };
    assert_eq!(cycle.kind(), CycleKind::GSingle);
    let txns: Vec<String> = cycle.txns().map(|txn| txn.to_string()).collect();
    assert_eq!(txns, ["T2", "T0", "T1"], "{cycle}");
    assert_eq!(
        cycle.to_string(),
        "G-single cycle: T2 -rw k0-> T0 -wr k0-> T1 -wr k1-> T2"
    );
}

#[test]
fn violations_say_what_is_wrong() {
    let violations = check(&history(vec![
        (Steps::default().append(X, 1), Fate::Aborted),
        (Steps::default().append(X, 2).append(X, 3), C),
        (Steps::default().read(X, &[1]), C),
        (Steps::default().read(X, &[2]), C),
        (Steps::default().read(Y, &[9]).append(Y, 4).read(Y, &[]), C),
        (Steps::default().append(Y, 4), C),
    ]))
    .expect_err("anomalies");
    let expected = [
        "6 anomalies:",
        "  T4 read k1 unlike what it read and wrote",
        "  4 appended twice",
        "  G1a: T2 read 1 in k0 of aborted T0",
        "  G1b: T3 read k0 up to 2, an intermediate version of T1",
        "  T4 read 9 in k1, which nobody wrote",
        "  k0 read as [1] and as [2]",
    ];
    assert_eq!(violations.to_string(), expected.join("\n"));
}

#[test]
fn write_skew_is_g2() {
    let kind = cycle(vec![
        (Steps::default().read(X, &[]).read(Y, &[]).append(X, 1), C),
        (Steps::default().read(X, &[]).read(Y, &[]).append(Y, 2), C),
    ]);
    assert_eq!(kind, CycleKind::G2);
    let found = anomalies(vec![
        (Steps::default().append(X, 1).append(Y, 4), C),
        (Steps::default().append(X, 2).append(Y, 3), C),
        (Steps::default().read(X, &[1, 2]).read(Y, &[3, 4]), C),
    ]);
    assert_eq!(
        found.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["G0 cycle: T0 -ww k0-> T1 -ww k1-> T0"],
        "a write cycle"
    );
}

#[test]
fn a_lost_update_leaves_incompatible_versions() {
    let found = anomalies(vec![
        (Steps::default().read(X, &[]).append(X, 1), C),
        (Steps::default().read(X, &[]).append(X, 2), C),
    ]);
    assert!(
        matches!(found.as_slice(), [Anomaly::Incompatible { key: X, .. }]),
        "{found:?}"
    );
    let forked = anomalies(vec![
        (Steps::default().read(X, &[]).append(X, 1), C),
        (Steps::default().read(X, &[1]).append(X, 2), C),
        (Steps::default().read(X, &[1]).append(X, 3), C),
    ]);
    let said: Vec<String> = forked.iter().map(ToString::to_string).collect();
    assert_eq!(said, ["k0 read as [1, 2] and as [1, 3]"]);
}

#[test]
fn reading_an_aborted_write_is_g1a() {
    let found = anomalies(vec![
        (Steps::default().append(X, 1), Fate::Aborted),
        (Steps::default().read(X, &[1]), C),
    ]);
    assert!(
        matches!(found.as_slice(), [Anomaly::AbortedRead { key: X, .. }]),
        "{found:?}"
    );
}

#[test]
fn reading_an_intermediate_version_is_g1b() {
    let found = anomalies(vec![
        (Steps::default().append(X, 1).append(X, 2), C),
        (Steps::default().read(X, &[1]), C),
    ]);
    assert!(
        matches!(found.as_slice(), [Anomaly::IntermediateRead { key: X, .. }]),
        "{found:?}"
    );
}

#[test]
fn reading_what_nobody_wrote_is_garbage() {
    let found = anomalies(vec![
        (Steps::default().append(Y, 1), C),
        (Steps::default().read(X, &[1]), C),
        (Steps::default().read(X, &[7]), C),
    ]);
    let garbage = found
        .iter()
        .filter(|anomaly| matches!(anomaly, Anomaly::Garbage { key: X, .. }))
        .count();
    assert_eq!(garbage, 2, "{found:?}");
}

#[test]
fn not_reading_ones_own_write_is_internal() {
    let unread = anomalies(vec![(Steps::default().append(X, 1).read(X, &[]), C)]);
    assert!(
        matches!(unread.as_slice(), [Anomaly::Internal { key: X, .. }]),
        "{unread:?}"
    );
    let found = anomalies(vec![
        (Steps::default().append(X, 1), C),
        (Steps::default().read(X, &[]).read(X, &[1]), C),
    ]);
    assert!(
        matches!(found.as_slice(), [Anomaly::Internal { key: X, .. }]),
        "a read that changed: {found:?}"
    );
}

#[test]
fn a_version_that_repeats_an_element_is_repeated() {
    let found = anomalies(vec![
        (Steps::default().append(X, 1), C),
        (Steps::default().read(X, &[1, 1]), C),
    ]);
    assert!(
        matches!(found.as_slice(), [Anomaly::Repeated { key: X, .. }]),
        "{found:?}"
    );
    let said: Vec<String> = found.iter().map(ToString::to_string).collect();
    assert_eq!(said, ["k0 holds 1 twice"]);
}

#[test]
fn appending_an_element_twice_is_malformed() {
    let found = anomalies(vec![
        (Steps::default().append(X, 1), C),
        (Steps::default().append(Y, 1), C),
    ]);
    assert!(
        matches!(found.as_slice(), [Anomaly::Duplicated { .. }]),
        "{found:?}"
    );
}

#[test]
fn an_unknown_outcome_commits_when_it_is_read() {
    let checked = check(&history(vec![
        (Steps::default().append(X, 1), Fate::Unknown),
        (Steps::default().read(X, &[1]), C),
        (Steps::default().append(X, 2), Fate::Unknown),
    ]))
    .unwrap();
    assert_eq!(checked.committed(), 2, "the unread one does not: {checked}");
}

#[test]
fn an_unknown_outcome_read_only_by_an_aborted_one_stays_unknown() {
    let checked = check(&history(vec![
        (Steps::default().append(X, 1), Fate::Unknown),
        (Steps::default().read(X, &[1]), Fate::Aborted),
        (Steps::default().read(X, &[]), C),
    ]))
    .unwrap();
    assert_eq!(checked.committed(), 1, "{checked}");
}

/// One step of a generated transaction: read or append to a key.
#[derive(Debug, Clone, Copy)]
enum Gen {
    Read(u64),
    Append(u64),
}

fn txn() -> impl Strategy<Value = (Vec<Gen>, u8)> {
    let step = prop_oneof![
        (0..3_u64).prop_map(Gen::Read),
        (0..3_u64).prop_map(Gen::Append),
    ];
    (prop::collection::vec(step, 1..5), 0..10_u8)
}

/// Runs `txns` one after another against a model, recording what each
/// saw: a transaction whose roll is 0 aborts, one whose roll is 1 ends
/// unknown, either way.
fn serial(txns: &[(Vec<Gen>, u8)]) -> (History, usize) {
    let mut state: BTreeMap<Key, Vec<Element>> = BTreeMap::new();
    let mut history = History::new();
    let mut next = 0_u64;
    let mut committed = 0_usize;
    for (steps, roll) in txns {
        let mut scratch = state.clone();
        let mut txn = Txn::new();
        for step in steps {
            match *step {
                Gen::Read(key) => {
                    let key = Key::new(key);
                    txn.read(key, scratch.get(&key).cloned().unwrap_or_default());
                }
                Gen::Append(key) => {
                    let key = Key::new(key);
                    next = next.checked_add(1).unwrap();
                    txn.append(key, Element::new(next));
                    scratch.entry(key).or_default().push(Element::new(next));
                }
            }
        }
        let fate = match roll {
            0 => Fate::Aborted,
            1 if next.is_multiple_of(2) => Fate::Unknown,
            1 => {
                state = scratch;
                Fate::Unknown
            }
            _ => {
                state = scratch;
                committed = committed.checked_add(1).unwrap();
                Fate::Committed
            }
        };
        let _index = history.record(txn, fate);
    }
    (history, committed)
}

proptest! {
    #[test]
    fn serial_executions_pass(txns in prop::collection::vec(txn(), 0..24)) {
        let (history, committed) = serial(&txns);
        let checked = check(&history);
        prop_assert!(checked.is_ok(), "{:?}", checked);
        let checked = checked.unwrap();
        prop_assert!(checked.committed() >= committed, "{checked}");
    }
}
