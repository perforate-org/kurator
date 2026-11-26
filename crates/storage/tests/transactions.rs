//! Transactions on a single-node database: snapshot reads, conflicts, and
//! decisions that survive recovery.
#![expect(
    clippy::unwrap_used,
    clippy::shadow_unrelated,
    reason = "test code: a failure should panic with context"
)]

use std::ops::Bound::{Included, Unbounded};

use criterion as _;
use kurator_storage::{Abort, Db, Error, Options, Version};
use proptest as _;

fn open(dir: &std::path::Path) -> Db {
    Db::open(dir, Options::default()).unwrap()
}

fn number(bytes: Option<Vec<u8>>) -> u64 {
    bytes.map_or(0, |bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
}

/// Adds one to the counter, starting over until the transaction commits.
fn increment(db: &Db) {
    loop {
        let mut txn = db.begin();
        let value = number(txn.get(b"counter").unwrap());
        txn.put("counter", value.checked_add(1).unwrap().to_le_bytes());
        let committed = txn.commit();
        if !matches!(committed, Err(Error::Aborted(_))) {
            let _: Version = committed.unwrap();
            return;
        }
    }
}

#[test]
fn a_transaction_reads_one_version_while_others_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let _: Version = db.put("a", "1").unwrap();
    let mut txn = db.begin();
    assert_eq!(txn.read_at(), Version::new(1), "read version");
    let _: Version = db.put("a", "2").unwrap();
    let _: Version = db.put("b", "2").unwrap();
    assert_eq!(txn.get(b"a").unwrap(), Some(b"1".to_vec()), "old value");
    assert_eq!(
        txn.range(Unbounded, Unbounded).unwrap(),
        [(b"a".to_vec(), b"1".to_vec())],
        "no later key"
    );
    assert_eq!(txn.commit().unwrap(), Version::new(1), "read only");
    assert_eq!(db.applied().get(), 3, "nothing logged");
}

#[test]
fn a_lost_update_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let _: Version = db.put("stock", "1").unwrap();
    let mut first = db.begin();
    let mut second = db.begin();
    for txn in [&mut first, &mut second] {
        assert_eq!(txn.get(b"stock").unwrap(), Some(b"1".to_vec()), "read");
        txn.put("stock", "0");
        txn.put("order", txn.read_at().get().to_le_bytes());
    }
    assert_eq!(first.commit().unwrap(), Version::new(2), "first wins");
    let Err(Error::Aborted(Abort::Conflict { key, version })) = second.commit() else {
        panic!("the second sold the same stock");
    };
    assert_eq!((key, version), (b"stock".to_vec(), Version::new(2)), "why");
    assert_eq!(db.get(b"stock"), Some(b"0".to_vec()), "sold once");
}

#[test]
fn a_key_that_appears_in_a_scanned_range_aborts() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let mut txn = db.begin();
    assert!(
        txn.range(Included(b"user/"), Included(b"user/~"))
            .unwrap()
            .is_empty(),
        "no users yet"
    );
    txn.put("user/first", "admin");
    let _: Version = db.put("user/other", "admin").unwrap();
    assert!(
        matches!(txn.commit(), Err(Error::Aborted(Abort::Conflict { .. }))),
        "two first users"
    );
    assert_eq!(db.get(b"user/first"), None, "nothing written");
}

#[test]
fn concurrent_increments_lose_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let (writers, increments) = (4, 25);
    std::thread::scope(|scope| {
        for _ in 0..writers {
            let _writer = scope.spawn(|| {
                (0..increments).for_each(|_| increment(&db));
            });
        }
    });
    assert_eq!(
        number(db.get(b"counter")),
        writers * increments,
        "every increment"
    );
}

#[test]
fn recovery_replays_the_same_decisions() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path());
        let _: Version = db.put("k", "1").unwrap();
        let mut stale = db.begin();
        let _read = stale.get(b"k").unwrap();
        stale.put("k", "stale");
        let _: Version = db.put("k", "2").unwrap();
        assert!(stale.commit().is_err(), "aborted");
        let _snapshot = db.checkpoint().unwrap();
        let mut fresh = db.begin();
        let _read = fresh.get(b"k").unwrap();
        fresh.put("k", "3");
        let mut late = db.begin();
        let _read = late.get(b"k").unwrap();
        late.put("k", "late");
        let _: Version = fresh.commit().unwrap();
        assert!(late.commit().is_err(), "aborted after the snapshot");
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"k"), Some(b"3".to_vec()), "same value");
    assert_eq!(db.applied().get(), 5, "every record replayed");
    assert_eq!(
        db.read(|state| state
            .view(Version::new(2))
            .unwrap()
            .get(b"k")
            .map(<[u8]>::to_vec)),
        Some(b"2".to_vec()),
        "old versions survive recovery"
    );
}
