//! Durability and crash recovery of a whole database.
#![expect(
    clippy::unwrap_used,
    clippy::shadow_unrelated,
    reason = "test code: a failure should panic with context"
)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::thread;

use criterion as _;
use kurator_storage::{Db, Error, Lsn, Options, WriteBatch};
use kurator_wal::{InvalidFrame, TailDamage};
use proptest::prelude::*;
use tempfile::TempDir;

/// What a crash leaves: a byte-for-byte copy of the directory, taken while
/// the database is still open.
fn crash_image(dir: &Path) -> TempDir {
    let image = tempfile::tempdir().unwrap();
    copy_dir(dir, image.path());
    image
}

fn copy_dir(from: &Path, to: &Path) {
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            fs::create_dir_all(&target).unwrap();
            copy_dir(&entry.path(), &target);
        } else {
            let _bytes: u64 = fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn newest_segment(dir: &Path) -> std::path::PathBuf {
    let mut segments: Vec<_> = fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    segments.sort();
    segments.pop().unwrap()
}

fn tear_tail(dir: &Path, garbage: &[u8]) {
    OpenOptions::new()
        .append(true)
        .open(newest_segment(dir))
        .unwrap()
        .write_all(garbage)
        .unwrap();
}

fn contents(db: &Db) -> BTreeMap<Vec<u8>, Vec<u8>> {
    db.read(|state| {
        state
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect()
    })
}

#[test]
fn acknowledged_writes_survive_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.put("a", "1").unwrap(), Lsn::new(1), "first record");
    let _: Lsn = db
        .write(WriteBatch::new().put("b", "2").delete("a"))
        .unwrap();
    let image = crash_image(dir.path());
    drop(db);

    let recovered = Db::open(image.path(), Options::default()).unwrap();
    assert_eq!(recovered.get(b"a"), None, "deleted");
    assert_eq!(
        recovered.get(b"b").as_deref(),
        Some(b"2".as_slice()),
        "kept"
    );
    assert_eq!(recovered.recovery().replayed, 2, "replayed from the log");
}

#[test]
fn a_torn_write_at_the_crash_is_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let _: Lsn = db.put("a", "1").unwrap();
    let image = crash_image(dir.path());
    tear_tail(image.path(), &[9, 0, 0, 0, 0xAB]);

    let recovered = Db::open(image.path(), Options::default()).unwrap();
    assert_eq!(
        recovered.get(b"a").as_deref(),
        Some(b"1".as_slice()),
        "kept"
    );
    let tail = recovered.recovery().truncated_tail.as_ref().unwrap();
    assert_eq!(
        tail.damage,
        TailDamage::Frame(InvalidFrame::TruncatedHeader),
        "damage"
    );
    assert_eq!(
        recovered.put("b", "2").unwrap(),
        Lsn::new(2),
        "log continues"
    );
}

#[test]
fn checkpoints_bound_the_log_and_recover() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.wal.segment_size = 256;
    {
        let db = Db::open(dir.path(), options.clone()).unwrap();
        for i in 0_u32..100 {
            let _: Lsn = db.put(i.to_le_bytes(), [7; 32]).unwrap();
        }
        assert_eq!(db.checkpoint().unwrap(), Lsn::new(100), "snapshot position");
        let _: Lsn = db.delete(0_u32.to_le_bytes()).unwrap();
        let segments = fs::read_dir(dir.path().join("wal")).unwrap().count();
        assert!(segments <= 2, "old segments purged, {segments} left");
    }
    let db = Db::open(dir.path(), options).unwrap();
    assert_eq!(
        db.read(kurator_storage::KvState::len),
        99,
        "snapshot plus log"
    );
    assert_eq!(
        db.recovery().replayed,
        1,
        "only the record after the snapshot"
    );
}

#[test]
fn concurrent_writers_share_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let write_all = |writer: u8| {
        for i in 0_u8..50 {
            let _: Lsn = db.put([writer, i], [i]).unwrap();
        }
    };
    thread::scope(|scope| {
        for writer in 0_u8..8 {
            drop(scope.spawn(move || write_all(writer)));
        }
    });
    assert_eq!(
        db.read(kurator_storage::KvState::len),
        400,
        "every write applied"
    );
    assert_eq!(db.applied(), Lsn::new(400), "every record");
    drop(db);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(
        db.read(kurator_storage::KvState::len),
        400,
        "every write durable"
    );
}

#[test]
fn a_directory_opens_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert!(
        matches!(
            Db::open(dir.path(), Options::default()),
            Err(Error::Locked { .. })
        ),
        "second open"
    );
    drop(db);
    drop(Db::open(dir.path(), Options::default()).unwrap());
}

#[test]
fn invalid_batches_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert!(
        matches!(db.write(WriteBatch::new()), Err(Error::EmptyBatch)),
        "empty"
    );
    assert!(
        matches!(
            db.put("k", vec![0; kurator_wal::MAX_PAYLOAD_LEN]),
            Err(Error::BatchTooLarge)
        ),
        "too large"
    );
    assert_eq!(db.put("k", "v").unwrap(), Lsn::new(1), "nothing consumed");
}

#[test]
fn a_damaged_snapshot_stops_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        let _: Lsn = db.put("k", "v").unwrap();
        let _: Lsn = db.checkpoint().unwrap();
    }
    let snapshot = fs::read_dir(dir.path().join("snapshots"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = fs::read(&snapshot).unwrap();
    bytes[0] ^= 1;
    fs::write(&snapshot, bytes).unwrap();
    assert!(
        matches!(
            Db::open(dir.path(), Options::default()),
            Err(Error::Snapshot { .. })
        ),
        "refuses to guess"
    );
}

/// A step of the model test.
#[derive(Debug, Clone)]
enum Step {
    Put(u8, u8),
    Delete(u8),
    Checkpoint,
    Reopen,
    /// Crash, leaving `garbage` after the last record.
    Crash(Vec<u8>),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        8 => (0_u8..16, any::<u8>()).prop_map(|(key, value)| Step::Put(key, value)),
        3 => (0_u8..16).prop_map(Step::Delete),
        1 => Just(Step::Checkpoint),
        1 => Just(Step::Reopen),
        1 => proptest::collection::vec(any::<u8>(), 0..24).prop_map(Step::Crash),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Whatever happens, the database holds exactly the acknowledged writes.
    #[test]
    fn matches_a_map_across_checkpoints_and_crashes(steps in proptest::collection::vec(step(), 1..40)) {
        let mut options = Options::default();
        options.wal.segment_size = 128;
        let mut dir = tempfile::tempdir().unwrap();
        let mut db = Db::open(dir.path(), options.clone()).unwrap();
        let mut model = BTreeMap::new();
        for step in steps {
            match step {
                Step::Put(key, value) => {
                    let _: Lsn = db.put([key], [value]).unwrap();
                    drop(model.insert(vec![key], vec![value]));
                }
                Step::Delete(key) => {
                    let _: Lsn = db.delete([key]).unwrap();
                    drop(model.remove(&vec![key]));
                }
                Step::Checkpoint => {
                    let _: Lsn = db.checkpoint().unwrap();
                }
                Step::Reopen => {
                    drop(db);
                    db = Db::open(dir.path(), options.clone()).unwrap();
                }
                Step::Crash(garbage) => {
                    let image = crash_image(dir.path());
                    tear_tail(image.path(), &garbage);
                    drop(db);
                    dir = image;
                    db = Db::open(dir.path(), options.clone()).unwrap();
                }
            }
            prop_assert_eq!(&contents(&db), &model);
        }
    }
}
