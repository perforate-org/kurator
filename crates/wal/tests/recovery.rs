//! Opening, appending, crash repair and purging, against real files.
#![expect(
    clippy::unwrap_used,
    clippy::let_underscore_untyped,
    clippy::shadow_unrelated,
    reason = "test code: a failure should panic with context"
)]

use std::convert::Infallible;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use oleaph_wal::{Corruption, Error, InvalidFrame, Lsn, Options, Start, TailDamage, Wal};
use proptest as _;

type Records = Vec<(u64, Vec<u8>)>;

fn open(dir: &Path, after: u64) -> Result<(Wal, Records, oleaph_wal::RecoveryReport), Error> {
    open_with(dir, Options::default(), after)
}

fn open_with(
    dir: &Path,
    options: Options,
    after: u64,
) -> Result<(Wal, Records, oleaph_wal::RecoveryReport), Error> {
    let mut records = Vec::new();
    let (wal, report) = Wal::open(
        dir,
        options,
        Start::After(Lsn::new(after)),
        |lsn, payload| {
            records.push((lsn.get(), payload.to_vec()));
            Ok::<(), Infallible>(())
        },
    )?;
    Ok((wal, records, report))
}

fn small_segments() -> Options {
    let mut options = Options::default();
    options.segment_size = 64;
    options
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .collect();
    paths.sort();
    paths
}

fn append_bytes(path: &Path, bytes: &[u8]) {
    OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}

#[test]
fn a_new_log_starts_after_the_given_position() {
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, records, report) = open(dir.path(), 0).unwrap();
    assert!(records.is_empty(), "nothing to replay");
    assert_eq!(report.truncated_tail, None, "nothing to repair");
    assert_eq!(wal.next_lsn(), Lsn::new(1), "first record is 1");
    let range = wal.append_batch([b"a", b"b"]).unwrap().unwrap();
    assert_eq!((range.first().get(), range.last().get()), (1, 2), "batch");
    assert_eq!(
        wal.append_batch(Vec::<Vec<u8>>::new()).unwrap(),
        None,
        "empty batch"
    );
}

#[test]
fn records_survive_reopening_in_order() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open(dir.path(), 0).unwrap();
        let _ = wal.append_batch([b"one".as_slice()]).unwrap();
        let _ = wal.append_batch([b"two".as_slice(), b"three"]).unwrap();
    }
    let (mut wal, records, report) = open(dir.path(), 0).unwrap();
    assert_eq!(
        records,
        [
            (1, b"one".to_vec()),
            (2, b"two".to_vec()),
            (3, b"three".to_vec())
        ],
        "replay"
    );
    assert_eq!(report.replayed, 3, "count");
    assert_eq!(wal.next_lsn(), Lsn::new(4), "continues");
    let _ = wal.append_batch([b"four"]).unwrap();
    drop(wal);
    let (_, records, _) = open(dir.path(), 3).unwrap();
    assert_eq!(records, [(4, b"four".to_vec())], "only after 3");
}

#[test]
fn a_torn_final_record_is_cut_off() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open(dir.path(), 0).unwrap();
        let _ = wal.append_batch([b"kept"]).unwrap();
    }
    let segment = segments(dir.path()).remove(0);
    let valid_len = fs::metadata(&segment).unwrap().len();
    append_bytes(&segment, &[7, 0, 0, 0, 1, 2]);

    let (mut wal, records, report) = open(dir.path(), 0).unwrap();
    assert_eq!(records, [(1, b"kept".to_vec())], "valid prefix replayed");
    let tail = report.truncated_tail.unwrap();
    assert_eq!(
        tail.damage,
        TailDamage::Frame(InvalidFrame::TruncatedHeader),
        "damage"
    );
    assert_eq!((tail.offset, tail.discarded_bytes), (valid_len, 6), "cut");
    assert_eq!(
        fs::metadata(&segment).unwrap().len(),
        valid_len,
        "file truncated"
    );

    let _ = wal.append_batch([b"after"]).unwrap();
    drop(wal);
    let (_, records, report) = open(dir.path(), 0).unwrap();
    assert_eq!(records.len(), 2, "new record follows the cut");
    assert_eq!(report.truncated_tail, None, "clean now");
}

#[test]
fn a_damaged_final_record_is_cut_off() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open(dir.path(), 0).unwrap();
        let _ = wal.append_batch([b"kept".as_slice()]).unwrap();
        let _ = wal.append_batch([b"lost".as_slice()]).unwrap();
    }
    let segment = segments(dir.path()).remove(0);
    let mut bytes = fs::read(&segment).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&segment, bytes).unwrap();

    let (_, records, report) = open(dir.path(), 0).unwrap();
    assert_eq!(records, [(1, b"kept".to_vec())], "valid prefix replayed");
    assert_eq!(
        report.truncated_tail.unwrap().damage,
        TailDamage::Frame(InvalidFrame::ChecksumMismatch),
        "damage"
    );
}

#[test]
fn a_segment_with_a_torn_header_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open_with(dir.path(), small_segments(), 0).unwrap();
        let _ = wal.append_batch([[1_u8; 64]]).unwrap();
    }
    let torn = dir.path().join("00000000000000000002.wal");
    fs::write(&torn, b"KRW").unwrap();

    let (mut wal, records, report) = open_with(dir.path(), small_segments(), 0).unwrap();
    assert_eq!(records.len(), 1, "first segment replayed");
    assert_eq!(
        report.truncated_tail.unwrap().damage,
        TailDamage::Header,
        "damage"
    );
    assert_eq!(wal.next_lsn(), Lsn::new(2), "continues");
    let _ = wal.append_batch([b"x"]).unwrap();
    drop(wal);
    let (_, records, _) = open_with(dir.path(), small_segments(), 0).unwrap();
    assert_eq!(records.len(), 2, "segment recreated");
}

#[test]
fn damage_in_an_older_segment_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open_with(dir.path(), small_segments(), 0).unwrap();
        for _ in 0..3 {
            let _ = wal.append_batch([[1_u8; 64]]).unwrap();
        }
    }
    let first = segments(dir.path()).remove(0);
    let mut bytes = fs::read(&first).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&first, bytes).unwrap();

    let error = open_with(dir.path(), small_segments(), 0).unwrap_err();
    assert!(
        matches!(
            error,
            Error::Corrupt {
                reason: Corruption::Frame(InvalidFrame::ChecksumMismatch),
                offset: 16,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn segments_roll_and_purge_keeps_what_is_needed() {
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _, _) = open_with(dir.path(), small_segments(), 0).unwrap();
    for _ in 0..4 {
        let _ = wal.append_batch([[1_u8; 64]]).unwrap();
    }
    assert_eq!(segments(dir.path()).len(), 4, "one record per segment");
    assert_eq!(
        wal.purge_through(Lsn::new(2)).unwrap(),
        2,
        "records 1 and 2"
    );
    assert_eq!(wal.purge_through(Lsn::new(2)).unwrap(), 0, "idempotent");
    assert_eq!(wal.purge_through(Lsn::new(100)).unwrap(), 1, "active kept");
    drop(wal);

    assert!(
        matches!(
            open_with(dir.path(), small_segments(), 0),
            Err(Error::Gap { first, .. }) if first == Lsn::new(4)
        ),
        "records 1..=3 are gone"
    );
    let (_, records, _) = open_with(dir.path(), small_segments(), 3).unwrap();
    assert_eq!(records.len(), 1, "record 4");
}

#[test]
fn a_log_that_ends_before_the_snapshot_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open(dir.path(), 0).unwrap();
        let _ = wal.append_batch([b"a"]).unwrap();
    }
    assert!(
        matches!(open(dir.path(), 5), Err(Error::Behind { end, .. }) if end == Lsn::new(2)),
        "snapshot at 5, log ends at 1"
    );
}

#[test]
fn a_missing_segment_in_the_middle_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open_with(dir.path(), small_segments(), 0).unwrap();
        for _ in 0..3 {
            let _ = wal.append_batch([[1_u8; 64]]).unwrap();
        }
    }
    fs::remove_file(&segments(dir.path())[1]).unwrap();
    assert!(
        matches!(
            open_with(dir.path(), small_segments(), 0),
            Err(Error::Corrupt {
                reason: Corruption::LsnMismatch { .. },
                ..
            })
        ),
        "record 2 is missing"
    );
}

#[test]
fn replay_errors_stop_opening() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open(dir.path(), 0).unwrap();
        let _ = wal.append_batch([b"a"]).unwrap();
    }
    let result = Wal::open(
        dir.path(),
        Options::default(),
        Start::After(Lsn::ZERO),
        |_, _| Err::<(), _>(std::io::Error::other("bad record")),
    );
    assert!(
        matches!(result, Err(Error::Replay { lsn, .. }) if lsn == Lsn::new(1)),
        "callback error surfaces"
    );
}

#[test]
fn oversized_payloads_are_rejected_before_writing() {
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _, _) = open(dir.path(), 0).unwrap();
    let huge = vec![0_u8; oleaph_wal::MAX_PAYLOAD_LEN + 1];
    assert!(
        matches!(wal.append_batch([huge]), Err(Error::PayloadTooLarge { .. })),
        "rejected"
    );
    assert_eq!(wal.next_lsn(), Lsn::new(1), "nothing consumed");
    let _ = wal.append_batch([b"ok"]).unwrap();
}

#[test]
fn a_full_length_header_that_is_wrong_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    drop(open(dir.path(), 0).unwrap());
    fs::write(&segments(dir.path())[0], [0xAB; 16]).unwrap();
    assert!(
        matches!(
            open(dir.path(), 0),
            Err(Error::Corrupt {
                reason: Corruption::BadHeader,
                ..
            })
        ),
        "a torn header is shorter than a header"
    );
}

#[test]
fn a_torn_header_in_an_older_segment_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _, _) = open_with(dir.path(), small_segments(), 0).unwrap();
        for _ in 0..2 {
            let _ = wal.append_batch([[1_u8; 64]]).unwrap();
        }
    }
    fs::write(&segments(dir.path())[0], b"KRW").unwrap();
    assert!(
        matches!(
            open_with(dir.path(), small_segments(), 0),
            Err(Error::Corrupt {
                reason: Corruption::BadHeader,
                ..
            })
        ),
        "only the newest segment can be torn"
    );
}

#[test]
fn purging_tolerates_segments_that_are_already_gone() {
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _, _) = open_with(dir.path(), small_segments(), 0).unwrap();
    for _ in 0..3 {
        let _ = wal.append_batch([[1_u8; 64]]).unwrap();
    }
    fs::remove_file(&segments(dir.path())[0]).unwrap();
    assert_eq!(wal.purge_through(Lsn::new(2)).unwrap(), 2, "both counted");
    assert_eq!(segments(dir.path()).len(), 1, "active kept");
}
