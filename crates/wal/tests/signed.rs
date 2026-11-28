//! Signed logs: the hash chain and signatures, against real files.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::shadow_unrelated,
    reason = "test code: a failure should panic with context"
)]

use std::convert::Infallible;
use std::fs;
use std::path::{Path, PathBuf};

use oleaph_crypto::{Algorithm, SigningKey};
use oleaph_wal::{
    Corruption, Digest, Error, Integrity, Lsn, Options, Protection, SealDamage, Start, TailDamage,
    Wal,
};
use proptest as _;

/// Bytes in front of the first frame of a signed segment.
const HEADER_LEN: usize = 48;
/// Bytes in front of a frame's payload.
const FRAME_HEADER_LEN: usize = 16;

fn key(seed: u8) -> SigningKey {
    SigningKey::from_seed(Algorithm::Ed25519, [seed; 32])
}

fn signed(seed: u8) -> Options {
    let mut options = Options::default();
    options.integrity = Integrity::Signed(key(seed));
    options.segment_size = 128;
    options
}

fn open(dir: &Path, options: Options, start: Start) -> Result<(Wal, Vec<Vec<u8>>), Error> {
    let mut records = Vec::new();
    let (wal, _) = Wal::open(dir, options, start, |_, payload| {
        records.push(payload.to_vec());
        Ok::<(), Infallible>(())
    })?;
    Ok((wal, records))
}

fn payloads(count: u8) -> Vec<Vec<u8>> {
    (0..count).map(|byte| vec![byte; 20]).collect()
}

/// A log of `count` signed records in segments of about two records.
fn written(count: u8) -> (tempfile::TempDir, Option<Digest>) {
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    for payload in payloads(count) {
        let _: Option<_> = wal.append_batch([payload]).unwrap();
    }
    let head = wal.chain_head();
    (dir, head)
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    paths.sort();
    paths
}

/// Rewrites the frame checksum of the first record of `bytes`, so that
/// only the seal can tell that its payload changed.
fn fix_first_checksum(bytes: &mut [u8]) {
    let frame = &bytes[HEADER_LEN..];
    let len = usize::try_from(u32::from_le_bytes(frame[..4].try_into().unwrap())).unwrap();
    let crc = [&frame[..4], &frame[8..16], &frame[16..16 + len]]
        .iter()
        .fold(0, |crc, part| crc32c::crc32c_append(crc, part));
    bytes[HEADER_LEN + 4..HEADER_LEN + 8].copy_from_slice(&crc.to_le_bytes());
}

fn corruption(result: Result<(Wal, Vec<Vec<u8>>), Error>) -> Corruption {
    match result {
        Err(Error::Corrupt { reason, .. }) => reason,
        other => panic!("expected corruption, got {other:?}"),
    }
}

#[test]
fn signed_records_replay_across_segments_and_reopens() {
    let (dir, head) = written(9);
    assert!(segments(dir.path()).len() > 2, "several segments");
    let (wal, records) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    assert_eq!(records, payloads(9), "payloads without seals");
    assert_eq!(wal.chain_head(), head, "same head after reopening");
    assert_ne!(head, Some(Digest::GENESIS), "the chain moved");
}

#[test]
fn a_new_record_continues_the_chain_after_reopening() {
    let (dir, _) = written(3);
    let (mut wal, _) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    let _: Option<_> = wal.append_batch([b"more"]).unwrap();
    drop(wal);
    let (_, records) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    assert_eq!(records.len(), 4, "every record checks out");
}

#[test]
fn a_changed_payload_fails_its_signature() {
    let (dir, _) = written(4);
    let path = &segments(dir.path())[0];
    let mut bytes = fs::read(path).unwrap();
    bytes[HEADER_LEN + FRAME_HEADER_LEN] ^= 1;
    fix_first_checksum(&mut bytes);
    fs::write(path, &bytes).unwrap();
    assert_eq!(
        corruption(open(dir.path(), signed(1), Start::Oldest)),
        Corruption::Seal {
            lsn: Lsn::new(1),
            damage: SealDamage::Signature
        },
        "tampered payload"
    );
}

#[test]
fn another_key_fails_the_signatures() {
    let (dir, _) = written(2);
    assert_eq!(
        corruption(open(dir.path(), signed(2), Start::Oldest)),
        Corruption::Seal {
            lsn: Lsn::new(1),
            damage: SealDamage::Signature
        },
        "stranger's key"
    );
}

#[test]
fn a_replaced_segment_breaks_the_chain() {
    let (dir, _) = written(6);
    let paths = segments(dir.path());
    let mut bytes = fs::read(&paths[1]).unwrap();
    // Point the segment's anchor somewhere else.
    bytes[HEADER_LEN - 1] ^= 1;
    fs::write(&paths[1], &bytes).unwrap();
    let first_of_second = Lsn::new(u64::from_le_bytes(bytes[8..16].try_into().unwrap()));
    assert_eq!(
        corruption(open(dir.path(), signed(1), Start::Oldest)),
        Corruption::Seal {
            lsn: first_of_second,
            damage: SealDamage::Chain
        },
        "anchor does not match"
    );
}

#[test]
fn the_protection_must_match() {
    let (dir, _) = written(1);
    assert_eq!(
        corruption(open(dir.path(), Options::default(), Start::Oldest)),
        Corruption::Protection {
            expected: Protection::Checksum,
            found: Protection::Signature
        },
        "signed opened as checksummed"
    );
    let plain = tempfile::tempdir().unwrap();
    let (mut wal, _) = open(plain.path(), Options::default(), Start::Oldest).unwrap();
    let _: Option<_> = wal.append_batch([b"plain"]).unwrap();
    drop(wal);
    assert_eq!(
        corruption(open(plain.path(), signed(1), Start::Oldest)),
        Corruption::Protection {
            expected: Protection::Signature,
            found: Protection::Checksum
        },
        "checksummed opened as signed"
    );
}

#[test]
fn purged_logs_still_check_from_the_oldest_anchor() {
    let (dir, head) = written(9);
    let (mut wal, _) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    assert!(wal.purge_through(Lsn::new(5)).unwrap() > 0, "purged");
    drop(wal);
    let (wal, records) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    assert_eq!(records.last(), payloads(9).last(), "newest kept");
    assert!(records.len() < 9, "oldest gone");
    assert_eq!(wal.chain_head(), head, "head unchanged");
}

#[test]
fn a_torn_signed_record_is_cut_off() {
    let (dir, _) = written(3);
    let path = segments(dir.path()).pop().unwrap();
    let len = fs::metadata(&path).unwrap().len();
    let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(len - 10).unwrap();
    drop(file);
    let mut records = Vec::new();
    let (wal, report) = Wal::open(dir.path(), signed(1), Start::Oldest, |_, payload| {
        records.push(payload.to_vec());
        Ok::<(), Infallible>(())
    })
    .unwrap();
    assert!(
        matches!(
            report.truncated_tail.map(|tail| tail.damage),
            Some(TailDamage::Frame(_))
        ),
        "torn frame"
    );
    assert_eq!(records, payloads(2), "the torn record is gone");
    drop(wal);
    let (_, records) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    assert_eq!(records.len(), 2, "and stays gone");
}

#[test]
fn checksummed_logs_have_no_chain() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(dir.path(), Options::default(), Start::Oldest).unwrap();
    assert_eq!(wal.chain_head(), None, "checksummed");
    let fresh = tempfile::tempdir().unwrap();
    let (wal, _) = open(fresh.path(), signed(1), Start::Oldest).unwrap();
    assert_eq!(wal.chain_head(), Some(Digest::GENESIS), "fresh signed log");
}

/// Seal bytes after a payload: a linked record, and the signed record that
/// closes a batch.
const LINKED_SEAL_LEN: usize = 33;
/// An Ed25519 signed seal: digest, signature, algorithm and kind.
const SIGNED_SEAL_LEN: usize = 32 + 64 + 1 + 1;

/// A log with one batch of `count` records of 20 bytes, in one segment.
fn one_batch(count: u8) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut options = signed(1);
    options.segment_size = 1 << 20;
    let (mut wal, _) = open(dir.path(), options, Start::Oldest).unwrap();
    let _: Option<_> = wal.append_batch(payloads(count)).unwrap();
    dir
}

fn truncate(path: &Path, len: u64) {
    let file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(len).unwrap();
}

fn frame_len(seal: usize) -> u64 {
    u64::try_from(FRAME_HEADER_LEN + 20 + seal).unwrap()
}

#[test]
fn a_batch_is_signed_once_at_its_end() {
    let dir = one_batch(4);
    let path = segments(dir.path()).pop().unwrap();
    let expected = u64::try_from(HEADER_LEN).unwrap()
        + 3 * frame_len(LINKED_SEAL_LEN)
        + frame_len(SIGNED_SEAL_LEN);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        expected,
        "three linked, one signed"
    );
    let (_, records) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    assert_eq!(records, payloads(4), "the whole batch replays");
}

#[test]
fn a_batch_cut_before_its_signature_is_dropped_whole() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = signed(1);
    options.segment_size = 1 << 20;
    let (mut wal, _) = open(dir.path(), options.clone(), Start::Oldest).unwrap();
    let _: Option<_> = wal.append_batch([b"kept".to_vec()]).unwrap();
    let after_kept = wal.next_lsn();
    let _: Option<_> = wal.append_batch(payloads(3)).unwrap();
    drop(wal);
    let path = segments(dir.path()).pop().unwrap();
    let full = fs::metadata(&path).unwrap().len();
    // Two complete linked records survive; the signed one does not.
    truncate(&path, full - frame_len(SIGNED_SEAL_LEN));
    let mut records = Vec::new();
    let (mut wal, report) = Wal::open(dir.path(), options.clone(), Start::Oldest, |_, payload| {
        records.push(payload.to_vec());
        Ok::<(), Infallible>(())
    })
    .unwrap();
    let tail = report.truncated_tail.unwrap();
    assert_eq!(tail.damage, TailDamage::Unsigned, "unsigned tail");
    assert_eq!(
        tail.discarded_bytes,
        2 * frame_len(LINKED_SEAL_LEN),
        "both linked records"
    );
    assert_eq!(records, vec![b"kept".to_vec()], "only the signed batch");
    assert_eq!(wal.next_lsn(), after_kept, "positions are reused");
    let _: Option<_> = wal.append_batch([b"after".to_vec()]).unwrap();
    drop(wal);
    let (_, records) = open(dir.path(), options, Start::Oldest).unwrap();
    assert_eq!(
        records,
        vec![b"kept".to_vec(), b"after".to_vec()],
        "the chain goes on"
    );
}

#[test]
fn a_torn_frame_inside_a_batch_drops_the_batch() {
    let dir = one_batch(3);
    let path = segments(dir.path()).pop().unwrap();
    let header = u64::try_from(HEADER_LEN).unwrap();
    truncate(&path, header + frame_len(LINKED_SEAL_LEN) + 5);
    let (wal, report) = Wal::open(dir.path(), signed(1), Start::Oldest, |_, _| {
        Ok::<(), Infallible>(())
    })
    .unwrap();
    let tail = report.truncated_tail.unwrap();
    assert!(matches!(tail.damage, TailDamage::Frame(_)), "torn frame");
    assert_eq!(tail.offset, header, "cut back to the start of the batch");
    assert_eq!(report.replayed, 0, "nothing of the batch replays");
    assert_eq!(
        wal.chain_head(),
        Some(Digest::GENESIS),
        "the chain is back at its start"
    );
}

#[test]
fn a_changed_linked_record_breaks_the_chain() {
    let dir = one_batch(2);
    let path = segments(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[HEADER_LEN + FRAME_HEADER_LEN] ^= 1;
    fix_first_checksum(&mut bytes);
    fs::write(&path, &bytes).unwrap();
    assert_eq!(
        corruption(open(dir.path(), signed(1), Start::Oldest)),
        Corruption::Seal {
            lsn: Lsn::new(2),
            damage: SealDamage::Chain
        },
        "the signed record no longer follows"
    );
}

#[test]
fn an_older_segment_that_ends_unsigned_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _) = open(dir.path(), signed(1), Start::Oldest).unwrap();
    // The segment size is 128 bytes, so the second batch starts a new one.
    let _: Option<_> = wal.append_batch(payloads(2)).unwrap();
    let _: Option<_> = wal.append_batch(payloads(1)).unwrap();
    drop(wal);
    let oldest = segments(dir.path())[0].clone();
    let header = u64::try_from(HEADER_LEN).unwrap();
    truncate(&oldest, header + frame_len(LINKED_SEAL_LEN));
    assert_eq!(
        corruption(open(dir.path(), signed(1), Start::Oldest)),
        Corruption::Seal {
            lsn: Lsn::new(1),
            damage: SealDamage::Unsigned
        },
        "unsigned in the middle"
    );
}
