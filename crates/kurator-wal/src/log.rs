//! The log: opening with replay and tail repair, appending, purging.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::error::{Corruption, Error, InvalidFrame, Result};
use crate::frame::{self, Decoded};
use crate::lsn::{Lsn, LsnRange};
use crate::segment::{self, Segment, sync_dir};

/// How a log is laid out.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// A batch that starts once the active segment holds this many bytes
    /// goes to a new segment. Older segments can then be purged.
    pub segment_size: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024,
        }
    }
}

/// What [`Wal::open`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoveryReport {
    /// Records passed to the replay callback.
    pub replayed: u64,
    /// The damaged tail that was cut off, if there was one.
    pub truncated_tail: Option<TruncatedTail>,
}

/// A damaged tail that [`Wal::open`] cut off the newest segment.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TruncatedTail {
    /// The segment.
    pub segment: PathBuf,
    /// Where the valid part of the segment ends.
    pub offset: u64,
    /// Bytes removed.
    pub discarded_bytes: u64,
    /// What the damage was.
    pub damage: TailDamage,
}

/// The kind of damage a crash left at the end of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TailDamage {
    /// The newest segment was created but its header never fully reached
    /// the disk; the file was removed.
    Header,
    /// The last frame was only partly written.
    Frame(InvalidFrame),
}

/// Whether a write may still go to the active segment.
#[derive(Debug)]
enum Health {
    /// Every write so far reached the disk.
    Ready,
    /// A write or sync failed. After a failed `fsync` the kernel may have
    /// dropped dirty pages, so retrying could report success for data that
    /// is gone: the log refuses writes until it is opened again.
    Poisoned,
}

/// An open log, holding its active segment for appending.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    options: Options,
    /// Oldest first; the last one is active.
    segments: Vec<Segment>,
    active: File,
    active_len: u64,
    next: Lsn,
    buffer: Vec<u8>,
    health: Health,
}

/// Where a segment is in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// A crash can only damage the newest segment.
    Newest,
    Older,
}

/// The outcome of reading one segment.
struct Scan {
    /// The position after the last valid record.
    next: Lsn,
    /// Bytes up to the end of the last valid record.
    valid_len: usize,
    replayed: u64,
    damage: Option<TailDamage>,
}

impl Wal {
    /// Opens the log in `dir`, creating it when it does not exist, and
    /// passes every record after `after` to `replay`, in order.
    ///
    /// `after` is what the caller already holds, usually the position of
    /// its latest snapshot; use [`Lsn::ZERO`] for everything. A damaged
    /// tail is cut off and returned in the report.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] for damage that a crash cannot explain,
    /// [`Error::Gap`] or [`Error::Behind`] when the log does not continue
    /// right after `after`, [`Error::Replay`] when `replay` fails, and
    /// [`Error::Io`].
    pub fn open<F, E>(
        dir: impl Into<PathBuf>,
        options: Options,
        after: Lsn,
        mut replay: F,
    ) -> Result<(Self, RecoveryReport)>
    where
        F: FnMut(Lsn, &[u8]) -> Result<(), E>,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut segments = Segment::list(&dir)?;
        let mut report = RecoveryReport {
            replayed: 0,
            truncated_tail: None,
        };
        let mut next = after.next();
        let mut active_len = 0;
        let count = segments.len();
        for (index, segment) in segments.iter().enumerate() {
            if index == 0 && segment.first() > next {
                return Err(Error::Gap {
                    after,
                    first: segment.first(),
                });
            }
            if index > 0 && segment.first() != next {
                return Err(corrupt(
                    segment,
                    0,
                    Corruption::LsnMismatch {
                        expected: next,
                        found: segment.first(),
                    },
                ));
            }
            let place = if index.saturating_add(1) == count {
                Place::Newest
            } else {
                Place::Older
            };
            let data = fs::read(segment.path())?;
            let scan = scan(segment, &data, place, after, &mut replay)?;
            report.replayed = report.replayed.saturating_add(scan.replayed);
            report.truncated_tail = scan.damage.map(|damage| TruncatedTail {
                segment: segment.path().to_path_buf(),
                offset: len_u64(scan.valid_len),
                discarded_bytes: len_u64(data.len().saturating_sub(scan.valid_len)),
                damage,
            });
            next = scan.next;
            active_len = len_u64(scan.valid_len);
        }
        if next <= after {
            return Err(Error::Behind { after, end: next });
        }
        let torn_header = report
            .truncated_tail
            .as_ref()
            .is_some_and(|tail| tail.damage == TailDamage::Header);
        let reopened = match segments.last() {
            Some(segment) if torn_header => {
                segment.remove()?;
                sync_dir(&dir)?;
                drop(segments.pop());
                None
            }
            Some(segment) => Some(segment.reopen(active_len)?),
            None => None,
        };
        let active = if let Some(file) = reopened {
            file
        } else {
            // Fresh segments start at `next`, which follows the last valid
            // record (or `after`), so the torn segment's name is reused.
            let (segment, file) = Segment::create(&dir, next)?;
            segments.push(segment);
            active_len = len_u64(segment::HEADER_LEN);
            file
        };
        let wal = Self {
            dir,
            options,
            segments,
            active,
            active_len,
            next,
            buffer: Vec::new(),
            health: Health::Ready,
        };
        Ok((wal, report))
    }

    /// Appends `payloads` as consecutive records and syncs them to disk
    /// with one `fdatasync`. Returns their positions, or `None` when there
    /// were no payloads.
    ///
    /// # Errors
    ///
    /// [`Error::PayloadTooLarge`] before anything is written, and
    /// [`Error::Io`] when the write fails, after which every call returns
    /// [`Error::Poisoned`].
    pub fn append_batch<I>(&mut self, payloads: I) -> Result<Option<LsnRange>>
    where
        I: IntoIterator,
        I::Item: AsRef<[u8]>,
    {
        if matches!(self.health, Health::Poisoned) {
            return Err(Error::Poisoned);
        }
        self.buffer.clear();
        let mut end = self.next;
        for payload in payloads {
            frame::encode(end, payload.as_ref(), &mut self.buffer)?;
            end = end.next();
        }
        let Some(range) = LsnRange::until(self.next, end) else {
            return Ok(None);
        };
        let written = self.write_buffer();
        if written.is_err() {
            self.health = Health::Poisoned;
        }
        written?;
        self.active_len = self.active_len.saturating_add(len_u64(self.buffer.len()));
        self.next = end;
        Ok(Some(range))
    }

    /// Deletes every segment whose records are all at or before `lsn`. The
    /// active segment is always kept. Returns how many were deleted.
    /// Checkpoints are rare, so the directory is synced even when nothing
    /// was deleted.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when a file cannot be deleted.
    pub fn purge_through(&mut self, lsn: Lsn) -> Result<usize> {
        let purgeable = self
            .segments
            .iter()
            .skip(1)
            .take_while(|segment| segment.first() <= lsn.next())
            .count();
        for segment in self.segments.drain(..purgeable) {
            segment.remove()?;
        }
        sync_dir(&self.dir)?;
        Ok(purgeable)
    }

    /// The position the next record will get.
    #[must_use]
    pub const fn next_lsn(&self) -> Lsn {
        self.next
    }

    /// The directory of the log.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn write_buffer(&mut self) -> std::io::Result<()> {
        if self.active_len >= self.options.segment_size {
            let (segment, file) = Segment::create(&self.dir, self.next)?;
            self.segments.push(segment);
            self.active = file;
            self.active_len = len_u64(segment::HEADER_LEN);
        }
        self.active.write_all(&self.buffer)?;
        self.active.sync_data()
    }
}

/// Reads the frames of one segment, replaying those after `after`.
fn scan<F, E>(
    segment: &Segment,
    data: &[u8],
    place: Place,
    after: Lsn,
    replay: &mut F,
) -> Result<Scan>
where
    F: FnMut(Lsn, &[u8]) -> Result<(), E>,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let Some(first) = data
        .get(..segment::HEADER_LEN)
        .and_then(segment::parse_header)
    else {
        if place == Place::Newest && data.len() < segment::HEADER_LEN {
            return Ok(Scan {
                next: segment.first(),
                valid_len: 0,
                replayed: 0,
                damage: Some(TailDamage::Header),
            });
        }
        return Err(corrupt(segment, 0, Corruption::BadHeader));
    };
    if first != segment.first() {
        return Err(corrupt(
            segment,
            0,
            Corruption::NameMismatch {
                named: segment.first(),
                header: first,
            },
        ));
    }
    let mut scan = Scan {
        next: first,
        valid_len: segment::HEADER_LEN,
        replayed: 0,
        damage: None,
    };
    loop {
        match frame::decode(data.get(scan.valid_len..).unwrap_or_default()) {
            Decoded::End => return Ok(scan),
            Decoded::Record { lsn, payload, len } => {
                if lsn != scan.next {
                    return Err(corrupt(
                        segment,
                        scan.valid_len,
                        Corruption::LsnMismatch {
                            expected: scan.next,
                            found: lsn,
                        },
                    ));
                }
                if lsn > after {
                    replay(lsn, payload).map_err(|source| Error::Replay {
                        lsn,
                        source: source.into(),
                    })?;
                    scan.replayed = scan.replayed.saturating_add(1);
                }
                scan.next = lsn.next();
                scan.valid_len = scan.valid_len.saturating_add(len);
            }
            Decoded::Invalid(invalid) => {
                if place == Place::Older {
                    return Err(corrupt(segment, scan.valid_len, Corruption::Frame(invalid)));
                }
                scan.damage = Some(TailDamage::Frame(invalid));
                return Ok(scan);
            }
        }
    }
}

fn corrupt(segment: &Segment, offset: usize, reason: Corruption) -> Error {
    Error::Corrupt {
        path: segment.path().to_path_buf(),
        offset: len_u64(offset),
        reason,
    }
}

/// A byte count as `u64`; lossless on every supported target.
fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}
