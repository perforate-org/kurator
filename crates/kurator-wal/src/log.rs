//! The log: opening with replay and tail repair, appending, purging.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::error::{Corruption, Error, InvalidFrame, Result};
use crate::frame::{self, Decoded};
use crate::lsn::{Lsn, LsnRange};
use crate::seal::{Digest, Head, Integrity, Opened, Seal, SealDamage, SigningKey, VerifyingKey};
use crate::segment::{self, Format, Header, Segment, sync_dir};

/// How a log is laid out.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// A batch that starts once the active segment holds this many bytes
    /// goes to a new segment. Older segments can then be purged.
    pub segment_size: u64,
    /// How records are protected. A log is always opened with the
    /// protection it was created with.
    pub integrity: Integrity,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024,
            integrity: Integrity::Checksum,
        }
    }
}

/// Which records [`Wal::open`] replays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Start {
    /// Every record after this one. The caller already holds the rest,
    /// usually from a snapshot taken at this position; [`Lsn::ZERO`] means
    /// every record.
    After(Lsn),
    /// Every record still in the log, from the oldest segment on. For
    /// callers that keep their own account of what was purged.
    Oldest,
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
    /// The last batch of a signed log ends before the record that signs
    /// it: the crash came before the batch was synced.
    Unsigned,
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

/// How appended records are sealed: the key and the digest of the last
/// record.
#[derive(Debug, Clone)]
enum Chain {
    Checksummed,
    Signed { key: SigningKey, head: Head },
}

impl Chain {
    fn new(integrity: Integrity, head: Head) -> Self {
        match integrity {
            Integrity::Checksum => Self::Checksummed,
            Integrity::Signed(key) => Self::Signed { key, head },
        }
    }

    /// The format of a segment that starts after the chain's head.
    const fn format(&self) -> Format {
        match self {
            Self::Checksummed => Format::Checksummed,
            Self::Signed { head, .. } => Format::Signed {
                anchor: head.digest(),
            },
        }
    }

    /// Seals `payload` as record `lsn` and moves the head past it: links
    /// it, or signs it when it closes its batch. `None` for a checksummed
    /// log.
    fn seal(&mut self, lsn: Lsn, payload: &[u8], place: InBatch) -> Option<Seal> {
        match self {
            Self::Checksummed => None,
            Self::Signed { key, head } => {
                let (seal, next) = match place {
                    InBatch::Inside => head.link(lsn.get(), payload),
                    InBatch::Last => head.sign(key, lsn.get(), payload),
                };
                *head = next;
                Some(seal)
            }
        }
    }
}

/// Where a record sits in the batch it is appended with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InBatch {
    /// Another record follows it.
    Inside,
    /// It is the last one, so a signed log signs it.
    Last,
}

/// How scanned records are checked: the expected format, and for a signed
/// log the key and the digest of the last record read.
#[derive(Debug, Clone)]
enum Check {
    Checksummed,
    Signed { key: VerifyingKey, head: Head },
}

impl Check {
    fn new(integrity: &Integrity) -> Self {
        match integrity {
            Integrity::Checksum => Self::Checksummed,
            Integrity::Signed(key) => Self::Signed {
                key: key.verifying_key(),
                head: Head::new(Digest::GENESIS),
            },
        }
    }

    /// The format a new segment would have here.
    const fn format(&self) -> Format {
        match self {
            Self::Checksummed => Format::Checksummed,
            Self::Signed { head, .. } => Format::Signed {
                anchor: head.digest(),
            },
        }
    }

    /// Checks that a segment in `format` whose first record is `first`
    /// continues the chain. The oldest segment's anchor starts it: the
    /// records before it are gone.
    fn enter(&mut self, format: Format, first: Lsn, place: Chained) -> Result<(), Corruption> {
        let expected = self.format();
        match (self, format) {
            (Self::Checksummed, Format::Checksummed) => Ok(()),
            (Self::Signed { head, .. }, Format::Signed { anchor }) => {
                if place == Chained::Oldest {
                    *head = Head::new(anchor);
                }
                if head.digest() == anchor {
                    Ok(())
                } else {
                    Err(Corruption::Seal {
                        lsn: first,
                        damage: SealDamage::Chain,
                    })
                }
            }
            (_, found) => Err(Corruption::Protection {
                expected: expected.protection(),
                found: found.protection(),
            }),
        }
    }

    /// Checks record `lsn` and returns its payload without the seal. Every
    /// record of a checksummed log stands alone, as if signed.
    fn open<'a>(&mut self, lsn: Lsn, record: &'a [u8]) -> Result<Opened<'a>, Corruption> {
        match self {
            Self::Checksummed => Ok(Opened::Signed(record)),
            Self::Signed { key, head } => {
                let (opened, next) = head
                    .open(key, lsn.get(), record)
                    .map_err(|damage| Corruption::Seal { lsn, damage })?;
                *head = next;
                Ok(opened)
            }
        }
    }

    const fn head(&self) -> Head {
        match self {
            Self::Checksummed => Head::new(Digest::GENESIS),
            Self::Signed { head, .. } => *head,
        }
    }

    /// Moves the head back to `head`, past records that were cut off.
    const fn rewind(&mut self, to: Head) {
        if let Self::Signed { head, .. } = self {
            *head = to;
        }
    }
}

/// Whether a segment is the first one read, whose anchor is taken on
/// trust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chained {
    Oldest,
    Later,
}

/// An open log, holding its active segment for appending.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    segment_size: u64,
    /// Oldest first; the last one is active.
    segments: Vec<Segment>,
    active: File,
    active_len: u64,
    next: Lsn,
    chain: Chain,
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

/// The records of a batch read so far, before the signed record that
/// closes it.
struct Batch<'a> {
    /// The linked records, in order.
    records: Vec<(Lsn, &'a [u8])>,
    /// The position after the last record read.
    next: Lsn,
    /// Bytes up to the end of the last record read.
    len: usize,
    /// The chain before the batch, to go back to when it is cut off.
    head: Head,
}

/// What reading on from a batch found.
enum Step<'a> {
    /// A signed record closed the batch: these are all its records.
    Closed(Vec<(Lsn, &'a [u8])>),
    /// The segment ends after the last batch.
    End,
    /// The segment ends inside a batch that no signed record closes.
    Unsigned,
    /// Bytes that are not a valid frame.
    Invalid(InvalidFrame),
}

/// Passes `records` to `deliver` in order and adds up what it returns.
fn replay_all(
    deliver: &mut impl FnMut(Lsn, &[u8]) -> Result<u64>,
    records: Vec<(Lsn, &[u8])>,
) -> Result<u64> {
    records
        .into_iter()
        .try_fold(0_u64, |count, (lsn, payload)| {
            Ok(count.saturating_add(deliver(lsn, payload)?))
        })
}

impl<'a> Batch<'a> {
    /// Adds the record `lsn`; returns the whole batch once a signed record
    /// closes it.
    fn add(&mut self, lsn: Lsn, opened: Opened<'a>) -> Option<Vec<(Lsn, &'a [u8])>> {
        match opened {
            Opened::Linked(payload) => {
                self.records.push((lsn, payload));
                None
            }
            Opened::Signed(payload) => {
                self.records.push((lsn, payload));
                Some(std::mem::take(&mut self.records))
            }
        }
    }
}

/// One segment's bytes and where it sits, while it is read.
struct Reading<'a> {
    segment: &'a Segment,
    data: &'a [u8],
    place: Place,
    after: Lsn,
}

impl Wal {
    /// Opens the log in `dir`, creating it when it does not exist, checks
    /// every record and passes those that `start` asks for to `replay`, in
    /// order.
    ///
    /// A damaged tail is cut off and returned in the report.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] for damage that a crash cannot explain, including
    /// a signed record that does not check out, [`Error::Gap`] or
    /// [`Error::Behind`] when the log does not continue right after
    /// [`Start::After`], [`Error::Replay`] when `replay` fails, and
    /// [`Error::Io`].
    pub fn open<F, E>(
        dir: impl Into<PathBuf>,
        options: Options,
        start: Start,
        mut replay: F,
    ) -> Result<(Self, RecoveryReport)>
    where
        F: FnMut(Lsn, &[u8]) -> Result<(), E>,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut segments = Segment::list(&dir)?;
        let after = match start {
            Start::After(after) => after,
            Start::Oldest => segments.first().map_or(Lsn::ZERO, |oldest| {
                Lsn::new(oldest.first().get().saturating_sub(1))
            }),
        };
        let mut check = Check::new(&options.integrity);
        let Recovered {
            next,
            mut active_len,
            report,
        } = recover(&segments, &mut check, after, &mut replay)?;
        if next <= after {
            return Err(Error::Behind { after, end: next });
        }
        let chain = Chain::new(options.integrity, check.head());
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
            let format = chain.format();
            let (segment, file) = Segment::create(&dir, next, format)?;
            segments.push(segment);
            active_len = len_u64(format.header_len());
            file
        };
        let wal = Self {
            dir,
            segment_size: options.segment_size,
            segments,
            active,
            active_len,
            next,
            chain,
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
        let mut chain = self.chain.clone();
        let mut end = self.next;
        let mut payloads = payloads.into_iter().peekable();
        while let Some(payload) = payloads.next() {
            let payload = payload.as_ref();
            let place = if payloads.peek().is_some() {
                InBatch::Inside
            } else {
                InBatch::Last
            };
            let seal = chain.seal(end, payload, place);
            frame::encode(
                end,
                payload,
                seal.as_ref().map(Seal::as_bytes).unwrap_or_default(),
                &mut self.buffer,
            )?;
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
        self.chain = chain;
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

    /// The digest of the last record of a signed log, which the next record
    /// will name as its predecessor; `None` for a checksummed log.
    #[must_use]
    pub const fn chain_head(&self) -> Option<Digest> {
        match &self.chain {
            Chain::Checksummed => None,
            Chain::Signed { head, .. } => Some(head.digest()),
        }
    }

    /// The directory of the log.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn write_buffer(&mut self) -> std::io::Result<()> {
        if self.active_len >= self.segment_size {
            // The new segment's anchor is the head before this batch.
            let format = self.chain.format();
            let (segment, file) = Segment::create(&self.dir, self.next, format)?;
            self.segments.push(segment);
            self.active = file;
            self.active_len = len_u64(format.header_len());
        }
        self.active.write_all(&self.buffer)?;
        self.active.sync_data()
    }
}

/// What reading every segment found.
struct Recovered {
    /// The position after the last valid record.
    next: Lsn,
    /// Bytes up to the end of the last valid record in the newest segment.
    active_len: u64,
    report: RecoveryReport,
}

/// Reads `segments` in order, checking that each one continues the one
/// before it, and replays the records after `after`.
fn recover<F, E>(
    segments: &[Segment],
    check: &mut Check,
    after: Lsn,
    replay: &mut F,
) -> Result<Recovered>
where
    F: FnMut(Lsn, &[u8]) -> Result<(), E>,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut recovered = Recovered {
        next: after.next(),
        active_len: 0,
        report: RecoveryReport {
            replayed: 0,
            truncated_tail: None,
        },
    };
    let count = segments.len();
    for (index, segment) in segments.iter().enumerate() {
        let next = recovered.next;
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
        let chained = if index == 0 {
            Chained::Oldest
        } else {
            Chained::Later
        };
        let data = fs::read(segment.path())?;
        let reading = Reading {
            segment,
            data: &data,
            place,
            after,
        };
        let scan = reading.scan(check, chained, replay)?;
        let report = &mut recovered.report;
        report.replayed = report.replayed.saturating_add(scan.replayed);
        report.truncated_tail = scan.damage.map(|damage| TruncatedTail {
            segment: segment.path().to_path_buf(),
            offset: len_u64(scan.valid_len),
            discarded_bytes: len_u64(data.len().saturating_sub(scan.valid_len)),
            damage,
        });
        recovered.next = scan.next;
        recovered.active_len = len_u64(scan.valid_len);
    }
    Ok(recovered)
}

impl Reading<'_> {
    /// Reads the frames of the segment, checking each one and replaying
    /// those after `after`.
    fn scan<F, E>(self, check: &mut Check, chained: Chained, replay: &mut F) -> Result<Scan>
    where
        F: FnMut(Lsn, &[u8]) -> Result<(), E>,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let Some(header) = self.header(check, chained)? else {
            return Ok(Scan {
                next: self.segment.first(),
                valid_len: 0,
                replayed: 0,
                damage: Some(TailDamage::Header),
            });
        };
        let after = self.after;
        let mut deliver = |lsn: Lsn, payload: &[u8]| -> Result<u64> {
            if lsn <= after {
                return Ok(0);
            }
            replay(lsn, payload).map_err(|source| Error::Replay {
                lsn,
                source: source.into(),
            })?;
            Ok(1)
        };
        let mut scan = Scan {
            next: header.first(),
            valid_len: header.format().header_len(),
            replayed: 0,
            damage: None,
        };
        // Linked records read since the last signed one; they count once
        // a signed record closes their batch.
        let mut batch = Batch {
            records: Vec::new(),
            next: scan.next,
            len: scan.valid_len,
            head: check.head(),
        };
        loop {
            match self.next_batch(check, &mut batch)? {
                Step::Closed(records) => {
                    let replayed = replay_all(&mut deliver, records)?;
                    scan.replayed = scan.replayed.saturating_add(replayed);
                    scan.next = batch.next;
                    scan.valid_len = batch.len;
                    batch.head = check.head();
                }
                Step::End => return Ok(scan),
                Step::Unsigned => return self.unsigned(scan, &batch, check),
                Step::Invalid(invalid) => {
                    check.rewind(batch.head);
                    return self.cut(scan, invalid);
                }
            }
        }
    }

    /// Reads records into `batch` until one closes it.
    fn next_batch<'a>(&self, check: &mut Check, batch: &mut Batch<'a>) -> Result<Step<'a>>
    where
        Self: 'a,
    {
        loop {
            let (lsn, record, len) =
                match frame::decode(self.data.get(batch.len..).unwrap_or_default()) {
                    Decoded::End if batch.records.is_empty() => return Ok(Step::End),
                    Decoded::End => return Ok(Step::Unsigned),
                    Decoded::Invalid(invalid) => return Ok(Step::Invalid(invalid)),
                    Decoded::Record { lsn, payload, len } => (lsn, payload, len),
                };
            let opened = self.open_record(check, batch, lsn, record)?;
            // Every frame moves the batch on, so reading always ends.
            batch.next = lsn.next();
            batch.len = batch.len.saturating_add(len);
            if let Some(closed) = batch.add(lsn, opened) {
                return Ok(Step::Closed(closed));
            }
        }
    }

    /// The segment header, checked against the chain; `None` when a crash
    /// cut it short.
    fn header(&self, check: &mut Check, chained: Chained) -> Result<Option<Header>> {
        let Some(header) = segment::parse_header(self.data) else {
            if self.place == Place::Newest && self.data.len() < check.format().header_len() {
                return Ok(None);
            }
            return Err(self.corrupt(0, Corruption::BadHeader));
        };
        if header.first() != self.segment.first() {
            return Err(self.corrupt(
                0,
                Corruption::NameMismatch {
                    named: self.segment.first(),
                    header: header.first(),
                },
            ));
        }
        check
            .enter(header.format(), header.first(), chained)
            .map_err(|reason| self.corrupt(0, reason))?;
        Ok(Some(header))
    }

    /// Checks that the record at the end of `batch` is the next one and
    /// returns it without its seal.
    fn open_record<'a>(
        &self,
        check: &mut Check,
        batch: &Batch<'_>,
        lsn: Lsn,
        record: &'a [u8],
    ) -> Result<Opened<'a>> {
        if lsn != batch.next {
            return Err(self.corrupt(
                batch.len,
                Corruption::LsnMismatch {
                    expected: batch.next,
                    found: lsn,
                },
            ));
        }
        check
            .open(lsn, record)
            .map_err(|reason| self.corrupt(batch.len, reason))
    }

    /// Ends the scan at the end of a segment whose last batch is not
    /// signed: a batch the crash kept from being synced in the newest
    /// segment, corruption anywhere else.
    fn unsigned(&self, mut scan: Scan, batch: &Batch<'_>, check: &mut Check) -> Result<Scan> {
        if self.place == Place::Older {
            return Err(self.corrupt(
                scan.valid_len,
                Corruption::Seal {
                    lsn: scan.next,
                    damage: SealDamage::Unsigned,
                },
            ));
        }
        check.rewind(batch.head);
        scan.damage = Some(TailDamage::Unsigned);
        Ok(scan)
    }

    /// Ends the scan at an invalid frame: the torn tail of the newest
    /// segment, or corruption anywhere else.
    fn cut(&self, mut scan: Scan, invalid: InvalidFrame) -> Result<Scan> {
        if self.place == Place::Older {
            return Err(self.corrupt(scan.valid_len, Corruption::Frame(invalid)));
        }
        scan.damage = Some(TailDamage::Frame(invalid));
        Ok(scan)
    }

    fn corrupt(&self, offset: usize, reason: Corruption) -> Error {
        corrupt(self.segment, offset, reason)
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
