//! Transactions: reads at one version, writes at a later one, and the
//! check that nothing they read changed in between.
//!
//! A transaction reads a fixed version of the state, its *read version*,
//! and remembers what it read: each key, and each range it scanned. Its
//! writes wait in a [`Draft`](crate::Draft) until it commits. Committing appends one
//! [`Transaction`] command to the log, and applying that command checks
//! whether a command after the read version wrote any key it read or any
//! key in a range it scanned:
//!
//! - if none did, its writes apply at the command's version, as if the
//!   whole transaction ran at that one instant;
//! - if one did, it aborts with [`Abort::Conflict`](crate::Abort::Conflict) and writes nothing.
//!
//! This is optimistic concurrency control, and the result is serializable:
//! every committed transaction behaves as if it ran alone at its commit
//! version. The check runs where the command is applied, so every replica
//! that applies the same log decides the same way, and recovery replays
//! the same decisions.

use std::collections::BTreeSet;
use std::ops::Bound;

use oleaph_codec::{DecodeError, Reader, put_bytes};

use crate::batch::WriteBatch;
use crate::error::{Error, Result};
use crate::span::Span;
use crate::version::Version;

/// A transaction as it goes into the log: what it read, at which version,
/// and what it writes.
///
/// Only a [`Draft`](crate::Draft) and the log decoder make one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    read_at: Version,
    reads: ReadSet,
    writes: WriteBatch,
}

impl Transaction {
    pub(crate) const fn new(read_at: Version, reads: ReadSet, writes: WriteBatch) -> Self {
        Self {
            read_at,
            reads,
            writes,
        }
    }

    /// The version its reads saw.
    #[must_use]
    pub const fn read_at(&self) -> Version {
        self.read_at
    }

    /// What it writes when it commits.
    #[must_use]
    pub const fn writes(&self) -> &WriteBatch {
        &self.writes
    }

    /// Whether every key it reads or writes, and every range it scans, is
    /// in `span`.
    #[must_use]
    pub fn fits(&self, span: &Span) -> bool {
        let reads = self.reads.keys.iter().all(|key| span.contains(key));
        let scans = self
            .reads
            .ranges
            .iter()
            .all(|range| span.holds(range.start(), range.end()));
        reads && scans && self.writes.fits(span)
    }

    pub(crate) const fn reads(&self) -> &ReadSet {
        &self.reads
    }

    /// Whether it writes `key`.
    pub(crate) fn writes_key(&self, key: &[u8]) -> bool {
        self.writes.ops().iter().any(|op| op.key() == key)
    }

    /// Whether it read `key`, or scanned a range that holds it.
    pub(crate) fn read_key(&self, key: &[u8]) -> bool {
        self.reads.keys.contains(key) || self.reads.ranges.iter().any(|range| range.contains(key))
    }

    pub(crate) fn into_writes(self) -> WriteBatch {
        self.writes
    }

    /// Appends `read_at u64 | keys u32 | key* | ranges u32 | (start bound |
    /// end bound)*`, then the writes as [`WriteBatch`] encodes them. A
    /// bound is tag 0 unbounded, 1 with an included key, 2 with an
    /// excluded one; keys are length prefixed.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        out.extend_from_slice(&self.read_at.get().to_le_bytes());
        let keys = u32::try_from(self.reads.keys.len()).map_err(|_| Error::BatchTooLarge)?;
        out.extend_from_slice(&keys.to_le_bytes());
        for key in &self.reads.keys {
            put_bytes(out, key).map_err(Error::too_large)?;
        }
        let ranges = u32::try_from(self.reads.ranges.len()).map_err(|_| Error::BatchTooLarge)?;
        out.extend_from_slice(&ranges.to_le_bytes());
        for range in &self.reads.ranges {
            put_bound(out, range.start())?;
            put_bound(out, range.end())?;
        }
        self.writes.encode(out)
    }

    /// Reads a transaction written by [`Transaction::encode`].
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let read_at = Version::new(reader.read()?);
        let mut keys = BTreeSet::new();
        for _ in 0..reader.read::<u32>()? {
            let _new: bool = keys.insert(reader.bytes()?.to_vec());
        }
        let mut ranges = Vec::new();
        for _ in 0..reader.read::<u32>()? {
            let start = bound(reader)?;
            let end = bound(reader)?;
            ranges.push(KeyRange::new(start, end));
        }
        let writes = WriteBatch::decode(reader)?;
        Ok(Self::new(read_at, ReadSet::new(keys, ranges), writes))
    }
}

/// What a transaction read: single keys, and ranges it scanned. A write
/// to a key in a scanned range conflicts even when the key was not there
/// at the read version, so a transaction never misses a row that appears
/// behind its back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReadSet {
    keys: BTreeSet<Vec<u8>>,
    ranges: Vec<KeyRange>,
}

impl ReadSet {
    pub(crate) const fn new(keys: BTreeSet<Vec<u8>>, ranges: Vec<KeyRange>) -> Self {
        Self { keys, ranges }
    }

    pub(crate) const fn keys(&self) -> &BTreeSet<Vec<u8>> {
        &self.keys
    }

    pub(crate) fn ranges(&self) -> &[KeyRange] {
        &self.ranges
    }

    pub(crate) fn add_key(&mut self, key: &[u8]) {
        let _new: bool = self.keys.insert(key.to_vec());
    }

    pub(crate) fn add_range(&mut self, range: KeyRange) {
        self.ranges.push(range);
    }
}

/// The keys between two bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyRange {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl KeyRange {
    pub(crate) fn new(start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
        Self {
            start: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
        }
    }

    pub(crate) fn start(&self) -> Bound<&[u8]> {
        self.start.as_ref().map(Vec::as_slice)
    }

    pub(crate) fn end(&self) -> Bound<&[u8]> {
        self.end.as_ref().map(Vec::as_slice)
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        let after_start = match self.start() {
            Bound::Included(start) => key >= start,
            Bound::Excluded(start) => key > start,
            Bound::Unbounded => true,
        };
        let before_end = match self.end() {
            Bound::Included(end) => key <= end,
            Bound::Excluded(end) => key < end,
            Bound::Unbounded => true,
        };
        after_start && before_end
    }
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

fn put_bound(out: &mut Vec<u8>, bound: Bound<&[u8]>) -> Result<()> {
    let (tag, key) = match bound {
        Bound::Unbounded => (BOUND_UNBOUNDED, None),
        Bound::Included(key) => (BOUND_INCLUDED, Some(key)),
        Bound::Excluded(key) => (BOUND_EXCLUDED, Some(key)),
    };
    out.push(tag);
    key.map_or(Ok(()), |key| put_bytes(out, key).map_err(Error::too_large))
}

fn bound<'a>(reader: &mut Reader<'a>) -> Result<Bound<&'a [u8]>, DecodeError> {
    match reader.read::<u8>()? {
        BOUND_UNBOUNDED => Ok(Bound::Unbounded),
        BOUND_INCLUDED => Ok(Bound::Included(reader.bytes()?)),
        BOUND_EXCLUDED => Ok(Bound::Excluded(reader.bytes()?)),
        other => Err(DecodeError::Tag(other)),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::{Excluded, Included, Unbounded};

    use super::KeyRange;

    #[test]
    fn ranges_hold_their_bounds() {
        let range = KeyRange::new(Excluded(b"a"), Included(b"c"));
        assert!(!range.contains(b"a"), "excluded start");
        assert!(range.contains(b"b"), "inside");
        assert!(range.contains(b"c"), "included end");
        assert!(!range.contains(b"d"), "after");
        let half_open = KeyRange::new(Included(b"b"), Excluded(b"c"));
        assert!(!half_open.contains(b"a"), "before");
        assert!(half_open.contains(b"b"), "included start");
        assert!(!half_open.contains(b"c"), "excluded end");
        assert!(KeyRange::new(Unbounded, Unbounded).contains(b""), "all");
    }
}

#[cfg(kani)]
mod proofs {
    use super::{bound, put_bound};
    use oleaph_codec::Reader;

    /// Decoding a bound never panics, and a decoded bound encodes back to
    /// exactly the bytes it was read from.
    #[kani::proof]
    #[kani::unwind(10)]
    fn bounds_decode_totally_and_roundtrip() {
        let buf: [u8; 8] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let mut reader = Reader::new(&buf[..len]);
        if let Ok(decoded) = bound(&mut reader) {
            let read = len - reader.rest().len();
            let mut out = Vec::new();
            assert!(put_bound(&mut out, decoded).is_ok(), "small key");
            assert_eq!(out.as_slice(), &buf[..read], "roundtrip");
        }
    }
}
