//! What a state holds for transactions across shards: the parts it
//! prepared, and how it decided each transaction it was told about.

use std::collections::BTreeMap;
use std::io::{self, Write};

use kurator_codec::{DecodeError, Reader, put_bytes};

use crate::batch::{KvOp, WriteBatch};
use crate::outcome::{Abort, Outcome};
use crate::span::Span;
use crate::two_phase::{Decided, Prepare};
use crate::txn::ReadSet;
use crate::txn_id::TxnId;
use crate::version::Version;

/// The transactions across shards a state took part in.
///
/// A prepared transaction locks every key it read or writes: a write to a
/// key it read or writes, and a read of a key it writes, conflict with it
/// until it is decided. Decisions are kept, so a transaction is decided
/// once whatever is told twice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Locks {
    prepared: BTreeMap<TxnId, Prepare>,
    decided: BTreeMap<TxnId, Record>,
}

/// A decision, and the primary key of its transaction, which tells which
/// half of a split keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    primary: Vec<u8>,
    decided: Decided,
}

const DECIDED_COMMITTED: u8 = 1;
const DECIDED_ABORTED: u8 = 2;

impl Locks {
    /// No transaction at all.
    pub(super) const EMPTY: Self = Self {
        prepared: BTreeMap::new(),
        decided: BTreeMap::new(),
    };

    /// The first lock that reading `reads` and writing `writes` runs into.
    pub(super) fn conflict(&self, reads: &ReadSet, writes: &WriteBatch) -> Option<Abort> {
        self.prepared.values().find_map(|prepared| {
            let theirs = prepared.transaction();
            let written = writes
                .ops()
                .iter()
                .map(KvOp::key)
                .find(|key| theirs.writes_key(key) || theirs.read_key(key));
            let read = || reads.keys().iter().find(|key| theirs.writes_key(key));
            let scanned = || {
                theirs
                    .writes()
                    .ops()
                    .iter()
                    .map(KvOp::key)
                    .find(|key| reads.ranges().iter().any(|range| range.contains(key)))
            };
            let key = written
                .or_else(|| read().map(Vec::as_slice))
                .or_else(scanned)?;
            Some(Abort::Locked {
                key: key.to_vec(),
                by: prepared.id(),
                primary: prepared.primary().to_vec(),
            })
        })
    }

    /// What `id` came to, as an outcome, if it was decided.
    pub(super) fn told(&self, id: TxnId) -> Option<Outcome> {
        Some(match self.decided.get(&id)?.decided {
            Decided::Committed(version) => Outcome::Committed(version),
            Decided::Aborted => Outcome::Aborted(Abort::RolledBack(id)),
        })
    }

    /// How `id` was decided here, if it was.
    pub(super) fn decided(&self, id: TxnId) -> Option<Decided> {
        Some(self.decided.get(&id)?.decided)
    }

    /// Whether `id` is prepared and not decided yet.
    pub(super) fn is_prepared(&self, id: TxnId) -> bool {
        self.prepared.contains_key(&id)
    }

    /// Whether every prepared part is in `span`.
    pub(super) fn fits(&self, span: &Span) -> bool {
        self.prepared.values().all(|prepared| prepared.fits(span))
    }

    /// Whether any transaction is prepared and not decided yet.
    pub(super) fn is_locked(&self) -> bool {
        !self.prepared.is_empty()
    }

    /// The transactions prepared and not decided yet, with their primary
    /// keys.
    pub(super) fn pending(&self) -> impl Iterator<Item = (TxnId, &[u8])> {
        self.prepared
            .values()
            .map(|prepared| (prepared.id(), prepared.primary()))
    }

    /// Holds `prepare` until it is decided.
    pub(super) fn lock(&mut self, prepare: Prepare) {
        drop(self.prepared.insert(prepare.id(), prepare));
    }

    /// Records that `id`, whose primary key is `primary`, came to
    /// `decided`, and hands back its prepared part, if it had one.
    pub(super) fn decide(
        &mut self,
        id: TxnId,
        primary: &[u8],
        decided: Decided,
    ) -> Option<Prepare> {
        let record = Record {
            primary: primary.to_vec(),
            decided,
        };
        drop(self.decided.insert(id, record));
        self.prepared.remove(&id)
    }

    /// Moves what belongs to the keys at or after `at` into the locks this
    /// returns: the transactions whose primary key is there.
    pub(super) fn split_off(&mut self, at: &[u8]) -> Self {
        Self {
            prepared: split_off(&mut self.prepared, |prepared| prepared.primary() >= at),
            decided: split_off(&mut self.decided, |record| record.primary.as_slice() >= at),
        }
    }

    /// Writes `prepared u32 | prepare* | decided u32 | (id u128 | primary
    /// | tag u8 | version u64?)*`, each in id order, prepares as
    /// [`Prepare`] encodes them; tag 1 is a commit at the version, tag 2
    /// an abort.
    pub(super) fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        put_count(out, self.prepared.len())?;
        for prepared in self.prepared.values() {
            let mut encoded = Vec::new();
            prepared.encode(&mut encoded).map_err(io::Error::other)?;
            out.write_all(&encoded)?;
        }
        put_count(out, self.decided.len())?;
        for (id, record) in &self.decided {
            out.write_all(&id.get().to_le_bytes())?;
            put_bytes(out, &record.primary)?;
            match record.decided {
                Decided::Committed(version) => {
                    out.write_all(&[DECIDED_COMMITTED])?;
                    out.write_all(&version.get().to_le_bytes())?;
                }
                Decided::Aborted => out.write_all(&[DECIDED_ABORTED])?,
            }
        }
        Ok(())
    }

    /// Reads locks written by [`Locks::encode`] off the front of `reader`.
    pub(super) fn decode(reader: &mut Reader<'_>, newest: Version) -> Result<Self, DecodeError> {
        let mut locks = Self::EMPTY;
        for _ in 0..reader.read::<u32>()? {
            let prepared = Prepare::decode(reader)?;
            if locks
                .prepared
                .last_key_value()
                .is_some_and(|(last, _)| *last >= prepared.id())
            {
                return Err(DecodeError::Invalid);
            }
            locks.lock(prepared);
        }
        for _ in 0..reader.read::<u32>()? {
            let id = TxnId::decode(reader)?;
            let primary = reader.bytes()?.to_vec();
            let decided = match reader.read::<u8>()? {
                DECIDED_COMMITTED => Decided::Committed(Version::new(reader.read()?)),
                DECIDED_ABORTED => Decided::Aborted,
                other => return Err(DecodeError::Tag(other)),
            };
            let ordered = locks
                .decided
                .last_key_value()
                .is_none_or(|(last, _)| *last < id);
            let past = !matches!(decided, Decided::Committed(version) if version > newest);
            if !ordered || !past || locks.prepared.contains_key(&id) {
                return Err(DecodeError::Invalid);
            }
            drop(locks.decided.insert(id, Record { primary, decided }));
        }
        Ok(locks)
    }
}

/// Moves the entries of `map` that belong `right` into the map it returns.
fn split_off<V>(map: &mut BTreeMap<TxnId, V>, right: impl Fn(&V) -> bool) -> BTreeMap<TxnId, V> {
    let (moved, kept) = std::mem::take(map)
        .into_iter()
        .partition(|(_, value)| right(value));
    *map = kept;
    moved
}

/// Writes `len` as a `u32` count.
fn put_count(out: &mut (impl Write + ?Sized), len: usize) -> io::Result<()> {
    let count = u32::try_from(len).map_err(io::Error::other)?;
    out.write_all(&count.to_le_bytes())
}
