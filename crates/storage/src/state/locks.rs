//! What a state holds for transactions across shards: the parts it
//! prepared, and how it decided each transaction it was told about.

use std::collections::BTreeMap;
use std::io::{self, Write};

use oleaph_codec::{DecodeError, Reader, put_bytes};

use crate::across::{Deadline, TxnId};
use crate::batch::{KvOp, WriteBatch};
use crate::outcome::{Abort, Outcome};
use crate::span::Span;
use crate::two_phase::{Decided, Prepare};
use crate::txn::ReadSet;
use crate::version::Version;

/// The transactions across shards a state took part in.
///
/// A prepared transaction locks every key it read or writes: a write to a
/// key it read or writes, and a read of a key it writes, conflict with it
/// until it is decided. Decisions are kept, so a transaction is decided
/// once whatever is told twice, until a [`Collect`](crate::Collect)
/// forgets them: behind the fence, nothing is prepared or decided any
/// more, and forgotten decisions are not missed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Locks {
    fence: Deadline,
    prepared: BTreeMap<TxnId, Prepare>,
    decided: BTreeMap<TxnId, Record>,
}

/// A decision, the primary key of its transaction, which tells which half
/// of a split keeps it, and its deadline, which tells when it may be
/// forgotten.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    primary: Vec<u8>,
    deadline: Deadline,
    decided: Decided,
}

const DECIDED_COMMITTED: u8 = 1;
const DECIDED_ABORTED: u8 = 2;

impl Locks {
    /// No transaction at all.
    pub(super) const EMPTY: Self = Self {
        fence: Deadline::new(0),
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
                deadline: prepared.deadline(),
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

    /// Every decision kept, in id order.
    pub(super) fn decisions(&self) -> impl Iterator<Item = (TxnId, Decided)> + '_ {
        self.decided
            .iter()
            .map(|(id, record)| (*id, record.decided))
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

    /// The transactions prepared and not decided yet.
    pub(super) fn pending(&self) -> impl Iterator<Item = &Prepare> {
        self.prepared.values()
    }

    /// Whether a transaction with `deadline` is behind the fence: one
    /// this state takes no part in any more.
    pub(super) fn is_fenced(&self, deadline: Deadline) -> bool {
        deadline < self.fence
    }

    /// Moves the fence up to `horizon`, unless a transaction with a
    /// deadline before it is prepared: then that transaction.
    pub(super) fn fence(&mut self, horizon: Deadline) -> Result<(), Abort> {
        let held = self
            .prepared
            .values()
            .find(|prepared| prepared.deadline() < horizon);
        if let Some(held) = held {
            return Err(Abort::Held {
                by: held.id(),
                primary: held.primary().to_vec(),
                deadline: held.deadline(),
            });
        }
        self.fence = self.fence.max(horizon);
        Ok(())
    }

    /// Forgets the decisions on the transactions whose deadline is before
    /// `horizon` and the fence.
    pub(super) fn forget(&mut self, horizon: Deadline) {
        let before = horizon.min(self.fence);
        self.decided.retain(|_, record| record.deadline >= before);
    }

    /// Holds `prepare` until it is decided.
    pub(super) fn lock(&mut self, prepare: Prepare) {
        drop(self.prepared.insert(prepare.id(), prepare));
    }

    /// Records that `id`, whose primary key is `primary` and whose
    /// deadline is `deadline`, came to `decided`, and hands back its
    /// prepared part, if it had one. The deadline its part was prepared
    /// with stands over `deadline`.
    pub(super) fn decide(
        &mut self,
        id: TxnId,
        primary: &[u8],
        deadline: Deadline,
        decided: Decided,
    ) -> Option<Prepare> {
        let prepared = self.prepared.remove(&id);
        let record = Record {
            primary: primary.to_vec(),
            deadline: prepared.as_ref().map_or(deadline, Prepare::deadline),
            decided,
        };
        drop(self.decided.insert(id, record));
        prepared
    }

    /// Moves what belongs to the keys at or after `at` into the locks this
    /// returns: the transactions whose primary key is there. Both halves
    /// keep the fence.
    pub(super) fn split_off(&mut self, at: &[u8]) -> Self {
        Self {
            fence: self.fence,
            prepared: split_off(&mut self.prepared, |prepared| prepared.primary() >= at),
            decided: split_off(&mut self.decided, |record| record.primary.as_slice() >= at),
        }
    }

    /// Writes `fence u64 | prepared u32 | prepare* | decided u32 | (id
    /// u128 | primary | deadline u64 | tag u8 | version u64?)*`, each in
    /// id order, prepares as [`Prepare`] encodes them; tag 1 is a commit
    /// at the version, tag 2 an abort.
    pub(super) fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        out.write_all(&self.fence.millis().to_le_bytes())?;
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
            out.write_all(&record.deadline.millis().to_le_bytes())?;
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
        locks.fence = Deadline::new(reader.read()?);
        for _ in 0..reader.read::<u32>()? {
            let prepared = Prepare::decode(reader)?;
            let after = locks
                .prepared
                .last_key_value()
                .is_none_or(|(last, _)| *last < prepared.id());
            if !after || locks.is_fenced(prepared.deadline()) {
                return Err(DecodeError::Invalid);
            }
            locks.lock(prepared);
        }
        for _ in 0..reader.read::<u32>()? {
            let id = TxnId::decode(reader)?;
            let primary = reader.bytes()?.to_vec();
            let deadline = Deadline::new(reader.read()?);
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
            let record = Record {
                primary,
                deadline,
                decided,
            };
            drop(locks.decided.insert(id, record));
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

#[cfg(test)]
mod tests {
    use oleaph_codec::{DecodeError, Reader, put_bytes};

    use super::{DECIDED_ABORTED, DECIDED_COMMITTED, Locks};
    use crate::across::{Deadline, TxnId};
    use crate::draft::Draft;
    use crate::version::Version;

    /// Locks fenced at `fence` with the transactions `prepared`, then
    /// `decided`, each committed at its version or aborted; every deadline
    /// is 1.
    fn encode(fence: u64, prepared: &[u128], decided: &[(u128, Option<u64>)]) -> Vec<u8> {
        let mut out = fence.to_le_bytes().to_vec();
        out.extend_from_slice(&u32::try_from(prepared.len()).unwrap().to_le_bytes());
        for id in prepared {
            let prepare = Draft::new(Version::ZERO).into_prepare(
                TxnId::new(*id),
                b"p".to_vec(),
                Deadline::new(1),
            );
            prepare.encode(&mut out).unwrap();
        }
        out.extend_from_slice(&u32::try_from(decided.len()).unwrap().to_le_bytes());
        for (id, committed) in decided {
            out.extend_from_slice(&id.to_le_bytes());
            put_bytes(&mut out, b"p").unwrap();
            out.extend_from_slice(&1_u64.to_le_bytes());
            match committed {
                Some(version) => {
                    out.push(DECIDED_COMMITTED);
                    out.extend_from_slice(&version.to_le_bytes());
                }
                None => out.push(DECIDED_ABORTED),
            }
        }
        out
    }

    /// The locks `encoded` holds, read back as of version 3.
    fn decode(encoded: &[u8]) -> Result<Locks, DecodeError> {
        let mut reader = Reader::new(encoded);
        let locks = Locks::decode(&mut reader, Version::new(3))?;
        reader.finish()?;
        Ok(locks)
    }

    #[test]
    fn malformed_locks_are_rejected() {
        let valid = encode(1, &[1], &[(2, Some(3)), (3, None)]);
        assert!(decode(&valid).is_ok(), "valid");
        let behind = encode(2, &[], &[(2, Some(3))]);
        assert!(decode(&behind).is_ok(), "a decision behind the fence");
        for (fence, prepared, decided, what) in [
            (0, &[1, 1][..], &[][..], "prepared twice"),
            (0, &[2, 1], &[], "prepared out of order"),
            (0, &[], &[(2, None), (2, None)], "decided twice"),
            (0, &[], &[(3, None), (2, None)], "decided out of order"),
            (0, &[], &[(2, Some(4))], "committed after the state"),
            (0, &[1], &[(1, None)], "prepared and decided"),
            (2, &[1], &[], "prepared behind the fence"),
        ] {
            let malformed = encode(fence, prepared, decided);
            assert_eq!(decode(&malformed), Err(DecodeError::Invalid), "{what}");
        }
    }
}
