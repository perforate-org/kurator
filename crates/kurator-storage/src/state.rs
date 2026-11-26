//! The in-memory state: every key and value.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::ops::Bound;

use kurator_codec::{DecodeError, Reader, put_bytes};
use kurator_wal::Lsn;

use crate::batch::{Command, KvOp};

/// The key-value state: every key and its value.
///
/// A state changes only by [`KvState::apply`], one [`Command`] at a time,
/// so replicas that apply the same commands in the same order hold the same
/// state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvState {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KvState {
    /// Applies `command`.
    pub fn apply(&mut self, command: Command) {
        let Command::Kv(batch) = command;
        for op in batch.into_ops() {
            match op {
                KvOp::Put { key, value } => drop(self.entries.insert(key, value)),
                KvOp::Delete { key } => drop(self.entries.remove(&key)),
            }
        }
    }

    /// Writes every entry as `count u64` followed by that many length
    /// prefixed `key | value` pairs, in key order.
    ///
    /// # Errors
    ///
    /// When `out` fails or a key or value is too long for its prefix.
    pub fn encode(&self, out: &mut impl Write) -> io::Result<()> {
        let count = u64::try_from(self.len()).map_err(io::Error::other)?;
        out.write_all(&count.to_le_bytes())?;
        for (key, value) in self.iter() {
            put_bytes(out, key)?;
            put_bytes(out, value)?;
        }
        Ok(())
    }

    /// Reads entries written by [`KvState::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] when the input ends early.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let count = reader.u64()?;
        let mut entries = BTreeMap::new();
        for _ in 0..count {
            let (key, value) = (reader.bytes()?, reader.bytes()?);
            drop(entries.insert(key.to_vec(), value.to_vec()));
        }
        Ok(Self { entries })
    }

    /// The value of `key`.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    /// The entries with keys between `start` and `end`, in key order. An
    /// empty or reversed range yields nothing.
    pub fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        is_ordered(start, end)
            .then(|| self.entries.range::<[u8], _>((start, end)))
            .into_iter()
            .flatten()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
    }

    /// Every entry, in key order.
    #[must_use = "iterators do nothing unless consumed"]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&[u8], &[u8])> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
    }

    /// How many keys there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// A [`KvState`] and the last log record applied to it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LoggedState {
    state: KvState,
    applied: Lsn,
}

impl LoggedState {
    pub(crate) const fn new(state: KvState, applied: Lsn) -> Self {
        Self { state, applied }
    }

    /// Applies the command logged at `lsn`, which must come after
    /// [`LoggedState::applied`].
    pub(crate) fn apply(&mut self, lsn: Lsn, command: Command) {
        debug_assert!(lsn > self.applied, "records apply in log order");
        self.state.apply(command);
        self.applied = lsn;
    }

    pub(crate) const fn state(&self) -> &KvState {
        &self.state
    }

    /// The last record applied, or [`Lsn::ZERO`].
    pub(crate) const fn applied(&self) -> Lsn {
        self.applied
    }
}

/// Whether `BTreeMap::range` accepts the bounds: it panics when start is
/// after end, or when both exclude the same key.
fn is_ordered(start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    match (start, end) {
        (Bound::Excluded(start), Bound::Excluded(end)) => start < end,
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => start <= end,
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::{Excluded, Included, Unbounded};

    use proptest::prelude::*;

    use super::{KvState, LoggedState};
    use crate::batch::{Command, WriteBatch};
    use kurator_codec::Reader;
    use kurator_wal::Lsn;

    fn logged() -> LoggedState {
        let mut state = LoggedState::default();
        state.apply(
            Lsn::new(1),
            Command::Kv(WriteBatch::new().put("a", "1").put("b", "2").put("c", "3")),
        );
        state.apply(
            Lsn::new(2),
            Command::Kv(WriteBatch::new().delete("b").put("a", "9")),
        );
        state
    }

    fn state() -> KvState {
        logged().state().clone()
    }

    fn keys<'a>(entries: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Vec<&'a [u8]> {
        entries.map(|(key, _)| key).collect()
    }

    #[test]
    fn batches_apply_in_order() {
        assert_eq!(logged().applied(), Lsn::new(2), "applied");
        let state = state();
        assert_eq!(state.get(b"a"), Some(b"9".as_slice()), "overwritten");
        assert_eq!(state.get(b"b"), None, "deleted");
        assert_eq!((state.len(), state.is_empty()), (2, false), "size");
        assert!(KvState::default().is_empty(), "empty");
    }

    #[test]
    fn ranges_follow_their_bounds() {
        let state = state();
        assert_eq!(keys(state.range(Unbounded, Unbounded)), [b"a", b"c"], "all");
        assert_eq!(
            keys(state.range(Excluded(b"a"), Unbounded)),
            [b"c"],
            "after a"
        );
        assert_eq!(
            keys(state.range(Included(b"a"), Excluded(b"c"))),
            [b"a"],
            "half open"
        );
        assert_eq!(
            keys(state.range(Included(b"c"), Included(b"a"))),
            Vec::<&[u8]>::new(),
            "reversed"
        );
        assert_eq!(
            keys(state.range(Excluded(b"a"), Excluded(b"a"))),
            Vec::<&[u8]>::new(),
            "empty"
        );
        assert_eq!(
            keys(state.range(Included(b"a"), Excluded(b"a"))),
            Vec::<&[u8]>::new(),
            "empty half open"
        );
    }

    #[test]
    fn encoding_roundtrips() {
        let state = state();
        let mut out = Vec::new();
        state.encode(&mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(KvState::decode(&mut reader), Ok(state), "roundtrip");
        assert_eq!(reader.finish(), Ok(()), "every byte read");
    }

    proptest! {
        #[test]
        fn decoding_arbitrary_bytes_never_panics(buf in proptest::collection::vec(any::<u8>(), 0..64)) {
            drop(KvState::decode(&mut Reader::new(&buf)));
        }

        #[test]
        fn range_never_panics(start in proptest::option::of(any::<(bool, u8)>()), end in proptest::option::of(any::<(bool, u8)>())) {
            let state = state();
            let bound = |bound: Option<(bool, u8)>, byte: &[u8; 1]| -> std::ops::Bound<Vec<u8>> {
                match bound {
                    None => Unbounded,
                    Some((true, _)) => Included(byte.to_vec()),
                    Some((false, _)) => Excluded(byte.to_vec()),
                }
            };
            let (start_key, end_key) = ([start.map_or(0, |(_, b)| b)], [end.map_or(0, |(_, b)| b)]);
            let (start, end) = (bound(start, &start_key), bound(end, &end_key));
            let found = keys(state.range(start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice))).len();
            prop_assert!(found <= state.len(), "subset");
        }
    }
}
