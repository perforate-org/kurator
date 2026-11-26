//! The in-memory state: every key and value, and the last applied record.

use std::collections::BTreeMap;
use std::ops::Bound;

use kurator_wal::Lsn;

use crate::batch::{Command, KvOp};

/// The key-value state as of [`KvState::applied`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvState {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
    applied: Lsn,
}

impl KvState {
    /// A state restored from a snapshot taken at `applied`.
    pub(crate) fn restore<'a>(
        applied: Lsn,
        entries: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    ) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            applied,
        }
    }

    /// Applies the command logged at `lsn`, which must come after
    /// [`KvState::applied`].
    pub(crate) fn apply(&mut self, lsn: Lsn, command: Command) {
        debug_assert!(lsn > self.applied, "records apply in log order");
        let Command::Kv(batch) = command;
        for op in batch.into_ops() {
            match op {
                KvOp::Put { key, value } => drop(self.entries.insert(key, value)),
                KvOp::Delete { key } => drop(self.entries.remove(&key)),
            }
        }
        self.applied = lsn;
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

    /// The last record applied, or [`Lsn::ZERO`].
    #[must_use]
    pub const fn applied(&self) -> Lsn {
        self.applied
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

    use super::KvState;
    use crate::batch::{Command, WriteBatch};
    use kurator_wal::Lsn;

    fn state() -> KvState {
        let mut state = KvState::default();
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

    fn keys<'a>(entries: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Vec<&'a [u8]> {
        entries.map(|(key, _)| key).collect()
    }

    #[test]
    fn batches_apply_in_order() {
        let state = state();
        assert_eq!(state.get(b"a"), Some(b"9".as_slice()), "overwritten");
        assert_eq!(state.get(b"b"), None, "deleted");
        assert_eq!(state.applied(), Lsn::new(2), "applied");
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

    proptest! {
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
