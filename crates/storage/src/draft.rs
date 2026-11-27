//! Transactions being built, before they go into the log.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use crate::batch::WriteBatch;
use crate::outcome::Unreadable;
use crate::state::KvState;
use crate::two_phase::Prepare;
use crate::txn::{KeyRange, ReadSet, Transaction};
use crate::txn_id::TxnId;
use crate::version::Version;

/// A key and its value, as a transaction reads them.
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// A transaction being built: its read version, what it read so far, and
/// the writes it holds back until it commits.
///
/// Reads see the transaction's own writes first, then the state at the
/// read version. A database or a replica hands each read the state to
/// read from, so the same draft works on either.
///
/// ```
/// use kurator_storage::{Command, Draft, KvState, Version, WriteBatch};
///
/// let mut state = KvState::default();
/// let _ = state.apply(Version::new(1), Command::Kv(WriteBatch::new().put("n", "1")));
///
/// let mut draft = Draft::new(state.version());
/// assert_eq!(draft.get(&state, b"n")?, Some(b"1".to_vec()));
/// draft.put("n", "2");
/// assert_eq!(draft.get(&state, b"n")?, Some(b"2".to_vec()), "its own write");
/// let transaction = draft.into_transaction().expect("it writes");
/// assert_eq!(transaction.read_at(), Version::new(1));
/// # Ok::<(), kurator_storage::Unreadable>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Draft {
    read_at: Version,
    reads: ReadSet,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Draft {
    /// A transaction that reads the state at `read_at`.
    #[must_use]
    pub const fn new(read_at: Version) -> Self {
        Self {
            read_at,
            reads: ReadSet::new(BTreeSet::new(), Vec::new()),
            writes: BTreeMap::new(),
        }
    }

    /// The version its reads see.
    #[must_use]
    pub const fn read_at(&self) -> Version {
        self.read_at
    }

    /// The value of `key`: the transaction's own write of it, or else its
    /// value in `state` at the read version.
    ///
    /// # Errors
    ///
    /// When `state` cannot read the read version.
    pub fn get(&mut self, state: &KvState, key: &[u8]) -> Result<Option<Vec<u8>>, Unreadable> {
        if let Some(written) = self.writes.get(key) {
            return Ok(written.clone());
        }
        let value = state.view(self.read_at)?.get(key).map(<[u8]>::to_vec);
        self.reads.add_key(key);
        Ok(value)
    }

    /// The entries between `start` and `end` in key order, with the
    /// transaction's own writes laid over the state at the read version.
    ///
    /// # Errors
    ///
    /// When `state` cannot read the read version.
    pub fn range(
        &mut self,
        state: &KvState,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<Vec<KeyValue>, Unreadable> {
        let range = KeyRange::new(start, end);
        let mut entries: BTreeMap<Vec<u8>, Vec<u8>> = state
            .view(self.read_at)?
            .range(start, end)
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        for (key, written) in self.writes.iter().filter(|(key, _)| range.contains(key)) {
            match written {
                Some(value) => drop(entries.insert(key.clone(), value.clone())),
                None => drop(entries.remove(key)),
            }
        }
        self.reads.add_range(range);
        Ok(entries.into_iter().collect())
    }

    /// Sets `key` to `value` when the transaction commits.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        drop(self.writes.insert(key.into(), Some(value.into())));
    }

    /// Removes `key` when the transaction commits.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        drop(self.writes.insert(key.into(), None));
    }

    /// The first key it writes, in key order, if it writes any.
    #[must_use]
    pub fn first_write(&self) -> Option<&[u8]> {
        self.writes.keys().next().map(Vec::as_slice)
    }

    /// This shard's part of the transaction across shards `id`, whose
    /// primary key is `primary`, to prepare: what it read and writes here,
    /// which may be nothing written. Its reads must be checked even then,
    /// since the other shards read at other versions.
    #[must_use]
    pub fn into_prepare(self, id: TxnId, primary: Vec<u8>) -> Prepare {
        let Self {
            read_at,
            reads,
            writes,
        } = self;
        Prepare::new(id, primary, Transaction::new(read_at, reads, batch(writes)))
    }

    /// The transaction to commit, or `None` when it writes nothing: its
    /// reads all saw one version, so a read-only transaction is already
    /// serializable at its read version and needs no commit.
    #[must_use]
    pub fn into_transaction(self) -> Option<Transaction> {
        if self.writes.is_empty() {
            return None;
        }
        Some(Transaction::new(
            self.read_at,
            self.reads,
            batch(self.writes),
        ))
    }
}

/// `writes`, each a value or a removal, as one batch.
fn batch(writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> WriteBatch {
    writes
        .into_iter()
        .fold(WriteBatch::new(), |batch, (key, value)| match value {
            Some(value) => batch.put(key, value),
            None => batch.delete(key),
        })
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::{Excluded, Unbounded};

    use super::Draft;
    use crate::batch::WriteBatch;
    use crate::command::Command;
    use crate::state::KvState;
    use crate::version::Version;

    fn state() -> KvState {
        let mut state = KvState::default();
        let _outcome = state.apply(
            Version::new(1),
            Command::Kv(WriteBatch::new().put("a", "1").put("b", "2").put("c", "3")),
        );
        state
    }

    #[test]
    fn reads_see_their_own_writes_over_the_state() {
        let state = state();
        let mut draft = Draft::new(state.version());
        draft.put("b", "9");
        draft.delete("c");
        draft.put("d", "4");
        assert_eq!(draft.get(&state, b"a"), Ok(Some(b"1".to_vec())), "state");
        assert_eq!(draft.get(&state, b"c"), Ok(None), "own delete");
        let entries = draft.range(&state, Excluded(b"a"), Unbounded).unwrap();
        assert_eq!(
            entries,
            [
                (b"b".to_vec(), b"9".to_vec()),
                (b"d".to_vec(), b"4".to_vec())
            ],
            "overlay"
        );
        let outside = draft.range(&state, Unbounded, Excluded(b"b")).unwrap();
        assert_eq!(outside, [(b"a".to_vec(), b"1".to_vec())], "writes outside");
        let transaction = draft.into_transaction().unwrap();
        assert_eq!(transaction.read_at(), Version::new(1), "read version");
        assert_eq!(transaction.reads().keys().len(), 1, "own writes not read");
        assert_eq!(transaction.reads().ranges().len(), 2, "ranges");
        assert_eq!(transaction.writes().ops().len(), 3, "writes");
    }

    #[test]
    fn a_read_only_draft_has_nothing_to_commit() {
        let state = state();
        let mut draft = Draft::new(state.version());
        assert_eq!(draft.read_at(), Version::new(1), "read version");
        assert_eq!(draft.get(&state, b"a"), Ok(Some(b"1".to_vec())), "read");
        assert_eq!(draft.into_transaction(), None, "nothing to commit");
    }

    #[test]
    fn reads_fail_where_the_state_cannot_look() {
        let state = state();
        let mut draft = Draft::new(Version::new(5));
        assert!(draft.get(&state, b"a").is_err(), "ahead");
        assert!(draft.range(&state, Unbounded, Unbounded).is_err(), "ahead");
    }
}
