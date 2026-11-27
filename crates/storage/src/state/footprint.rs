//! How much a state's keys take: what decides when a shard splits, and
//! where.

use super::History;

/// The bytes a state's keys take as [`KvState::encode`] writes them.
///
/// It counts each key with every version of its value the state still
/// keeps, removals included. The snapshot of a shard is about this large,
/// and so is the shard in memory.
///
/// [`KvState::encode`]: super::KvState::encode
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Footprint(u64);

impl Footprint {
    /// Nothing at all.
    pub const ZERO: Self = Self(0);

    /// A footprint of `bytes`.
    #[must_use]
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// The bytes.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }

    /// The footprint of one key and its history, as it is encoded: the
    /// length prefixed key, the count of versions, and each version with
    /// its tag and, unless it is a removal, its length prefixed value.
    pub(super) fn of(key: &[u8], history: &History) -> Self {
        let versions = history.0.iter().map(|(_, value)| {
            let value = value.as_ref().map_or(0, |value| prefixed(value.len()));
            VERSION.saturating_add(TAG).saturating_add(value)
        });
        Self(
            versions
                .fold(prefixed(key.len()), u64::saturating_add)
                .saturating_add(COUNT),
        )
    }

    pub(super) const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

/// The bytes of a length prefix, and of the count of a key's versions.
const COUNT: u64 = 4;
/// The bytes of a version.
const VERSION: u64 = 8;
/// The bytes of the tag that tells a value from a removal.
const TAG: u64 = 1;

/// `len` bytes after their length prefix.
fn prefixed(len: usize) -> u64 {
    u64::try_from(len).map_or(u64::MAX, |len| len.saturating_add(COUNT))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::Footprint;
    use crate::batch::WriteBatch;
    use crate::command::Command;
    use crate::state::KvState;
    use crate::version::Version;

    fn encoded(state: &KvState) -> u64 {
        let mut out = Vec::new();
        state.encode(&mut out).unwrap();
        u64::try_from(out.len()).unwrap()
    }

    fn written(writes: &[(u8, Option<usize>)]) -> KvState {
        let mut state = KvState::default();
        for (step, (key, value)) in (1..).zip(writes) {
            let batch = value.map_or_else(
                || WriteBatch::new().delete([*key]),
                |len| WriteBatch::new().put([*key], vec![0; len]),
            );
            let _outcome = state.apply(Version::new(step), Command::Kv(batch));
        }
        state
    }

    #[test]
    fn a_footprint_counts_every_kept_version() {
        assert_eq!(KvState::default().footprint(), Footprint::ZERO, "empty");
        // Key: 4 + 1, count 4, then 8 + 1 + 4 + 3 and 8 + 1 for the removal.
        let state = written(&[(1, Some(3)), (1, None)]);
        assert_eq!(state.footprint(), Footprint::new(5 + 4 + 16 + 9), "a key");
        assert_eq!(Footprint::new(7).bytes(), 7, "bytes");
    }

    #[test]
    fn the_middle_cuts_the_footprint_in_halves() {
        assert_eq!(KvState::default().middle(), None, "empty");
        assert_eq!(written(&[(1, Some(9))]).middle(), None, "one key");
        let even = written(&[(1, Some(9)), (2, Some(9)), (3, Some(9)), (4, Some(9))]);
        assert_eq!(even.middle(), Some([3].as_slice()), "even");
        let heavy_last = written(&[(1, Some(1)), (2, Some(1)), (3, Some(90))]);
        assert_eq!(heavy_last.middle(), Some([3].as_slice()), "the last key");
        let heavy_first = written(&[(1, Some(90)), (2, Some(1)), (3, Some(1))]);
        assert_eq!(heavy_first.middle(), Some([2].as_slice()), "the first key");
    }

    fn writes() -> impl Strategy<Value = Vec<(u8, Option<usize>)>> {
        proptest::collection::vec((0_u8..8, proptest::option::of(0_usize..40)), 0..24)
    }

    proptest! {
        #[test]
        fn a_footprint_is_the_encoded_keys(writes in writes()) {
            let state = written(&writes);
            let empty = KvState::new(state.retention());
            let header = encoded(&empty);
            prop_assert_eq!(state.footprint().bytes(), encoded(&state).checked_sub(header).unwrap());
        }

        #[test]
        fn the_middle_leaves_keys_on_both_sides(writes in writes()) {
            let state = written(&writes);
            let keys: Vec<&[u8]> = state.keys().collect();
            match state.middle() {
                None => prop_assert!(keys.len() < 2),
                Some(middle) => {
                    prop_assert!(keys.first().is_some_and(|first| *first < middle));
                    prop_assert!(keys.contains(&middle));
                }
            }
        }

        #[test]
        fn no_other_key_cuts_more_evenly_on_the_heavy_side(writes in writes()) {
            let state = written(&writes);
            let Some(middle) = state.middle() else { return Ok(()) };
            let total = state.footprint().bytes();
            let left = |at: &[u8]| -> u64 {
                let mut left = state.clone();
                let _right = left.split_off(at);
                left.footprint().bytes()
            };
            let cut = left(middle);
            // The left side holds at least half, unless the last key
            // alone holds more; and the key before holds less than half.
            let last = state.keys().last();
            prop_assert!(cut.saturating_mul(2) >= total || Some(middle) == last);
            let before = state.keys().take_while(|key| *key < middle).last();
            if let Some(before) = before.filter(|key| Some(*key) != state.keys().next()) {
                prop_assert!(left(before).saturating_mul(2) < total);
            }
        }
    }
}
