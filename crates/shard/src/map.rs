//! Shard maps: which shard holds which key.

use std::collections::BTreeMap;
use std::ops::Bound;

use kurator_storage::End;

use crate::id::ShardId;
use crate::shard::{Halves, Shard};

/// Shards that hold every key exactly once, ordered by their spans.
///
/// A map changes only by whole splits, so it never has a gap or an
/// overlap: [`ShardMap::locate`] always finds a shard.
///
/// ```
/// use kurator_shard::{Epoch, Shard, ShardId, ShardMap, Split};
///
/// let mut map = ShardMap::new();
/// let halves = Shard::first()
///     .split(&Split::new("m", ShardId::new(2), Epoch::ZERO))
///     .expect("inside");
/// map.split(&halves).expect("the shard the map holds");
/// assert_eq!(map.locate(b"apple").id(), ShardId::FIRST);
/// assert_eq!(map.locate(b"melon").id(), ShardId::new(2));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    /// The shard that holds the empty key, and so starts every span.
    first: Shard,
    /// Every other shard, by the first key it holds.
    rest: BTreeMap<Vec<u8>, Shard>,
}

/// A split the map cannot take: the shard it splits is not in the map as
/// it was before the split, or the new shard's id is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the map does not hold {0} as it was before the split")]
pub struct Stale(ShardId);

impl Default for ShardMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardMap {
    /// The map of a new cluster: [`Shard::first`] holds every key.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            first: Shard::first(),
            rest: BTreeMap::new(),
        }
    }

    /// The map of `shards`, or `None` unless they hold every key exactly
    /// once and their ids differ. The meta shard is in no map.
    #[must_use]
    pub fn from_shards(shards: impl IntoIterator<Item = Shard>) -> Option<Self> {
        let mut by_start: BTreeMap<Vec<u8>, Shard> = BTreeMap::new();
        for shard in shards {
            let start = shard.span().start().to_vec();
            if by_start.insert(start, shard).is_some() {
                return None;
            }
        }
        let (_, first) = by_start.pop_first()?;
        if !first.span().start().is_empty() || first.id() == ShardId::META {
            return None;
        }
        let mut end = first.span().end().clone();
        let mut ids = vec![ShardId::META, first.id()];
        for shard in by_start.values() {
            if end != End::Before(shard.span().start().to_vec()) || ids.contains(&shard.id()) {
                return None;
            }
            ids.push(shard.id());
            end = shard.span().end().clone();
        }
        (end == End::Unbounded).then_some(Self {
            first,
            rest: by_start,
        })
    }

    /// The shard that holds `key`.
    #[must_use]
    pub fn locate(&self, key: &[u8]) -> &Shard {
        self.rest
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()
            .map_or(&self.first, |(_, shard)| shard)
    }

    /// The shard named `id`, if the map holds it.
    #[must_use]
    pub fn get(&self, id: ShardId) -> Option<&Shard> {
        self.iter().find(|shard| shard.id() == id)
    }

    /// The shards that hold some key between `start` and `end`, in key
    /// order.
    pub fn overlapping<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&'a [u8]>,
    ) -> impl Iterator<Item = &'a Shard> + 'a {
        let first_key = match start {
            Bound::Unbounded => Vec::new(),
            Bound::Included(key) => key.to_vec(),
            Bound::Excluded(key) => [key, &[0]].concat(),
        };
        let from = self.locate(&first_key).span().start().to_vec();
        let before_end = move |key: &[u8]| match end {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= end,
            Bound::Excluded(end) => key < end,
        };
        let nonempty = before_end(&first_key);
        self.iter()
            .skip_while(move |shard| shard.span().start() < from.as_slice())
            .take_while(move |shard| {
                nonempty && before_end(shard.span().start().max(first_key.as_slice()))
            })
    }

    /// Every shard, in key order.
    pub fn iter(&self) -> impl Iterator<Item = &Shard> {
        std::iter::once(&self.first).chain(self.rest.values())
    }

    /// Replaces the shard `halves` split with its two halves.
    ///
    /// # Errors
    ///
    /// [`Stale`] when the map does not hold that shard as it was before the
    /// split: at an earlier epoch, with the span of both halves.
    pub fn split(&mut self, halves: &Halves) -> Result<(), Stale> {
        let (left, right) = (halves.left(), halves.right());
        let stale = || Stale(left.id());
        let before = self.get(left.id()).ok_or_else(stale)?;
        let whole = before.span().start() == left.span().start()
            && before.span().end() == right.span().end()
            && before.epoch() < left.epoch();
        if !whole || self.get(right.id()).is_some() {
            return Err(stale());
        }
        if left.span().start().is_empty() {
            self.first = left.clone();
        } else {
            drop(self.rest.insert(left.span().start().to_vec(), left.clone()));
        }
        drop(
            self.rest
                .insert(right.span().start().to_vec(), right.clone()),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::{self, Excluded, Included, Unbounded};

    use proptest::prelude::*;

    use super::ShardMap;
    use crate::id::{Epoch, ShardId};
    use crate::shard::{Shard, Split};
    use kurator_storage::Span;

    /// A map split at each of `keys` in turn, where it can be.
    fn split_at(keys: &[Vec<u8>]) -> ShardMap {
        let mut map = ShardMap::new();
        let mut next = 2;
        for at in keys {
            let shard = map.locate(at).clone();
            if let Some(halves) =
                shard.split(&Split::new(at.clone(), ShardId::new(next), shard.epoch()))
            {
                map.split(&halves).unwrap();
                next = next.checked_add(1).unwrap();
            }
        }
        map
    }

    fn key() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(0_u8..4, 0..3)
    }

    fn borrowed(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
        bound.as_ref().map(Vec::as_slice)
    }

    fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
        prop_oneof![
            Just(Unbounded),
            key().prop_map(Included),
            key().prop_map(Excluded),
        ]
    }

    #[test]
    fn stale_splits_are_refused() {
        let mut map = ShardMap::new();
        assert_eq!(map.iter().count(), 1, "one shard");
        let halves = Shard::first()
            .split(&Split::new("m", ShardId::new(2), Epoch::ZERO))
            .unwrap();
        map.split(&halves).unwrap();
        assert_eq!(map.iter().count(), 2, "two shards");
        assert!(map.split(&halves).is_err(), "twice");
        let again = Shard::first()
            .split(&Split::new("f", ShardId::new(3), Epoch::ZERO))
            .unwrap();
        let error = map.split(&again).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the map does not hold s1 as it was before the split",
            "display"
        );
        let reused = halves
            .left()
            .split(&Split::new("f", ShardId::new(2), halves.left().epoch()))
            .unwrap();
        assert!(map.split(&reused).is_err(), "an id in use");
        let left = halves.left();
        let same_epoch = Shard::new(ShardId::FIRST, left.span().clone(), Epoch::ZERO)
            .split(&Split::new("f", ShardId::new(5), Epoch::ZERO))
            .unwrap();
        assert_eq!(same_epoch.left().epoch(), left.epoch(), "the same epoch");
        assert!(
            map.split(&same_epoch).is_err(),
            "a split of the span the map holds, but not at a later epoch"
        );
        let moved = Shard::new(ShardId::FIRST, Span::FULL, halves.left().epoch())
            .split(&Split::new("f", ShardId::new(6), halves.left().epoch()))
            .unwrap();
        assert!(
            map.split(&moved).is_err(),
            "a later split of a span the map does not hold"
        );
        let unknown = Shard::new(ShardId::new(9), halves.right().span().clone(), Epoch::ZERO)
            .split(&Split::new("x", ShardId::new(10), Epoch::ZERO))
            .unwrap();
        assert!(map.split(&unknown).is_err(), "an unknown shard");
        assert_eq!(map.get(ShardId::new(2)), Some(halves.right()), "by id");
        assert_eq!(map.get(ShardId::new(9)), None, "not there");
    }

    #[test]
    fn maps_from_shards_must_tile_the_keys() {
        let map = split_at(&[b"f".to_vec(), b"m".to_vec()]);
        let shards: Vec<Shard> = map.iter().cloned().collect();
        assert_eq!(ShardMap::from_shards(shards.clone()), Some(map), "tiled");
        assert_eq!(ShardMap::from_shards(Vec::new()), None, "nothing");
        assert_eq!(
            ShardMap::from_shards(shards[1..].to_vec()),
            None,
            "no first"
        );
        assert_eq!(ShardMap::from_shards(shards[..2].to_vec()), None, "no last");
        let gap = vec![shards[0].clone(), shards[2].clone()];
        assert_eq!(ShardMap::from_shards(gap), None, "a gap");
        let twice = vec![
            shards[0].clone(),
            shards[1].clone(),
            shards[1].clone(),
            shards[2].clone(),
        ];
        assert_eq!(ShardMap::from_shards(twice), None, "an overlap");
        let renamed = Shard::new(shards[0].id(), shards[2].span().clone(), shards[2].epoch());
        let same_id = vec![shards[0].clone(), shards[1].clone(), renamed];
        assert_eq!(ShardMap::from_shards(same_id), None, "an id twice");
        let meta = [Shard::whole(ShardId::META)];
        assert_eq!(ShardMap::from_shards(meta), None, "the meta shard alone");
        let mut with_meta = shards.clone();
        with_meta[1] = Shard::new(ShardId::META, shards[1].span().clone(), shards[1].epoch());
        assert_eq!(
            ShardMap::from_shards(with_meta),
            None,
            "the meta shard inside"
        );
    }

    #[test]
    fn overlapping_follows_the_bounds() {
        let map = split_at(&[b"f".to_vec(), b"m".to_vec()]);
        let ids = |start: Bound<&[u8]>, end: Bound<&[u8]>| -> Vec<u64> {
            map.overlapping(start, end)
                .map(|shard| shard.id().get())
                .collect()
        };
        assert_eq!(ids(Unbounded, Unbounded), [1, 2, 3], "all");
        assert_eq!(ids(Included(b"f"), Excluded(b"m")), [2], "exactly one");
        assert_eq!(ids(Excluded(b"e"), Included(b"m")), [1, 2, 3], "edges");
        assert_eq!(
            ids(Included(b"m"), Excluded(b"m")),
            Vec::<u64>::new(),
            "empty"
        );
        assert_eq!(ids(Included(b"z"), Unbounded), [3], "last");
    }

    proptest! {
        #[test]
        fn every_key_has_one_shard(keys in proptest::collection::vec(key(), 0..8), probe in key()) {
            let map = split_at(&keys);
            let holders: Vec<&Shard> = map.iter().filter(|shard| shard.span().contains(&probe)).collect();
            prop_assert_eq!(holders.len(), 1, "exactly one holder");
            prop_assert_eq!(map.locate(&probe), holders[0], "located");
            let shards: Vec<Shard> = map.iter().cloned().collect();
            prop_assert_eq!(ShardMap::from_shards(shards), Some(map.clone()), "tiled");
        }

        #[test]
        fn overlapping_finds_every_holder(
            keys in proptest::collection::vec(key(), 0..8),
            start in bound(),
            end in bound(),
            probes in proptest::collection::vec(key(), 0..16),
        ) {
            let map = split_at(&keys);
            let found: Vec<ShardId> = map
                .overlapping(borrowed(&start), borrowed(&end))
                .map(Shard::id)
                .collect();
            let range = (borrowed(&start), borrowed(&end));
            for probe in &probes {
                if std::ops::RangeBounds::contains(&range, &probe.as_slice()) {
                    prop_assert!(found.contains(&map.locate(probe).id()), "holder of {:?}", probe);
                }
            }
            for shard in map.overlapping(borrowed(&start), borrowed(&end)) {
                let (from, to) = shard.span().clip(borrowed(&start), borrowed(&end));
                let first = match from {
                    Unbounded => Vec::new(),
                    Included(key) => key.to_vec(),
                    Excluded(key) => [key, &[0]].concat(),
                };
                prop_assert!(std::ops::RangeBounds::contains(&(from, to), &first.as_slice()), "{} holds a key", shard);
            }
        }
    }
}
