//! The records of the meta shard, and the registry they make.

use std::collections::BTreeMap;
use std::io;
use std::ops::Bound;

use kurator_codec::{DecodeError, Reader, put_bytes};
use kurator_shard::{Shard, ShardId, ShardMap};
use kurator_storage::KvState;

use crate::placement::Placement;

/// The key of the next id.
pub(crate) const NEXT: &[u8] = b"next";
/// The prefix of every placement's key.
const SHARDS: &[u8] = b"shard/";
/// The first key after every placement's.
const SHARDS_END: &[u8] = b"shard0";

/// The key of the placement of `id`. Big-endian, so placements sort by id.
pub(crate) fn shard_key(id: ShardId) -> Vec<u8> {
    [SHARDS, &id.get().to_be_bytes()].concat()
}

/// The bounds of every placement's key.
pub(crate) const fn shard_keys() -> (Bound<&'static [u8]>, Bound<&'static [u8]>) {
    (Bound::Included(SHARDS), Bound::Excluded(SHARDS_END))
}

/// The id stored as `next`, or the first id after [`ShardId::FIRST`] when
/// none is.
pub(crate) fn next_id(stored: Option<&[u8]>) -> Result<ShardId, DecodeError> {
    let Some(bytes) = stored else {
        return Ok(first_free());
    };
    let raw: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::Invalid)?;
    Ok(ShardId::new(u64::from_le_bytes(raw)))
}

/// The id after the shards a cluster is founded with.
const fn first_free() -> ShardId {
    ShardId::new(ShardId::FIRST.get().saturating_add(1))
}

/// What the meta shard records: every shard but itself, where its replicas
/// are, and the id the next split gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    placements: BTreeMap<ShardId, Placement>,
    next: ShardId,
}

impl Registry {
    /// The registry the meta shard's state `kv` holds. The first shard is
    /// there from the start, at its first epoch, until a report replaces
    /// it; its replicas are not known until then.
    ///
    /// # Errors
    ///
    /// When a record does not decode, or is stored under another shard's
    /// key.
    pub fn read(kv: &KvState) -> Result<Self, DecodeError> {
        let (start, end) = shard_keys();
        let mut placements = BTreeMap::new();
        for (key, value) in kv.range(start, end) {
            let placement = Placement::decode(value)?;
            let id = placement.id();
            if key != shard_key(id) {
                return Err(DecodeError::Invalid);
            }
            drop(placements.insert(id, placement));
        }
        let _founding: &mut Placement = placements.entry(ShardId::FIRST).or_insert_with(|| {
            Placement::new(Shard::first(), std::collections::BTreeSet::new(), 0)
        });
        Ok(Self {
            placements,
            next: next_id(kv.get(NEXT))?,
        })
    }

    /// The placement of `id`, if the registry records it.
    #[must_use]
    pub fn placement(&self, id: ShardId) -> Option<&Placement> {
        self.placements.get(&id)
    }

    /// Every placement, by id.
    pub fn placements(&self) -> impl Iterator<Item = &Placement> {
        self.placements.values()
    }

    /// The id the next split will get.
    #[must_use]
    pub const fn next(&self) -> ShardId {
        self.next
    }

    /// The map clients route by, or `None` while the shards recorded do not
    /// hold every key exactly once: a split reported by one half.
    #[must_use]
    pub fn map(&self) -> Option<ShardMap> {
        ShardMap::from_shards(self.placements().map(|placement| placement.shard().clone()))
    }

    /// Writes the registry as `next u64 | count u32 | bytes(placement)*`,
    /// by id, each placement as [`Placement::encode`] writes it.
    ///
    /// # Errors
    ///
    /// When `out` fails, or something is too long for its prefix.
    pub fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        out.extend_from_slice(&self.next.get().to_le_bytes());
        let count = u32::try_from(self.placements.len()).map_err(io::Error::other)?;
        out.extend_from_slice(&count.to_le_bytes());
        for placement in self.placements.values() {
            let mut encoded = Vec::new();
            placement.encode(&mut encoded)?;
            put_bytes(out, &encoded)?;
        }
        Ok(())
    }

    /// Reads a registry written by [`Registry::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// When it does not decode, or names a shard twice, out of order, or
    /// the meta shard.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let next = ShardId::new(reader.read()?);
        let mut placements = BTreeMap::new();
        for _ in 0..reader.read::<u32>()? {
            let placement = Placement::decode(reader.bytes()?)?;
            let id = placement.id();
            let ascending = placements
                .last_key_value()
                .is_none_or(|(last, _)| *last < id);
            if !ascending || id == ShardId::META {
                return Err(DecodeError::Invalid);
            }
            drop(placements.insert(id, placement));
        }
        Ok(Self { placements, next })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use kurator_codec::DecodeError;
    use kurator_raft::NodeId;
    use kurator_shard::{Epoch, Shard, ShardId, ShardMap, Split};
    use kurator_storage::{Command, KvState, Version, WriteBatch};

    use super::{NEXT, Registry, shard_key};
    use crate::placement::Placement;

    fn record(state: &mut KvState, version: u64, key: Vec<u8>, value: Vec<u8>) {
        let write = WriteBatch::new().put(key, value);
        let _outcome = state.apply(Version::new(version), Command::Kv(write));
    }

    fn placed(shard: &Shard) -> Placement {
        Placement::new(shard.clone(), BTreeSet::from([NodeId::new(1)]), 1)
    }

    fn encoded(placement: &Placement) -> Vec<u8> {
        let mut out = Vec::new();
        placement.encode(&mut out).unwrap();
        out
    }

    #[test]
    fn an_empty_meta_shard_holds_the_first_shard() {
        let registry = Registry::read(&KvState::default()).unwrap();
        assert_eq!(registry.map(), Some(ShardMap::new()), "the first shard");
        assert_eq!(registry.next(), ShardId::new(2), "the next id");
        let first = registry.placement(ShardId::FIRST).unwrap();
        assert!(first.replicas().is_empty(), "no replicas known");
        assert_eq!(registry.placement(ShardId::new(2)), None, "nothing else");
    }

    #[test]
    fn the_map_tiles_only_once_both_halves_are_recorded() {
        let halves = Shard::first()
            .split(&Split::new("m", ShardId::new(2), Epoch::ZERO))
            .unwrap();
        let mut state = KvState::default();
        let left = placed(halves.left());
        record(&mut state, 1, shard_key(ShardId::FIRST), encoded(&left));
        assert_eq!(Registry::read(&state).unwrap().map(), None, "a gap");
        let right = placed(halves.right());
        record(&mut state, 2, shard_key(ShardId::new(2)), encoded(&right));
        record(&mut state, 3, NEXT.to_vec(), 3_u64.to_le_bytes().to_vec());
        let registry = Registry::read(&state).unwrap();
        let mut expected = ShardMap::new();
        expected.split(&halves).unwrap();
        assert_eq!(registry.map(), Some(expected), "both halves");
        assert_eq!(registry.next(), ShardId::new(3), "next");
        assert_eq!(registry.placements().count(), 2, "two");
        assert_eq!(registry.placement(ShardId::new(2)), Some(&right), "by id");
    }

    #[test]
    fn misfiled_or_damaged_records_are_rejected() {
        let mut misfiled = KvState::default();
        let first = encoded(&placed(&Shard::first()));
        record(&mut misfiled, 1, shard_key(ShardId::new(7)), first);
        assert_eq!(
            Registry::read(&misfiled),
            Err(DecodeError::Invalid),
            "misfiled"
        );
        let mut damaged = KvState::default();
        record(&mut damaged, 1, shard_key(ShardId::FIRST), vec![1]);
        assert_eq!(
            Registry::read(&damaged),
            Err(DecodeError::Truncated),
            "damaged"
        );
        let mut short = KvState::default();
        record(&mut short, 1, NEXT.to_vec(), vec![1, 2]);
        assert_eq!(
            Registry::read(&short),
            Err(DecodeError::Invalid),
            "a short next"
        );
    }
}
