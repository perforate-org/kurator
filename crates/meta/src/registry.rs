//! The records of the meta shard, and the registry they make.

use std::collections::BTreeMap;
use std::io;
use std::ops::Bound;

use oleaph_codec::{DecodeError, Reader, put_bytes};
use oleaph_shard::{NamespaceId, Shard, ShardId, ShardMap};
use oleaph_storage::KvState;

use crate::namespace::{Namespace, NamespaceName};
use crate::placement::Placement;

/// The key of the next id.
pub(crate) const NEXT: &[u8] = b"next";
/// The key of the next namespace's id.
pub(crate) const NEXT_NAMESPACE: &[u8] = b"next-namespace";
/// The prefix of every placement's key.
const SHARDS: &[u8] = b"shard/";
/// The first key after every placement's.
const SHARDS_END: &[u8] = b"shard0";
/// The prefix of every namespace's key.
const NAMESPACES: &[u8] = b"namespace/";
/// The first key after every namespace's.
const NAMESPACES_END: &[u8] = b"namespace0";

/// The key of the namespace called `name`, so that no two share one.
pub(crate) fn namespace_key(name: &NamespaceName) -> Vec<u8> {
    [NAMESPACES, name.as_str().as_bytes()].concat()
}

/// Every namespace, by name: the default and those the records hold. No
/// two share a name or an id.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Namespaces(BTreeMap<NamespaceName, Namespace>);

impl Namespaces {
    /// The default namespace alone.
    fn founding() -> Self {
        Self(BTreeMap::from([(
            NamespaceName::default_name(),
            Namespace::founding(),
        )]))
    }

    /// Adds `namespace`, called `name`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Invalid`] when a namespace has that name or that id
    /// already.
    fn add(&mut self, name: NamespaceName, namespace: Namespace) -> Result<(), DecodeError> {
        let taken =
            self.0.contains_key(&name) || self.0.values().any(|other| other.id() == namespace.id());
        if taken {
            return Err(DecodeError::Invalid);
        }
        let _new: Option<Namespace> = self.0.insert(name, namespace);
        Ok(())
    }

    /// Every namespace a cluster makes, which every registry does not
    /// hold from the start: all but the default.
    fn made(&self) -> impl Iterator<Item = (&NamespaceName, &Namespace)> {
        self.0
            .iter()
            .filter(|(_, namespace)| namespace.id() != NamespaceId::DEFAULT)
    }
}

/// The id stored as `next-namespace`, or the first id after
/// [`NamespaceId::DEFAULT`] when none is.
pub(crate) fn next_namespace(stored: Option<&[u8]>) -> Result<NamespaceId, DecodeError> {
    let Some(bytes) = stored else {
        return Ok(NamespaceId::new(
            NamespaceId::DEFAULT.get().saturating_add(1),
        ));
    };
    let raw: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::Invalid)?;
    Ok(NamespaceId::new(u32::from_le_bytes(raw)))
}

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

/// What the meta shard records: every namespace, every shard but itself,
/// where its replicas are, and the id the next split gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    namespaces: Namespaces,
    placements: BTreeMap<ShardId, Placement>,
    next: ShardId,
}

impl Registry {
    /// The registry the meta shard's state `kv` holds. The default
    /// namespace is there from the start, and so is its first shard, at
    /// its first epoch, until a report replaces it; its replicas are not
    /// known until then.
    ///
    /// # Errors
    ///
    /// When a record does not decode, is stored under another shard's
    /// key, or a namespace's key names no namespace, or the default, or
    /// two namespaces share an id.
    pub fn read(kv: &KvState) -> Result<Self, DecodeError> {
        let mut namespaces = Namespaces::founding();
        for (key, value) in kv.range(Bound::Included(NAMESPACES), Bound::Excluded(NAMESPACES_END)) {
            let name = key.strip_prefix(NAMESPACES).ok_or(DecodeError::Invalid)?;
            let name = NamespaceName::from_bytes(name)?;
            namespaces.add(name, Namespace::decode(value)?)?;
        }
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
            namespaces,
            placements,
            next: next_id(kv.get(NEXT))?,
        })
    }

    /// The namespace called `name`, if there is one.
    #[must_use]
    pub fn namespace(&self, name: &NamespaceName) -> Option<&Namespace> {
        self.namespaces.0.get(name)
    }

    /// Every namespace, by name.
    pub fn namespaces(&self) -> impl Iterator<Item = (&NamespaceName, &Namespace)> {
        self.namespaces.0.iter()
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

    /// The map clients route by in `namespace`, or `None` while the shards
    /// recorded there do not hold every key of it exactly once: a split
    /// reported by one half, or a namespace there is not.
    #[must_use]
    pub fn map(&self, namespace: NamespaceId) -> Option<ShardMap> {
        let shards = self.placements().map(|placement| placement.shard().clone());
        ShardMap::from_shards(namespace, shards)
    }

    /// Writes the registry as `next u64 | count u32 | bytes(placement)*
    /// | count u32 (bytes(name) bytes(namespace))*`, placements by id and
    /// namespaces by name, each as [`Placement::encode`] and
    /// [`Namespace::encode`] write it. The default namespace is left out,
    /// since every registry holds it.
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
        let made_count = u32::try_from(self.namespaces.made().count()).map_err(io::Error::other)?;
        out.extend_from_slice(&made_count.to_le_bytes());
        for (name, namespace) in self.namespaces.made() {
            put_bytes(out, name.as_str().as_bytes())?;
            let mut encoded = Vec::new();
            namespace.encode(&mut encoded)?;
            put_bytes(out, &encoded)?;
        }
        Ok(())
    }

    /// Reads a registry written by [`Registry::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// When it does not decode, or names a shard or a namespace twice or
    /// out of order, or names the meta shard, or the default namespace or
    /// its id.
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
        let mut namespaces = Namespaces::founding();
        for _ in 0..reader.read::<u32>()? {
            let name = NamespaceName::from_bytes(reader.bytes()?)?;
            // Above the last, so in order and once.
            if namespaces
                .made()
                .last()
                .is_some_and(|(last, _)| *last >= name)
            {
                return Err(DecodeError::Invalid);
            }
            namespaces.add(name, Namespace::decode(reader.bytes()?)?)?;
        }
        Ok(Self {
            namespaces,
            placements,
            next,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use oleaph_codec::DecodeError;
    use oleaph_consensus::NodeId;
    use oleaph_shard::{Epoch, NamespaceId, Shard, ShardId, ShardMap, Split};
    use oleaph_storage::{Command, KvState, Version, WriteBatch};

    use oleaph_codec::{Reader, put_bytes};

    use super::{NEXT, Registry, namespace_key, next_namespace, shard_key};
    use crate::namespace::{Assurance, Namespace, NamespaceName};
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
        assert_eq!(
            registry.map(NamespaceId::DEFAULT),
            Some(ShardMap::new()),
            "the first shard"
        );
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
        assert_eq!(
            Registry::read(&state).unwrap().map(NamespaceId::DEFAULT),
            None,
            "a gap"
        );
        let right = placed(halves.right());
        record(&mut state, 2, shard_key(ShardId::new(2)), encoded(&right));
        record(&mut state, 3, NEXT.to_vec(), 3_u64.to_le_bytes().to_vec());
        let registry = Registry::read(&state).unwrap();
        let mut expected = ShardMap::new();
        expected.split(&halves).unwrap();
        assert_eq!(
            registry.map(NamespaceId::DEFAULT),
            Some(expected),
            "both halves"
        );
        assert_eq!(registry.next(), ShardId::new(3), "next");
        assert_eq!(registry.placements().count(), 2, "two");
        assert_eq!(registry.placement(ShardId::new(2)), Some(&right), "by id");
    }

    #[test]
    fn namespaces_route_by_their_own_shards() {
        let name: NamespaceName = "payments".parse().unwrap();
        let other = NamespaceId::new(1);
        let namespace = Namespace::new(other, Assurance::Cft);
        let mut bytes = Vec::new();
        namespace.encode(&mut bytes).unwrap();
        let mut state = KvState::default();
        record(&mut state, 1, namespace_key(&name), bytes);
        let own = Shard::whole(other, ShardId::new(2));
        record(&mut state, 2, shard_key(own.id()), encoded(&placed(&own)));
        let registry = Registry::read(&state).unwrap();
        assert_eq!(registry.namespace(&name), Some(&namespace), "by name");
        let default = registry.namespace(&NamespaceName::default_name());
        assert_eq!(default, Some(&Namespace::founding()), "the default");
        let names: Vec<&str> = registry
            .namespaces()
            .map(|(called, _)| called.as_str())
            .collect();
        assert_eq!(names, ["default", "payments"], "both, by name");
        assert_eq!(
            registry.map(NamespaceId::DEFAULT),
            Some(ShardMap::new()),
            "default"
        );
        let own_map = registry.map(other).unwrap();
        assert_eq!(own_map.locate(b"k"), &own, "its own shard");
        assert_eq!(registry.map(NamespaceId::new(9)), None, "no such namespace");
        let mut out = Vec::new();
        registry.encode(&mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(Registry::decode(&mut reader), Ok(registry), "roundtrip");
        assert!(reader.finish().is_ok(), "nothing left");
    }

    /// A namespace's record.
    fn namespace(id: u32) -> Vec<u8> {
        let mut out = Vec::new();
        Namespace::new(NamespaceId::new(id), Assurance::Cft)
            .encode(&mut out)
            .unwrap();
        out
    }

    /// The bytes of a registry with no placements and `namespaces`, each
    /// a name and its record, in the order given.
    fn wire(namespaces: &[(&str, u32)]) -> Vec<u8> {
        let mut out = 2_u64.to_le_bytes().to_vec();
        out.extend_from_slice(&0_u32.to_le_bytes());
        out.extend_from_slice(&u32::try_from(namespaces.len()).unwrap().to_le_bytes());
        for (name, id) in namespaces {
            put_bytes(&mut out, name.as_bytes()).unwrap();
            put_bytes(&mut out, &namespace(*id)).unwrap();
        }
        out
    }

    #[test]
    fn misnamed_or_twice_numbered_namespaces_are_rejected() {
        let payments: NamespaceName = "payments".parse().unwrap();
        let ledger: NamespaceName = "ledger".parse().unwrap();
        let mut misnamed = KvState::default();
        record(&mut misnamed, 1, b"namespace/Bad".to_vec(), namespace(1));
        assert_eq!(
            Registry::read(&misnamed),
            Err(DecodeError::Invalid),
            "misnamed"
        );
        let mut twice = KvState::default();
        record(&mut twice, 1, namespace_key(&payments), namespace(1));
        record(&mut twice, 2, namespace_key(&ledger), namespace(1));
        assert_eq!(
            Registry::read(&twice),
            Err(DecodeError::Invalid),
            "an id twice"
        );
        let mut default_id = KvState::default();
        record(&mut default_id, 1, namespace_key(&ledger), namespace(0));
        assert_eq!(
            Registry::read(&default_id),
            Err(DecodeError::Invalid),
            "the default's id"
        );
        let mut default_name = KvState::default();
        let default = NamespaceName::default_name();
        record(&mut default_name, 1, namespace_key(&default), namespace(3));
        assert_eq!(
            Registry::read(&default_name),
            Err(DecodeError::Invalid),
            "the default's name"
        );
        let decode =
            |namespaces: &[(&str, u32)]| Registry::decode(&mut Reader::new(&wire(namespaces)));
        assert!(
            decode(&[("ledger", 2), ("payments", 1)]).is_ok(),
            "in order"
        );
        for (bad, why) in [
            (&[("payments", 1), ("ledger", 2)][..], "out of order"),
            (&[("ledger", 1), ("ledger", 2)][..], "a name twice"),
            (&[("ledger", 1), ("payments", 1)][..], "an id twice"),
            (&[("default", 1)][..], "the default's name"),
            (&[("ledger", 0)][..], "the default's id"),
            (&[("Ledger", 1)][..], "a bad name"),
        ] {
            assert_eq!(decode(bad), Err(DecodeError::Invalid), "{why}");
        }
    }

    #[test]
    fn next_namespace_ids_follow_the_default() {
        assert_eq!(next_namespace(None), Ok(NamespaceId::new(1)), "first");
        let stored = 5_u32.to_le_bytes();
        assert_eq!(
            next_namespace(Some(&stored)),
            Ok(NamespaceId::new(5)),
            "stored"
        );
        assert_eq!(
            next_namespace(Some(&[1])),
            Err(DecodeError::Invalid),
            "short"
        );
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
