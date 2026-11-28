//! Where one shard is: its descriptor and its replicas.

use std::collections::BTreeSet;
use std::fmt;
use std::io::{self, Write};

use oleaph_codec::{DecodeError, Reader};
use oleaph_consensus::{NodeId, Replicated};
use oleaph_shard::{Epoch, Shard, ShardId};

/// A shard as the registry records it: the shard, the nodes in its
/// membership, and the log entry of that membership in the shard's own
/// log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    shard: Shard,
    replicas: BTreeSet<NodeId>,
    membership: u64,
}

/// What a report did to the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reported {
    /// The registry now records the placement.
    Recorded,
    /// The registry records a newer placement of the shard already, or the
    /// same one: it kept that.
    Stale,
}

impl Placement {
    /// `shard` on `replicas`, whose membership is the entry at index
    /// `membership` of the shard's log.
    #[must_use]
    pub const fn new(shard: Shard, replicas: BTreeSet<NodeId>, membership: u64) -> Self {
        Self {
            shard,
            replicas,
            membership,
        }
    }

    /// Where `replica`'s shard is, as `replica` knows it.
    #[must_use]
    pub fn of(replica: &impl Replicated) -> Self {
        let members = replica.members();
        let replicas = members.peers().map(|(id, _)| id).collect();
        Self::new(replica.shard(), replicas, members.position())
    }

    /// The shard.
    #[must_use]
    pub const fn shard(&self) -> &Shard {
        &self.shard
    }

    /// The shard's id.
    #[must_use]
    pub const fn id(&self) -> ShardId {
        self.shard.id()
    }

    /// The nodes that hold a replica of it, voters and learners.
    #[must_use]
    pub const fn replicas(&self) -> &BTreeSet<NodeId> {
        &self.replicas
    }

    /// Whether this placement is newer than `other`, a placement of the
    /// same shard: a later epoch, or the same epoch and a later membership.
    #[must_use]
    pub fn supersedes(&self, other: &Self) -> bool {
        self.order() > other.order()
    }

    const fn order(&self) -> (Epoch, u64) {
        (self.shard.epoch(), self.membership)
    }

    /// Writes the placement as `shard | membership u64 | count u32 | id
    /// u64 ...`, the shard as [`Shard::encode`] writes it.
    ///
    /// # Errors
    ///
    /// When `out` fails, or a key or the replica set is too long for its
    /// prefix.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        self.shard.encode(out)?;
        out.write_all(&self.membership.to_le_bytes())?;
        let count = u32::try_from(self.replicas.len()).map_err(io::Error::other)?;
        out.write_all(&count.to_le_bytes())?;
        for id in &self.replicas {
            out.write_all(&id.get().to_le_bytes())?;
        }
        Ok(())
    }

    /// Decodes what [`Placement::encode`] wrote, all of `bytes`.
    ///
    /// # Errors
    ///
    /// When `bytes` is not one whole placement, or names a replica twice
    /// or out of order.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let shard = Shard::decode(&mut reader)?;
        let membership = reader.read()?;
        let count: u32 = reader.read()?;
        let mut replicas = BTreeSet::new();
        for _ in 0..count {
            let id = NodeId::new(reader.read()?);
            // Strictly ascending, so no id comes twice.
            if replicas.last().is_some_and(|last| *last >= id) {
                return Err(DecodeError::Invalid);
            }
            let _new: bool = replicas.insert(id);
        }
        reader.finish()?;
        Ok(Self::new(shard, replicas, membership))
    }
}

/// The shard, then `on` and its replicas: `s1 [, m) at e1 on n1 n2`.
impl fmt::Display for Placement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} on", self.shard)?;
        for id in &self.replicas {
            write!(f, " {id}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeSet;

    use oleaph_codec::DecodeError;
    use oleaph_consensus::NodeId;
    use oleaph_shard::{Epoch, Shard, ShardId};
    use oleaph_storage::{End, Span};
    use proptest::prelude::*;

    use super::Placement;

    pub(crate) fn placements() -> impl Strategy<Value = Placement> {
        (
            any::<u64>(),
            any::<u64>(),
            proptest::collection::vec(any::<u8>(), 0..4),
            proptest::collection::btree_set(any::<u64>(), 0..6),
            any::<u64>(),
        )
            .prop_map(|(id, epoch, start, replicas, membership)| {
                let span = Span::new(start, End::Unbounded).unwrap();
                let shard = Shard::new(ShardId::new(id), span, Epoch::new(epoch));
                let replicas = replicas.into_iter().map(NodeId::new).collect();
                Placement::new(shard, replicas, membership)
            })
    }

    fn placement(epoch: u64, membership: u64) -> Placement {
        let replicas = BTreeSet::from([NodeId::new(1), NodeId::new(2)]);
        let shard = Shard::new(ShardId::FIRST, Span::FULL, Epoch::new(epoch));
        Placement::new(shard, replicas, membership)
    }

    #[test]
    fn later_epochs_then_later_memberships_supersede() {
        let base = placement(1, 5);
        assert!(placement(2, 0).supersedes(&base), "a later epoch");
        assert!(placement(1, 6).supersedes(&base), "a later membership");
        assert!(!base.supersedes(&base), "not itself");
        assert!(!placement(0, 9).supersedes(&base), "an earlier epoch");
        assert!(!placement(1, 4).supersedes(&base), "an earlier membership");
        assert_eq!(
            base.shard(),
            &Shard::new(ShardId::FIRST, Span::FULL, Epoch::new(1)),
            "shard"
        );
        assert_eq!(base.replicas().len(), 2, "replicas");
        assert_eq!(base.id(), ShardId::FIRST, "id");
        assert_eq!(base.to_string(), "s1 [, ..) at e1 on n1 n2", "display");
    }

    #[test]
    fn replicas_out_of_order_or_twice_are_rejected() {
        let mut out = Vec::new();
        placement(1, 5).encode(&mut out).unwrap();
        let (head, ids) = out.split_at(out.len().checked_sub(16).unwrap());
        let (one, two) = ids.split_at(8);
        let swapped = [head, two, one].concat();
        assert_eq!(
            Placement::decode(&swapped),
            Err(DecodeError::Invalid),
            "out of order"
        );
        let twice = [head, one, one].concat();
        assert_eq!(
            Placement::decode(&twice),
            Err(DecodeError::Invalid),
            "twice"
        );
        let longer = [out.as_slice(), &[0]].concat();
        assert_eq!(
            Placement::decode(&longer),
            Err(DecodeError::Trailing),
            "trailing"
        );
    }

    proptest! {
        #[test]
        fn placements_roundtrip(placement in placements()) {
            let mut out = Vec::new();
            placement.encode(&mut out).unwrap();
            prop_assert_eq!(Placement::decode(&out), Ok(placement));
            for cut in 0..out.len() {
                let (head, _) = out.split_at(cut);
                prop_assert!(Placement::decode(head).is_err(), "cut at {}", cut);
            }
        }
    }
}
