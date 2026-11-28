//! What a shard is, and how one splits in two.

use std::fmt;
use std::io::{self, Write};

use oleaph_codec::{DecodeError, Reader, put_bytes};
use oleaph_storage::Span;

use crate::id::{Epoch, NamespaceId, ShardId};

/// A shard as routing sees it: the namespace it belongs to, its id, the
/// keys of the namespace it holds, and its epoch.
///
/// A shard belongs to one namespace for as long as it exists, and both
/// halves of a split belong to it too.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Shard {
    namespace: NamespaceId,
    id: ShardId,
    span: Span,
    epoch: Epoch,
}

impl Shard {
    /// The shard `id` of `namespace` holding `span` at `epoch`.
    #[must_use]
    pub const fn new(namespace: NamespaceId, id: ShardId, span: Span, epoch: Epoch) -> Self {
        Self {
            namespace,
            id,
            span,
            epoch,
        }
    }

    /// The shard a new cluster starts with: [`ShardId::FIRST`], holding
    /// every key of [`NamespaceId::DEFAULT`], at [`Epoch::ZERO`].
    #[must_use]
    pub const fn first() -> Self {
        Self::whole(NamespaceId::DEFAULT, ShardId::FIRST)
    }

    /// Shard `id` holding every key of `namespace` at [`Epoch::ZERO`]: how
    /// the first shard of a namespace and the meta shard start, and how a
    /// replica that holds no state yet describes its shard.
    #[must_use]
    pub const fn whole(namespace: NamespaceId, id: ShardId) -> Self {
        Self::new(namespace, id, Span::FULL, Epoch::ZERO)
    }

    /// The namespace it belongs to.
    #[must_use]
    pub const fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    /// Its id.
    #[must_use]
    pub const fn id(&self) -> ShardId {
        self.id
    }

    /// The keys it holds.
    #[must_use]
    pub const fn span(&self) -> &Span {
        &self.span
    }

    /// Its epoch.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The two shards `split` makes of this one, or `None` when it does not
    /// apply: it names another epoch, or a key not strictly inside the
    /// span, or this shard's own id or [`ShardId::META`] for the new shard.
    /// The meta shard never splits.
    #[must_use]
    pub fn split(&self, split: &Split) -> Option<Halves> {
        let renames_itself = split.right == self.id;
        let meta = [self.id, split.right].contains(&ShardId::META);
        if renames_itself || meta || split.epoch != self.epoch {
            return None;
        }
        let epoch = self.epoch.next()?;
        let (left, right) = self.span.split_at(&split.at)?;
        Some(Halves {
            left: Self::new(self.namespace, self.id, left, epoch),
            right: Self::new(self.namespace, split.right, right, epoch),
        })
    }

    /// Writes the shard as `namespace u32 | id u64 | epoch u64 | span`,
    /// the span as [`Span::encode`] writes it.
    ///
    /// # Errors
    ///
    /// When `out` fails or a key is too long for its prefix.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        out.write_all(&self.namespace.get().to_le_bytes())?;
        out.write_all(&self.id.get().to_le_bytes())?;
        out.write_all(&self.epoch.get().to_le_bytes())?;
        self.span.encode(out)
    }

    /// Reads a shard written by [`Shard::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// As [`Span::decode`].
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let namespace = NamespaceId::new(reader.read()?);
        let id = ShardId::new(reader.read()?);
        let epoch = Epoch::new(reader.read()?);
        Ok(Self::new(namespace, id, Span::decode(reader)?, epoch))
    }
}

impl fmt::Display for Shard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} at {} in {}",
            self.id, self.span, self.epoch, self.namespace
        )
    }
}

/// A request to split a shard: the keys from `at` on go to a new shard,
/// `right`, if the shard is still at `epoch`.
///
/// The epoch makes a split apply at most once, and only to the shard its
/// proposer saw: a split proposed twice, or after another split, finds a
/// later epoch and changes nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    at: Vec<u8>,
    right: ShardId,
    epoch: Epoch,
}

impl Split {
    /// Splits the shard at `epoch` before `at`, naming the new right half
    /// `right`.
    #[must_use]
    pub fn new(at: impl Into<Vec<u8>>, right: ShardId, epoch: Epoch) -> Self {
        Self {
            at: at.into(),
            right,
            epoch,
        }
    }

    /// The first key of the new shard.
    #[must_use]
    pub fn at(&self) -> &[u8] {
        &self.at
    }

    /// The new shard's id.
    #[must_use]
    pub const fn right(&self) -> ShardId {
        self.right
    }

    /// The epoch the shard must be at.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Writes the split as `epoch u64 | right u64 | at`, the key length
    /// prefixed.
    ///
    /// # Errors
    ///
    /// When `out` fails or the key is too long for its prefix.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        out.write_all(&self.epoch.get().to_le_bytes())?;
        out.write_all(&self.right.get().to_le_bytes())?;
        put_bytes(out, &self.at)
    }

    /// Reads a split written by [`Split::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] when the input ends early.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let epoch = Epoch::new(reader.read()?);
        let right = ShardId::new(reader.read()?);
        Ok(Self::new(reader.bytes()?, right, epoch))
    }
}

/// The two shards a split made: `left` keeps the shard's id and the keys
/// before the split key, `right` is new and holds the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Halves {
    left: Shard,
    right: Shard,
}

impl Halves {
    /// The half that kept the id.
    #[must_use]
    pub const fn left(&self) -> &Shard {
        &self.left
    }

    /// The new half.
    #[must_use]
    pub const fn right(&self) -> &Shard {
        &self.right
    }
}

#[cfg(test)]
mod tests {
    use oleaph_codec::{DecodeError, Reader};
    use oleaph_storage::{End, Span};
    use proptest::prelude::*;

    use super::{Shard, Split};
    use crate::id::{Epoch, NamespaceId, ShardId};

    fn key() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..4)
    }

    fn shards() -> impl Strategy<Value = Shard> {
        (
            any::<u32>(),
            any::<u64>(),
            any::<u64>(),
            key(),
            proptest::option::of(key()),
        )
            .prop_filter_map("empty", |(namespace, id, epoch, start, end)| {
                let span = Span::new(start, end.map_or(End::Unbounded, End::Before))?;
                let namespace = NamespaceId::new(namespace);
                Some(Shard::new(
                    namespace,
                    ShardId::new(id),
                    span,
                    Epoch::new(epoch),
                ))
            })
    }

    #[test]
    fn a_split_applies_once_and_only_inside() {
        let first = Shard::first();
        assert_eq!(first.to_string(), "s1 [, ..) at e0 in ns0", "display");
        let split = Split::new("m", ShardId::new(2), Epoch::ZERO);
        let halves = first.split(&split).unwrap();
        assert_eq!(halves.left().to_string(), "s1 [, m) at e1 in ns0", "left");
        assert_eq!(
            halves.right().to_string(),
            "s2 [m, ..) at e1 in ns0",
            "right"
        );
        let other = NamespaceId::new(3);
        let apart = Shard::whole(other, ShardId::new(9)).split(&split).unwrap();
        assert_eq!(apart.left().namespace(), other, "the left half stays");
        assert_eq!(apart.right().namespace(), other, "so does the right");
        assert_eq!(halves.left().split(&split), None, "a later epoch");
        let own = Split::new("m", ShardId::FIRST, Epoch::ZERO);
        assert_eq!(first.split(&own), None, "its own id");
        let at_start = Split::new("", ShardId::new(2), Epoch::ZERO);
        assert_eq!(first.split(&at_start), None, "at the start");
        let last = Shard::new(
            NamespaceId::DEFAULT,
            ShardId::FIRST,
            Span::FULL,
            Epoch::new(u64::MAX),
        );
        let at_last = Split::new("m", ShardId::new(2), Epoch::new(u64::MAX));
        assert_eq!(last.split(&at_last), None, "no next epoch");
        let into_meta = Split::new("m", ShardId::META, Epoch::ZERO);
        assert_eq!(first.split(&into_meta), None, "the meta shard's id");
        let meta = Shard::whole(NamespaceId::DEFAULT, ShardId::META);
        assert_eq!(meta.split(&split), None, "the meta shard");
    }

    #[test]
    fn truncated_shards_are_rejected() {
        let mut out = Vec::new();
        Shard::first().encode(&mut out).unwrap();
        for len in 0..out.len() {
            assert_eq!(
                Shard::decode(&mut Reader::new(&out[..len])),
                Err(DecodeError::Truncated),
                "{len} bytes"
            );
        }
    }

    proptest! {
        #[test]
        fn shards_roundtrip(shard in shards()) {
            let mut out = Vec::new();
            shard.encode(&mut out).unwrap();
            let mut reader = Reader::new(&out);
            prop_assert_eq!(Shard::decode(&mut reader), Ok(shard));
            prop_assert!(reader.finish().is_ok(), "nothing left");
        }

        #[test]
        fn splits_roundtrip(at in key(), right in any::<u64>(), epoch in any::<u64>()) {
            let split = Split::new(at, ShardId::new(right), Epoch::new(epoch));
            let mut out = Vec::new();
            split.encode(&mut out).unwrap();
            let mut reader = Reader::new(&out);
            prop_assert_eq!(Split::decode(&mut reader), Ok(split));
            prop_assert!(reader.finish().is_ok(), "nothing left");
        }

        #[test]
        fn halves_partition_the_shard(shard in shards(), at in key(), id in any::<u64>(), probe in key()) {
            let split = Split::new(at, ShardId::new(id), shard.epoch());
            if let Some(halves) = shard.split(&split) {
                let (left, right) = (halves.left(), halves.right());
                prop_assert_eq!(left.id(), shard.id(), "left keeps the id");
                prop_assert_eq!(left.namespace(), shard.namespace(), "left stays");
                prop_assert_eq!(right.namespace(), shard.namespace(), "right stays");
                prop_assert_eq!(left.epoch(), right.epoch(), "same epoch");
                prop_assert!(left.epoch() > shard.epoch(), "a later epoch");
                prop_assert_eq!(
                    shard.span().contains(&probe),
                    left.span().contains(&probe) || right.span().contains(&probe),
                    "covered"
                );
            }
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::Split;
    use oleaph_codec::Reader;

    /// Decoding a split never panics, and a decoded split encodes back to
    /// exactly the bytes it was read from.
    #[kani::proof]
    #[kani::unwind(24)]
    fn splits_decode_totally_and_roundtrip() {
        let buf: [u8; 22] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let mut reader = Reader::new(&buf[..len]);
        if let Ok(decoded) = Split::decode(&mut reader) {
            let read = len - reader.rest().len();
            let mut out = Vec::new();
            assert!(decoded.encode(&mut out).is_ok(), "small key");
            assert_eq!(out.as_slice(), &buf[..read], "roundtrip");
        }
    }
}
