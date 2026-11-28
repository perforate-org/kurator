//! A snapshot cut into chunks, so that sending it takes many short
//! requests, each within the deadline of one, instead of one request that
//! grows with the shard.
//!
//! ```text
//! chunk = meta | offset u64 | total u64 | bytes(part)
//! ```
//!
//! The leader sends a snapshot's chunks in order, each carrying the
//! snapshot's metadata, where its part starts and how long the whole
//! snapshot is. The receiver appends each part to the snapshot it is
//! assembling and hands the snapshot over once the last part is in. A
//! chunk at offset 0 starts a new snapshot; a chunk that does not continue
//! the one being assembled is [stray](Received::Stray), and the leader
//! starts over.

use std::io;
use std::num::NonZeroUsize;

use oleaph_codec::{DecodeError, Reader, put_bytes};

use crate::codec::{meta, put_meta};
use crate::snapshot::{Snapshot, SnapshotBytes};
use crate::types::{SnapshotMeta, Vote};

/// One part of a snapshot, as the leader sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunk {
    meta: SnapshotMeta,
    offset: u64,
    total: u64,
    part: Vec<u8>,
}

impl SnapshotChunk {
    /// `part` of a `total`-byte snapshot, starting at `offset`. `None`
    /// when the part runs past the end.
    fn new(meta: SnapshotMeta, offset: u64, total: u64, part: Vec<u8>) -> Option<Self> {
        let end = offset.checked_add(len(&part))?;
        (end <= total).then_some(Self {
            meta,
            offset,
            total,
            part,
        })
    }

    /// Whether its part ends the snapshot.
    #[must_use]
    pub fn is_last(&self) -> bool {
        self.offset.saturating_add(len(&self.part)) == self.total
    }

    /// The length of the whole snapshot.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// Appends the chunk as it goes over the wire.
    pub(crate) fn put(&self, out: &mut Vec<u8>) -> io::Result<()> {
        put_meta(out, &self.meta);
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.total.to_le_bytes());
        put_bytes(out, &self.part)
    }

    /// Reads a chunk written by [`SnapshotChunk::put`].
    pub(crate) fn read(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let meta = meta(reader)?;
        let offset = reader.read()?;
        let total = reader.read()?;
        let part = reader.bytes()?.to_vec();
        Self::new(meta, offset, total, part).ok_or(DecodeError::Invalid)
    }
}

/// `snapshot` cut into chunks of at most `size` bytes, in order. An empty
/// snapshot is one empty chunk, so there is always a last chunk.
pub(crate) fn chunks(
    snapshot: &Snapshot,
    size: NonZeroUsize,
) -> impl Iterator<Item = SnapshotChunk> + '_ {
    let bytes = snapshot.snapshot.as_slice();
    let total = len(bytes);
    bytes
        .chunks(size.get())
        .chain(bytes.is_empty().then_some(bytes))
        .scan(0, move |offset: &mut u64, part| {
            let start = *offset;
            *offset = start.saturating_add(len(part));
            SnapshotChunk::new(snapshot.meta.clone(), start, total, part.to_vec())
        })
}

/// What a chunk did to the snapshot being assembled.
#[derive(Debug)]
#[must_use]
pub(crate) enum Received {
    /// It continues no snapshot being assembled: dropped.
    Stray,
    /// It was the last part, but the snapshot does not decode as the one
    /// its metadata names: dropped, with everything before it.
    Damaged,
    /// It was kept; more parts are to come.
    Part,
    /// It was the last part: the whole snapshot.
    Whole(Snapshot),
}

/// The snapshot a replica is receiving, part by part.
#[derive(Debug, Default)]
pub(crate) struct Assembly {
    partial: Option<Partial>,
}

/// A snapshot whose first parts are in.
#[derive(Debug)]
struct Partial {
    vote: Vote,
    meta: SnapshotMeta,
    total: u64,
    bytes: Vec<u8>,
}

impl Partial {
    /// Whether `chunk`, from the leader of `vote`, carries the next part.
    fn continues(&self, vote: &Vote, chunk: &SnapshotChunk) -> bool {
        self.vote == *vote
            && self.meta == chunk.meta
            && self.total == chunk.total
            && len(&self.bytes) == chunk.offset
    }
}

impl Assembly {
    /// Adds `chunk`, from the leader of `vote`. A chunk at offset 0 drops
    /// whatever was being assembled and starts anew.
    pub(crate) fn add(&mut self, vote: Vote, chunk: SnapshotChunk) -> Received {
        if chunk.offset == 0 {
            self.partial = Some(Partial {
                vote,
                meta: chunk.meta.clone(),
                total: chunk.total,
                bytes: Vec::new(),
            });
        }
        let Some(partial) = self
            .partial
            .as_mut()
            .filter(|partial| partial.continues(&vote, &chunk))
        else {
            return Received::Stray;
        };
        let last = chunk.is_last();
        partial.bytes.extend(chunk.part);
        if !last {
            return Received::Part;
        }
        let Some(Partial { meta, bytes, .. }) = self.partial.take() else {
            return Received::Stray;
        };
        let snapshot = SnapshotBytes::from_vec(bytes);
        if snapshot.names(&meta) {
            Received::Whole(Snapshot { meta, snapshot })
        } else {
            Received::Damaged
        }
    }
}

fn len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use oleaph_codec::{DecodeError, Reader};
    use oleaph_shard::ShardState;
    use oleaph_storage::{Command, Version, WriteBatch};
    use proptest::prelude::*;

    use super::{Assembly, Received, SnapshotChunk, chunks};
    use crate::snapshot::{Snapshot, SnapshotBytes};
    use crate::types::testing::membership;
    use crate::types::{LeaderId, LogId, NodeId, SnapshotMeta, StoredMembership, Vote};

    fn meta(index: u64) -> SnapshotMeta {
        let log_id = LogId::new(
            LeaderId {
                term: 1,
                node_id: NodeId::new(1),
            },
            index,
        );
        SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::new(Some(log_id), membership(&[1, 2], &[])),
        }
    }

    fn snapshot(keys: usize, index: u64) -> Snapshot {
        let mut state = ShardState::default();
        let mut batch = WriteBatch::new();
        for key in 0..keys {
            batch = batch.put(key.to_le_bytes(), [7; 5]);
        }
        let _committed = state.apply(Version::new(1), Command::Kv(batch));
        let meta = meta(index);
        let snapshot = SnapshotBytes::encode(&meta, &state).unwrap();
        Snapshot { meta, snapshot }
    }

    fn vote() -> Vote {
        Vote::new_committed(2, NodeId::new(1))
    }

    fn size(bytes: usize) -> NonZeroUsize {
        NonZeroUsize::new(bytes).unwrap()
    }

    /// What `received` says, as a word, with the whole snapshot's parts.
    fn seen(received: &Received) -> (&'static str, Option<(SnapshotMeta, SnapshotBytes)>) {
        match received {
            Received::Stray => ("stray", None),
            Received::Damaged => ("damaged", None),
            Received::Part => ("part", None),
            Received::Whole(whole) => ("whole", Some((whole.meta.clone(), whole.snapshot.clone()))),
        }
    }

    fn whole(snapshot: &Snapshot) -> (&'static str, Option<(SnapshotMeta, SnapshotBytes)>) {
        (
            "whole",
            Some((snapshot.meta.clone(), snapshot.snapshot.clone())),
        )
    }

    /// Feeds every chunk of `snapshot` and returns what the last one did.
    fn assemble(assembly: &mut Assembly, snapshot: &Snapshot, at: usize) -> Received {
        let mut received = Received::Stray;
        for chunk in chunks(snapshot, size(at)) {
            received = assembly.add(vote(), chunk);
        }
        received
    }

    proptest! {
        #[test]
        fn chunks_reassemble_into_the_snapshot(keys in 0_usize..40, at in 1_usize..300) {
            let snapshot = snapshot(keys, 9);
            let parts: Vec<_> = chunks(&snapshot, size(at)).collect();
            prop_assert!(parts.iter().all(|chunk| chunk.part.len() <= at), "bounded");
            prop_assert_eq!(parts.iter().filter(|chunk| chunk.is_last()).count(), 1, "one last");
            prop_assert!(parts.last().is_some_and(SnapshotChunk::is_last), "last at the end");
            let mut assembly = Assembly::default();
            let mut received = Vec::new();
            for chunk in parts {
                received.push(assembly.add(vote(), chunk));
            }
            let whole = received.pop();
            prop_assert!(received.iter().all(|step| matches!(step, Received::Part)), "{received:?}");
            prop_assert_eq!(whole.as_ref().map(seen), Some(self::whole(&snapshot)));
        }

        #[test]
        fn chunks_roundtrip_and_every_cut_is_rejected(at in 1_usize..64, cut in any::<prop::sample::Index>()) {
            for chunk in chunks(&snapshot(3, 4), size(at)) {
                let mut encoded = Vec::new();
                chunk.put(&mut encoded).unwrap();
                let mut reader = Reader::new(&encoded);
                prop_assert_eq!(SnapshotChunk::read(&mut reader), Ok(chunk.clone()));
                prop_assert_eq!(reader.finish(), Ok(()));
                let len = cut.index(encoded.len());
                prop_assert!(SnapshotChunk::read(&mut Reader::new(&encoded[..len])).is_err());
            }
        }
    }

    #[test]
    fn an_empty_snapshot_is_one_last_chunk() {
        let empty = Snapshot {
            meta: meta(1),
            snapshot: SnapshotBytes::from_vec(Vec::new()),
        };
        let parts: Vec<_> = chunks(&empty, size(4)).collect();
        assert_eq!(parts.len(), 1, "one chunk");
        assert!(parts.iter().all(SnapshotChunk::is_last), "last");
        assert_eq!(
            parts.iter().map(SnapshotChunk::total).sum::<u64>(),
            0,
            "empty"
        );
        assert_eq!(
            seen(&Assembly::default().add(vote(), parts.into_iter().next().unwrap())).0,
            "damaged",
            "no format"
        );
    }

    #[test]
    fn a_part_past_the_end_does_not_decode() {
        let chunk = chunks(&snapshot(1, 1), size(1 << 20)).next().unwrap();
        let mut encoded = Vec::new();
        chunk.put(&mut encoded).unwrap();
        // The total sits just before the part's u32 length.
        let total_at = encoded.len() - chunk.part.len() - 4 - 8;
        encoded[total_at..total_at + 8].copy_from_slice(&1_u64.to_le_bytes());
        assert_eq!(
            SnapshotChunk::read(&mut Reader::new(&encoded)),
            Err(DecodeError::Invalid),
            "past the end"
        );
        assert_eq!(
            SnapshotChunk::new(meta(1), u64::MAX, u64::MAX, vec![1]),
            None,
            "overflow"
        );
    }

    #[test]
    fn chunks_that_continue_nothing_are_stray() {
        let first = snapshot(30, 5);
        let parts: Vec<_> = chunks(&first, size(64)).collect();
        assert!(parts.len() > 3, "several parts");
        let mut assembly = Assembly::default();
        assert_eq!(
            seen(&assembly.add(vote(), parts[1].clone())).0,
            "stray",
            "no start"
        );
        assert_eq!(
            seen(&assembly.add(vote(), parts[0].clone())).0,
            "part",
            "start"
        );
        assert_eq!(
            seen(&assembly.add(vote(), parts[2].clone())).0,
            "stray",
            "gap"
        );
        assert_eq!(
            seen(&assembly.add(vote(), parts[0].clone())).0,
            "part",
            "restart"
        );
        let other = Vote::new_committed(3, NodeId::new(2));
        assert_eq!(
            seen(&assembly.add(other, parts[1].clone())).0,
            "stray",
            "other leader"
        );
        let later = snapshot(30, 6);
        let later_part = chunks(&later, size(64)).nth(1).unwrap();
        assert_eq!(
            seen(&assembly.add(vote(), later_part)).0,
            "stray",
            "other snapshot"
        );
        let shorter = chunks(&snapshot(29, 5), size(64)).nth(1).unwrap();
        assert_eq!(
            seen(&assembly.add(vote(), shorter)).0,
            "stray",
            "other length"
        );
        for chunk in parts.into_iter().skip(1) {
            let _received = assembly.add(vote(), chunk);
        }
        assert_eq!(
            assembly.partial.as_ref().map(|partial| partial.total),
            None,
            "handed over"
        );
        assert_eq!(
            seen(&assemble(&mut assembly, &first, 64)),
            whole(&first),
            "again"
        );
    }

    #[test]
    fn a_damaged_snapshot_is_not_handed_over() {
        let mut whole = snapshot(10, 3);
        let mut bytes = whole.snapshot.as_slice().to_vec();
        bytes[12] ^= 1;
        whole.snapshot = SnapshotBytes::from_vec(bytes);
        let mut assembly = Assembly::default();
        assert_eq!(
            seen(&assemble(&mut assembly, &whole, 50)).0,
            "damaged",
            "flipped"
        );
        let mut mislabelled = snapshot(10, 3);
        mislabelled.meta = meta(4);
        assert_eq!(
            seen(&assemble(&mut assembly, &mislabelled, 50)).0,
            "damaged",
            "other meta"
        );
    }
}
