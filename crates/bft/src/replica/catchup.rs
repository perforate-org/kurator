//! Taking the state at a certified checkpoint from the others, for a
//! replica too far behind to decide the blocks up to it: the others let
//! those blocks go once they kept the state.
//!
//! It asks for the [`Manifest`] first, which it checks against the
//! certificate's root, then for the chunks, a few at a time, which it
//! checks against the manifest. It asks each member in turn, so that one
//! that does not answer, or answers wrong, only slows it down.

use std::collections::{BTreeMap, BTreeSet};

use oleaph_consensus::NodeId;

use crate::checkpoint::{CheckpointCertificate, Height, Manifest, Part};
use crate::message::Message;

/// How many chunks it asks for at once.
const WINDOW: usize = 8;

/// The state at a certified checkpoint, as far as the replica took it in.
#[derive(Debug)]
pub(crate) struct Catchup {
    certificate: CheckpointCertificate,
    manifest: Option<Manifest>,
    chunks: BTreeMap<u32, Vec<u8>>,
    /// The parts asked for and not yet taken in.
    asked: BTreeSet<Part>,
    /// How many asks it sent, to ask each member in turn.
    turn: usize,
}

impl Catchup {
    /// Starts taking the state `certificate` certifies.
    pub(crate) const fn new(certificate: CheckpointCertificate) -> Self {
        Self {
            certificate,
            manifest: None,
            chunks: BTreeMap::new(),
            asked: BTreeSet::new(),
            turn: 0,
        }
    }

    /// The height of the checkpoint.
    pub(crate) const fn height(&self) -> Height {
        self.certificate.height()
    }

    /// Forgets what it asked for, so that it asks again: the answers may
    /// have been lost.
    pub(crate) fn again(&mut self) {
        self.asked.clear();
    }

    /// What to ask `members` for next, each in turn.
    pub(crate) fn asks(&mut self, members: &[NodeId]) -> Vec<(NodeId, Message)> {
        let parts: Vec<Part> = self.manifest.as_ref().map_or_else(
            || vec![Part::Manifest],
            |manifest| {
                (0..u32::try_from(manifest.count()).unwrap_or(u32::MAX))
                    .filter(|index| !self.chunks.contains_key(index))
                    .map(Part::Chunk)
                    .collect()
            },
        );
        let room = WINDOW.saturating_sub(self.asked.len());
        let parts: Vec<Part> = parts
            .into_iter()
            .filter(|part| !self.asked.contains(part))
            .take(room)
            .collect();
        self.asked.extend(&parts);
        let height = self.height();
        parts
            .into_iter()
            .filter_map(|part| {
                let member = *members.get(self.turn.checked_rem(members.len())?)?;
                self.turn = self.turn.wrapping_add(1);
                Some((member, Message::Want(height, part)))
            })
            .collect()
    }

    /// Takes in `manifest` of the state at `height`, if it is the one the
    /// certificate names, and says whether it was.
    pub(crate) fn manifest(&mut self, height: Height, manifest: Manifest) -> bool {
        let named = self.manifest.is_none()
            && height == self.height()
            && manifest.root() == self.certificate.root();
        if named {
            let _asked = self.asked.remove(&Part::Manifest);
            self.manifest = Some(manifest);
        }
        named
    }

    /// Takes in `bytes`, if they are chunk `index` of the state at
    /// `height`, and says whether they were.
    pub(crate) fn chunk(&mut self, height: Height, index: u32, bytes: Vec<u8>) -> bool {
        let named = height == self.height()
            && self
                .manifest
                .as_ref()
                .is_some_and(|manifest| manifest.holds(index, &bytes));
        if named {
            let _asked = self.asked.remove(&Part::Chunk(index));
            drop(self.chunks.insert(index, bytes));
        }
        named
    }

    /// The certificate and the state it certifies, encoded, once every
    /// chunk is in; or else itself.
    pub(crate) fn finish(self) -> Result<(CheckpointCertificate, Vec<u8>), Box<Self>> {
        let complete = self
            .manifest
            .as_ref()
            .is_some_and(|manifest| manifest.count() == self.chunks.len());
        if !complete {
            return Err(Box::new(self));
        }
        let encoded = self.chunks.into_values().flatten().collect();
        Ok((self.certificate, encoded))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use oleaph_consensus::NodeId;

    use super::{Catchup, WINDOW};
    use crate::checkpoint::{CHUNK, Checkpoint, CheckpointCertificate, Height, Manifest, Part};
    use crate::message::Message;
    use crate::view::View;
    use crate::vote::BlockId;

    /// A state of a dozen chunks and a half, and its certificate.
    fn state() -> (Vec<u8>, CheckpointCertificate) {
        let length = CHUNK * 12 + 1000;
        let bytes: Vec<u8> = (0..length)
            .map(|at| u8::try_from(at % 251).unwrap())
            .collect();
        let root = Manifest::over(&bytes).root();
        let checkpoint = Checkpoint::new(Height::new(9), BlockId::new([9; 32]), View::new(4), root);
        (
            bytes,
            CheckpointCertificate::new(checkpoint, BTreeMap::new()),
        )
    }

    fn wanted(asks: &[(NodeId, Message)]) -> Vec<Part> {
        asks.iter()
            .filter_map(|(_, message)| {
                if let Message::Want(height, part) = message
                    && *height == Height::new(9)
                {
                    Some(*part)
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn it_takes_the_manifest_then_the_chunks_and_checks_both() {
        let (bytes, certificate) = state();
        let manifest = Manifest::over(&bytes);
        let members = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let mut catchup = Catchup::new(certificate);
        assert_eq!(catchup.height(), Height::new(9), "its height");
        let first = catchup.asks(&members);
        assert_eq!(wanted(&first), vec![Part::Manifest], "the manifest first");
        assert!(catchup.asks(&members).is_empty(), "asked once");
        catchup.again();
        assert_eq!(
            wanted(&catchup.asks(&members)),
            vec![Part::Manifest],
            "again"
        );

        let other = Manifest::over(b"another state");
        assert!(!catchup.manifest(Height::new(9), other), "another root");
        assert!(
            !catchup.manifest(Height::new(8), manifest.clone()),
            "another height"
        );
        assert!(
            catchup.manifest(Height::new(9), manifest.clone()),
            "the one"
        );
        assert!(!catchup.manifest(Height::new(9), manifest), "only once");

        let asks = catchup.asks(&members);
        let parts = wanted(&asks);
        assert_eq!(parts.len(), WINDOW, "a window of chunks");
        let askees: Vec<NodeId> = asks.iter().map(|(to, _)| *to).collect();
        assert!(
            members.iter().all(|member| askees.contains(member)),
            "each in turn"
        );
        assert!(catchup.asks(&members).is_empty(), "the window is full");

        let chunks: Vec<&[u8]> = bytes.chunks(CHUNK).collect();
        assert!(
            !catchup.chunk(Height::new(9), 0, b"garbage".to_vec()),
            "checked"
        );
        assert!(
            !catchup.chunk(Height::new(8), 0, chunks[0].to_vec()),
            "another height"
        );
        for (index, chunk) in (0_u32..).zip(&chunks) {
            assert!(
                catchup.chunk(Height::new(9), index, chunk.to_vec()),
                "{index}"
            );
            let more = catchup.asks(&members);
            assert!(
                wanted(&more)
                    .iter()
                    .all(|part| matches!(part, Part::Chunk(at) if *at > index)),
                "only missing ones"
            );
        }
        let (certified, encoded) = catchup.finish().unwrap();
        assert_eq!(certified.height(), Height::new(9), "the certificate");
        assert_eq!(encoded, bytes, "the state");
    }

    #[test]
    fn it_finishes_only_with_every_chunk() {
        let (bytes, certificate) = state();
        let catchup = Catchup::new(certificate);
        let Err(mut catchup) = catchup.finish() else {
            panic!("no manifest yet");
        };
        assert!(
            catchup.manifest(Height::new(9), Manifest::over(&bytes)),
            "taken"
        );
        assert!(
            catchup.chunk(Height::new(9), 0, bytes[..CHUNK].to_vec()),
            "one"
        );
        assert!(catchup.finish().is_err(), "one chunk is not all");
    }
}
