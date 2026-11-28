//! What replicas send each other, and its binary encoding.

use std::io;

use oleaph_codec::{DecodeError, Format, Reader, put_bytes};

use crate::block::{Block, Proposal};
use crate::checkpoint::{CheckpointCertificate, Height, Manifest, Part, Share, Succession};
use crate::timeout::{Timeout, TimeoutCertificate};
use crate::vote::{BlockId, Vote};
use crate::wire::{put_commands, put_count, read_commands, read_count};

/// Every message: magic `OBFT`, version 1, a tag, the body, a CRC-32C.
const FORMAT: Format = Format::new(*b"OBFT", 1);

const PROPOSAL: u8 = 1;
const VOTE: u8 = 2;
const TIMEOUT: u8 = 3;
const FETCH: u8 = 4;
const DELIVER: u8 = 5;
const SKIP: u8 = 6;
const FORWARD: u8 = 7;
const CHECKPOINT: u8 = 8;
const CERTIFIED: u8 = 9;
const WANT: u8 = 10;
const MANIFEST: u8 = 11;
const CHUNK: u8 = 12;
const HANDOFF: u8 = 13;

/// How [`Part`]s are tagged.
const PART_MANIFEST: u8 = 0;
const PART_CHUNK: u8 = 1;

/// A message between the replicas of a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
    /// A leader proposes a block, to every replica.
    Proposal(Proposal),
    /// A vote for a block, to the leader of the next view.
    Vote(Vote),
    /// A replica gives up on a view, to every replica.
    Timeout(Timeout),
    /// Asks for a block this replica lacks.
    Fetch(BlockId),
    /// A block someone asked for.
    Deliver(Block),
    /// The certificate that ended a view in a timeout, for replicas that
    /// missed the timeouts it is made of.
    Skip(TimeoutCertificate),
    /// Commands submitted to a replica, for the leader of its view to
    /// propose.
    Forward(Vec<Vec<u8>>),
    /// A member's share of a checkpoint, to every replica.
    Checkpoint(Share),
    /// The newest certified checkpoint, for a replica that asked for a
    /// block from before it.
    Certified(CheckpointCertificate),
    /// Asks for a part of the state at the checkpoint of this height.
    Want(Height, Part),
    /// The manifest of the state at the checkpoint of this height.
    Manifest(Height, Manifest),
    /// A chunk of the state at the checkpoint of this height: its index
    /// and its bytes.
    Chunk(Height, u32, Vec<u8>),
    /// How each epoch since the charter's succeeded the one before, for a
    /// replica that sent what only an epoch the sender left can check.
    Handoff(Vec<Succession>),
}

impl Message {
    /// The message as bytes.
    ///
    /// # Errors
    ///
    /// When a list or command is longer than its `u32` length prefix.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        FORMAT.write(Vec::new(), |out| match self {
            Self::Proposal(proposal) => {
                out.write_all(&[PROPOSAL])?;
                proposal.encode(out)
            }
            Self::Vote(vote) => {
                out.write_all(&[VOTE])?;
                vote.encode(out)
            }
            Self::Timeout(timeout) => {
                out.write_all(&[TIMEOUT])?;
                timeout.encode(out)
            }
            Self::Fetch(block) => {
                out.write_all(&[FETCH])?;
                block.encode(out)
            }
            Self::Deliver(block) => {
                out.write_all(&[DELIVER])?;
                block.encode(out)
            }
            Self::Skip(certificate) => {
                out.write_all(&[SKIP])?;
                certificate.encode(out)
            }
            Self::Forward(commands) => {
                out.write_all(&[FORWARD])?;
                put_commands(out, commands)
            }
            Self::Checkpoint(share) => {
                out.write_all(&[CHECKPOINT])?;
                share.encode(out)
            }
            Self::Certified(certificate) => {
                out.write_all(&[CERTIFIED])?;
                certificate.encode(out)
            }
            Self::Want(height, part) => {
                out.write_all(&[WANT])?;
                out.write_all(&height.get().to_le_bytes())?;
                match part {
                    Part::Manifest => out.write_all(&[PART_MANIFEST]),
                    Part::Chunk(index) => {
                        out.write_all(&[PART_CHUNK])?;
                        out.write_all(&index.to_le_bytes())
                    }
                }
            }
            Self::Manifest(height, manifest) => {
                out.write_all(&[MANIFEST])?;
                out.write_all(&height.get().to_le_bytes())?;
                manifest.encode(out)
            }
            Self::Chunk(height, index, bytes) => {
                out.write_all(&[CHUNK])?;
                out.write_all(&height.get().to_le_bytes())?;
                out.write_all(&index.to_le_bytes())?;
                put_bytes(out, bytes)
            }
            Self::Handoff(successions) => {
                out.write_all(&[HANDOFF])?;
                put_count(out, successions.len())?;
                successions
                    .iter()
                    .try_for_each(|succession| succession.encode(out))
            }
        })
    }

    /// Reads a message written by [`Message::encode`].
    ///
    /// # Errors
    ///
    /// When `bytes` are not one. Decoding checks the format, not the
    /// signatures: the replica does that.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = FORMAT.read(bytes)?;
        let message = Self::body(&mut reader)?;
        reader.finish()?;
        Ok(message)
    }

    fn body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match reader.read::<u8>()? {
            PROPOSAL => Ok(Self::Proposal(Proposal::decode(reader)?)),
            VOTE => Ok(Self::Vote(Vote::decode(reader)?)),
            TIMEOUT => Ok(Self::Timeout(Timeout::decode(reader)?)),
            FETCH => Ok(Self::Fetch(BlockId::decode(reader)?)),
            DELIVER => Ok(Self::Deliver(Block::decode(reader)?)),
            SKIP => Ok(Self::Skip(TimeoutCertificate::decode(reader)?)),
            FORWARD => Ok(Self::Forward(read_commands(reader)?)),
            CHECKPOINT => Ok(Self::Checkpoint(Share::decode(reader)?)),
            CERTIFIED => Ok(Self::Certified(CheckpointCertificate::decode(reader)?)),
            WANT => {
                let height = Height::new(reader.read()?);
                let part = match reader.read::<u8>()? {
                    PART_MANIFEST => Part::Manifest,
                    PART_CHUNK => Part::Chunk(reader.read()?),
                    other => return Err(DecodeError::Tag(other)),
                };
                Ok(Self::Want(height, part))
            }
            MANIFEST => {
                let height = Height::new(reader.read()?);
                Ok(Self::Manifest(height, Manifest::decode(reader)?))
            }
            CHUNK => {
                let height = Height::new(reader.read()?);
                let index = reader.read()?;
                Ok(Self::Chunk(height, index, reader.bytes()?.to_vec()))
            }
            HANDOFF => {
                let count = read_count(reader)?;
                let successions = (0..count)
                    .map(|_| Succession::decode(reader))
                    .collect::<Result<_, _>>()?;
                Ok(Self::Handoff(successions))
            }
            other => Err(DecodeError::Tag(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::fixture::{DOMAIN, certify, key, succession_to};
    use std::collections::BTreeMap;

    use oleaph_codec::DecodeError;
    use oleaph_consensus::NodeId;
    use proptest::prelude::*;

    use super::Message;
    use crate::block::{Block, Proposal};
    use crate::checkpoint::{Checkpoint, Height, Manifest, Part, Share, StateRoot};
    use crate::timeout::{Timeout, TimeoutCertificate};
    use crate::view::View;
    use crate::vote::{BlockId, Certificate, Justify, Vote};

    fn justify() -> impl Strategy<Value = Justify> {
        prop_oneof![
            Just(Justify::Genesis),
            (
                any::<u64>(),
                any::<[u8; 32]>(),
                proptest::collection::btree_set(1..6_u64, 0..4)
            )
                .prop_map(|(view, block, signers)| {
                    let (view, block) = (View::new(view), BlockId::new(block));
                    let signatures = signers
                        .into_iter()
                        .map(|node| {
                            let vote =
                                Vote::sign(&key(node), DOMAIN, NodeId::new(node), view, block);
                            (NodeId::new(node), vote.into_signature())
                        })
                        .collect();
                    Justify::Certified(Certificate::new(view, block, signatures))
                }),
        ]
    }

    fn certificate() -> impl Strategy<Value = TimeoutCertificate> {
        (
            any::<u64>(),
            proptest::collection::btree_map(1..6_u64, any::<u64>(), 0..4),
        )
            .prop_map(|(view, highs)| {
                let entries: Vec<_> = highs
                    .into_iter()
                    .map(|(node, high)| {
                        let high = Justify::Certified(Certificate::new(
                            View::new(high),
                            BlockId::GENESIS,
                            BTreeMap::new(),
                        ));
                        Timeout::sign(&key(node), DOMAIN, NodeId::new(node), View::new(view), high)
                    })
                    .collect();
                TimeoutCertificate::gather(View::new(view), entries)
            })
    }

    fn block() -> impl Strategy<Value = Block> {
        (
            any::<u64>(),
            justify(),
            proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..12), 0..4),
        )
            .prop_map(|(view, justify, commands)| Block::new(View::new(view), justify, commands))
    }

    fn message() -> impl Strategy<Value = Message> {
        prop_oneof![
            (block(), proptest::option::of(certificate()), 1..6_u64).prop_map(
                |(block, timeout, node)| {
                    Message::Proposal(Proposal::sign(
                        &key(node),
                        DOMAIN,
                        NodeId::new(node),
                        block,
                        timeout,
                    ))
                }
            ),
            (any::<u64>(), any::<[u8; 32]>(), 1..6_u64).prop_map(|(view, block, node)| {
                Message::Vote(Vote::sign(
                    &key(node),
                    DOMAIN,
                    NodeId::new(node),
                    View::new(view),
                    BlockId::new(block),
                ))
            }),
            (any::<u64>(), justify(), 1..6_u64).prop_map(|(view, high, node)| {
                Message::Timeout(Timeout::sign(
                    &key(node),
                    DOMAIN,
                    NodeId::new(node),
                    View::new(view),
                    high,
                ))
            }),
            any::<[u8; 32]>().prop_map(|id| Message::Fetch(BlockId::new(id))),
            block().prop_map(Message::Deliver),
            certificate().prop_map(Message::Skip),
            proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..12), 0..4)
                .prop_map(Message::Forward),
            (any::<u64>(), any::<[u8; 32]>(), any::<[u8; 32]>(), 1..6_u64).prop_map(
                |(height, block, root, node)| {
                    let checkpoint = Checkpoint::new(
                        Height::new(height),
                        BlockId::new(block),
                        View::new(height),
                        StateRoot::new(root),
                    );
                    Message::Checkpoint(Share::sign(
                        &key(node),
                        DOMAIN,
                        NodeId::new(node),
                        checkpoint,
                    ))
                }
            ),
            (any::<u64>(), any::<[u8; 32]>()).prop_map(|(height, root)| {
                Message::Certified(certify(Checkpoint::new(
                    Height::new(height),
                    BlockId::new(root),
                    View::new(height),
                    StateRoot::new(root),
                )))
            }),
            (any::<u64>(), proptest::option::of(any::<u32>())).prop_map(|(height, chunk)| {
                Message::Want(
                    Height::new(height),
                    chunk.map_or(Part::Manifest, Part::Chunk),
                )
            }),
            (any::<u64>(), proptest::collection::vec(any::<u8>(), 0..64)).prop_map(
                |(height, state)| Message::Manifest(Height::new(height), Manifest::over(&state))
            ),
            (
                any::<u64>(),
                any::<u32>(),
                proptest::collection::vec(any::<u8>(), 0..64)
            )
                .prop_map(|(height, index, bytes)| Message::Chunk(
                    Height::new(height),
                    index,
                    bytes
                )),
            proptest::collection::vec(any::<[u8; 32]>(), 0..3).prop_map(|blocks| {
                Message::Handoff(
                    (1..)
                        .zip(blocks)
                        .map(|(epoch, block)| succession_to(epoch, &[1, 2, 3], BlockId::new(block)))
                        .collect(),
                )
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn messages_round_trip(message in message()) {
            let bytes = message.encode().unwrap();
            prop_assert_eq!(Message::decode(&bytes).unwrap(), message);
        }

        #[test]
        fn decoding_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _decoded = Message::decode(&bytes);
        }

        #[test]
        fn a_damaged_message_is_refused(message in message(), at in any::<prop::sample::Index>(), flip in 1..=255_u8) {
            let mut bytes = message.encode().unwrap();
            let at = at.index(bytes.len());
            bytes[at] ^= flip;
            prop_assert!(Message::decode(&bytes).is_err(), "damaged");
        }
    }

    /// Writes `body` as a whole message.
    fn framed(body: &[u8]) -> Vec<u8> {
        super::FORMAT
            .write(Vec::new(), |out| out.write_all(body))
            .unwrap()
    }

    #[test]
    fn unknown_tags_and_unordered_sets_are_refused() {
        assert_eq!(
            Message::decode(&framed(&[14])),
            Err(DecodeError::Tag(14)),
            "message tag"
        );
        let mut want = vec![super::WANT];
        want.extend_from_slice(&[0; 8]);
        want.push(2);
        assert_eq!(
            Message::decode(&framed(&want)),
            Err(DecodeError::Tag(2)),
            "part tag"
        );
        // A manifest of one byte takes one chunk, not none.
        let mut manifest = vec![super::MANIFEST];
        manifest.extend_from_slice(&[0; 8]);
        manifest.extend_from_slice(&1_u64.to_le_bytes());
        manifest.extend_from_slice(&0_u32.to_le_bytes());
        assert_eq!(
            Message::decode(&framed(&manifest)),
            Err(DecodeError::Invalid),
            "chunks its length takes"
        );
        let mut vote = vec![super::VOTE];
        vote.extend_from_slice(&[0; 8 + 32 + 8]);
        vote.push(77);
        assert_eq!(
            Message::decode(&framed(&vote)),
            Err(DecodeError::Tag(77)),
            "signature algorithm"
        );
        let mut fetch = vec![super::FETCH];
        fetch.extend_from_slice(&[0; 33]);
        assert_eq!(
            Message::decode(&framed(&fetch)),
            Err(DecodeError::Trailing),
            "trailing"
        );
        // A timeout certificate whose signers are not in order.
        let signature = Vote::sign(
            &key(1),
            DOMAIN,
            NodeId::new(1),
            View::GENESIS,
            BlockId::GENESIS,
        )
        .into_signature();
        let mut skip = vec![super::SKIP];
        skip.extend_from_slice(&3_u64.to_le_bytes());
        skip.extend_from_slice(&2_u32.to_le_bytes());
        for node in [2_u64, 1] {
            skip.extend_from_slice(&node.to_le_bytes());
            skip.extend_from_slice(&0_u64.to_le_bytes());
            skip.push(signature.algorithm().id());
            skip.extend_from_slice(&64_u32.to_le_bytes());
            skip.extend_from_slice(signature.as_bytes());
        }
        assert_eq!(
            Message::decode(&framed(&skip)),
            Err(DecodeError::Invalid),
            "unordered"
        );
        let mut proposal = vec![super::PROPOSAL];
        proposal.extend_from_slice(&[0; 8]);
        proposal.push(5);
        assert_eq!(
            Message::decode(&framed(&proposal)),
            Err(DecodeError::Tag(5)),
            "justify tag"
        );
    }
}
