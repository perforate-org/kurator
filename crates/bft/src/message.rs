//! What replicas send each other, and its binary encoding.

use std::io;

use oleaph_codec::{DecodeError, Format, Reader};

use crate::block::{Block, Proposal};
use crate::checkpoint::Share;
use crate::timeout::{Timeout, TimeoutCertificate};
use crate::vote::{BlockId, Vote};
use crate::wire::{put_commands, read_commands};

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
            other => Err(DecodeError::Tag(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::fixture::{DOMAIN, key};
    use std::collections::BTreeMap;

    use oleaph_codec::DecodeError;
    use oleaph_consensus::NodeId;
    use proptest::prelude::*;

    use super::Message;
    use crate::block::{Block, Proposal};
    use crate::checkpoint::{Checkpoint, Height, Share, StateRoot};
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
                let entries: BTreeMap<_, _> = highs
                    .into_iter()
                    .map(|(node, high)| {
                        let high = Justify::Certified(Certificate::new(
                            View::new(high),
                            BlockId::GENESIS,
                            BTreeMap::new(),
                        ));
                        let timeout = Timeout::sign(
                            &key(node),
                            DOMAIN,
                            NodeId::new(node),
                            View::new(view),
                            high,
                        );
                        let (voter, seen, signature) = timeout.into_entry();
                        (voter, (seen, signature))
                    })
                    .collect();
                TimeoutCertificate::new(View::new(view), entries)
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
            Message::decode(&framed(&[9])),
            Err(DecodeError::Tag(9)),
            "message tag"
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
