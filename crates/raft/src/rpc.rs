//! The messages replicas exchange, and their binary encoding.
//!
//! One [`Message`], a [`Request`] for the replica of one shard, goes out
//! and one [`Response`] comes back. A transport carries the bytes of
//! [`Message::encode`] and [`Response::encode`] and nothing else, so every
//! transport speaks the same protocol, and the deterministic simulator
//! runs the same encoding as the network.
//!
//! ```text
//! message   = shard u64 | request
//! request   = 1 append | 2 vote | 3 pre-vote | 4 snapshot
//! append    = vote | option(log id) prev | option(log id) commit
//!             | count u32 bytes(entry)*
//! vote      = vote | option(log id) last | option(vote) transfer from
//! pre-vote  = as vote
//! snapshot  = vote | meta | bytes(snapshot)
//!
//! response  = 0 refusal | 1 appended | 2 voted | 3 pre-voted | 4 installed
//! refusal   = 1 stopped | 2 forged | 3 malformed
//! appended  = 0 option(log id) accepted
//!           | 1 log id conflict | option(count u32 log id*) hint
//!           | 2 vote higher vote
//! voted     = vote | flag granted | option(log id) last
//! installed = vote
//! ```
//!
//! `vote`, `log id`, `option`, `flag`, `entry` and `meta` are as in the
//! Raft log (see `crate::codec`); `bytes(x)` is `x` behind a `u32` length,
//! as `oleaph_codec::put_bytes` writes it.

use std::fmt;

use oleaph_codec::{DecodeError, Reader, put_bytes};
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, ConflictHint, SnapshotResponse, VoteRequest,
    VoteResponse,
};

use crate::codec::{
    entry, flag, log_id, meta, option, put_entry, put_len, put_log_id, put_meta, put_option,
    put_vote, vote,
};
use crate::config::Config;
use crate::snapshot::{Snapshot, SnapshotBytes};
use crate::types::{LogId, NodeId, Vote};
use oleaph_shard::ShardId;

const REFUSED: u8 = 0;
const APPEND: u8 = 1;
const VOTE: u8 = 2;
const PRE_VOTE: u8 = 3;
const SNAPSHOT: u8 = 4;

const ACCEPTED: u8 = 0;
const CONFLICT: u8 = 1;
const HIGHER_VOTE: u8 = 2;

const NONE: u8 = 0;
const SOME: u8 = 1;

/// A message from one replica to another.
#[derive(Debug)]
#[non_exhaustive]
pub enum Request<C: Config> {
    /// Entries to append, or a heartbeat, from the leader.
    AppendEntries(AppendEntriesRequest<C>),
    /// A candidate asks for a vote.
    Vote(VoteRequest<C>),
    /// A would-be candidate asks whether it would get a vote.
    PreVote(VoteRequest<C>),
    /// The leader sends its snapshot to a replica that is too far behind
    /// for its log.
    InstallSnapshot {
        /// The leader's vote.
        vote: Vote,
        /// The snapshot.
        snapshot: Snapshot,
    },
}

/// The answer to a [`Request`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Response<C: Config> {
    /// The receiver did not handle the request.
    Refused(Refusal),
    /// The answer to [`Request::AppendEntries`].
    AppendEntries(AppendResult<C>),
    /// The answer to [`Request::Vote`].
    Vote(VoteResponse<C>),
    /// The answer to [`Request::PreVote`].
    PreVote(VoteResponse<C>),
    /// The answer to [`Request::InstallSnapshot`].
    InstallSnapshot(SnapshotResponse<C>),
}

/// Why a replica did not handle a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refusal {
    /// The replica has stopped.
    Stopped,
    /// The request claims to come from another replica than the one that
    /// authenticated the connection.
    Forged,
    /// The request does not decode.
    Malformed,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stopped => "the replica has stopped",
            Self::Forged => "the request claims another sender",
            Self::Malformed => "the request does not decode",
        })
    }
}

impl<C: Config> Request<C> {
    /// The replica the request claims to come from: the leader or
    /// candidate whose vote it carries.
    #[must_use]
    pub const fn sender(&self) -> NodeId {
        let vote = match self {
            Self::AppendEntries(request) => &request.vote,
            Self::Vote(request) | Self::PreVote(request) => &request.vote,
            Self::InstallSnapshot { vote, .. } => vote,
        };
        vote.leader_id.node_id
    }

    /// Appends the request as it goes over the wire.
    fn put(&self, out: &mut Vec<u8>) -> std::io::Result<()> {
        match self {
            Self::AppendEntries(request) => {
                out.push(APPEND);
                put_vote(out, &request.vote);
                put_option(out, request.prev_log_id.as_ref());
                put_option(out, request.leader_commit.as_ref());
                put_len(out, request.entries.len());
                let mut encoded = Vec::new();
                for entry in &request.entries {
                    encoded.clear();
                    put_entry(&mut encoded, entry);
                    put_bytes(out, &encoded)?;
                }
            }
            Self::Vote(request) => {
                out.push(VOTE);
                put_vote_request(out, request);
            }
            Self::PreVote(request) => {
                out.push(PRE_VOTE);
                put_vote_request(out, request);
            }
            Self::InstallSnapshot { vote, snapshot } => {
                out.push(SNAPSHOT);
                put_vote(out, vote);
                put_meta(out, &snapshot.meta);
                put_bytes(out, snapshot.snapshot.as_slice())?;
            }
        }
        Ok(())
    }

    /// Reads a request written by [`Request::put`].
    fn read(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let request = match reader.read::<u8>()? {
            APPEND => {
                let vote = vote(reader)?;
                let prev_log_id = option(reader)?;
                let leader_commit = option(reader)?;
                let mut entries = Vec::new();
                for _ in 0..reader.read::<u32>()? {
                    let mut encoded = Reader::new(reader.bytes()?);
                    entries.push(entry(&mut encoded)?);
                    encoded.finish()?;
                }
                Self::AppendEntries(AppendEntriesRequest {
                    vote,
                    prev_log_id,
                    entries,
                    leader_commit,
                })
            }
            VOTE => Self::Vote(vote_request(reader)?),
            PRE_VOTE => Self::PreVote(vote_request(reader)?),
            SNAPSHOT => {
                let vote = vote(reader)?;
                let meta = meta(reader)?;
                let snapshot = SnapshotBytes::from_vec(reader.bytes()?.to_vec());
                Self::InstallSnapshot {
                    vote,
                    snapshot: Snapshot { meta, snapshot },
                }
            }
            other => return Err(DecodeError::Tag(other)),
        };
        Ok(request)
    }
}

/// A [`Request`] for one shard's replica on the receiving node.
#[derive(Debug)]
pub struct Message<C: Config> {
    shard: ShardId,
    request: Request<C>,
}

impl<C: Config> Message<C> {
    /// `request` for the replica of `shard`.
    #[must_use]
    pub const fn new(shard: ShardId, request: Request<C>) -> Self {
        Self { shard, request }
    }

    /// The shard it is for.
    #[must_use]
    pub const fn shard(&self) -> ShardId {
        self.shard
    }

    /// The replica it claims to come from; see [`Request::sender`].
    #[must_use]
    pub const fn sender(&self) -> NodeId {
        self.request.sender()
    }

    /// The request.
    #[must_use]
    pub fn into_request(self) -> Request<C> {
        self.request
    }

    /// The message as it goes over the wire: `shard u64 | request`.
    ///
    /// # Errors
    ///
    /// When an entry or the snapshot is longer than `u32::MAX` bytes.
    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        let mut out = self.shard.get().to_le_bytes().to_vec();
        self.request.put(&mut out)?;
        Ok(out)
    }

    /// Decodes what [`Message::encode`] wrote.
    ///
    /// # Errors
    ///
    /// When `bytes` is not a whole message.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let shard = ShardId::new(reader.read()?);
        let request = Request::read(&mut reader)?;
        reader.finish()?;
        Ok(Self { shard, request })
    }
}

impl<C: Config> Response<C> {
    /// The response as it goes over the wire.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Refused(refusal) => {
                out.push(REFUSED);
                out.push(match refusal {
                    Refusal::Stopped => 1,
                    Refusal::Forged => 2,
                    Refusal::Malformed => 3,
                });
            }
            Self::AppendEntries(result) => {
                out.push(APPEND);
                put_append_result(&mut out, result);
            }
            Self::Vote(response) => {
                out.push(VOTE);
                put_vote_response(&mut out, response);
            }
            Self::PreVote(response) => {
                out.push(PRE_VOTE);
                put_vote_response(&mut out, response);
            }
            Self::InstallSnapshot(response) => {
                out.push(SNAPSHOT);
                put_vote(&mut out, &response.vote);
            }
        }
        out
    }

    /// Decodes what [`Response::encode`] wrote.
    ///
    /// # Errors
    ///
    /// When `bytes` is not a whole response.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let response = match reader.read::<u8>()? {
            REFUSED => Self::Refused(match reader.read::<u8>()? {
                1 => Refusal::Stopped,
                2 => Refusal::Forged,
                3 => Refusal::Malformed,
                other => return Err(DecodeError::Tag(other)),
            }),
            APPEND => Self::AppendEntries(append_result(&mut reader)?),
            VOTE => Self::Vote(vote_response(&mut reader)?),
            PRE_VOTE => Self::PreVote(vote_response(&mut reader)?),
            SNAPSHOT => Self::InstallSnapshot(SnapshotResponse::new(vote(&mut reader)?)),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(response)
    }
}

fn put_vote_request<C: Config>(out: &mut Vec<u8>, request: &VoteRequest<C>) {
    put_vote(out, &request.vote);
    put_option(out, request.last_log_id.as_ref());
    match &request.leadership_transfer_from {
        None => out.push(NONE),
        Some(from) => {
            out.push(SOME);
            put_vote(out, from);
        }
    }
}

fn vote_request<C: Config>(reader: &mut Reader<'_>) -> Result<VoteRequest<C>, DecodeError> {
    let vote = vote(reader)?;
    let last_log_id = option(reader)?;
    let leadership_transfer_from = match reader.read::<u8>()? {
        NONE => None,
        SOME => Some(self::vote(reader)?),
        other => return Err(DecodeError::Tag(other)),
    };
    Ok(VoteRequest {
        vote,
        last_log_id,
        leadership_transfer_from,
    })
}

fn put_vote_response<C: Config>(out: &mut Vec<u8>, response: &VoteResponse<C>) {
    put_vote(out, &response.vote);
    out.push(u8::from(response.vote_granted));
    put_option(out, response.last_log_id.as_ref());
}

fn vote_response<C: Config>(reader: &mut Reader<'_>) -> Result<VoteResponse<C>, DecodeError> {
    Ok(VoteResponse {
        vote: vote(reader)?,
        vote_granted: flag(reader)?,
        last_log_id: option(reader)?,
    })
}

fn put_append_result<C: Config>(out: &mut Vec<u8>, result: &AppendResult<C>) {
    match result {
        Ok(accepted) => {
            out.push(ACCEPTED);
            put_option(out, accepted.as_ref());
        }
        Err(AppendError::Conflict { log_id, hint }) => {
            out.push(CONFLICT);
            put_log_id(out, log_id);
            match hint {
                None => out.push(NONE),
                Some(hint) => {
                    out.push(SOME);
                    put_len(out, hint.log_ids.len());
                    for sample in &hint.log_ids {
                        put_log_id(out, sample);
                    }
                }
            }
        }
        Err(AppendError::HigherVote(higher)) => {
            out.push(HIGHER_VOTE);
            put_vote(out, higher);
        }
    }
}

fn append_result<C: Config>(reader: &mut Reader<'_>) -> Result<AppendResult<C>, DecodeError> {
    match reader.read::<u8>()? {
        ACCEPTED => Ok(Ok(option(reader)?)),
        CONFLICT => {
            let conflict = log_id(reader)?;
            let hint = match reader.read::<u8>()? {
                NONE => None,
                SOME => {
                    let mut log_ids: Vec<LogId> = Vec::new();
                    for _ in 0..reader.read::<u32>()? {
                        log_ids.push(log_id(reader)?);
                    }
                    Some(ConflictHint::new(log_ids))
                }
                other => return Err(DecodeError::Tag(other)),
            };
            Ok(Err(AppendError::Conflict {
                log_id: conflict,
                hint,
            }))
        }
        HIGHER_VOTE => Ok(Err(AppendError::HigherVote(vote(reader)?))),
        other => Err(DecodeError::Tag(other)),
    }
}

#[cfg(test)]
mod tests {
    use oleaph_codec::DecodeError;
    use oleaph_shard::ShardState;
    use oleaph_storage::{Command, WriteBatch};
    use openraft::EntryPayload;
    use openraft::raft::{
        AppendEntriesRequest, AppendError, ConflictHint, SnapshotResponse, VoteRequest,
        VoteResponse,
    };
    use proptest::prelude::*;

    use super::{Message, Refusal, Request, Response};
    use crate::config::Entry;
    use crate::proposal::Proposal;
    use crate::snapshot::{Snapshot, SnapshotBytes};
    use crate::types::testing::membership;
    use crate::types::{LeaderId, LogId, NodeId, SnapshotMeta, StoredMembership, Vote};
    use oleaph_shard::ShardId;

    crate::declare_config!(Test, openraft_rt_sim::SimRuntime);

    fn log_id() -> impl Strategy<Value = LogId> {
        (0_u64..4, 0_u64..4, 0_u64..1000).prop_map(|(term, node, index)| {
            LogId::new(
                LeaderId {
                    term,
                    node_id: NodeId::new(node),
                },
                index,
            )
        })
    }

    fn vote() -> impl Strategy<Value = Vote> {
        (0_u64..9, 0_u64..4, any::<bool>()).prop_map(|(term, node, committed)| {
            let mut vote = Vote::new(term, NodeId::new(node));
            vote.committed = committed;
            vote
        })
    }

    fn entry() -> impl Strategy<Value = Entry> {
        let bytes = || proptest::collection::vec(any::<u8>(), 0..8);
        prop_oneof![
            (log_id(), bytes(), bytes()).prop_map(|(log_id, key, value)| Entry {
                log_id,
                payload: EntryPayload::Normal(
                    Proposal::new(Command::Kv(WriteBatch::new().put(key, value))).unwrap()
                ),
            }),
            log_id().prop_map(|log_id| Entry {
                log_id,
                payload: EntryPayload::Blank,
            }),
            log_id().prop_map(|log_id| Entry {
                log_id,
                payload: EntryPayload::Membership(membership(&[1, 2], &[3])),
            }),
        ]
    }

    fn vote_request() -> impl Strategy<Value = VoteRequest<Test>> {
        (
            vote(),
            proptest::option::of(log_id()),
            proptest::option::of(vote()),
        )
            .prop_map(
                |(vote, last_log_id, leadership_transfer_from)| VoteRequest {
                    vote,
                    last_log_id,
                    leadership_transfer_from,
                },
            )
    }

    fn vote_response() -> impl Strategy<Value = VoteResponse<Test>> {
        (vote(), any::<bool>(), proptest::option::of(log_id())).prop_map(
            |(vote, vote_granted, last_log_id)| VoteResponse {
                vote,
                vote_granted,
                last_log_id,
            },
        )
    }

    fn snapshot() -> impl Strategy<Value = Snapshot> {
        (proptest::option::of(log_id()), log_id()).prop_map(|(last_log_id, membership_id)| {
            let meta = SnapshotMeta {
                last_log_id,
                last_membership: StoredMembership::new(Some(membership_id), membership(&[1], &[2])),
            };
            let mut state = ShardState::default();
            let _committed = state.apply(
                oleaph_storage::Version::new(1),
                Command::Kv(WriteBatch::new().put("k", "v")),
            );
            let snapshot = SnapshotBytes::encode(&meta, &state).unwrap();
            Snapshot { meta, snapshot }
        })
    }

    fn request() -> impl Strategy<Value = Request<Test>> {
        prop_oneof![
            (
                vote(),
                proptest::option::of(log_id()),
                proptest::option::of(log_id()),
                proptest::collection::vec(entry(), 0..4),
            )
                .prop_map(|(vote, prev_log_id, leader_commit, entries)| {
                    Request::AppendEntries(AppendEntriesRequest {
                        vote,
                        prev_log_id,
                        entries,
                        leader_commit,
                    })
                }),
            vote_request().prop_map(Request::Vote),
            vote_request().prop_map(Request::PreVote),
            (vote(), snapshot())
                .prop_map(|(vote, snapshot)| Request::InstallSnapshot { vote, snapshot }),
        ]
    }

    fn response() -> impl Strategy<Value = Response<Test>> {
        prop_oneof![
            prop_oneof![
                Just(Refusal::Stopped),
                Just(Refusal::Forged),
                Just(Refusal::Malformed)
            ]
            .prop_map(Response::Refused),
            proptest::option::of(log_id())
                .prop_map(|accepted| Response::AppendEntries(Ok(accepted))),
            (
                log_id(),
                proptest::option::of(proptest::collection::vec(log_id(), 0..4))
            )
                .prop_map(|(log_id, hint)| Response::AppendEntries(Err(
                    AppendError::Conflict {
                        log_id,
                        hint: hint.map(ConflictHint::new),
                    }
                ))),
            vote().prop_map(|vote| Response::AppendEntries(Err(AppendError::HigherVote(vote)))),
            vote_response().prop_map(Response::Vote),
            vote_response().prop_map(Response::PreVote),
            vote().prop_map(|vote| Response::InstallSnapshot(SnapshotResponse::new(vote))),
        ]
    }

    #[test]
    fn a_request_names_the_sender_of_its_vote() {
        let request = Request::<Test>::Vote(VoteRequest::new(Vote::new(3, NodeId::new(2)), None));
        assert_eq!(request.sender(), NodeId::new(2), "candidate");
    }

    #[test]
    fn refusals_explain_themselves() {
        for (refusal, text) in [
            (Refusal::Stopped, "the replica has stopped"),
            (Refusal::Forged, "the request claims another sender"),
            (Refusal::Malformed, "the request does not decode"),
        ] {
            assert_eq!(refusal.to_string(), text, "{refusal:?}");
        }
    }

    #[test]
    fn unknown_tags_and_trailing_bytes_are_rejected() {
        for bytes in [vec![9], vec![0, 4]] {
            assert!(
                matches!(Response::<Test>::decode(&bytes), Err(DecodeError::Tag(_))),
                "{bytes:?}"
            );
        }
        let unknown = [&1_u64.to_le_bytes()[..], &[0]].concat();
        assert!(
            matches!(Message::<Test>::decode(&unknown), Err(DecodeError::Tag(0))),
            "request tag"
        );
        let mut encoded = Response::<Test>::Refused(Refusal::Stopped).encode();
        encoded.push(0);
        assert!(
            matches!(
                Response::<Test>::decode(&encoded),
                Err(DecodeError::Trailing)
            ),
            "trailing"
        );
    }

    proptest! {
        #[test]
        fn messages_roundtrip(request in request(), shard in any::<u64>()) {
            let message = Message::new(ShardId::new(shard), request);
            let encoded = message.encode().unwrap();
            let decoded = Message::<Test>::decode(&encoded).unwrap();
            prop_assert_eq!(decoded.shard(), message.shard());
            prop_assert_eq!(decoded.sender(), message.sender());
            prop_assert_eq!(decoded.encode().unwrap(), encoded);
            prop_assert_eq!(format!("{decoded:?}"), format!("{message:?}"));
            prop_assert_eq!(format!("{:?}", decoded.into_request()), format!("{:?}", message.into_request()));
        }

        #[test]
        fn responses_roundtrip(response in response()) {
            let encoded = response.encode();
            let decoded = Response::<Test>::decode(&encoded).unwrap();
            prop_assert_eq!(decoded.encode(), encoded);
            prop_assert_eq!(format!("{decoded:?}"), format!("{response:?}"));
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..96)) {
            drop(Message::<Test>::decode(&bytes));
            drop(Response::<Test>::decode(&bytes));
        }

        #[test]
        fn every_cut_of_a_message_is_rejected(request in request(), cut in any::<prop::sample::Index>()) {
            let encoded = Message::new(ShardId::FIRST, request).encode().unwrap();
            let len = cut.index(encoded.len());
            prop_assert!(Message::<Test>::decode(&encoded[..len]).is_err());
        }
    }
}
