//! The records of the Raft log, as they are stored in the WAL.
//!
//! ```text
//! record    = tag u8 | body
//! entry     = 1 | log id | payload
//! truncate  = 2 | option(log id)          entries after it are gone
//! purge     = 3 | log id                  entries up to it are gone
//! vote      = 4 | term u64 | node u64 | committed u8
//! committed = 5 | option(log id)          the newest entry known committed
//!
//! log id    = term u64 | node u64 | index u64
//! option(x) = 0 | 1 x
//! payload   = 0 (blank) | 1 command | 2 membership
//! command   = the rest of the record, as `kurator_storage::Command::encode`
//!             wrote it
//! membership= configs u32 (voters u32 node u64*)* | nodes u32 node u64*
//! ```

use std::collections::BTreeSet;

use kurator_codec::{DecodeError, Reader};
use kurator_storage::Encoded;
use openraft::EntryPayload;

use crate::config::Entry;
use crate::proposal::Proposal;
use crate::types::{LeaderId, LogId, Membership, NodeId, Vote};

const ENTRY: u8 = 1;
const TRUNCATE: u8 = 2;
const PURGE: u8 = 3;
const VOTE: u8 = 4;
const COMMITTED: u8 = 5;

const BLANK: u8 = 0;
const NORMAL: u8 = 1;
const MEMBERSHIP: u8 = 2;

const NONE: u8 = 0;
const SOME: u8 = 1;

/// One record of the Raft log.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Record {
    Entry(Entry),
    TruncateAfter(Option<LogId>),
    PurgeThrough(LogId),
    Vote(Vote),
    Committed(Option<LogId>),
}

impl Record {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Entry(entry) => {
                out.push(ENTRY);
                put_log_id(&mut out, &entry.log_id);
                put_payload(&mut out, &entry.payload);
            }
            Self::TruncateAfter(log_id) => {
                out.push(TRUNCATE);
                put_option(&mut out, log_id.as_ref());
            }
            Self::PurgeThrough(log_id) => {
                out.push(PURGE);
                put_log_id(&mut out, log_id);
            }
            Self::Vote(vote) => {
                out.push(VOTE);
                put_leader_id(&mut out, &vote.leader_id);
                out.push(u8::from(vote.committed));
            }
            Self::Committed(log_id) => {
                out.push(COMMITTED);
                put_option(&mut out, log_id.as_ref());
            }
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let record = match reader.read::<u8>()? {
            ENTRY => {
                let log_id = log_id(&mut reader)?;
                let payload = payload(&mut reader)?;
                Self::Entry(Entry { log_id, payload })
            }
            TRUNCATE => Self::TruncateAfter(option(&mut reader)?),
            PURGE => Self::PurgeThrough(log_id(&mut reader)?),
            VOTE => {
                let leader_id = leader_id(&mut reader)?;
                let committed = match reader.read::<u8>()? {
                    0 => false,
                    1 => true,
                    other => return Err(DecodeError::Tag(other)),
                };
                Self::Vote(Vote {
                    leader_id,
                    committed,
                })
            }
            COMMITTED => Self::Committed(option(&mut reader)?),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(record)
    }
}

/// Encodes a membership on its own, for snapshots.
pub(crate) fn put_membership(out: &mut Vec<u8>, membership: &Membership) {
    let configs = membership.get_joint_config();
    put_len(out, configs.len());
    for voters in configs {
        put_len(out, voters.len());
        for voter in voters {
            out.extend_from_slice(&voter.get().to_le_bytes());
        }
    }
    let nodes: Vec<NodeId> = membership.nodes().map(|(id, _)| *id).collect();
    put_len(out, nodes.len());
    for node in nodes {
        out.extend_from_slice(&node.get().to_le_bytes());
    }
}

/// Decodes what [`put_membership`] wrote.
pub(crate) fn membership(reader: &mut Reader<'_>) -> Result<Membership, DecodeError> {
    let mut configs = Vec::new();
    for _ in 0..reader.read::<u32>()? {
        let mut voters = BTreeSet::new();
        for _ in 0..reader.read::<u32>()? {
            let _: bool = voters.insert(node_id(reader)?);
        }
        configs.push(voters);
    }
    let mut nodes = Vec::new();
    for _ in 0..reader.read::<u32>()? {
        nodes.push(node_id(reader)?);
    }
    Ok(Membership::new_with_defaults(configs, nodes))
}

pub(crate) fn put_option(out: &mut Vec<u8>, log_id: Option<&LogId>) {
    match log_id {
        None => out.push(NONE),
        Some(log_id) => {
            out.push(SOME);
            put_log_id(out, log_id);
        }
    }
}

pub(crate) fn option(reader: &mut Reader<'_>) -> Result<Option<LogId>, DecodeError> {
    match reader.read::<u8>()? {
        NONE => Ok(None),
        SOME => log_id(reader).map(Some),
        other => Err(DecodeError::Tag(other)),
    }
}

fn put_payload(out: &mut Vec<u8>, payload: &EntryPayload<Proposal, Membership>) {
    match payload {
        EntryPayload::Blank => out.push(BLANK),
        EntryPayload::Normal(proposal) => {
            out.push(NORMAL);
            out.extend_from_slice(proposal.encoded().payload());
        }
        EntryPayload::Membership(membership) => {
            out.push(MEMBERSHIP);
            put_membership(out, membership);
        }
    }
}

fn payload(reader: &mut Reader<'_>) -> Result<EntryPayload<Proposal, Membership>, DecodeError> {
    match reader.read::<u8>()? {
        BLANK => Ok(EntryPayload::Blank),
        NORMAL => {
            let command = reader.rest().to_vec();
            Ok(EntryPayload::Normal(Proposal::decoded(Encoded::decode(
                command,
            )?)))
        }
        MEMBERSHIP => membership(reader).map(EntryPayload::Membership),
        other => Err(DecodeError::Tag(other)),
    }
}

fn put_leader_id(out: &mut Vec<u8>, leader_id: &LeaderId) {
    out.extend_from_slice(&leader_id.term.to_le_bytes());
    out.extend_from_slice(&leader_id.node_id.get().to_le_bytes());
}

fn leader_id(reader: &mut Reader<'_>) -> Result<LeaderId, DecodeError> {
    Ok(LeaderId {
        term: reader.read::<u64>()?,
        node_id: node_id(reader)?,
    })
}

fn put_log_id(out: &mut Vec<u8>, log_id: &LogId) {
    put_leader_id(out, &log_id.leader_id);
    out.extend_from_slice(&log_id.index.to_le_bytes());
}

fn log_id(reader: &mut Reader<'_>) -> Result<LogId, DecodeError> {
    let leader_id = leader_id(reader)?;
    Ok(LogId::new(leader_id, reader.read::<u64>()?))
}

fn node_id(reader: &mut Reader<'_>) -> Result<NodeId, DecodeError> {
    reader.read::<u64>().map(NodeId::new)
}

/// Writes a count. Memberships and snapshots never hold `u32::MAX` items,
/// so the count saturates rather than fails.
fn put_len(out: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use kurator_codec::DecodeError;
    use kurator_storage::{Command, WriteBatch};
    use openraft::EntryPayload;
    use proptest::prelude::*;

    use super::Record;
    use crate::config::Entry;
    use crate::proposal::{ENTRY_OVERHEAD, Proposal};
    use crate::types::{LeaderId, LogId, Membership, NodeId, Vote};

    fn log_id(term: u64, node: u64, index: u64) -> LogId {
        LogId::new(
            LeaderId {
                term,
                node_id: NodeId::new(node),
            },
            index,
        )
    }

    fn membership(voters: &[u64], learners: &[u64]) -> Membership {
        let ids = |ids: &[u64]| {
            ids.iter()
                .copied()
                .map(NodeId::new)
                .collect::<BTreeSet<_>>()
        };
        Membership::new_with_defaults(vec![ids(voters)], ids(learners))
    }

    fn proposal(key: &[u8], value: &[u8]) -> Proposal {
        Proposal::new(Command::Kv(WriteBatch::new().put(key, value))).unwrap()
    }

    fn record() -> impl Strategy<Value = Record> {
        let id = || (0_u64..4, 0_u64..4, 0_u64..1000).prop_map(|(t, n, i)| log_id(t, n, i));
        let bytes = || proptest::collection::vec(any::<u8>(), 0..8);
        prop_oneof![
            (id(), bytes(), bytes()).prop_map(|(log_id, key, value)| Record::Entry(Entry {
                log_id,
                payload: EntryPayload::Normal(proposal(&key, &value)),
            })),
            id().prop_map(|log_id| Record::Entry(Entry {
                log_id,
                payload: EntryPayload::Blank
            })),
            (id(), proptest::collection::btree_set(0_u64..8, 1..4)).prop_map(|(log_id, ids)| {
                let voters: Vec<u64> = ids.into_iter().collect();
                Record::Entry(Entry {
                    log_id,
                    payload: EntryPayload::Membership(membership(&voters, &[9])),
                })
            }),
            proptest::option::of(id()).prop_map(Record::TruncateAfter),
            id().prop_map(Record::PurgeThrough),
            (0_u64..9, 0_u64..4, any::<bool>()).prop_map(|(term, node, committed)| {
                let mut vote = Vote::new(term, NodeId::new(node));
                vote.committed = committed;
                Record::Vote(vote)
            }),
            proptest::option::of(id()).prop_map(Record::Committed),
        ]
    }

    #[test]
    fn the_entry_overhead_is_what_an_entry_adds() {
        let proposal = proposal(b"k", b"v");
        let entry = Record::Entry(Entry {
            log_id: log_id(1, 2, 3),
            payload: EntryPayload::Normal(proposal.clone()),
        });
        assert_eq!(
            entry.encode().len(),
            proposal.encoded().payload().len() + ENTRY_OVERHEAD,
            "overhead"
        );
    }

    #[test]
    fn unknown_tags_are_rejected() {
        for (bytes, tag) in [
            (vec![9], 9),
            (vec![2, 7], 7),
            (
                vec![
                    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
                ],
                3,
            ),
        ] {
            assert_eq!(
                Record::decode(&bytes),
                Err(DecodeError::Tag(tag)),
                "{bytes:?}"
            );
        }
        let mut vote = Record::Vote(Vote::new(1, NodeId::new(1))).encode();
        *vote.last_mut().unwrap() = 2;
        assert_eq!(
            Record::decode(&vote),
            Err(DecodeError::Tag(2)),
            "committed flag"
        );
        let mut truncate = Record::TruncateAfter(None).encode();
        truncate.push(0);
        assert_eq!(
            Record::decode(&truncate),
            Err(DecodeError::Trailing),
            "trailing"
        );
    }

    proptest! {
        #[test]
        fn records_roundtrip(record in record()) {
            prop_assert_eq!(Record::decode(&record.encode()), Ok(record));
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            drop(Record::decode(&bytes));
        }
    }
}
