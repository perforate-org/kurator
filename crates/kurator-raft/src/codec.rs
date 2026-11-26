//! The records of the Raft log, as they are stored in the WAL, and the
//! values that snapshots and messages share with them.
//!
//! ```text
//! record    = tag u8 | body
//! entry     = 1 | log id | payload
//! truncate  = 2 | option(log id)          entries after it are gone
//! purge     = 3 | log id                  entries up to it are gone
//! vote      = 4 | vote
//! committed = 5 | option(log id)          the newest entry known committed
//!
//! vote      = term u64 | node u64 | committed flag
//! log id    = term u64 | node u64 | index u64
//! option(x) = 0 | 1 x
//! flag      = 0 | 1
//! payload   = 0 (blank) | 1 command | 2 membership
//! command   = the rest of the record, as `kurator_storage::Command::encode`
//!             wrote it
//! membership= configs u32 (voters u32 node u64*)* | nodes u32 (node u64 peer)*
//! peer      = addr | key [32]
//! addr      = 4 [4] port u16 | 6 [16] port u16
//! meta      = option(log id) | option(log id) | membership
//!             a snapshot's last entry, its membership's entry, and the
//!             membership
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use kurator_codec::{DecodeError, Reader};
use kurator_storage::Encoded;
use kurator_wal::VerifyingKey;
use openraft::EntryPayload;

use crate::config::Entry;
use crate::proposal::Proposal;
use crate::types::{
    LeaderId, LogId, Membership, NodeId, Peer, SnapshotMeta, StoredMembership, Vote,
};

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

const V4: u8 = 4;
const V6: u8 = 6;

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
                put_entry(&mut out, entry);
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
                put_vote(&mut out, vote);
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
            ENTRY => Self::Entry(entry(&mut reader)?),
            TRUNCATE => Self::TruncateAfter(option(&mut reader)?),
            PURGE => Self::PurgeThrough(log_id(&mut reader)?),
            VOTE => Self::Vote(vote(&mut reader)?),
            COMMITTED => Self::Committed(option(&mut reader)?),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(record)
    }
}

/// Encodes an entry. A command takes the rest of the output, so an entry
/// is always last: messages wrap each entry in `kurator_codec::put_bytes`.
pub(crate) fn put_entry(out: &mut Vec<u8>, entry: &Entry) {
    put_log_id(out, &entry.log_id);
    put_payload(out, &entry.payload);
}

/// Decodes what [`put_entry`] wrote, taking the rest of `reader`.
pub(crate) fn entry(reader: &mut Reader<'_>) -> Result<Entry, DecodeError> {
    let log_id = log_id(reader)?;
    let payload = payload(reader)?;
    Ok(Entry { log_id, payload })
}

/// Encodes a membership on its own, for snapshots and messages.
pub(crate) fn put_membership(out: &mut Vec<u8>, membership: &Membership) {
    let configs = membership.get_joint_config();
    put_len(out, configs.len());
    for voters in configs {
        put_len(out, voters.len());
        for voter in voters {
            put_node_id(out, *voter);
        }
    }
    let nodes: Vec<(&NodeId, &Peer)> = membership.nodes().collect();
    put_len(out, nodes.len());
    for (id, peer) in nodes {
        put_node_id(out, *id);
        put_peer(out, peer);
    }
}

/// Decodes what [`put_membership`] wrote.
///
/// # Errors
///
/// [`DecodeError::Invalid`] when a node appears twice or a voter has no
/// peer.
pub(crate) fn membership(reader: &mut Reader<'_>) -> Result<Membership, DecodeError> {
    let mut configs = Vec::new();
    for _ in 0..reader.read::<u32>()? {
        let mut voters = BTreeSet::new();
        for _ in 0..reader.read::<u32>()? {
            let _: bool = voters.insert(node_id(reader)?);
        }
        configs.push(voters);
    }
    let mut nodes = BTreeMap::new();
    for _ in 0..reader.read::<u32>()? {
        let id = node_id(reader)?;
        if nodes.insert(id, peer(reader)?).is_some() {
            return Err(DecodeError::Invalid);
        }
    }
    Membership::new(configs, nodes).map_err(|_| DecodeError::Invalid)
}

/// Encodes the metadata of a snapshot.
pub(crate) fn put_meta(out: &mut Vec<u8>, meta: &SnapshotMeta) {
    put_option(out, meta.last_log_id.as_ref());
    put_option(out, meta.last_membership.log_id().as_ref());
    put_membership(out, meta.last_membership.membership());
}

/// Decodes what [`put_meta`] wrote.
pub(crate) fn meta(reader: &mut Reader<'_>) -> Result<SnapshotMeta, DecodeError> {
    let last_log_id = option(reader)?;
    let membership_log_id = option(reader)?;
    let membership = membership(reader)?;
    Ok(SnapshotMeta {
        last_log_id,
        last_membership: StoredMembership::new(membership_log_id, membership),
    })
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

pub(crate) fn put_vote(out: &mut Vec<u8>, vote: &Vote) {
    put_leader_id(out, &vote.leader_id);
    out.push(u8::from(vote.committed));
}

pub(crate) fn vote(reader: &mut Reader<'_>) -> Result<Vote, DecodeError> {
    let leader_id = leader_id(reader)?;
    let committed = flag(reader)?;
    Ok(Vote {
        leader_id,
        committed,
    })
}

/// Decodes one of `OpenRaft`'s `bool` fields, written as `u8::from`.
pub(crate) fn flag(reader: &mut Reader<'_>) -> Result<bool, DecodeError> {
    match reader.read::<u8>()? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(DecodeError::Tag(other)),
    }
}

pub(crate) fn put_log_id(out: &mut Vec<u8>, log_id: &LogId) {
    put_leader_id(out, &log_id.leader_id);
    out.extend_from_slice(&log_id.index.to_le_bytes());
}

pub(crate) fn log_id(reader: &mut Reader<'_>) -> Result<LogId, DecodeError> {
    let leader_id = leader_id(reader)?;
    Ok(LogId::new(leader_id, reader.read::<u64>()?))
}

/// Writes a count. Memberships, batches and snapshots never hold
/// `u32::MAX` items, so the count saturates rather than fails.
pub(crate) fn put_len(out: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
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
    put_node_id(out, leader_id.node_id);
}

fn leader_id(reader: &mut Reader<'_>) -> Result<LeaderId, DecodeError> {
    Ok(LeaderId {
        term: reader.read::<u64>()?,
        node_id: node_id(reader)?,
    })
}

fn put_node_id(out: &mut Vec<u8>, id: NodeId) {
    out.extend_from_slice(&id.get().to_le_bytes());
}

fn node_id(reader: &mut Reader<'_>) -> Result<NodeId, DecodeError> {
    reader.read::<u64>().map(NodeId::new)
}

fn put_peer(out: &mut Vec<u8>, peer: &Peer) {
    match peer.addr() {
        SocketAddr::V4(addr) => {
            out.push(V4);
            out.extend_from_slice(&addr.ip().octets());
        }
        SocketAddr::V6(addr) => {
            out.push(V6);
            out.extend_from_slice(&addr.ip().octets());
        }
    }
    out.extend_from_slice(&peer.addr().port().to_le_bytes());
    out.extend_from_slice(&peer.key().to_bytes());
}

fn peer(reader: &mut Reader<'_>) -> Result<Peer, DecodeError> {
    let ip = match reader.read::<u8>()? {
        V4 => Ipv4Addr::from(reader.read::<[u8; 4]>()?).into(),
        V6 => Ipv6Addr::from(reader.read::<[u8; 16]>()?).into(),
        other => return Err(DecodeError::Tag(other)),
    };
    let port = reader.read::<u16>()?;
    let key = VerifyingKey::from_bytes(&reader.read::<[u8; 32]>()?).ok_or(DecodeError::Invalid)?;
    Ok(Peer::new(SocketAddr::new(ip, port), key))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{Ipv6Addr, SocketAddr};

    use kurator_codec::{DecodeError, Reader};
    use kurator_storage::{Command, WriteBatch};
    use openraft::EntryPayload;
    use proptest::prelude::*;

    use super::{Record, membership as decode_membership, put_membership};
    use crate::config::Entry;
    use crate::proposal::{ENTRY_OVERHEAD, Proposal};
    use crate::types::testing::{membership, peer};
    use crate::types::{LeaderId, LogId, Membership, NodeId, Peer, Vote};

    fn log_id(term: u64, node: u64, index: u64) -> LogId {
        LogId::new(
            LeaderId {
                term,
                node_id: NodeId::new(node),
            },
            index,
        )
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

    fn decoded(membership: &Membership) -> Result<Membership, DecodeError> {
        let mut out = Vec::new();
        put_membership(&mut out, membership);
        let mut reader = Reader::new(&out);
        let decoded = decode_membership(&mut reader)?;
        reader.finish()?;
        Ok(decoded)
    }

    #[test]
    fn memberships_keep_their_peers() {
        let v6 = Peer::new(
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 7001),
            peer(1).key().clone(),
        );
        let nodes = BTreeMap::from([(NodeId::new(1), v6), (NodeId::new(2), peer(2))]);
        let joint = Membership::new(
            vec![
                BTreeSet::from([NodeId::new(1)]),
                BTreeSet::from([NodeId::new(2)]),
            ],
            nodes,
        )
        .unwrap();
        assert_eq!(decoded(&joint), Ok(joint.clone()), "joint, IPv6");
        assert_eq!(
            decoded(&membership(&[1, 2], &[3])),
            Ok(membership(&[1, 2], &[3])),
            "learners"
        );
    }

    #[test]
    fn invalid_memberships_are_rejected() {
        let mut encoded = Vec::new();
        put_membership(&mut encoded, &membership(&[1], &[]));
        let key_at = encoded.len() - 32;
        // Not a point on the curve.
        let mut bad_key = encoded.clone();
        let mut off_curve = [0; 32];
        off_curve[0] = 2;
        bad_key[key_at..].copy_from_slice(&off_curve);
        // An address family that does not exist.
        let mut bad_family = encoded.clone();
        bad_family[key_at - 7] = 5;
        // A voter whose peer is missing: one config of one voter, no nodes.
        let orphan = [1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        // The same node twice.
        let mut twice = encoded[..12].to_vec();
        let node = &encoded[16..];
        twice.extend_from_slice(&2_u32.to_le_bytes());
        twice.extend_from_slice(node);
        twice.extend_from_slice(node);
        for (bytes, error, why) in [
            (bad_key, DecodeError::Invalid, "key"),
            (bad_family, DecodeError::Tag(5), "family"),
            (orphan.to_vec(), DecodeError::Invalid, "orphan voter"),
            (twice, DecodeError::Invalid, "duplicate node"),
        ] {
            let mut reader = Reader::new(&bytes);
            assert_eq!(decode_membership(&mut reader), Err(error), "{why}");
        }
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
