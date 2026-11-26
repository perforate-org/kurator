//! The types a replica is built from: ids, log positions, votes and
//! memberships.

use std::fmt;
use std::net::SocketAddr;

use kurator_wal::VerifyingKey;
use openraft::impls::leader_id_adv;

/// Identifies a replica within a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct NodeId(u64);

impl NodeId {
    /// Wraps a raw id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// How to reach a replica and how to know it is the one answering: its
/// address and the public key of its transport identity.
///
/// A peer is part of the replicated membership, so every replica learns a
/// new member's address and key from the log entry that adds it, as ICP
/// nodes learn theirs from the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    addr: SocketAddr,
    key: VerifyingKey,
}

impl Peer {
    /// The replica at `addr` that proves it holds the secret half of `key`.
    #[must_use]
    pub const fn new(addr: SocketAddr, key: VerifyingKey) -> Self {
        Self { addr, key }
    }

    /// Where it listens.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The public key it authenticates with.
    #[must_use]
    pub const fn key(&self) -> &VerifyingKey {
        &self.key
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.addr, self.key)
    }
}

/// How far a learner is from joining the quorum, as the leader sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Progress {
    /// This replica is not the leader, so it does not know.
    Unknown,
    /// The node is not in the membership.
    Stranger,
    /// The node already votes.
    Voter,
    /// The learner holds less than the leader has committed.
    Behind {
        /// The last entry the learner holds.
        matched: Option<LogId>,
        /// The last entry the leader committed.
        committed: Option<LogId>,
    },
    /// The learner holds everything the leader committed: it can vote
    /// without holding back commits.
    CaughtUp,
}

/// The leader id of a term: the term and the node that leads it.
pub type LeaderId = leader_id_adv::LeaderId<u64, NodeId>;
/// A position in the Raft log: the leader that wrote an entry, and its
/// index.
pub type LogId = openraft::LogId<LeaderId>;
/// A replica's vote: the leader it accepts, and whether a quorum granted
/// it.
pub type Vote = openraft::impls::Vote<LeaderId>;
/// Voters and learners, each with the [`Peer`] it is reached at.
pub type Membership = openraft::Membership<NodeId, Peer>;
/// A membership and the log position that set it.
pub type StoredMembership = openraft::StoredMembership<LeaderId, Membership>;
/// What a snapshot holds besides the state.
pub type SnapshotMeta = openraft::SnapshotMeta<LeaderId, Membership>;

/// Peers and memberships for tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{Ipv4Addr, SocketAddr};

    use super::{Membership, NodeId, Peer};
    use crate::identity::{KeyPurpose, NodeSecret};

    /// Replica `id`'s peer: a loopback address and a key from its id.
    pub(crate) fn peer(id: u64) -> Peer {
        let port = u16::try_from(id).unwrap_or(u16::MAX);
        let seed = [u8::try_from(id).unwrap_or(u8::MAX); 32];
        Peer::new(
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port),
            NodeSecret::from_seed(seed).public_key(KeyPurpose::Transport),
        )
    }

    /// `voters` and `learners`, each with its [`peer`].
    pub(crate) fn membership(voters: &[u64], learners: &[u64]) -> Membership {
        let voter_ids: BTreeSet<NodeId> = voters.iter().copied().map(NodeId::new).collect();
        let nodes: BTreeMap<NodeId, Peer> = voters
            .iter()
            .chain(learners)
            .map(|id| (NodeId::new(*id), peer(*id)))
            .collect();
        Membership::new(vec![voter_ids], nodes).unwrap_or_default()
    }
}
