//! The types a replica is built from: ids, log positions, votes and
//! memberships.

use std::fmt;

use openraft::impls::{EmptyNode, leader_id_adv};

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

/// The leader id of a term: the term and the node that leads it.
pub type LeaderId = leader_id_adv::LeaderId<u64, NodeId>;
/// A position in the Raft log: the leader that wrote an entry, and its
/// index.
pub type LogId = openraft::LogId<LeaderId>;
/// A replica's vote: the leader it accepts, and whether a quorum granted
/// it.
pub type Vote = openraft::impls::Vote<LeaderId>;
/// Voters and learners. Nodes carry no data yet: a transport adds
/// addresses, and the federation's registry adds public keys.
pub type Membership = openraft::Membership<NodeId, EmptyNode>;
/// A membership and the log position that set it.
pub type StoredMembership = openraft::StoredMembership<LeaderId, Membership>;
/// What a snapshot holds besides the state.
pub type SnapshotMeta = openraft::SnapshotMeta<LeaderId, Membership>;
