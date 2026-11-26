//! The types a replica is built from, bound into an `OpenRaft` type
//! configuration by [`declare_config!`](crate::declare_config).

use std::fmt;

use kurator_storage::{Command, Encoded};
use openraft::RaftTypeConfig;
use openraft::impls::{EmptyNode, leader_id_adv};

use crate::codec::ENTRY_OVERHEAD;
use crate::error::{Error, Result};
use crate::snapshot::SnapshotBytes;

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

/// A command on its way through the Raft log: the application data of an
/// entry.
///
/// Only [`Proposal::new`] and the log decoder make one, so a proposal
/// always fits one log record together with its entry header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal(Encoded);

impl Proposal {
    /// Encodes `command` for the log.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] when the command is empty or too large to encode,
    /// and [`Error::ProposalTooLarge`] when it does not fit one log record
    /// together with its entry header.
    pub fn new(command: Command) -> Result<Self> {
        let encoded = command.encode()?;
        let len = encoded.payload().len();
        if len.saturating_add(ENTRY_OVERHEAD) > kurator_wal::MAX_PAYLOAD_LEN {
            return Err(Error::ProposalTooLarge { len });
        }
        Ok(Self(encoded))
    }

    /// Wraps a payload read back from the log.
    pub(crate) const fn decoded(encoded: Encoded) -> Self {
        Self(encoded)
    }

    /// The encoded command.
    #[must_use]
    pub const fn encoded(&self) -> &Encoded {
        &self.0
    }

    /// Consumes the proposal, keeping the command.
    #[must_use]
    pub fn into_command(self) -> Command {
        self.0.into_command()
    }
}

impl fmt::Display for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "proposal of {} bytes", self.0.payload().len())
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
/// What an entry carries.
pub type Payload = openraft::EntryPayload<Proposal, Membership>;
/// A Raft log entry.
pub type Entry = openraft::Entry<LeaderId, Payload>;
/// What a snapshot holds besides the state.
pub type SnapshotMeta = openraft::SnapshotMeta<LeaderId, Membership>;
/// A snapshot with its metadata, as Raft hands it around.
pub type Snapshot = openraft::Snapshot<LeaderId, Membership, SnapshotBytes>;

/// The `OpenRaft` type configurations a replica works with: every type is
/// fixed except the async runtime, so the same replica runs on Tokio in
/// production and on a deterministic simulator in tests.
///
/// Declare one with [`declare_config!`](crate::declare_config).
pub trait Config:
    RaftTypeConfig<
        D = Proposal,
        R = (),
        NodeId = NodeId,
        Node = EmptyNode,
        Term = u64,
        LeaderId = LeaderId,
        Vote = Vote,
        Membership = Membership,
        Payload = Payload,
        Entry = Entry,
    >
{
}

impl<C> Config for C where
    C: RaftTypeConfig<
            D = Proposal,
            R = (),
            NodeId = NodeId,
            Node = EmptyNode,
            Term = u64,
            LeaderId = LeaderId,
            Vote = Vote,
            Membership = Membership,
            Payload = Payload,
            Entry = Entry,
        >
{
}

/// Declares a [`Config`] that runs on `$runtime`.
///
/// ```
/// kurator_raft::declare_config!(
///     /// Replicas on the deterministic simulator.
///     pub Simulated, openraft_rt_sim::SimRuntime
/// );
/// fn assert_config<C: kurator_raft::Config>() {}
/// assert_config::<Simulated>();
/// ```
#[macro_export]
macro_rules! declare_config {
    ($(#[$meta:meta])* $vis:vis $name:ident, $runtime:ty) => {
        $crate::openraft::declare_raft_types!(
            $(#[$meta])*
            #[non_exhaustive]
            $vis $name:
                D = $crate::Proposal,
                R = (),
                NodeId = $crate::NodeId,
                Node = $crate::openraft::impls::EmptyNode,
                Term = u64,
                LeaderId = $crate::LeaderId,
                Vote = $crate::Vote,
                Membership = $crate::Membership,
                Payload = $crate::Payload,
                Entry = $crate::Entry,
                AsyncRuntime = $runtime,
        );
    };
}
