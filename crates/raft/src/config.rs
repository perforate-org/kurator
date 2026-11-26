//! The `OpenRaft` type configuration a replica runs with, bound to an
//! async runtime by [`declare_config!`](crate::declare_config).

use openraft::RaftTypeConfig;
use openraft::impls::ChangeMembers;

use kurator_shard::Applied;

use crate::proposal::Proposal;
use crate::types::{LeaderId, Membership, NodeId, Peer, Vote};

/// How a membership change is described.
pub type MembershipEdit = ChangeMembers<NodeId, Peer>;

/// What an entry carries.
pub type Payload = openraft::EntryPayload<Proposal, Membership>;
/// A Raft log entry.
pub type Entry = openraft::Entry<LeaderId, Payload>;
/// The `OpenRaft` type configurations a replica works with: every type is
/// fixed except the async runtime, so the same replica runs on Tokio in
/// production and on a deterministic simulator in tests.
///
/// Declare one with [`declare_config!`](crate::declare_config).
pub trait Config:
    RaftTypeConfig<
        D = Proposal,
        R = Applied,
        NodeId = NodeId,
        Node = Peer,
        Term = u64,
        LeaderId = LeaderId,
        Vote = Vote,
        Membership = Membership,
        MembershipEdit = MembershipEdit,
        Payload = Payload,
        Entry = Entry,
    >
{
}

impl<C> Config for C where
    C: RaftTypeConfig<
            D = Proposal,
            R = Applied,
            NodeId = NodeId,
            Node = Peer,
            Term = u64,
            LeaderId = LeaderId,
            Vote = Vote,
            Membership = Membership,
            MembershipEdit = MembershipEdit,
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
                R = $crate::Applied,
                NodeId = $crate::NodeId,
                Node = $crate::Peer,
                Term = u64,
                LeaderId = $crate::LeaderId,
                Vote = $crate::Vote,
                Membership = $crate::Membership,
                MembershipEdit = $crate::MembershipEdit,
                Payload = $crate::Payload,
                Entry = $crate::Entry,
                AsyncRuntime = $runtime,
        );
    };
}
