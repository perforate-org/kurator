//! The `OpenRaft` type configuration a replica runs with, bound to an
//! async runtime by [`declare_config!`](crate::declare_config).

use openraft::RaftTypeConfig;
use openraft::impls::EmptyNode;

use crate::proposal::Proposal;
use crate::types::{LeaderId, Membership, NodeId, Vote};

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
