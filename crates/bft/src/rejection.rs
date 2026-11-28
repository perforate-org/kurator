//! Why a replica ignored a message.

use oleaph_consensus::NodeId;

use crate::view::View;

/// Why a replica ignored what it was sent.
///
/// Faulty replicas send anything, so a rejection is not an error: the
/// replica drops the message and goes on. It says why, so tests and operators can tell a bug from an attack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Rejection {
    /// Signed by a node that is not a member.
    #[error("{0} is not a member")]
    Stranger(NodeId),
    /// A signature that is not its signer's.
    #[error("a signature claimed by {0} is not theirs")]
    Forged(NodeId),
    /// A certificate with fewer signatures than a quorum.
    #[error("{have} signatures where {need} are needed")]
    Short {
        /// Signatures in the certificate.
        have: usize,
        /// Signatures in a quorum.
        need: usize,
    },
    /// A genesis certificate for a block that is not the genesis block.
    #[error("a genesis certificate for another block")]
    NotGenesis,
    /// A proposal from a node that does not lead its view.
    #[error("{0} does not lead {1}")]
    NotLeader(NodeId, View),
    /// About a view this replica has left, or not reached.
    #[error("about {0}, while this replica is in {1}")]
    OtherView(View, View),
    /// A proposal that follows neither a certificate for the view before
    /// it nor a timeout certificate for that view that its certificate is
    /// at least as new as every signer's.
    #[error("a proposal in {0} that does not follow from the view before")]
    Unjustified(View),
    /// A proposal that extends a block older than this replica's lock: a
    /// vote for it could undo a commit.
    #[error("a proposal on {0}, older than the lock at {1}")]
    Unsafe(View, View),
    /// A second proposal or vote in a view this replica already voted or
    /// gave up in.
    #[error("already voted in {0}")]
    Voted(View),
    /// A vote sent to a replica that does not lead the next view.
    #[error("a vote in {0} for a replica that does not lead the next view")]
    Misdirected(View),
    /// A block nobody asked for.
    #[error("a block nobody asked for")]
    Unwanted,
}
