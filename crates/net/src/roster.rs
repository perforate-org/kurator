//! Who may talk to a node.
//!
//! The memberships decide: a node accepts requests from the nodes that
//! the membership of any of its replicas lists, by their transport keys,
//! and from no one else. A node that knows no membership yet, because it
//! is new or the cluster is not founded, accepts the peers it was started
//! with instead: the operator vouches for them, as the registry does for a
//! new ICP node.

use std::collections::BTreeMap;

use kurator_crypto::VerifyingKey;
use kurator_raft::{Memberships, NodeId};

use crate::Tokio;

/// The nodes a node trusts, and how to tell them apart.
#[derive(Debug)]
pub(crate) struct Roster {
    /// Trusted until the node knows a membership.
    bootstrap: BTreeMap<NodeId, VerifyingKey>,
    /// The memberships that decide.
    memberships: Memberships<Tokio>,
}

impl Roster {
    /// Trusts `memberships`, and `bootstrap` while there are none.
    pub(crate) const fn new(
        bootstrap: BTreeMap<NodeId, VerifyingKey>,
        memberships: Memberships<Tokio>,
    ) -> Self {
        Self {
            bootstrap,
            memberships,
        }
    }

    /// The node that holds `key`, if it is trusted.
    pub(crate) fn identify(&self, key: &VerifyingKey) -> Option<NodeId> {
        let members = self.memberships.nodes();
        if members.is_empty() {
            self.bootstrap
                .iter()
                .find(|(_, trusted)| *trusted == key)
                .map(|(id, _)| *id)
        } else {
            members
                .iter()
                .find(|(_, peer)| peer.key() == key)
                .map(|(id, _)| *id)
        }
    }
}
