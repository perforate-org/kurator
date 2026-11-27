//! Who may talk to a node.
//!
//! The memberships decide: a node accepts requests from the nodes that
//! the membership of any of its replicas lists, by their transport keys,
//! and from no one else. A node that knows no membership yet, because it
//! is new or the cluster is not founded, accepts the peers it was started
//! with instead: the operator vouches for them, as the registry does for a
//! new ICP node.
//!
//! Clients are not in any membership. A node accepts the client keys it
//! was started with, and serves them only as clients.

use std::collections::BTreeMap;

use kurator_crypto::VerifyingKey;
use kurator_raft::{Memberships, NodeId};

use crate::Tokio;

/// The nodes and clients a node trusts, and how to tell them apart.
#[derive(Debug)]
pub(crate) struct Roster {
    /// Trusted until the node knows a membership.
    bootstrap: BTreeMap<NodeId, VerifyingKey>,
    /// The memberships that decide.
    memberships: Memberships<Tokio>,
    /// The clients the node serves.
    clients: Vec<VerifyingKey>,
}

/// Who a trusted key belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Caller {
    /// A node: it may send Raft messages and calls on the meta shard as
    /// well as client requests.
    Node(NodeId),
    /// A client: it may send client requests only.
    Client,
}

impl Roster {
    /// Trusts `memberships`, `bootstrap` while there are none, and
    /// `clients` as clients.
    pub(crate) const fn new(
        bootstrap: BTreeMap<NodeId, VerifyingKey>,
        memberships: Memberships<Tokio>,
        clients: Vec<VerifyingKey>,
    ) -> Self {
        Self {
            bootstrap,
            memberships,
            clients,
        }
    }

    /// Who holds `key`, if it is trusted. A key that is a node's as well
    /// as a client's is the node's.
    pub(crate) fn identify(&self, key: &VerifyingKey) -> Option<Caller> {
        self.node(key)
            .map(Caller::Node)
            .or_else(|| self.clients.contains(key).then_some(Caller::Client))
    }

    /// The node that holds `key`, if it is trusted.
    fn node(&self, key: &VerifyingKey) -> Option<NodeId> {
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
