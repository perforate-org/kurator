//! Who may talk to a node.
//!
//! The memberships decide: a node accepts requests from the nodes that
//! the membership of any of its replicas lists, by their transport keys,
//! and from no one else. A node that knows no membership yet, because it
//! is new or the cluster is not founded, accepts the peers it was started
//! with instead: the operator vouches for them, as the registry does for a
//! new ICP node.
//!
//! The members of the committees of its BFT shards are trusted too, as
//! members only: they may take part in those shards' consensus, not in
//! Raft's, unless a membership lists them as well. They are looked up as
//! each caller arrives, since a committee change brings new members.
//!
//! Clients are not in any membership. A node accepts the client keys it
//! was started with, and serves them only as clients.

use std::collections::BTreeMap;
use std::fmt::Debug;

use oleaph_crypto::VerifyingKey;
use oleaph_raft::{Memberships, NodeId};

use crate::Tokio;

/// The nodes and clients a node trusts, and how to tell them apart.
#[derive(Debug)]
pub(crate) struct Roster {
    /// Trusted until the node knows a membership.
    bootstrap: BTreeMap<NodeId, VerifyingKey>,
    /// The memberships that decide.
    memberships: Memberships<Tokio>,
    /// The members of the committees of the node's BFT shards, if it may
    /// hold any.
    committees: Option<Box<dyn Members>>,
    /// The clients the node serves.
    clients: Vec<VerifyingKey>,
}

/// Who a trusted key belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Caller {
    /// A node: it may send Raft messages and calls on the meta shard as
    /// well as client requests.
    Node(NodeId),
    /// A member of a BFT shard's committee that is not a node: it may send
    /// that consensus's messages as well as client requests.
    Member(NodeId),
    /// A client: it may send client requests only.
    Client,
}

impl Roster {
    /// Trusts `memberships`, `bootstrap` while there are none, the
    /// `committees` of BFT shards, and `clients` as clients.
    pub(crate) const fn new(
        bootstrap: BTreeMap<NodeId, VerifyingKey>,
        memberships: Memberships<Tokio>,
        committees: Option<Box<dyn Members>>,
        clients: Vec<VerifyingKey>,
    ) -> Self {
        Self {
            bootstrap,
            memberships,
            committees,
            clients,
        }
    }

    /// Who holds `key`, if it is trusted. A key that is a node's as well
    /// as a member's or a client's is the node's, and a member's before a
    /// client's.
    pub(crate) fn identify(&self, key: &VerifyingKey) -> Option<Caller> {
        self.node(key)
            .map(Caller::Node)
            .or_else(|| {
                let committees = self.committees.as_ref()?;
                holder(&committees.keys(), key).map(Caller::Member)
            })
            .or_else(|| self.clients.contains(key).then_some(Caller::Client))
    }

    /// The node that holds `key`, if it is trusted.
    fn node(&self, key: &VerifyingKey) -> Option<NodeId> {
        let members = self.memberships.nodes();
        if members.is_empty() {
            holder(&self.bootstrap, key)
        } else {
            members
                .iter()
                .find(|(_, peer)| peer.key() == key)
                .map(|(id, _)| *id)
        }
    }
}

/// Those a node trusts as members of its BFT shards' committees.
pub(crate) trait Members: Debug + Send + Sync {
    /// The transport key of each, as it stands now.
    fn keys(&self) -> BTreeMap<NodeId, VerifyingKey>;
}

/// The node `keys` lists with `key`.
fn holder(keys: &BTreeMap<NodeId, VerifyingKey>, key: &VerifyingKey) -> Option<NodeId> {
    keys.iter()
        .find(|(_, trusted)| *trusted == key)
        .map(|(id, _)| *id)
}
