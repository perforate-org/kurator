//! Who may talk to a replica.
//!
//! The membership decides: a replica accepts requests from the nodes its
//! membership lists, by their transport keys, and from no one else. A
//! replica that has no membership yet, because it is new or the cluster
//! is not initialized, accepts the peers it was started with instead: the
//! operator vouches for them, as the registry does for a new ICP node.

use std::collections::BTreeMap;

use kurator_raft::{Inbox, NodeId};
use kurator_wal::VerifyingKey;
use parking_lot::RwLock;

use crate::Tokio;

/// The nodes a replica trusts, and how to tell them apart.
#[derive(Debug)]
pub(crate) struct Roster {
    /// Trusted until the replica knows a membership.
    bootstrap: BTreeMap<NodeId, VerifyingKey>,
    /// The replica, once it serves.
    inbox: RwLock<Option<Inbox<Tokio>>>,
}

impl Roster {
    pub(crate) const fn new(bootstrap: BTreeMap<NodeId, VerifyingKey>) -> Self {
        Self {
            bootstrap,
            inbox: RwLock::new(None),
        }
    }

    /// Follows the membership of the replica behind `inbox` from now on.
    pub(crate) fn follow(&self, inbox: Inbox<Tokio>) {
        *self.inbox.write() = Some(inbox);
    }

    /// The node that holds `key`, if it is trusted.
    pub(crate) fn identify(&self, key: &VerifyingKey) -> Option<NodeId> {
        let membership = self.inbox.read().as_ref().map(Inbox::membership);
        match membership {
            Some(membership) if membership.nodes().next().is_some() => membership
                .nodes()
                .find(|(_, peer)| peer.key() == key)
                .map(|(id, _)| *id),
            Some(_) | None => self
                .bootstrap
                .iter()
                .find(|(_, trusted)| *trusted == key)
                .map(|(id, _)| *id),
        }
    }
}
