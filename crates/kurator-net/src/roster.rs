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

use crate::Tokio;

/// The nodes a replica trusts, and how to tell them apart.
#[derive(Debug)]
pub(crate) struct Roster {
    /// Trusted until the replica knows a membership.
    bootstrap: BTreeMap<NodeId, VerifyingKey>,
    /// The replica whose membership decides.
    inbox: Inbox<Tokio>,
}

impl Roster {
    /// Trusts the membership of the replica behind `inbox`, and `bootstrap`
    /// while it has none.
    pub(crate) const fn new(
        bootstrap: BTreeMap<NodeId, VerifyingKey>,
        inbox: Inbox<Tokio>,
    ) -> Self {
        Self { bootstrap, inbox }
    }

    /// The node that holds `key`, if it is trusted.
    pub(crate) fn identify(&self, key: &VerifyingKey) -> Option<NodeId> {
        let membership = self.inbox.membership();
        if membership.nodes().next().is_some() {
            membership
                .nodes()
                .find(|(_, peer)| peer.key() == key)
                .map(|(id, _)| *id)
        } else {
            self.bootstrap
                .iter()
                .find(|(_, trusted)| *trusted == key)
                .map(|(id, _)| *id)
        }
    }
}
