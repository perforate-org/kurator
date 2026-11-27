//! Who takes part in replicating a shard: node ids, and how to reach and
//! authenticate each node.

use std::fmt;
use std::net::SocketAddr;

use kurator_crypto::VerifyingKey;

/// Identifies a node within a cluster.
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

/// How to reach a replica and how to know it is the one answering: its
/// address and the public key of its transport identity.
///
/// A peer is part of the replicated membership, so every replica learns a
/// new member's address and key from the log entry that adds it, as ICP
/// nodes learn theirs from the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    addr: SocketAddr,
    key: VerifyingKey,
}

impl Peer {
    /// The replica at `addr` that proves it holds the secret half of `key`.
    #[must_use]
    pub const fn new(addr: SocketAddr, key: VerifyingKey) -> Self {
        Self { addr, key }
    }

    /// Where it listens.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The public key it authenticates with.
    #[must_use]
    pub const fn key(&self) -> &VerifyingKey {
        &self.key
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.addr, self.key)
    }
}
