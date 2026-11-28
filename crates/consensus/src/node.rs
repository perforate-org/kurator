//! Who takes part in replicating a shard: node ids, and how to reach and
//! authenticate each node.

use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use oleaph_codec::{DecodeError, Reader};
use oleaph_crypto::{Algorithm, VerifyingKey};

const V4: u8 = 4;
const V6: u8 = 6;

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

    /// Appends `addr | key`, where `addr` is `4 [4] port u16` or
    /// `6 [16] port u16` and `key` is `algorithm u8 | len u32 | public key`,
    /// the algorithm as [`Algorithm::id`] names it.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self.addr {
            SocketAddr::V4(addr) => {
                out.push(V4);
                out.extend_from_slice(&addr.ip().octets());
            }
            SocketAddr::V6(addr) => {
                out.push(V6);
                out.extend_from_slice(&addr.ip().octets());
            }
        }
        out.extend_from_slice(&self.addr.port().to_le_bytes());
        out.push(self.key.algorithm().id());
        // A public key is a few kilobytes at most.
        let len = u32::try_from(self.key.as_bytes().len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(self.key.as_bytes());
    }

    /// Reads a peer written by [`Peer::encode`].
    ///
    /// # Errors
    ///
    /// When the input is not one, or its key is not a valid key of its
    /// algorithm.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let ip = match reader.read::<u8>()? {
            V4 => Ipv4Addr::from(reader.read::<[u8; 4]>()?).into(),
            V6 => Ipv6Addr::from(reader.read::<[u8; 16]>()?).into(),
            other => return Err(DecodeError::Tag(other)),
        };
        let port = reader.read::<u16>()?;
        let id = reader.read::<u8>()?;
        let algorithm = Algorithm::from_id(id).ok_or(DecodeError::Tag(id))?;
        let key =
            VerifyingKey::from_parts(algorithm, reader.bytes()?).ok_or(DecodeError::Invalid)?;
        Ok(Self::new(SocketAddr::new(ip, port), key))
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.addr, self.key)
    }
}
