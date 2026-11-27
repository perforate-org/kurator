//! The id of a transaction across shards.

use std::fmt;

use kurator_codec::{DecodeError, Reader};

/// A transaction across shards, named by its coordinator. Ids must not
/// repeat: a shard that decided a transaction treats any later command
/// with its id as that transaction's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(u128);

impl TxnId {
    /// The transaction `id`.
    #[must_use]
    pub const fn new(id: u128) -> Self {
        Self(id)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }

    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0.to_le_bytes());
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self(reader.read()?))
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{:x}", self.0)
    }
}
