//! What names a transaction across shards, and until when its
//! coordinator may be at work on it.

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

/// Until when the coordinator of a transaction across shards may still be
/// at work on it.
///
/// It is in milliseconds on the clock coordinators share. Past it,
/// whoever finds the transaction's locks may resolve it at its primary.
///
/// Shards only keep a deadline and hand it to whoever runs into the lock:
/// applying entries never reads a clock, so every replica decides alike.
/// A clock that is off only resolves a transaction too early, which
/// aborts it, or too late, which keeps its keys locked longer; the
/// primary decides either way, so the outcome is the same everywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Deadline(u64);

impl Deadline {
    /// The moment `millis` milliseconds into the shared clock.
    #[must_use]
    pub const fn new(millis: u64) -> Self {
        Self(millis)
    }

    /// Milliseconds into the shared clock.
    #[must_use]
    pub const fn millis(self) -> u64 {
        self.0
    }

    /// Whether the deadline has passed at `now`.
    #[must_use]
    pub fn has_passed(self, now: Self) -> bool {
        now > self
    }

    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0.to_le_bytes());
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self(reader.read()?))
    }
}

impl fmt::Display for Deadline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}
