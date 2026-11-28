//! What names a transaction across shards, where it is decided, and
//! until when its coordinator may be at work on it.

use std::{fmt, io};

use oleaph_codec::{DecodeError, Reader, put_bytes};

use crate::span::NamespaceId;

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
/// Where a transaction across shards is decided: its primary key, and the
/// namespace that key is a key of. A transaction may span namespaces, so
/// the key alone does not say which shard holds it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Primary {
    namespace: NamespaceId,
    key: Vec<u8>,
}

impl Primary {
    /// `key` of `namespace`.
    #[must_use]
    pub fn new(namespace: NamespaceId, key: impl Into<Vec<u8>>) -> Self {
        Self {
            namespace,
            key: key.into(),
        }
    }

    /// The namespace.
    #[must_use]
    pub const fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    /// The key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Appends `namespace u32 | key`, the key length prefixed.
    ///
    /// # Errors
    ///
    /// When the key is too long for its prefix.
    pub fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        out.extend_from_slice(&self.namespace.get().to_le_bytes());
        put_bytes(out, &self.key)
    }

    /// Reads a primary written by [`Primary::encode`].
    ///
    /// # Errors
    ///
    /// When the input is not one.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let namespace = NamespaceId::new(reader.read()?);
        Ok(Self::new(namespace, reader.bytes()?))
    }
}
