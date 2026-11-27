//! Transactions across shards: two-phase commit, as Percolator runs it.
//!
//! A transaction that touches several shards cannot commit in one log
//! entry, since each shard has a log of its own. It commits in two phases
//! instead, each phase a command in the log of every shard it touched:
//!
//! 1. **Prepare.** Each shard checks the part of the transaction it holds,
//!    as it checks a [`Transaction`]: nothing it read changed since its
//!    read version, and no other prepared transaction holds a key it
//!    touches. If so, the shard *prepares* it: it holds the writes back
//!    and locks every key the transaction read or writes, so that nothing
//!    can change them, and nothing can read what it writes, until the
//!    transaction is decided.
//! 2. **Decide.** Once every shard prepared, the *primary*, the shard that
//!    holds the primary key, is told to commit. That entry is the commit
//!    point: the transaction committed exactly when the primary applied
//!    it. The other shards are then told the same, and apply the writes
//!    they held back. When a shard refuses to prepare, every shard is told
//!    to abort.
//!
//! Every shard records each decision it applies, so a decision told twice
//! is told once, a prepare that arrives after its transaction was decided
//! locks nothing, and a commit can never be undone by a late abort: the
//! abort answers that the transaction committed. Whoever finds a lock left
//! behind, its coordinator gone, asks the primary how it was decided, and
//! aborts it there when it was not: the primary's log orders that abort
//! and the coordinator's commit, so they agree.
//!
//! Every shard held all the locks of the transaction at its commit point,
//! so its reads on every shard were still current then: committed
//! transactions are serializable, each at its commit point.

use kurator_codec::{DecodeError, Reader, put_bytes};

use crate::error::{Error, Result};
use crate::span::Span;
use crate::txn::Transaction;
use crate::txn_id::TxnId;
use crate::version::Version;

/// The first phase on one shard: its part of the transaction, to check and
/// lock. Its writes may be empty, on a shard the transaction only read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepare {
    id: TxnId,
    primary: Vec<u8>,
    transaction: Transaction,
}

impl Prepare {
    /// Prepares `transaction`, this shard's part of the transaction `id`,
    /// which commits when the shard that holds `primary` commits it.
    #[must_use]
    pub const fn new(id: TxnId, primary: Vec<u8>, transaction: Transaction) -> Self {
        Self {
            id,
            primary,
            transaction,
        }
    }

    /// The transaction it is part of.
    #[must_use]
    pub const fn id(&self) -> TxnId {
        self.id
    }

    /// The primary key.
    #[must_use]
    pub fn primary(&self) -> &[u8] {
        &self.primary
    }

    /// This shard's part.
    #[must_use]
    pub const fn transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub(crate) fn into_transaction(self) -> Transaction {
        self.transaction
    }

    /// Whether this shard's part is in `span`. The primary key may be
    /// anywhere.
    #[must_use]
    pub fn fits(&self, span: &Span) -> bool {
        self.transaction.fits(span)
    }

    /// Appends `id u128 | primary | transaction`, the primary length
    /// prefixed and the transaction as [`Transaction`] encodes it, its
    /// writes possibly none.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        self.id.encode(out);
        put_bytes(out, &self.primary).map_err(Error::too_large)?;
        self.transaction.encode(out)
    }

    /// Reads a prepare written by [`Prepare::encode`].
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let id = TxnId::decode(reader)?;
        let primary = reader.bytes()?.to_vec();
        Ok(Self::new(id, primary, Transaction::decode(reader)?))
    }
}

/// What the coordinator decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Decision {
    /// Apply the writes held back.
    Commit,
    /// Drop them.
    Abort,
}

/// The second phase on one shard: the decision on a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decide {
    id: TxnId,
    primary: Vec<u8>,
    decision: Decision,
}

const DECISION_COMMIT: u8 = 1;
const DECISION_ABORT: u8 = 2;

impl Decide {
    /// Tells a shard that the transaction `id`, whose primary key is
    /// `primary`, is decided as `decision`.
    #[must_use]
    pub const fn new(id: TxnId, primary: Vec<u8>, decision: Decision) -> Self {
        Self {
            id,
            primary,
            decision,
        }
    }

    /// The transaction.
    #[must_use]
    pub const fn id(&self) -> TxnId {
        self.id
    }

    /// Its primary key.
    #[must_use]
    pub fn primary(&self) -> &[u8] {
        &self.primary
    }

    /// The decision.
    #[must_use]
    pub const fn decision(&self) -> Decision {
        self.decision
    }

    /// Appends `id u128 | primary | decision u8`: commit 1, abort 2.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        self.id.encode(out);
        put_bytes(out, &self.primary).map_err(Error::too_large)?;
        out.push(match self.decision {
            Decision::Commit => DECISION_COMMIT,
            Decision::Abort => DECISION_ABORT,
        });
        Ok(())
    }

    /// Reads a decision written by [`Decide::encode`].
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let id = TxnId::decode(reader)?;
        let primary = reader.bytes()?.to_vec();
        let decision = match reader.read::<u8>()? {
            DECISION_COMMIT => Decision::Commit,
            DECISION_ABORT => Decision::Abort,
            other => return Err(DecodeError::Tag(other)),
        };
        Ok(Self::new(id, primary, decision))
    }
}

/// How a shard decided a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Decided {
    /// Its writes on the shard applied at this version.
    Committed(Version),
    /// It wrote nothing on the shard.
    Aborted,
}

#[cfg(kani)]
mod proofs {
    use super::Decide;
    use kurator_codec::Reader;

    /// Decoding a decision never panics, and a decoded decision encodes
    /// back to exactly the bytes it was read from.
    #[kani::proof]
    #[kani::unwind(26)]
    fn decisions_decode_totally_and_roundtrip() {
        let buf: [u8; 24] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let mut reader = Reader::new(&buf[..len]);
        if let Ok(decoded) = Decide::decode(&mut reader) {
            let read = len - reader.rest().len();
            let mut out = Vec::new();
            assert!(decoded.encode(&mut out).is_ok(), "small key");
            assert_eq!(out.as_slice(), &buf[..read], "roundtrip");
        }
    }
}
