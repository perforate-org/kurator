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
//! behind past its [`Deadline`](crate::Deadline), its coordinator gone,
//! asks the primary how it was decided, and aborts it there when it was
//! not: the primary's log orders that abort and the coordinator's commit,
//! so they agree.
//!
//! Decisions are kept until a [`Collect`] forgets them, in two steps over
//! every shard: first a fence, after which a shard holds no lock and
//! takes no part in any transaction whose deadline is before the fence,
//! then forgetting the decisions of those transactions. Once every shard
//! is fenced, no one can come to ask how they were decided.
//!
//! Every shard held all the locks of the transaction at its commit point,
//! so its reads on every shard were still current then: committed
//! transactions are serializable, each at its commit point. Between the
//! commit point and the decision reaching a shard, that shard holds the
//! part locked, so a transaction on it alone that read a key the part
//! writes must not commit either: a logged one aborts as it applies, and
//! one that only read is checked the same way by
//! [`KvState::confirm`](crate::KvState::confirm).

use oleaph_codec::{DecodeError, Reader, put_bytes};

use crate::across::{Deadline, TxnId};
use crate::error::{Error, Result};
use crate::span::Span;
use crate::txn::Transaction;
use crate::version::Version;

/// The first phase on one shard: its part of the transaction, to check and
/// lock. Its writes may be empty, on a shard the transaction only read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepare {
    id: TxnId,
    primary: Vec<u8>,
    deadline: Deadline,
    transaction: Transaction,
}

impl Prepare {
    /// Prepares `transaction`, this shard's part of the transaction `id`,
    /// which commits when the shard that holds `primary` commits it. Its
    /// coordinator means to decide it before `deadline`.
    #[must_use]
    pub const fn new(
        id: TxnId,
        primary: Vec<u8>,
        deadline: Deadline,
        transaction: Transaction,
    ) -> Self {
        Self {
            id,
            primary,
            deadline,
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

    /// Until when its coordinator may still be at work on it.
    #[must_use]
    pub const fn deadline(&self) -> Deadline {
        self.deadline
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

    /// Appends `id u128 | primary | deadline u64 | transaction`, the
    /// primary length prefixed and the transaction as [`Transaction`]
    /// encodes it, its writes possibly none.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        self.id.encode(out);
        put_bytes(out, &self.primary).map_err(Error::too_large)?;
        self.deadline.encode(out);
        self.transaction.encode(out)
    }

    /// Reads a prepare written by [`Prepare::encode`].
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let id = TxnId::decode(reader)?;
        let primary = reader.bytes()?.to_vec();
        let deadline = Deadline::decode(reader)?;
        Ok(Self::new(
            id,
            primary,
            deadline,
            Transaction::decode(reader)?,
        ))
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
    deadline: Deadline,
    decision: Decision,
}

const DECISION_COMMIT: u8 = 1;
const DECISION_ABORT: u8 = 2;

impl Decide {
    /// Tells a shard that the transaction `id`, whose primary key is
    /// `primary` and whose prepares carry `deadline`, is decided as
    /// `decision`.
    #[must_use]
    pub const fn new(id: TxnId, primary: Vec<u8>, deadline: Deadline, decision: Decision) -> Self {
        Self {
            id,
            primary,
            deadline,
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

    /// The deadline its prepares carry, which tells when its decision
    /// may be forgotten.
    #[must_use]
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// The decision.
    #[must_use]
    pub const fn decision(&self) -> Decision {
        self.decision
    }

    /// Appends `id u128 | primary | deadline u64 | decision u8`: commit 1,
    /// abort 2.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        self.id.encode(out);
        put_bytes(out, &self.primary).map_err(Error::too_large)?;
        self.deadline.encode(out);
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
        let deadline = Deadline::decode(reader)?;
        let decision = match reader.read::<u8>()? {
            DECISION_COMMIT => Decision::Commit,
            DECISION_ABORT => Decision::Abort,
            other => return Err(DecodeError::Tag(other)),
        };
        Ok(Self::new(id, primary, deadline, decision))
    }
}

/// A step of forgetting decisions: the same step, at the same horizon, is
/// told every shard before the next step is told any.
///
/// 1. [`Collect::Fence`] makes a shard refuse, from then on, every prepare
///    and every decision it does not know yet of a transaction whose
///    deadline is before the horizon. It fails with
///    [`Abort::Held`](crate::Abort::Held) while the shard holds such a
///    transaction prepared: decide it first, at its primary.
/// 2. [`Collect::Forget`] drops the decisions of the transactions whose
///    deadline is before the horizon, and before the shard's fence.
///
/// Once every shard is fenced, none holds a lock of those transactions
/// or ever will, so no one comes to ask their primaries how they were
/// decided, and a late prepare or decision of one changes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Collect {
    /// Take no part in the transactions whose deadline is before this.
    Fence(Deadline),
    /// Forget the decisions on the transactions whose deadline is before
    /// this.
    Forget(Deadline),
}

const COLLECT_FENCE: u8 = 1;
const COLLECT_FORGET: u8 = 2;

impl Collect {
    /// Appends `step u8 | horizon u64`: fence 1, forget 2.
    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        let (step, horizon) = match self {
            Self::Fence(horizon) => (COLLECT_FENCE, horizon),
            Self::Forget(horizon) => (COLLECT_FORGET, horizon),
        };
        out.push(step);
        horizon.encode(out);
    }

    /// Reads a step written by [`Collect::encode`].
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let step = reader.read::<u8>()?;
        let horizon = Deadline::decode(reader)?;
        match step {
            COLLECT_FENCE => Ok(Self::Fence(horizon)),
            COLLECT_FORGET => Ok(Self::Forget(horizon)),
            other => Err(DecodeError::Tag(other)),
        }
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
    use super::{Collect, Decide};
    use oleaph_codec::Reader;

    /// Decoding a decision never panics, and a decoded decision encodes
    /// back to exactly the bytes it was read from.
    #[kani::proof]
    #[kani::unwind(34)]
    fn decisions_decode_totally_and_roundtrip() {
        let buf: [u8; 32] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let mut reader = Reader::new(&buf[..len]);
        if let Ok(decoded) = Decide::decode(&mut reader) {
            let read = len - reader.rest().len();
            let mut out = Vec::new();
            assert!(decoded.encode(&mut out).is_ok(), "small key");
            assert_eq!(out.as_slice(), &buf[..read], "roundtrip");
        }
    }

    /// Decoding a step of collecting never panics, and a decoded step
    /// encodes back to exactly the bytes it was read from.
    #[kani::proof]
    #[kani::unwind(12)]
    fn collect_steps_decode_totally_and_roundtrip() {
        let buf: [u8; 10] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let mut reader = Reader::new(&buf[..len]);
        if let Ok(decoded) = Collect::decode(&mut reader) {
            let read = len - reader.rest().len();
            let mut out = Vec::new();
            decoded.encode(&mut out);
            assert_eq!(out.as_slice(), &buf[..read], "roundtrip");
        }
    }
}
