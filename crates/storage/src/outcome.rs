//! What applying a command did, and why a version cannot be read.

use crate::across::{Deadline, TxnId};
use crate::version::Version;

/// What applying a command did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// Its writes applied at this version.
    Committed(Version),
    /// The shard prepared its part of a transaction across shards, at this
    /// version, and holds its writes back until it is decided.
    Prepared(Version),
    /// It wrote nothing.
    Aborted(Abort),
}

impl Outcome {
    /// The version the writes made, or the prepare applied at, or why
    /// there were none.
    ///
    /// # Errors
    ///
    /// The [`Abort`] when the command aborted.
    pub fn into_result(self) -> Result<Version, Abort> {
        match self {
            Self::Committed(version) | Self::Prepared(version) => Ok(version),
            Self::Aborted(abort) => Err(abort),
        }
    }
}

/// Why a transaction aborted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Abort {
    /// A command after the read version wrote a key the transaction read.
    /// Running it again, at a newer version, may succeed.
    #[error("{key:?} changed at {version}, after the transaction read it")]
    Conflict {
        /// The key.
        key: Vec<u8>,
        /// The version that wrote it.
        version: Version,
    },
    /// A transaction across shards that is prepared but not decided yet
    /// holds `key`: it may write it, or it read it. Running again once the
    /// transaction `by`, whose primary key is `primary`, is decided may
    /// succeed.
    #[error("{key:?} is locked by {by}, prepared but not decided yet")]
    Locked {
        /// The key.
        key: Vec<u8>,
        /// The transaction that holds it.
        by: TxnId,
        /// Its primary key, where it is decided.
        primary: Vec<u8>,
        /// Until when its coordinator may still be at work on it; past
        /// it, the transaction may be resolved at its primary.
        deadline: Deadline,
    },
    /// The transaction across shards was aborted: by its coordinator, or by
    /// whoever found it undecided.
    #[error("{0} was aborted")]
    RolledBack(TxnId),
    /// The read version is not readable any more, or not yet.
    #[error(transparent)]
    Unreadable(#[from] Unreadable),
}

/// Why a version cannot be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Unreadable {
    /// The version is behind the horizon of the retention: what it held is
    /// forgotten.
    #[error("{at} is forgotten: the oldest readable version is {horizon}")]
    Forgotten {
        /// The version asked for.
        at: Version,
        /// The oldest version still readable.
        horizon: Version,
    },
    /// The version is ahead of the newest one applied.
    #[error("{at} is not applied yet: the newest version is {newest}")]
    Ahead {
        /// The version asked for.
        at: Version,
        /// The newest version.
        newest: Version,
    },
}
