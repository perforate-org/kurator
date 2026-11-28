//! What applying a command did, and why a version cannot be read.
//!
//! An outcome travels to clients, so it has an encoding:
//!
//! ```text
//! outcome = 1 committed version | 2 prepared version | 3 aborted abort
//! abort   = 1 conflict bytes(key) version
//!         | 2 locked bytes(key) txn bytes(primary) deadline
//!         | 3 rolled-back txn | 4 fenced txn
//!         | 5 held txn bytes(primary) deadline
//!         | 6 unreadable unreadable
//! unreadable = 1 forgotten version version | 2 ahead version version
//! version = u64, txn = u128, deadline = u64
//! ```

use std::io;

use oleaph_codec::{DecodeError, Reader, put_bytes};

use crate::across::{Deadline, TxnId};
use crate::version::Version;

const COMMITTED: u8 = 1;
const PREPARED: u8 = 2;
const ABORTED: u8 = 3;

const CONFLICT: u8 = 1;
const LOCKED: u8 = 2;
const ROLLED_BACK: u8 = 3;
const FENCED: u8 = 4;
const HELD: u8 = 5;
const UNREADABLE: u8 = 6;

const FORGOTTEN: u8 = 1;
const AHEAD: u8 = 2;

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
    /// Appends the encoding in the module docs.
    ///
    /// # Errors
    ///
    /// When a key is too long to encode.
    pub fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        match self {
            Self::Committed(version) => put_version(out, COMMITTED, *version),
            Self::Prepared(version) => put_version(out, PREPARED, *version),
            Self::Aborted(abort) => {
                out.push(ABORTED);
                abort.encode(out)?;
            }
        }
        Ok(())
    }

    /// Reads an outcome written by [`Outcome::encode`].
    ///
    /// # Errors
    ///
    /// When the input is not one.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match reader.read::<u8>()? {
            COMMITTED => Ok(Self::Committed(read_version(reader)?)),
            PREPARED => Ok(Self::Prepared(read_version(reader)?)),
            ABORTED => Ok(Self::Aborted(Abort::decode(reader)?)),
            other => Err(DecodeError::Tag(other)),
        }
    }

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
    /// The transaction's deadline is before the shard's fence: its
    /// decision may be forgotten, so the shard takes no part in it any
    /// more.
    #[error("{0} is past the fence")]
    Fenced(TxnId),
    /// A fence found the transaction `by`, whose primary key is `primary`,
    /// prepared with a deadline before the fence. Fencing again once it
    /// is decided may succeed.
    #[error("{by} is prepared from before the fence")]
    Held {
        /// The transaction.
        by: TxnId,
        /// Its primary key, where it is decided.
        primary: Vec<u8>,
        /// Its deadline.
        deadline: Deadline,
    },
    /// The read version is not readable any more, or not yet.
    #[error(transparent)]
    Unreadable(#[from] Unreadable),
}

impl Abort {
    /// Appends the encoding in the module docs.
    ///
    /// # Errors
    ///
    /// When a key is too long to encode.
    pub fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        match self {
            Self::Conflict { key, version } => {
                out.push(CONFLICT);
                put_bytes(out, key)?;
                out.extend_from_slice(&version.get().to_le_bytes());
            }
            Self::Locked {
                key,
                by,
                primary,
                deadline,
            } => {
                out.push(LOCKED);
                put_bytes(out, key)?;
                by.encode(out);
                put_bytes(out, primary)?;
                deadline.encode(out);
            }
            Self::RolledBack(id) => {
                out.push(ROLLED_BACK);
                id.encode(out);
            }
            Self::Fenced(id) => {
                out.push(FENCED);
                id.encode(out);
            }
            Self::Held {
                by,
                primary,
                deadline,
            } => {
                out.push(HELD);
                by.encode(out);
                put_bytes(out, primary)?;
                deadline.encode(out);
            }
            Self::Unreadable(unreadable) => {
                out.push(UNREADABLE);
                let (tag, at, other) = match *unreadable {
                    Unreadable::Forgotten { at, horizon } => (FORGOTTEN, at, horizon),
                    Unreadable::Ahead { at, newest } => (AHEAD, at, newest),
                };
                put_version(out, tag, at);
                out.extend_from_slice(&other.get().to_le_bytes());
            }
        }
        Ok(())
    }

    /// Reads an abort written by [`Abort::encode`].
    ///
    /// # Errors
    ///
    /// When the input is not one.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let abort = match reader.read::<u8>()? {
            CONFLICT => Self::Conflict {
                key: reader.bytes()?.to_vec(),
                version: read_version(reader)?,
            },
            LOCKED => Self::Locked {
                key: reader.bytes()?.to_vec(),
                by: TxnId::decode(reader)?,
                primary: reader.bytes()?.to_vec(),
                deadline: Deadline::decode(reader)?,
            },
            ROLLED_BACK => Self::RolledBack(TxnId::decode(reader)?),
            FENCED => Self::Fenced(TxnId::decode(reader)?),
            HELD => Self::Held {
                by: TxnId::decode(reader)?,
                primary: reader.bytes()?.to_vec(),
                deadline: Deadline::decode(reader)?,
            },
            UNREADABLE => Self::Unreadable(match reader.read::<u8>()? {
                FORGOTTEN => Unreadable::Forgotten {
                    at: read_version(reader)?,
                    horizon: read_version(reader)?,
                },
                AHEAD => Unreadable::Ahead {
                    at: read_version(reader)?,
                    newest: read_version(reader)?,
                },
                other => return Err(DecodeError::Tag(other)),
            }),
            other => return Err(DecodeError::Tag(other)),
        };
        Ok(abort)
    }
}

fn put_version(out: &mut Vec<u8>, tag: u8, version: Version) {
    out.push(tag);
    out.extend_from_slice(&version.get().to_le_bytes());
}

fn read_version(reader: &mut Reader<'_>) -> Result<Version, DecodeError> {
    Ok(Version::new(reader.read()?))
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

#[cfg(test)]
mod tests {
    use oleaph_codec::Reader;
    use proptest::prelude::*;

    use super::{Abort, Outcome, Unreadable};
    use crate::across::{Deadline, TxnId};
    use crate::version::Version;

    fn abort() -> impl Strategy<Value = Abort> {
        let key = proptest::collection::vec(any::<u8>(), 0..8);
        let version = any::<u64>().prop_map(Version::new);
        let txn = any::<u128>().prop_map(TxnId::new);
        let deadline = any::<u64>().prop_map(Deadline::new);
        prop_oneof![
            (key.clone(), version.clone())
                .prop_map(|(key, version)| Abort::Conflict { key, version }),
            (key.clone(), txn.clone(), key.clone(), deadline.clone()).prop_map(
                |(key, by, primary, deadline)| Abort::Locked {
                    key,
                    by,
                    primary,
                    deadline
                }
            ),
            txn.clone().prop_map(Abort::RolledBack),
            txn.clone().prop_map(Abort::Fenced),
            (txn, key, deadline).prop_map(|(by, primary, deadline)| Abort::Held {
                by,
                primary,
                deadline
            }),
            (version.clone(), version.clone())
                .prop_map(|(at, horizon)| Abort::Unreadable(Unreadable::Forgotten { at, horizon })),
            (version.clone(), version)
                .prop_map(|(at, newest)| Abort::Unreadable(Unreadable::Ahead { at, newest })),
        ]
    }

    fn outcome() -> impl Strategy<Value = Outcome> {
        prop_oneof![
            any::<u64>().prop_map(|version| Outcome::Committed(Version::new(version))),
            any::<u64>().prop_map(|version| Outcome::Prepared(Version::new(version))),
            abort().prop_map(Outcome::Aborted),
        ]
    }

    #[test]
    fn an_outcome_has_a_fixed_encoding() {
        let mut out = Vec::new();
        Outcome::Committed(Version::new(7))
            .encode(&mut out)
            .unwrap();
        assert_eq!(out, [1, 7, 0, 0, 0, 0, 0, 0, 0], "committed");
        out.clear();
        Outcome::Aborted(Abort::Fenced(TxnId::new(2)))
            .encode(&mut out)
            .unwrap();
        assert_eq!(out.get(..2), Some([3, 4].as_slice()), "aborted, fenced");
        assert_eq!(out.len(), 2 + 16, "and the id");
    }

    proptest! {
        #[test]
        fn outcomes_roundtrip(outcome in outcome()) {
            let mut out = Vec::new();
            outcome.encode(&mut out).unwrap();
            let mut reader = Reader::new(&out);
            prop_assert_eq!(Outcome::decode(&mut reader), Ok(outcome));
            prop_assert_eq!(reader.finish(), Ok(()));
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..48)) {
            drop(Outcome::decode(&mut Reader::new(&bytes)));
        }
    }
}
