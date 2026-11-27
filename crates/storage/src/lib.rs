//! Memory-first storage engine for Kurator.
//!
//! The whole state lives in memory; durability comes from the write-ahead
//! log in [`kurator_wal`] and from snapshots. A [`Db`] is one directory:
//!
//! ```text
//! <dir>/LOCK        held while the database is open
//! <dir>/wal/        write-ahead log segments
//! <dir>/snapshots/  the latest snapshot
//! ```
//!
//! Writes go through a single commit thread that batches whatever is
//! waiting, appends it to the log with one `fdatasync` (group commit),
//! applies it to memory in log order, and only then answers the writers.
//! A write that returned is therefore durable and visible to every read
//! that starts after it.
//!
//! Every write gets a [`Version`], its log sequence number, and the state
//! keeps each key's values back to a [`Retention`] horizon, so a [`View`]
//! reads the state as it was at any version since. [`Db::begin`] starts a
//! [`Txn`]: it reads one version, holds its writes back, and commits them
//! only if no write after that version touched a key it read or a range it
//! scanned. Committed transactions are serializable.
//!
//! [`Db::open`] loads the latest snapshot and replays the log records after
//! it. [`Db::checkpoint`] writes a new snapshot and drops the log segments
//! it covers.
//!
//! ```
//! # fn main() -> Result<(), kurator_storage::Error> {
//! use kurator_storage::{Db, Options, WriteBatch};
//!
//! let dir = tempfile::tempdir()?;
//! let db = Db::open(dir.path(), Options::default())?;
//! db.write(WriteBatch::new().put("fruit", "apple").put("veg", "leek"))?;
//! assert_eq!(db.get(b"fruit").as_deref(), Some(b"apple".as_slice()));
//! # Ok(())
//! # }
//! ```

mod across;
mod batch;
mod command;
mod db;
mod draft;
mod error;
mod outcome;
mod span;
mod state;
mod two_phase;
mod txn;
mod version;

pub use across::{Deadline, TxnId};
pub use batch::{KvOp, WriteBatch};
pub use command::{Command, Encoded};
pub use db::{Db, Options, Txn};
pub use draft::{Draft, KeyValue};
pub use error::{Error, Result, SnapshotDamage};
pub use kurator_codec::DecodeError;
pub use kurator_wal::{Lsn, RecoveryReport};
pub use outcome::{Abort, Outcome, Unreadable};
pub use span::{End, Span};
pub use state::{KvState, View};
pub use two_phase::{Collect, Decide, Decided, Decision, Prepare};
pub use txn::Transaction;
pub use version::{Retention, Version};
