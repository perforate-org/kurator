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

mod batch;
mod codec;
mod commit;
mod core;
mod db;
mod error;
mod lock;
mod snapshot;
mod state;

pub use batch::{KvOp, WriteBatch};
pub use codec::DecodeError;
pub use db::{Db, Options};
pub use error::{Error, Result, SnapshotDamage};
pub use kurator_wal::{Lsn, RecoveryReport};
pub use state::KvState;
