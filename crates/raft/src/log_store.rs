//! The Raft log, kept in memory and made durable in a signed WAL.
//!
//! Every change is one record appended to the WAL with one `fdatasync`:
//! entries, truncations, purges, votes and the committed position. The WAL
//! stays append-only; truncations and purges are records too, and opening
//! the store replays them in order. A whole WAL segment is deleted once no
//! record in it is needed any more.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use kurator_wal::{Integrity, Lsn, SigningKey, Start, Wal};
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogState, OptionalSend, RaftLogReader};
use parking_lot::Mutex;

use crate::codec::Record;
use crate::config::{Config, Entry};
use crate::error::{Error, Result};
use crate::types::{LogId, Vote};

/// The log store of one replica. Clones share the same log.
#[derive(Debug, Clone)]
pub struct LogStore {
    log: Arc<Mutex<Log>>,
}

/// An entry and the WAL record that holds it.
#[derive(Debug, Clone)]
struct Stored {
    entry: Entry,
    lsn: Lsn,
}

#[derive(Debug)]
struct Log {
    wal: Wal,
    entries: BTreeMap<u64, Stored>,
    purged: Option<LogId>,
    vote: Option<Vote>,
    committed: Committed,
}

/// The newest entry known to be committed, and the newest one written down.
///
/// Losing the committed position is safe, so it is not synced on its own:
/// it goes into the WAL with the next record that is.
#[derive(Debug, Clone, Copy, Default)]
struct Committed {
    known: Option<LogId>,
    logged: Option<LogId>,
}

impl LogStore {
    /// Opens the log in `dir`, signing it with `key`, and replays it.
    ///
    /// # Errors
    ///
    /// When the WAL is damaged or was signed with another key, and when a
    /// record does not decode.
    pub fn open(dir: &Path, key: SigningKey) -> Result<Self> {
        let mut options = kurator_wal::Options::default();
        options.integrity = Integrity::Signed(key);
        let mut replayed = Replayed::default();
        let (wal, _) = Wal::open(dir, options, Start::Oldest, |lsn, payload| {
            let record = Record::decode(payload).map_err(|source| Error::Record { lsn, source })?;
            replayed.apply(lsn, record);
            Ok::<(), Error>(())
        })?;
        let Replayed {
            entries,
            purged,
            vote,
            committed,
        } = replayed;
        let log = Log {
            wal,
            entries,
            purged,
            vote,
            committed: Committed {
                known: committed,
                logged: committed,
            },
        };
        Ok(Self {
            log: Arc::new(Mutex::new(log)),
        })
    }
}

/// The log as replaying its records rebuilds it.
#[derive(Debug, Default)]
struct Replayed {
    entries: BTreeMap<u64, Stored>,
    purged: Option<LogId>,
    vote: Option<Vote>,
    committed: Option<LogId>,
}

impl Replayed {
    fn apply(&mut self, lsn: Lsn, record: Record) {
        match record {
            Record::Entry(entry) => {
                drop(
                    self.entries
                        .insert(entry.log_id.index, Stored { entry, lsn }),
                );
            }
            Record::TruncateAfter(last) => truncate_after(&mut self.entries, last.as_ref()),
            Record::PurgeThrough(log_id) => {
                purge_through(&mut self.entries, &log_id);
                self.purged = Some(log_id);
            }
            Record::Vote(vote) => self.vote = Some(vote),
            Record::Committed(committed) => self.committed = committed,
        }
    }
}

fn truncate_after(entries: &mut BTreeMap<u64, Stored>, last: Option<&LogId>) {
    match last {
        Some(last) => drop(entries.split_off(&last.index.saturating_add(1))),
        None => entries.clear(),
    }
}

fn purge_through(entries: &mut BTreeMap<u64, Stored>, log_id: &LogId) {
    *entries = entries.split_off(&log_id.index.saturating_add(1));
}

impl Log {
    /// Appends `records` with one sync and returns the position of the
    /// first. The committed position goes after them when it moved.
    fn write(&mut self, records: &[Record]) -> Result<Lsn> {
        let first = self.wal.next_lsn();
        let mut payloads: Vec<Vec<u8>> = records.iter().map(Record::encode).collect();
        let committed = self.committed;
        if committed.known != committed.logged {
            payloads.push(Record::Committed(committed.known).encode());
        }
        let _: Option<_> = self.wal.append_batch(&payloads)?;
        self.committed.logged = committed.known;
        Ok(first)
    }

    fn append(&mut self, entries: Vec<Entry>) -> Result<()> {
        let records: Vec<Record> = entries.into_iter().map(Record::Entry).collect();
        let first = self.write(&records)?;
        let mut lsn = first;
        for record in records {
            if let Record::Entry(entry) = record {
                drop(
                    self.entries
                        .insert(entry.log_id.index, Stored { entry, lsn }),
                );
            }
            lsn = lsn.next();
        }
        Ok(())
    }

    fn truncate_after(&mut self, last: Option<LogId>) -> Result<()> {
        let _: Lsn = self.write(&[Record::TruncateAfter(last)])?;
        truncate_after(&mut self.entries, last.as_ref());
        Ok(())
    }

    fn save_vote(&mut self, vote: Vote) -> Result<()> {
        let _: Lsn = self.write(&[Record::Vote(vote)])?;
        self.vote = Some(vote);
        Ok(())
    }

    const fn save_committed(&mut self, committed: Option<LogId>) {
        self.committed.known = committed;
    }

    /// Drops the entries up to `log_id`, then deletes the WAL segments that
    /// hold only records older than the oldest entry left.
    ///
    /// Everything from that entry on is replayed, so the records before it
    /// matter only for the vote and the committed position: both are
    /// written again here, after the purge record.
    fn purge(&mut self, log_id: LogId) -> Result<()> {
        purge_through(&mut self.entries, &log_id);
        let mut records = vec![Record::PurgeThrough(log_id)];
        records.extend(self.vote.map(Record::Vote));
        self.committed.logged = None;
        let first = self.write(&records)?;
        self.purged = Some(log_id);
        let keep_from = self
            .entries
            .values()
            .next()
            .map_or(first, |stored| stored.lsn);
        let _purged: usize = self
            .wal
            .purge_through(Lsn::new(keep_from.get().saturating_sub(1)))?;
        Ok(())
    }

    fn state(&self) -> (Option<LogId>, Option<LogId>) {
        let purged = self.purged;
        let last = self
            .entries
            .values()
            .next_back()
            .map(|stored| stored.entry.log_id)
            .or(purged);
        (purged, last)
    }

    fn entries<R: RangeBounds<u64>>(&self, range: R) -> Vec<Entry> {
        self.entries
            .range(range)
            .map(|(_, stored)| stored.entry.clone())
            .collect()
    }
}

impl<C: Config> RaftLogReader<C> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<Entry>> {
        Ok(self.log.lock().entries(range))
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        Ok(self.log.lock().vote)
    }
}

impl<C: Config> RaftLogStorage<C> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<C>> {
        let (last_purged_log_id, last_log_id) = self.log.lock().state();
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        self.log.lock().save_vote(*vote).map_err(Error::into_io)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> io::Result<()> {
        self.log.lock().save_committed(committed);
        Ok(())
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogId>> {
        Ok(self.log.lock().committed.known)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> io::Result<()>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // The append is synced before it returns, so the entries are durable
        // when the leader counts this replica towards a quorum.
        let appended = self.log.lock().append(entries.into_iter().collect());
        match appended {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                let error = error.into_io();
                callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
                Err(error)
            }
        }
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> io::Result<()> {
        self.log
            .lock()
            .truncate_after(last_log_id)
            .map_err(Error::into_io)
    }

    async fn purge(&mut self, log_id: LogId) -> io::Result<()> {
        self.log.lock().purge(log_id).map_err(Error::into_io)
    }
}
