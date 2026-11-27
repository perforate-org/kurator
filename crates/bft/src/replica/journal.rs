//! What a replica writes down before it acts on an event, and what it
//! reads back after a crash.
//!
//! A [`Step`] carries the records its event made: the blocks the replica
//! learned, its new [`Safety`] state and its new decided block. A
//! [`Journal`] appends them to a write-ahead log with one sync and only
//! then hands over the step's [`Effects`], so a vote or a timeout never
//! leaves a replica that could forget it. [`Journal::open`] replays the
//! log into a [`Recovered`] replica for [`Core::recover`](crate::Core::recover).
//!
//! # Record format
//!
//! Each record is a `kurator_codec` frame, magic `KBJL`, version 1:
//!
//! ```text
//! 1 block:     a block as messages encode it
//! 2 safety:    voted view u64 | lock (as a block's justify)
//! 3 committed: block id [32] | view u64
//! ```
//!
//! A step writes its blocks before its safety state and its safety state
//! before its decided block, so a crash in the middle of a batch leaves a
//! prefix that still makes sense: a decided block whose ancestors are all
//! recorded, and a safety state the replica had not acted on yet.

use std::io;
use std::path::PathBuf;

use kurator_codec::{DecodeError, Format};
use kurator_wal::{Lsn, Options, Start, Wal};

use crate::block::Block;
use crate::step::{Effects, Record, Recovered, Safety, Step};
use crate::vote::{BlockId, Justify};
use crate::wire::{put_view, read_view};

const FORMAT: Format = Format::new(*b"KBJL", 1);

const BLOCK: u8 = 1;
const SAFETY: u8 = 2;
const COMMITTED: u8 = 3;

impl Record {
    fn encode(&self) -> io::Result<Vec<u8>> {
        FORMAT.write(Vec::new(), |out| match self {
            Self::Block(block) => {
                out.write_all(&[BLOCK])?;
                block.encode(out)
            }
            Self::Safety(safety) => {
                out.write_all(&[SAFETY])?;
                put_view(out, safety.voted())?;
                safety.lock().encode(out)
            }
            Self::Committed(block, view) => {
                out.write_all(&[COMMITTED])?;
                block.encode(out)?;
                put_view(out, *view)
            }
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = FORMAT.read(bytes)?;
        let record = match reader.read::<u8>()? {
            BLOCK => Self::Block(Block::decode(&mut reader)?),
            SAFETY => {
                let voted = read_view(&mut reader)?;
                Self::Safety(Safety::new(voted, Justify::decode(&mut reader)?))
            }
            COMMITTED => {
                let block = BlockId::decode(&mut reader)?;
                Self::Committed(block, read_view(&mut reader)?)
            }
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(record)
    }
}

/// A replica's write-ahead log: the only way to get a [`Step`]'s
/// [`Effects`] outside this crate, so that what the replica sends never
/// runs ahead of what it would remember after a crash.
#[derive(Debug)]
pub struct Journal {
    wal: Wal,
}

impl Journal {
    /// Opens the journal in `dir`, creating it when it does not exist, and
    /// reads back what the replica recorded.
    ///
    /// # Errors
    ///
    /// As [`Wal::open`], with [`kurator_wal::Error::Replay`] for a record
    /// this build does not read.
    pub fn open(
        dir: impl Into<PathBuf>,
        options: Options,
    ) -> kurator_wal::Result<(Self, Recovered)> {
        let mut recovered = Recovered::default();
        let (wal, _report) = Wal::open(dir, options, Start::After(Lsn::ZERO), |_, bytes| {
            recovered.note(Record::decode(bytes)?);
            Ok::<(), DecodeError>(())
        })?;
        Ok((Self { wal }, recovered))
    }

    /// Makes `step`'s records durable with one sync, and hands over what
    /// the replica may now do.
    ///
    /// # Errors
    ///
    /// As [`Wal::append_batch`]. The effects are then lost, as if the
    /// replica had crashed before the event.
    pub fn persist(&mut self, step: Step) -> kurator_wal::Result<Effects> {
        let (records, effects) = step.into_parts();
        let encoded = records
            .iter()
            .map(Record::encode)
            .collect::<io::Result<Vec<_>>>()?;
        let _written = self.wal.append_batch(encoded)?;
        Ok(effects)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::block::Block;
    use crate::step::{Record, Recovered, Safety};
    use crate::view::View;
    use crate::vote::{BlockId, Justify};

    #[test]
    fn records_round_trip_and_damage_is_refused() {
        let block = Block::new(View::new(3), Justify::Genesis, vec![b"x".to_vec()]);
        let records = [
            Record::Block(block.clone()),
            Record::Safety(Safety::new(View::new(4), Justify::Genesis)),
            Record::Committed(block.id(), View::new(3)),
        ];
        for record in records {
            let bytes = record.encode().unwrap();
            assert_eq!(Record::decode(&bytes), Ok(record.clone()), "{record:?}");
            let mut cut = bytes.clone();
            let _last = cut.pop();
            assert!(Record::decode(&cut).is_err(), "cut {record:?}");
        }
    }

    #[test]
    fn the_decided_chain_runs_from_genesis() {
        let first = Block::new(View::new(1), Justify::Genesis, Vec::new());
        let orphan = Block::new(View::new(2), Justify::Genesis, vec![b"o".to_vec()]);
        let mut recovered = Recovered::default();
        assert!(recovered.decided().is_empty(), "nothing yet");
        recovered.note(Record::Block(first.clone()));
        recovered.note(Record::Block(orphan));
        recovered.note(Record::Committed(first.id(), View::new(1)));
        assert_eq!(recovered.decided(), vec![&first], "the chain only");
        assert_ne!(recovered, Recovered::default(), "remembers");
        let (safety, blocks, committed) = recovered.into_parts();
        assert_eq!(safety.voted(), View::GENESIS, "never voted");
        assert_eq!(blocks.len(), 2, "both blocks");
        assert_eq!(committed, (first.id(), View::new(1)), "committed");
        assert_ne!(first.id(), BlockId::GENESIS, "not genesis");
    }

    proptest! {
        #[test]
        fn decoding_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _decoded = Record::decode(&bytes);
        }
    }
}
