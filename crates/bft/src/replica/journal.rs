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
//! Each record is a `oleaph_codec` frame, magic `OBJL`, version 1:
//!
//! ```text
//! 1 block:     a block as messages encode it
//! 2 safety:    voted view u64 | lock (as a block's justify)
//! 3 committed:  block id [32] | view u64
//! 4 checkpoint: a checkpoint certificate
//! 5 epoch:      a change certificate | the new epoch's domain [32]
//! ```
//!
//! A step writes its blocks before its safety state and its safety state
//! before its decided block, so a crash in the middle of a batch leaves a
//! prefix that still makes sense: a decided block whose ancestors are all
//! recorded, and a safety state the replica had not acted on yet.
//!
//! # Compaction
//!
//! The state at each of the replica's checkpoints goes to `states/` as it
//! is reached. Once the committee certifies one the replica agrees with,
//! [`Journal::compact`] writes down all the replica still needs in one
//! batch, the checkpoint last, and purges every segment before it. A
//! journal that holds a checkpoint is replayed from that state, and the
//! state is checked against the certificate's root.

use std::io;
use std::path::PathBuf;

use oleaph_codec::{DecodeError, Format};
use oleaph_wal::{Lsn, Options, Start, Wal};

use super::states::{Snapshot, States};
use crate::block::Block;
use crate::checkpoint::{Checkpoint, CheckpointCertificate, Height, Manifest};
use crate::committee::{ChangeCertificate, Domain};
use crate::step::{Effects, Record, Recovered, Safety, Step};
use crate::vote::{BlockId, Justify};
use crate::wire::{put_view, read_view};

const FORMAT: Format = Format::new(*b"OBJL", 1);

const BLOCK: u8 = 1;
const SAFETY: u8 = 2;
const COMMITTED: u8 = 3;
const CHECKPOINT: u8 = 4;
const EPOCH: u8 = 5;

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
                put_view(out, safety.proposed())?;
                safety.lock().encode(out)
            }
            Self::Committed(block, view) => {
                out.write_all(&[COMMITTED])?;
                block.encode(out)?;
                put_view(out, *view)
            }
            Self::Checkpoint(certificate) => {
                out.write_all(&[CHECKPOINT])?;
                certificate.encode(out)
            }
            Self::Epoch(change, domain) => {
                out.write_all(&[EPOCH])?;
                change.encode(out)?;
                out.write_all(domain.as_bytes())
            }
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = FORMAT.read(bytes)?;
        let record = match reader.read::<u8>()? {
            BLOCK => Self::Block(Block::decode(&mut reader)?),
            SAFETY => {
                let voted = read_view(&mut reader)?;
                let proposed = read_view(&mut reader)?;
                Self::Safety(Safety::new(voted, proposed, Justify::decode(&mut reader)?))
            }
            COMMITTED => {
                let block = BlockId::decode(&mut reader)?;
                Self::Committed(block, read_view(&mut reader)?)
            }
            CHECKPOINT => Self::Checkpoint(CheckpointCertificate::decode(&mut reader)?),
            EPOCH => {
                let change = ChangeCertificate::decode(&mut reader)?;
                Self::Epoch(change, Domain::new(reader.read()?))
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
    states: States,
}

impl Journal {
    /// Opens the journal in `dir`, creating it when it does not exist, and
    /// reads back what the replica recorded.
    ///
    /// # Errors
    ///
    /// As [`Wal::open`], with [`oleaph_wal::Error::Replay`] for a record
    /// this build does not read.
    pub fn open(
        dir: impl Into<PathBuf>,
        options: Options,
    ) -> oleaph_wal::Result<(Self, Recovered)> {
        let dir = dir.into();
        let states = States::open(dir.join("states"))?;
        let mut recovered = Recovered::default();
        let (wal, _report) = Wal::open(dir, options, Start::Oldest, |_, bytes| {
            recovered.note(Record::decode(bytes)?);
            Ok::<(), DecodeError>(())
        })?;
        if let Some(certificate) = recovered.checkpoint() {
            let state = states.load(certificate)?;
            recovered.rest_on(state);
        }
        Ok((Self { wal, states }, recovered))
    }

    /// Keeps `snapshot`, the state at a checkpoint the replica reached,
    /// for when the committee certifies it, and returns that checkpoint.
    ///
    /// # Errors
    ///
    /// When the state cannot be written.
    pub(crate) fn keep(&self, snapshot: &Snapshot) -> io::Result<Checkpoint> {
        self.states.keep(snapshot)
    }

    /// The manifest of the state kept at `height`, for a replica catching
    /// up.
    ///
    /// # Errors
    ///
    /// When none was kept.
    pub(crate) fn manifest(&self, height: Height) -> io::Result<Manifest> {
        self.states.manifest(height)
    }

    /// Chunk `index` of the state kept at `height`, for a replica catching
    /// up.
    ///
    /// # Errors
    ///
    /// When none was kept.
    pub(crate) fn chunk(&self, height: Height, index: u32) -> io::Result<Option<Vec<u8>>> {
        self.states.chunk(height, index)
    }

    /// Writes `records`, all the replica still needs past `certificate`,
    /// a certified checkpoint whose state it keeps, and forgets every
    /// record and state before.
    ///
    /// # Errors
    ///
    /// As [`Wal::append_batch`] and [`Wal::purge_through`].
    pub(crate) fn compact(
        &mut self,
        certificate: &CheckpointCertificate,
        records: &[Record],
    ) -> oleaph_wal::Result<()> {
        let before = Lsn::new(self.wal.next_lsn().get().saturating_sub(1));
        self.append(records)?;
        let _purged = self.wal.purge_through(before)?;
        self.states.forget_below(certificate.height())?;
        Ok(())
    }

    /// Makes `step`'s records durable with one sync, and hands over what
    /// the replica may now do.
    ///
    /// # Errors
    ///
    /// As [`Wal::append_batch`]. The effects are then lost, as if the
    /// replica had crashed before the event.
    pub fn persist(&mut self, step: Step) -> oleaph_wal::Result<Effects> {
        let (records, effects) = step.into_parts();
        self.append(&records)?;
        Ok(effects)
    }

    /// Appends `records` with one sync.
    fn append(&mut self, records: &[Record]) -> oleaph_wal::Result<()> {
        let encoded = records
            .iter()
            .map(Record::encode)
            .collect::<io::Result<Vec<_>>>()?;
        let _written = self.wal.append_batch(encoded)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use oleaph_shard::ShardState;
    use oleaph_storage::{Command, Version, WriteBatch};
    use oleaph_wal::Options;
    use proptest::prelude::*;

    use super::super::states::Snapshot;
    use super::Journal;
    use crate::block::Block;
    use crate::checkpoint::Height;
    use crate::committee::Domain;
    use crate::fixture::{certify, change_to};
    use crate::step::{Record, Recovered, Safety};
    use crate::view::View;
    use crate::vote::{BlockId, Justify};

    #[test]
    fn records_round_trip_and_damage_is_refused() {
        let block = Block::new(View::new(3), Justify::Genesis, vec![b"x".to_vec()]);
        let records = [
            Record::Block(block.clone()),
            Record::Safety(Safety::new(View::new(4), View::new(2), Justify::Genesis)),
            Record::Committed(block.id(), View::new(3)),
            Record::Checkpoint(certify(
                Snapshot::take(
                    Height::new(1),
                    (block.id(), block.view()),
                    &ShardState::default(),
                )
                .unwrap()
                .checkpoint()
                .to_owned(),
            )),
            Record::Epoch(change_to(1, &[1, 2, 3]), Domain::new([8; 32])),
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
        let (safety, blocks, committed, certified, _) = recovered.into_parts();
        assert_eq!(certified, None, "no checkpoint");
        assert_eq!(safety.voted(), View::GENESIS, "never voted");
        assert_eq!(blocks.len(), 2, "both blocks");
        assert_eq!(committed, (first.id(), View::new(1)), "committed");
        assert_ne!(first.id(), BlockId::GENESIS, "not genesis");
    }

    #[test]
    fn a_new_epoch_forgets_all_but_the_state_it_starts_from() {
        let block = Block::new(View::new(3), Justify::Genesis, vec![b"x".to_vec()]);
        let snapshot = Snapshot::take(
            Height::new(1),
            (block.id(), block.view()),
            &ShardState::default(),
        )
        .unwrap();
        let base = certify(snapshot.checkpoint().to_owned());
        let epoch = Record::Epoch(change_to(1, &[1, 2, 3]), Domain::new([8; 32]));
        let mut recovered = Recovered::default();
        recovered.note(Record::Block(block.clone()));
        recovered.note(Record::Committed(block.id(), block.view()));
        recovered.note(Record::Checkpoint(base.clone()));
        recovered.note(epoch.clone());
        assert_eq!(
            recovered.checkpoint(),
            Some(&base),
            "the state it starts from"
        );
        assert!(recovered.decided().is_empty(), "nothing decided in it");
        let later = Block::new(View::new(1), Justify::Genesis, vec![b"y".to_vec()]);
        recovered.note(Record::Block(later.clone()));
        recovered.note(Record::Committed(later.id(), later.view()));
        recovered.note(epoch);
        assert_eq!(recovered.decided(), vec![&later], "the same epoch again");
        let Some((change, _)) = recovered.era() else {
            panic!("an era");
        };
        assert_eq!(change.epoch().get(), 1, "the first change");
    }

    /// How many segments the journal in `dir` has.
    fn segments(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter(|entry| entry.as_ref().unwrap().path().is_file())
            .count()
    }

    #[test]
    fn a_compacted_journal_starts_from_the_certified_state() {
        let dir = tempfile::tempdir().unwrap();
        // Nearly every batch starts a segment of its own.
        let mut options = Options::default();
        options.segment_size = 100;
        let first = Block::new(View::new(1), Justify::Genesis, vec![b"a".to_vec()]);
        let second = Block::new(View::new(2), Justify::Genesis, Vec::new());
        let later = Block::new(View::new(3), Justify::Genesis, vec![b"b".to_vec()]);
        let mut state = ShardState::default();
        let _applied = state.apply(
            Version::new(1),
            Command::Kv(WriteBatch::new().put("k", "v")),
        );
        let (mut journal, _) = Journal::open(dir.path(), options.clone()).unwrap();
        for record in [
            Record::Block(first.clone()),
            Record::Block(second.clone()),
            Record::Committed(second.id(), View::new(2)),
        ] {
            journal.append(&[record]).unwrap();
        }
        let older = Snapshot::take(
            Height::new(1),
            (first.id(), first.view()),
            &ShardState::default(),
        )
        .unwrap();
        let _older = journal.keep(&older).unwrap();
        let snapshot =
            Snapshot::take(Height::new(2), (second.id(), second.view()), &state).unwrap();
        let certificate = certify(journal.keep(&snapshot).unwrap());
        let before = segments(dir.path());
        let live = [
            Record::Block(second.clone()),
            Record::Block(later.clone()),
            Record::Safety(Safety::new(View::new(3), View::new(3), Justify::Genesis)),
            Record::Committed(second.id(), View::new(2)),
            Record::Checkpoint(certificate.clone()),
        ];
        journal.compact(&certificate, &live).unwrap();
        assert!(segments(dir.path()) < before, "purged");
        let states = dir.path().join("states");
        assert_eq!(
            std::fs::read_dir(&states).unwrap().count(),
            1,
            "the newest state only"
        );
        drop(journal);

        let (_journal, recovered) = Journal::open(dir.path(), options.clone()).unwrap();
        assert_eq!(recovered.checkpoint(), Some(&certificate), "the checkpoint");
        assert!(recovered.decided().is_empty(), "nothing decided past it");
        let (start, height, block) = recovered.start(ShardState::default());
        assert_eq!(start, state, "its state");
        assert_eq!(
            (height, block),
            (Height::new(2), (second.id(), second.view())),
            "where it stands"
        );
        let (safety, blocks, _, _, _) = recovered.into_parts();
        assert_eq!(safety.voted(), View::new(3), "its safety state");
        assert!(
            !blocks.contains_key(&first.id()),
            "forgot the decided block"
        );
        assert!(blocks.contains_key(&second.id()), "keeps the checkpoint's");
        assert!(blocks.contains_key(&later.id()), "keeps the later one");

        // A state that is not the certified one is refused.
        let file = std::fs::read_dir(&states)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(&file, b"another").unwrap();
        assert!(Journal::open(dir.path(), options).is_err(), "refused");
    }

    proptest! {
        #[test]
        fn decoding_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _decoded = Record::decode(&bytes);
        }
    }
}
