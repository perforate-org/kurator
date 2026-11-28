//! The shard state the decided blocks make.

use std::io;

use oleaph_codec::Reader;

use oleaph_shard::{Applied, Shard, ShardState};
use oleaph_storage::{Encoded, KvState, Version};

use super::states::Snapshot;
use crate::block::Block;
use crate::checkpoint::{CheckpointCertificate, Height, Interval};
use crate::step::Recovered;
use crate::view::View;
use crate::vote::BlockId;

/// What became of one command of a decided block.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Settled {
    /// It applied to the shard, or the shard refused it, as
    /// [`ShardState::apply`] decided.
    Applied(Applied),
    /// It is not a command: a faulty leader proposed bytes that decode to
    /// nothing. Every replica skips it alike, and it takes no version.
    Garbled,
    /// The shard has used every version there is, which it cannot do in
    /// any lifetime of the universe: nothing applies any more.
    Exhausted,
}

/// A shard's state as of the last decided block: each block's commands
/// applied in order, one version each.
///
/// Every honest replica decides the same blocks in the same order, so
/// every one holds the same state at the same version. A command is a
/// [`oleaph_storage::Command`] as it encodes; splitting a shard that BFT
/// replicates comes later.
///
/// Every [`Interval`] of blocks, it names the state it holds by a
/// [`Checkpoint`](crate::Checkpoint), for the committee to certify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    state: ShardState,
    /// How many blocks it applied.
    height: Height,
    /// The last of them, and its view.
    last: (BlockId, View),
    interval: Interval,
}

impl Ledger {
    /// The state `state`, before any block is decided, with a checkpoint
    /// every [`Interval::DEFAULT`] blocks.
    #[must_use]
    pub const fn new(state: ShardState) -> Self {
        Self {
            state,
            height: Height::ZERO,
            last: (BlockId::GENESIS, View::GENESIS),
            interval: Interval::DEFAULT,
        }
    }

    /// The same ledger, with a checkpoint every `interval` blocks.
    #[must_use]
    pub fn every(self, interval: Interval) -> Self {
        Self { interval, ..self }
    }

    /// The state that `recovered` kept at its checkpoint, or else `state`,
    /// the state before any block was decided, with the blocks that
    /// `recovered` decided past it applied again.
    #[must_use]
    pub fn replay(state: ShardState, recovered: &Recovered) -> Self {
        let (state, height, last) = recovered.start(state);
        let mut ledger = Self {
            height,
            last,
            ..Self::new(state)
        };
        for block in recovered.decided() {
            let _settled = ledger.apply(block);
        }
        ledger
    }

    /// Applies the commands of `block`, the block decided after the last
    /// one applied, and returns what became of each.
    pub fn apply(&mut self, block: &Block) -> Vec<Settled> {
        self.height = self.height.next().unwrap_or(self.height);
        self.last = (block.id(), block.view());
        block
            .commands()
            .iter()
            .map(|command| self.settle(command))
            .collect()
    }

    /// The state, when a checkpoint falls at the height it reached.
    ///
    /// # Errors
    ///
    /// When the state does not encode, as [`ShardState::encode`].
    pub(crate) fn checkpoint(&self) -> io::Result<Option<Snapshot>> {
        if !self.interval.at(self.height) {
            return Ok(None);
        }
        self.snapshot().map(Some)
    }

    /// The state, at the height it reached.
    ///
    /// # Errors
    ///
    /// As [`Ledger::checkpoint`].
    pub(crate) fn snapshot(&self) -> io::Result<Snapshot> {
        Snapshot::take(self.height, self.last, &self.state)
    }

    /// Starts again from `encoded`, the state that `certificate`
    /// certifies, as if it had applied the blocks up to it, and returns
    /// that state to keep.
    ///
    /// # Errors
    ///
    /// When the state is no further than its own, or does not decode, or
    /// encode again; the ledger is then as it was.
    pub(crate) fn restart(
        &mut self,
        certificate: &CheckpointCertificate,
        encoded: &[u8],
    ) -> io::Result<Snapshot> {
        if certificate.height() <= self.height {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the state is no further than the ledger's",
            ));
        }
        let mut reader = Reader::new(encoded);
        let state = ShardState::decode(&mut reader)
            .and_then(|state| reader.finish().map(|()| state))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let height = certificate.height();
        let last = (certificate.block(), certificate.view());
        let snapshot = Snapshot::take(height, last, &state)?;
        self.state = state;
        self.height = height;
        self.last = last;
        Ok(snapshot)
    }

    /// How many blocks it applied.
    #[must_use]
    pub const fn height(&self) -> Height {
        self.height
    }

    fn settle(&mut self, command: &[u8]) -> Settled {
        let Ok(encoded) = Encoded::decode(command.to_vec()) else {
            return Settled::Garbled;
        };
        let Some(next) = self.state.kv().version().get().checked_add(1) else {
            return Settled::Exhausted;
        };
        Settled::Applied(self.state.apply(Version::new(next), encoded.into_command()))
    }

    /// The shard it holds the state of.
    #[must_use]
    pub const fn shard(&self) -> &Shard {
        self.state.shard()
    }

    /// The shard's data.
    #[must_use]
    pub const fn kv(&self) -> &KvState {
        self.state.kv()
    }
}

#[cfg(test)]
mod tests {
    use oleaph_shard::{Applied, ShardState};
    use oleaph_storage::{Command, Outcome, Version, WriteBatch};

    use std::collections::BTreeMap;

    use super::{Ledger, Settled};
    use crate::block::Block;
    use crate::checkpoint::Manifest;
    use crate::checkpoint::{Checkpoint, CheckpointCertificate, Height, Interval, StateRoot};
    use crate::step::{Record, Recovered};
    use crate::view::View;
    use crate::vote::{BlockId, Justify};

    fn put(key: &str, value: &str) -> Vec<u8> {
        Command::Kv(WriteBatch::new().put(key, value))
            .encode()
            .unwrap()
            .payload()
            .to_vec()
    }

    #[test]
    fn decided_commands_apply_in_order_and_garbage_is_skipped() {
        let block = Block::new(
            View::new(1),
            Justify::Genesis,
            vec![put("a", "1"), b"garbage".to_vec(), put("a", "2")],
        );
        let mut ledger = Ledger::new(ShardState::default());
        let settled = ledger.apply(&block);
        assert_eq!(
            settled,
            vec![
                Settled::Applied(Applied::Data(Outcome::Committed(Version::new(1)))),
                Settled::Garbled,
                Settled::Applied(Applied::Data(Outcome::Committed(Version::new(2)))),
            ],
            "one version each"
        );
        assert_eq!(
            ledger.kv().get(b"a"),
            Some(b"2".as_slice()),
            "the later write wins"
        );
    }

    #[test]
    fn a_replay_rebuilds_the_state() {
        let first = Block::new(View::new(1), Justify::Genesis, vec![put("a", "1")]);
        let mut recovered = Recovered::default();
        recovered.note(Record::Block(first.clone()));
        recovered.note(Record::Committed(first.id(), View::new(1)));
        let mut ledger = Ledger::new(ShardState::default());
        let _settled = ledger.apply(&first);
        assert_eq!(
            Ledger::replay(ShardState::default(), &recovered),
            ledger,
            "the same state"
        );
    }

    #[test]
    fn a_checkpoint_falls_every_interval() {
        let every = Interval::new(std::num::NonZeroU64::new(2).unwrap());
        let mut ledger = Ledger::new(ShardState::default()).every(every);
        assert_eq!(ledger.checkpoint().unwrap(), None, "nothing decided");
        let first = Block::new(View::new(1), Justify::Genesis, vec![put("a", "1")]);
        let _first = ledger.apply(&first);
        assert_eq!(ledger.height(), Height::new(1), "one block");
        assert_eq!(ledger.checkpoint().unwrap(), None, "between");
        let second = Block::new(View::new(2), Justify::Genesis, Vec::new());
        let _second = ledger.apply(&second);
        let snapshot = ledger.checkpoint().unwrap().unwrap();
        let checkpoint = snapshot.checkpoint();
        assert_eq!(checkpoint.height(), Height::new(2), "at two");
        assert_eq!(checkpoint.block(), second.id(), "after the second");
        assert_eq!(
            checkpoint.root(),
            Manifest::of(&ledger.state).unwrap().root(),
            "of the state"
        );
    }

    #[test]
    fn a_restart_moves_only_forward() {
        let mut theirs = ShardState::default();
        let _applied = theirs.apply(
            Version::new(1),
            Command::Kv(WriteBatch::new().put("t", "1")),
        );
        let mut encoded = Vec::new();
        theirs.encode(&mut encoded).unwrap();
        let at = |height| {
            let checkpoint = Checkpoint::new(
                Height::new(height),
                BlockId::new([4; 32]),
                View::new(height),
                StateRoot::new([0; 32]),
            );
            CheckpointCertificate::new(checkpoint, BTreeMap::new())
        };
        let mut ledger = Ledger::new(ShardState::default());
        let first = Block::new(View::new(1), Justify::Genesis, vec![put("a", "1")]);
        let _first = ledger.apply(&first);
        let before = ledger.clone();
        for height in [0, 1] {
            assert!(ledger.restart(&at(height), &encoded).is_err(), "{height}");
            assert_eq!(ledger, before, "as it was");
        }
        assert!(ledger.restart(&at(3), b"garbage").is_err(), "undecodable");
        assert_eq!(ledger, before, "as it was");
        let snapshot = ledger.restart(&at(3), &encoded).unwrap();
        assert_eq!(
            snapshot.checkpoint().height(),
            Height::new(3),
            "at its height"
        );
        assert_eq!(ledger.height(), Height::new(3), "moved on");
        assert_eq!(ledger.kv().get(b"t"), Some(b"1".as_slice()), "their state");
    }
}
