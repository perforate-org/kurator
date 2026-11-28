//! The shard state the decided blocks make.

use std::io;

use oleaph_shard::{Applied, Shard, ShardState};
use oleaph_storage::{Encoded, KvState, Version};

use super::manifest::Manifest;
use crate::block::Block;
use crate::checkpoint::{Checkpoint, Height, Interval};
use crate::step::Recovered;
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
/// [`Checkpoint`], for the committee to certify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    state: ShardState,
    /// How many blocks it applied.
    height: Height,
    /// The last of them.
    last: BlockId,
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
            last: BlockId::GENESIS,
            interval: Interval::DEFAULT,
        }
    }

    /// The same ledger, with a checkpoint every `interval` blocks.
    #[must_use]
    pub fn every(self, interval: Interval) -> Self {
        Self { interval, ..self }
    }

    /// The state `state` was before any block was decided, with the blocks
    /// that `recovered` decided applied again.
    #[must_use]
    pub fn replay(state: ShardState, recovered: &Recovered) -> Self {
        let mut ledger = Self::new(state);
        for block in recovered.decided() {
            let _settled = ledger.apply(block);
        }
        ledger
    }

    /// Applies the commands of `block`, the block decided after the last
    /// one applied, and returns what became of each.
    pub fn apply(&mut self, block: &Block) -> Vec<Settled> {
        self.height = self.height.next().unwrap_or(self.height);
        self.last = block.id();
        block
            .commands()
            .iter()
            .map(|command| self.settle(command))
            .collect()
    }

    /// The checkpoint of the state, when one falls at the height it
    /// reached.
    ///
    /// # Errors
    ///
    /// When the state does not encode, as [`Manifest::of`].
    pub fn checkpoint(&self) -> io::Result<Option<Checkpoint>> {
        if !self.interval.at(self.height) {
            return Ok(None);
        }
        let root = Manifest::of(&self.state)?.root();
        Ok(Some(Checkpoint::new(self.height, self.last, root)))
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

    use super::{Ledger, Manifest, Settled};
    use crate::block::Block;
    use crate::checkpoint::{Height, Interval};
    use crate::step::{Record, Recovered};
    use crate::view::View;
    use crate::vote::Justify;

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
        let checkpoint = ledger.checkpoint().unwrap().unwrap();
        assert_eq!(checkpoint.height(), Height::new(2), "at two");
        assert_eq!(checkpoint.block(), second.id(), "after the second");
        assert_eq!(
            checkpoint.root(),
            Manifest::of(&ledger.state).unwrap().root(),
            "of the state"
        );
    }
}
