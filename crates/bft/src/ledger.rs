//! The shard state the decided blocks make.

use kurator_shard::{Applied, ShardState};
use kurator_storage::{Encoded, Version};

use crate::block::Block;
use crate::journal::Recovered;

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
/// [`kurator_storage::Command`] as it encodes; splitting a shard that BFT
/// replicates comes later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    state: ShardState,
}

impl Ledger {
    /// The state `state`, before any block is decided.
    #[must_use]
    pub const fn new(state: ShardState) -> Self {
        Self { state }
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
        block
            .commands()
            .iter()
            .map(|command| self.settle(command))
            .collect()
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

    /// The state.
    #[must_use]
    pub const fn state(&self) -> &ShardState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use kurator_shard::{Applied, ShardState};
    use kurator_storage::{Command, Outcome, Version, WriteBatch};

    use super::{Ledger, Settled};
    use crate::block::Block;
    use crate::journal::Recovered;
    use crate::step::Record;
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
            ledger.state().kv().get(b"a"),
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
}
