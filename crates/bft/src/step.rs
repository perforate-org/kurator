//! What a replica asks its driver to do after each event, in an order the
//! types enforce: make the safety state durable, then send.

use std::collections::BTreeMap;

use oleaph_shard::ShardState;

use crate::block::{Block, prune};
use crate::checkpoint::{CheckpointCertificate, Height};
use crate::message::Message;
use crate::rejection::Rejection;
use crate::view::View;
use crate::vote::{BlockId, Justify};

/// What a replica must not forget across a crash.
///
/// The last view it voted or gave up in, so it never votes twice in a
/// view; the last view it proposed in, so it never proposes two blocks in
/// a view; and the newest certificate it knows, so it never votes against
/// a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Safety {
    voted: View,
    proposed: View,
    lock: Justify,
}

impl Safety {
    pub(crate) const fn new(voted: View, proposed: View, lock: Justify) -> Self {
        Self {
            voted,
            proposed,
            lock,
        }
    }

    /// The last view the replica voted or gave up in.
    #[must_use]
    pub const fn voted(&self) -> View {
        self.voted
    }

    /// The last view the replica proposed in.
    #[must_use]
    pub const fn proposed(&self) -> View {
        self.proposed
    }

    /// The newest certificate the replica knows.
    #[must_use]
    pub const fn lock(&self) -> &Justify {
        &self.lock
    }
}

/// Where a message goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum To {
    /// Every other member.
    Others,
    /// One other member.
    One(oleaph_consensus::NodeId),
}

/// A message to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Outgoing {
    to: To,
    message: Message,
}

impl Outgoing {
    pub(crate) const fn new(to: To, message: Message) -> Self {
        Self { to, message }
    }

    /// Where it goes.
    #[cfg(test)]
    pub(crate) const fn to(&self) -> To {
        self.to
    }

    /// What it says.
    #[cfg(test)]
    pub(crate) const fn message(&self) -> &Message {
        &self.message
    }
}

/// A timer to arm.
///
/// When it expires, the driver hands the replica
/// [`Event::Expired`](crate::Event::Expired) with its view. Its length is
/// the driver's base timeout doubled `attempt` times, so that views grow
/// long enough to succeed once the network settles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timer {
    view: View,
    attempt: u32,
}

impl Timer {
    pub(crate) const fn new(view: View, attempt: u32) -> Self {
        Self { view, attempt }
    }

    /// The view it is for.
    #[must_use]
    pub const fn view(&self) -> View {
        self.view
    }

    /// How many views in a row timed out before this one.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// What carries out a step's [`Effects`]: the node a replica runs on.
pub trait Act {
    /// Applies `block`, the next decided block. Every honest replica
    /// decides the same blocks in the same order.
    fn decide(&mut self, block: &Block);

    /// Learns that a quorum certified `certificate`'s checkpoint, which
    /// the state it applied must match at that height.
    fn certify(&mut self, certificate: &CheckpointCertificate);

    /// Sends `message` where `to` says.
    fn send(&mut self, to: To, message: &Message);

    /// Arms `timer`, replacing any armed before.
    fn wait(&mut self, timer: Timer);
}

/// What a replica asks of its driver after an event, once its
/// [`Safety`] is durable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Effects {
    messages: Vec<Outgoing>,
    decided: Vec<Block>,
    certified: Option<CheckpointCertificate>,
    timer: Option<Timer>,
    rejected: Option<Rejection>,
}

impl Effects {
    pub(crate) fn send(&mut self, to: To, message: Message) {
        self.messages.push(Outgoing::new(to, message));
    }

    pub(crate) fn decide(&mut self, block: Block) {
        self.decided.push(block);
    }

    /// Hands on `certificate`. Should a step certify two checkpoints,
    /// the newer one says all the older one does.
    pub(crate) fn certify(&mut self, certificate: CheckpointCertificate) {
        let newer = self
            .certified
            .as_ref()
            .is_none_or(|older| certificate.supersedes(older));
        if newer {
            self.certified = Some(certificate);
        }
    }

    /// Arms `timer`. A step waits in one view only, the newest it
    /// reached, so the timer of a later view, or a later attempt in the
    /// same view, replaces an earlier one, and never the other way round.
    pub(crate) fn arm(&mut self, timer: Timer) {
        self.timer = self.timer.max(Some(timer));
    }

    pub(crate) fn reject(&mut self, rejection: Rejection) {
        let _first: &mut Rejection = self.rejected.get_or_insert(rejection);
    }

    /// Has `act` carry the effects out: apply the decided blocks, in
    /// order, then take in the certified checkpoint, then send the
    /// messages, in order, then arm the timer.
    pub fn carry_out(&self, act: &mut impl Act) {
        for block in &self.decided {
            act.decide(block);
        }
        if let Some(certificate) = &self.certified {
            act.certify(certificate);
        }
        for outgoing in &self.messages {
            act.send(outgoing.to, &outgoing.message);
        }
        if let Some(timer) = self.timer {
            act.wait(timer);
        }
    }

    /// The messages to send, in order.
    #[cfg(test)]
    pub(crate) fn messages(&self) -> &[Outgoing] {
        &self.messages
    }

    /// The checkpoint certified, if one was.
    #[cfg(test)]
    pub(crate) const fn certified(&self) -> Option<&CheckpointCertificate> {
        self.certified.as_ref()
    }

    /// The blocks decided, in the order to apply them.
    #[cfg(test)]
    pub(crate) fn decided(&self) -> &[Block] {
        &self.decided
    }

    /// The timer to arm, replacing any armed before.
    #[cfg(test)]
    pub(crate) const fn timer(&self) -> Option<Timer> {
        self.timer
    }

    /// Why the replica ignored the message of the event, if it did.
    #[must_use]
    pub const fn rejected(&self) -> Option<Rejection> {
        self.rejected
    }
}

/// One thing a replica must not forget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    /// A block whose body it holds.
    Block(Block),
    /// Its safety state.
    Safety(Safety),
    /// The last block it decided, and its view.
    Committed(BlockId, View),
    /// The newest checkpoint the committee certified that the replica
    /// keeps the state of: the blocks it decided up to it are forgotten.
    Checkpoint(CheckpointCertificate),
}

/// What a replica remembers from before it stopped: enough to rejoin
/// without voting twice in a view, voting against its lock or deciding a
/// block again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovered {
    safety: Safety,
    blocks: BTreeMap<BlockId, Block>,
    committed: (BlockId, View),
    /// The newest certified checkpoint it keeps the state of, and that
    /// state once the journal loaded it.
    base: Option<(CheckpointCertificate, Option<ShardState>)>,
}

impl Default for Recovered {
    /// A replica that never ran.
    fn default() -> Self {
        Self {
            safety: Safety::new(View::GENESIS, View::GENESIS, Justify::Genesis),
            blocks: BTreeMap::new(),
            committed: (BlockId::GENESIS, View::GENESIS),
            base: None,
        }
    }
}

impl Recovered {
    /// Takes in `record`, written after every record before it.
    pub(crate) fn note(&mut self, record: Record) {
        match record {
            Record::Block(block) => {
                let _stored: &mut Block = self.blocks.entry(block.id()).or_insert(block);
            }
            Record::Safety(safety) => self.safety = safety,
            Record::Committed(block, view) => self.committed = (block, view),
            Record::Checkpoint(certificate) => {
                prune(&mut self.blocks, certificate.block(), certificate.view());
                self.base = Some((certificate, None));
            }
        }
    }

    /// The certified checkpoint whose state the replica kept, if any.
    pub(crate) fn checkpoint(&self) -> Option<&CheckpointCertificate> {
        self.base.as_ref().map(|(certificate, _)| certificate)
    }

    /// Takes in `state`, the state at its checkpoint.
    pub(crate) fn rest_on(&mut self, state: ShardState) {
        if let Some((_, base)) = &mut self.base {
            *base = Some(state);
        }
    }

    /// The state to apply [`Recovered::decided`] to, its height, and the
    /// block it stands after with that block's view: the kept state at
    /// the checkpoint, or else `genesis`, before any block.
    #[must_use]
    pub fn start(&self, genesis: ShardState) -> (ShardState, Height, (BlockId, View)) {
        match &self.base {
            Some((certificate, Some(state))) => (
                state.clone(),
                certificate.height(),
                (certificate.block(), certificate.view()),
            ),
            Some((_, None)) | None => (genesis, Height::ZERO, (BlockId::GENESIS, View::GENESIS)),
        }
    }

    /// The blocks the replica decided past its checkpoint, oldest first,
    /// so that a state machine can apply them again.
    #[must_use]
    pub fn decided(&self) -> Vec<&Block> {
        let after = self
            .checkpoint()
            .map_or(BlockId::GENESIS, CheckpointCertificate::block);
        let mut chain = Vec::new();
        let mut id = self.committed.0;
        while id != after {
            let Some(block) = self.blocks.get(&id) else {
                break;
            };
            chain.push(block);
            id = block.parent();
        }
        chain.reverse();
        chain
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Safety,
        BTreeMap<BlockId, Block>,
        (BlockId, View),
        Option<CheckpointCertificate>,
    ) {
        let certified = self.base.map(|(certificate, _)| certificate);
        (self.safety, self.blocks, self.committed, certified)
    }
}

/// The outcome of one event.
///
/// The messages it sends may include a vote or a timeout, which commit
/// the replica to never voting again in that view, so they are only to be
/// had once what the event changed is durable:
/// [`Journal::persist`](crate::Journal::persist) writes it down and hands
/// them over.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "a step's messages and decisions are lost unless it is persisted"]
pub struct Step {
    records: Vec<Record>,
    effects: Effects,
}

impl Step {
    pub(crate) const fn new(records: Vec<Record>, effects: Effects) -> Self {
        Self { records, effects }
    }

    /// The safety state the event moved the replica to, if it did.
    #[must_use]
    pub fn safety(&self) -> Option<&Safety> {
        self.records.iter().find_map(|record| match record {
            Record::Safety(safety) => Some(safety),
            Record::Block(_) | Record::Committed(..) | Record::Checkpoint(_) => None,
        })
    }

    /// What to do, taken without writing anything down: for tests and
    /// simulations, which keep the records themselves.
    #[cfg(test)]
    pub(crate) fn persisted(self) -> Effects {
        self.effects
    }

    pub(crate) fn into_parts(self) -> (Vec<Record>, Effects) {
        (self.records, self.effects)
    }
}
