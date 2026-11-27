//! What a replica asks its driver to do after each event, in an order the
//! types enforce: make the safety state durable, then send.

use crate::block::Block;
use crate::message::Message;
use crate::rejection::Rejection;
use crate::view::View;
use crate::vote::{BlockId, Justify};

/// What a replica must not forget across a crash: the last view it voted
/// or gave up in, so it never votes twice in a view, and the newest
/// certificate it knows, so it never votes against a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Safety {
    voted: View,
    lock: Justify,
}

impl Safety {
    pub(crate) const fn new(voted: View, lock: Justify) -> Self {
        Self { voted, lock }
    }

    /// The last view the replica voted or gave up in.
    #[must_use]
    pub const fn voted(&self) -> View {
        self.voted
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
    One(kurator_consensus::NodeId),
}

/// A message to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    to: To,
    message: Message,
}

impl Outgoing {
    pub(crate) const fn new(to: To, message: Message) -> Self {
        Self { to, message }
    }

    /// Where it goes.
    #[must_use]
    pub const fn to(&self) -> To {
        self.to
    }

    /// What it says.
    #[must_use]
    pub const fn message(&self) -> &Message {
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

/// What a replica asks of its driver after an event, once its
/// [`Safety`] is durable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Effects {
    messages: Vec<Outgoing>,
    decided: Vec<Block>,
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

    /// Arms `timer`. A step waits in one view only, the newest it
    /// reached, so the timer of a later view, or a later attempt in the
    /// same view, replaces an earlier one, and never the other way round.
    pub(crate) fn arm(&mut self, timer: Timer) {
        self.timer = self.timer.max(Some(timer));
    }

    pub(crate) fn reject(&mut self, rejection: Rejection) {
        let _first: &mut Rejection = self.rejected.get_or_insert(rejection);
    }

    /// The messages to send, in order.
    #[must_use]
    pub fn messages(&self) -> &[Outgoing] {
        &self.messages
    }

    /// The blocks decided, in the order to apply them. Every honest
    /// replica decides the same blocks in the same order.
    #[must_use]
    pub fn decided(&self) -> &[Block] {
        &self.decided
    }

    /// The timer to arm, replacing any armed before.
    #[must_use]
    pub const fn timer(&self) -> Option<Timer> {
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
            Record::Block(_) | Record::Committed(..) => None,
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
