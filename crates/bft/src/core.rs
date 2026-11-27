//! One replica's part in the protocol, as a deterministic state machine:
//! events in, a [`Step`] out. It reads no clock, opens no socket and
//! writes no file, so a simulation and a real node drive the same code.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::{cmp, mem};

use kurator_consensus::NodeId;
use kurator_crypto::{Signature, SigningKey};

use crate::block::{Block, Proposal};
use crate::committee::{Committee, Domain};
use crate::message::Message;
use crate::rejection::Rejection;
use crate::step::Recovered;
use crate::step::{Effects, Record, Safety, Step, Timer, To};
use crate::timeout::{Timeout, TimeoutCertificate};
use crate::view::View;
use crate::vote::{BlockId, Certificate, Justify, Vote};

/// Who a replica is and whom it decides with.
#[derive(Debug)]
pub struct Config {
    node: NodeId,
    key: SigningKey,
    committee: Committee,
    domain: Domain,
}

impl Config {
    /// Replica `node`, which votes with `key`, in `committee`, signing in
    /// `domain`.
    #[must_use]
    pub const fn new(node: NodeId, key: SigningKey, committee: Committee, domain: Domain) -> Self {
        Self {
            node,
            key,
            committee,
            domain,
        }
    }
}

/// What happens to a replica.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// The replica starts, in the first view.
    Start,
    /// A message arrived from `from`, which the transport authenticated.
    Received {
        /// The sender.
        from: NodeId,
        /// What it sent.
        message: Message,
    },
    /// The timer of a view expired.
    Expired(View),
    /// A client asks for a command to be decided.
    Submitted(Vec<u8>),
}

/// One replica of a shard: `HotStuff` with a two-chain commit and a leader
/// for each view in turn.
///
/// - **Views.** The leader of a view proposes a block that extends the
///   newest certified block it knows. Replicas vote for it, sending their
///   votes to the next leader, whose quorum of them certifies the block
///   and starts the next view. A replica that waits too long gives up on
///   the view; a quorum of give-ups certifies a timeout and starts the
///   next view instead.
/// - **Voting.** A replica votes once in a view at most, and only for a
///   block that extends a certificate at least as new as the newest it
///   knows, its *lock*.
/// - **Commands.** A replica hands the commands submitted to it to the
///   leader who proposes next. A leader proposes only while there is
///   something to decide: commands, or blocks of commands the others have
///   not yet seen decided. An idle committee proposes nothing, and its
///   views time out, ever more slowly.
/// - **Commit.** A block commits when its child, proposed in the very
///   next view, is certified: a quorum then holds a lock on it, and no
///   later certificate can extend anything else. Committing a block
///   commits its ancestors with it.
#[derive(Debug)]
pub struct Core {
    config: Config,
    /// The view this replica is in.
    view: View,
    /// The last view it voted or gave up in.
    voted: View,
    /// The newest certificate it knows.
    lock: Justify,
    /// The last view it proposed in.
    proposed: View,
    /// The certificate of the view before, when that view timed out.
    entered: Option<TimeoutCertificate>,
    /// How many views in a row timed out.
    failures: u32,
    /// The last block it decided, and its view.
    committed: (BlockId, View),
    /// Every block it has the body of.
    blocks: BTreeMap<BlockId, Block>,
    /// Blocks it asked for.
    wanted: BTreeSet<BlockId>,
    /// The blocks certified in each view after the last decided block's.
    /// A view certifies one block at most, unless more than a third of
    /// the committee is faulty.
    certified: BTreeMap<View, BlockId>,
    /// Votes it collects as the next view's leader.
    votes: BTreeMap<View, BTreeMap<BlockId, BTreeMap<NodeId, Signature>>>,
    /// Timeouts of the current view.
    timeouts: BTreeMap<NodeId, (View, Signature)>,
    /// Commands submitted and not yet decided.
    pending: Vec<Vec<u8>>,
    /// Blocks the event in hand taught it, to write down.
    learned: Vec<Block>,
    /// What the event in hand asks of the driver.
    effects: Effects,
}

impl Core {
    /// A replica that never ran: it knows only the genesis block.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::recover(config, Recovered::default())
    }

    /// A replica that ran before and remembers `recovered`. It starts in
    /// the view after its lock's, or in the last view it voted or gave up
    /// in, whichever is later, and does not propose in a view it voted in:
    /// a leader votes for its own block, so it may have proposed there.
    #[must_use]
    pub fn recover(config: Config, recovered: Recovered) -> Self {
        let (safety, blocks, committed) = recovered.into_parts();
        Self {
            config,
            view: View::GENESIS,
            voted: safety.voted(),
            lock: safety.lock().clone(),
            proposed: safety.voted(),
            entered: None,
            failures: 0,
            committed,
            blocks,
            wanted: BTreeSet::new(),
            certified: BTreeMap::new(),
            votes: BTreeMap::new(),
            timeouts: BTreeMap::new(),
            pending: Vec::new(),
            learned: Vec::new(),
            effects: Effects::default(),
        }
    }

    /// The node it runs on.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.config.node
    }

    /// The view it is in.
    #[must_use]
    pub const fn view(&self) -> View {
        self.view
    }

    /// Whom it decides with.
    #[must_use]
    pub const fn committee(&self) -> &Committee {
        &self.config.committee
    }

    /// The leader of the view it is in.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.config.committee.leader(self.view)
    }

    /// The last block it decided.
    #[must_use]
    pub const fn committed(&self) -> BlockId {
        self.committed.0
    }

    /// Handles `event`.
    pub fn handle(&mut self, event: Event) -> Step {
        let before = (self.voted, self.lock.view());
        let committed = self.committed;
        let handled = match event {
            Event::Start => {
                self.start();
                Ok(())
            }
            Event::Received { from, message } => self.receive(from, message),
            Event::Expired(view) => {
                self.expire(view);
                Ok(())
            }
            Event::Submitted(command) => {
                self.hold(vec![command]);
                self.offer(self.lock.block());
                Ok(())
            }
        };
        if let Err(rejection) = handled {
            self.effects.reject(rejection);
        }
        // Ancestors first, so that any prefix of the records makes sense.
        let mut records: Vec<Record> = mem::take(&mut self.learned)
            .into_iter()
            .map(Record::Block)
            .collect();
        if before != (self.voted, self.lock.view()) {
            records.push(Record::Safety(Safety::new(self.voted, self.lock.clone())));
        }
        if committed != self.committed {
            records.push(Record::Committed(self.committed.0, self.committed.1));
        }
        Step::new(records, mem::take(&mut self.effects))
    }

    fn start(&mut self) {
        if self.view == View::GENESIS
            && let Some(first) = self.lock.view().next()
        {
            self.enter(cmp::max(first, self.voted));
        }
    }

    fn receive(&mut self, from: NodeId, message: Message) -> Result<(), Rejection> {
        match message {
            Message::Proposal(proposal) => self.on_proposal(proposal),
            Message::Vote(vote) => self.on_vote(vote),
            Message::Timeout(timeout) => self.on_timeout(timeout),
            Message::Fetch(id) => {
                if let Some(block) = self.blocks.get(&id).filter(|_| from != self.node()) {
                    self.effects
                        .send(To::One(from), Message::Deliver(block.clone()));
                }
                Ok(())
            }
            Message::Skip(certificate) => {
                certificate.verify(&self.config.committee, self.config.domain)?;
                self.timed_out(certificate);
                Ok(())
            }
            Message::Forward(commands) => {
                self.hold(commands);
                // Forwarded no further: the sender may be in another view,
                // and the two could pass commands back and forth for ever.
                if self.leads() {
                    self.propose(self.view);
                }
                Ok(())
            }
            Message::Deliver(block) => {
                if !self.wanted.remove(&block.id()) {
                    return Err(Rejection::Unwanted);
                }
                self.store(block);
                Ok(())
            }
        }
    }

    // --- Views ---

    /// Moves to `view` and, when this replica leads it, proposes.
    fn enter(&mut self, view: View) {
        self.view = view;
        self.timeouts.clear();
        // Votes of the view before can still certify it; older ones
        // cannot move anything on.
        let before = View::new(view.get().saturating_sub(1));
        self.votes = self.votes.split_off(&before);
        self.wait(self.failures);
        self.ask_again();
        self.offer(self.lock.block());
    }

    /// Whether this replica leads the view it is in.
    fn leads(&self) -> bool {
        self.config.committee.leader(self.view) == Some(self.node())
    }

    /// Keeps `commands` until they are decided.
    fn hold(&mut self, commands: Vec<Vec<u8>>) {
        for command in commands {
            if !self.pending.contains(&command) {
                self.pending.push(command);
            }
        }
    }

    /// Proposes in the current view when this replica leads it and has
    /// not yet, or hands the commands it holds that no block proposes yet
    /// to the leader who can propose them next: the current view's, or
    /// once this replica voted in the view, the next view's, who may wait
    /// idle for them. It offers on entering a view, on voting and on each
    /// command submitted.
    fn offer(&mut self, tip: BlockId) {
        if self.view == View::GENESIS {
            // Not started: it enters its first view, and offers, then.
            return;
        }
        if self.leads() && self.proposed < self.view {
            self.propose(self.view);
            return;
        }
        let commands = self.unproposed(tip);
        let view = if self.voted >= self.view {
            self.view.next()
        } else {
            Some(self.view)
        };
        let leader = view
            .and_then(|view| self.config.committee.leader(view))
            .filter(|leader| *leader != self.node());
        if let (false, Some(leader)) = (commands.is_empty(), leader) {
            self.effects
                .send(To::One(leader), Message::Forward(commands));
        }
    }

    /// Waits in the current view, `attempt` timeouts long.
    fn wait(&mut self, attempt: u32) {
        self.effects.arm(Timer::new(self.view, attempt));
    }

    /// Gives up on `view`, if this replica is still in it, and tells the
    /// others. Each time the timer expires it gives up again, asks again
    /// for the blocks it lacks and shows the timeout certificate it entered
    /// the view with, in case the messages were lost: replicas left behind
    /// in the view before then catch up.
    fn expire(&mut self, view: View) {
        if view != self.view {
            return;
        }
        self.voted = self.voted.max(view);
        let timeout = Timeout::sign(
            &self.config.key,
            self.config.domain,
            self.node(),
            view,
            self.lock.clone(),
        );
        self.effects
            .send(To::Others, Message::Timeout(timeout.clone()));
        self.ask_again();
        if let Some(certificate) = &self.entered {
            self.effects
                .send(To::Others, Message::Skip(certificate.clone()));
        }
        self.wait(self.failures.saturating_add(1));
        // Its own timeout is valid: it signed it.
        self.count_timeout(timeout);
    }

    fn on_timeout(&mut self, timeout: Timeout) -> Result<(), Rejection> {
        timeout.verify(&self.config.committee, self.config.domain)?;
        self.observe(timeout.high().clone());
        if timeout.view() != self.view {
            return Err(Rejection::OtherView(timeout.view(), self.view));
        }
        self.count_timeout(timeout);
        Ok(())
    }

    fn count_timeout(&mut self, timeout: Timeout) {
        let view = timeout.view();
        let (voter, high, signature) = timeout.into_entry();
        drop(self.timeouts.insert(voter, (high, signature)));
        if self.timeouts.len() >= self.config.committee.quorum() {
            let entries = mem::take(&mut self.timeouts);
            self.timed_out(TimeoutCertificate::new(view, entries));
        }
    }

    /// Leaves the view a timeout certificate ends.
    fn timed_out(&mut self, certificate: TimeoutCertificate) {
        let Some(next) = certificate.view().next().filter(|next| *next > self.view) else {
            return;
        };
        self.failures = self.failures.saturating_add(1);
        self.entered = Some(certificate);
        self.enter(next);
    }

    // --- Certificates and commits ---

    /// Learns of a certificate: it may be newer than the lock, end the
    /// current view, or commit a block.
    fn observe(&mut self, justify: Justify) {
        let view = justify.view();
        let block = justify.block();
        let lock = mem::replace(&mut self.lock, Justify::Genesis);
        self.lock = cmp::max_by_key(lock, justify, Justify::view);
        let _replaced: Option<BlockId> = self.certified.insert(view, block);
        if !self.blocks.contains_key(&block) {
            self.want(block);
        }
        // Decided first, so that the next view's leader knows what is left
        // to decide.
        self.find_commit();
        if let Some(next) = view.next().filter(|next| *next > self.view) {
            self.failures = 0;
            self.entered = None;
            self.enter(next);
        }
    }

    /// Finds the newest block a two-chain commits: the parent of a
    /// certified block proposed in the view right after the parent's.
    fn find_commit(&mut self) {
        let found = self.certified.iter().rev().find_map(|(view, id)| {
            let child = self.blocks.get(id).filter(|child| child.view() == *view)?;
            let parent = child.justify().view();
            child
                .view()
                .follows(parent)
                .then(|| (child.parent(), parent))
        });
        if let Some((target, view)) = found {
            self.decide(target, view);
        }
    }

    /// Decides `target`, from `view`, and its undecided ancestors, oldest
    /// first, once it has all their bodies.
    fn decide(&mut self, target: BlockId, view: View) {
        let mut chain = Vec::new();
        let mut id = target;
        while id != self.committed.0 {
            let Some(block) = self.blocks.get(&id) else {
                self.want(id);
                return;
            };
            if block.view() <= self.committed.1 {
                // Decided already, or not a descendant of the last decided
                // block, which only a quorum with more than a third faulty
                // can certify.
                return;
            }
            chain.push(id);
            id = block.parent();
        }
        self.committed = (target, view);
        self.certified = view
            .next()
            .map(|next| self.certified.split_off(&next))
            .unwrap_or_default();
        for decided in chain.iter().rev() {
            if let Some(block) = self.blocks.get(decided) {
                self.pending
                    .retain(|command| !block.commands().contains(command));
                self.effects.decide(block.clone());
            }
        }
    }

    /// Asks again for every block it lacks, in case the answers were
    /// lost: once in each view, since views move on without it.
    fn ask_again(&mut self) {
        for id in &self.wanted {
            self.effects.send(To::Others, Message::Fetch(*id));
        }
    }

    fn want(&mut self, id: BlockId) {
        if id != BlockId::GENESIS && self.wanted.insert(id) {
            self.effects.send(To::Others, Message::Fetch(id));
        }
    }

    fn store(&mut self, block: Block) {
        let id = block.id();
        let _known: bool = self.wanted.remove(&id);
        if let Entry::Vacant(entry) = self.blocks.entry(id) {
            self.learned.push(block.clone());
            let _stored: &mut Block = entry.insert(block);
        }
        self.find_commit();
    }

    // --- Proposing and voting ---

    /// Proposes a block in `view`, once, when there is something to
    /// decide: commands no block proposes yet, or blocks of commands the
    /// others have not decided, which only blocks after them show them
    /// decided. A leader with neither leaves its view idle, and the view
    /// times out.
    fn propose(&mut self, view: View) {
        if self.proposed >= view {
            return;
        }
        let commands = self.unproposed(self.lock.block());
        if commands.is_empty() && self.settled() {
            return;
        }
        // It entered the view by a timeout certificate, which then
        // justifies the proposal, or by a certificate of the view before,
        // which its lock now is.
        let timeout = self.entered.clone();
        self.proposed = view;
        let block = Block::new(view, self.lock.clone(), commands);
        let proposal = Proposal::sign(
            &self.config.key,
            self.config.domain,
            self.node(),
            block,
            timeout,
        );
        self.effects
            .send(To::Others, Message::Proposal(proposal.clone()));
        // It votes for its own block like any replica.
        if let Err(rejection) = self.on_proposal(proposal) {
            self.effects.reject(rejection);
        }
    }

    /// Whether the others decided every block of commands the next block
    /// would extend: whether the newest block it extends shows them the
    /// certificate that decides the last of those blocks.
    fn settled(&self) -> bool {
        let Some(shown) = self.shown() else {
            return self.lock.block() == BlockId::GENESIS;
        };
        let mut id = self.lock.block();
        while id != shown {
            match self.blocks.get(&id) {
                Some(block) if block.commands().is_empty() => id = block.parent(),
                // A block it lacks might hold commands.
                Some(_) | None => return false,
            }
        }
        true
    }

    /// The block that the certificate in the newest certified block
    /// decides, for whoever sees that block: the parent of the block it
    /// certifies, when that one follows its own certificate's view.
    fn shown(&self) -> Option<BlockId> {
        let tip = self.blocks.get(&self.lock.block())?;
        let certified = self.blocks.get(&tip.justify().block())?;
        certified
            .view()
            .follows(certified.justify().view())
            .then(|| certified.parent())
    }

    /// The pending commands that neither `tip` nor its undecided ancestors
    /// hold.
    fn unproposed(&self, tip: BlockId) -> Vec<Vec<u8>> {
        let mut proposed = BTreeSet::new();
        let mut id = tip;
        while let Some(block) = self.blocks.get(&id).filter(|_| id != self.committed.0) {
            proposed.extend(block.commands().iter());
            id = block.parent();
        }
        self.pending
            .iter()
            .filter(|command| !proposed.contains(command))
            .cloned()
            .collect()
    }

    fn on_proposal(&mut self, proposal: Proposal) -> Result<(), Rejection> {
        let (block, timeout) = proposal.open(&self.config.committee, self.config.domain)?;
        self.observe(block.justify().clone());
        if let Some(certificate) = timeout {
            self.timed_out(certificate);
        }
        let (view, id, extends) = (block.view(), block.id(), block.justify().view());
        self.store(block);
        self.check_vote(view, extends)?;
        self.voted = view;
        let vote = Vote::sign(&self.config.key, self.config.domain, self.node(), view, id);
        let counted = match view
            .next()
            .and_then(|next| self.config.committee.leader(next))
        {
            Some(leader) if leader == self.node() => self.on_vote(vote),
            Some(leader) => {
                self.effects.send(To::One(leader), Message::Vote(vote));
                Ok(())
            }
            None => Ok(()),
        };
        // The commands this block leaves out go with the vote.
        self.offer(id);
        counted
    }

    /// Whether this replica may vote for a valid proposal in `view` that
    /// extends a certificate from `extends`: in its own view, once, and
    /// not against its lock.
    fn check_vote(&self, view: View, extends: View) -> Result<(), Rejection> {
        if view != self.view {
            return Err(Rejection::OtherView(view, self.view));
        }
        if self.voted >= view {
            return Err(Rejection::Voted(view));
        }
        if extends < self.lock.view() {
            return Err(Rejection::Unsafe(extends, self.lock.view()));
        }
        Ok(())
    }

    fn on_vote(&mut self, vote: Vote) -> Result<(), Rejection> {
        let view = vote.view();
        let next = view.next();
        if next.and_then(|next| self.config.committee.leader(next)) != Some(self.node()) {
            return Err(Rejection::Misdirected(view));
        }
        if next.is_none_or(|next| next < self.view) {
            return Err(Rejection::OtherView(view, self.view));
        }
        vote.verify(&self.config.committee, self.config.domain)?;
        let block = vote.block();
        let voter = vote.voter();
        let ballot = self
            .votes
            .entry(view)
            .or_default()
            .entry(block)
            .or_default();
        drop(ballot.insert(voter, vote.into_signature()));
        if ballot.len() >= self.config.committee.quorum() {
            let certificate = Certificate::new(view, block, ballot.clone());
            self.observe(Justify::Certified(certificate));
        }
        Ok(())
    }
}

#[cfg(test)]
mod sim;
#[cfg(test)]
mod tests;
