//! One replica's part in the protocol, as a deterministic state machine:
//! events in, a [`Step`] out. It reads no clock, opens no socket and
//! writes no file, so a simulation and a real node drive the same code.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::{cmp, mem};

use oleaph_consensus::NodeId;
use oleaph_crypto::{Signature, SigningKey};

use crate::block::{Block, Order, Proposal, prune};
use crate::checkpoint::{Boundary, Checkpoint, CheckpointCertificate, Share, Succession, Tally};
use crate::committee::{ChangeCertificate, Committee, Domain};
use crate::message::Message;
use crate::rejection::Rejection;
use crate::step::Recovered;
use crate::step::{Effects, Record, Safety, Step, Timer, To};
use crate::timeout::{Timeout, TimeoutCertificate};
use crate::view::Epoch;
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
    /// `domain`: the charter's committee and domain, in the first epoch.
    /// A replica that ran before goes on in the epoch its journal
    /// recorded.
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
    /// The replica's state reached a checkpoint, for the committee to
    /// certify.
    Reached(Checkpoint),
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
/// - **Epochs.** A committee hands the shard on to the next by deciding
///   a block holding a [`ChangeCertificate`] it admits. It decides
///   nothing after that block, and votes no more: once it certifies the
///   checkpoint at that block, the next committee starts from that state,
///   in the first view and a domain of its own.
#[derive(Debug)]
pub struct Core {
    /// Who it is, and the committee and domain of the epoch it decides
    /// in.
    config: Config,
    /// How each epoch since the charter's succeeded the one before,
    /// oldest first: the last started the epoch it decides in.
    eras: Vec<Succession>,
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
    timeouts: BTreeMap<NodeId, Timeout>,
    /// Commands submitted and not yet decided.
    pending: Vec<Vec<u8>>,
    /// The certified checkpoint its blocks start after: those before it
    /// it let go, its state kept instead.
    base: Option<CheckpointCertificate>,
    /// The checkpoint shares it collects.
    tally: Tally,
    /// The last checkpoint share it signed.
    shared: Option<Share>,
    /// The change it decided, which ends the epoch at the last decided
    /// block.
    ending: Option<ChangeCertificate>,
    /// Those it showed how its epochs succeeded, since its view began or
    /// its timer last fired: once in that time each.
    handed: BTreeSet<NodeId>,
    /// Blocks the event in hand taught it, to write down.
    learned: Vec<Block>,
    /// What the event in hand asks of the driver.
    effects: Effects,
}

/// The state a replica that follows a handoff must take before it moves
/// on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Following {
    /// The state at the end of the epoch it ends, which starts the next.
    End(CheckpointCertificate),
    /// The state at a checkpoint that the committee of the epoch this
    /// succession starts certified, past the start.
    Past(Box<Succession>, CheckpointCertificate),
}

impl Core {
    /// A replica that never ran: it knows only the genesis block.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::recover(config, Recovered::default())
    }

    /// A replica that ran before and remembers `recovered`. It starts in
    /// the view after its lock's, or in the last view it voted or gave up
    /// in, whichever is later, and does not propose again in a view it
    /// proposed in.
    #[must_use]
    pub fn recover(config: Config, recovered: Recovered) -> Self {
        let (safety, blocks, committed, certified, eras) = recovered.into_parts();
        let mut core = Self {
            config,
            eras: Vec::new(),
            view: View::GENESIS,
            voted: safety.voted(),
            lock: safety.lock().clone(),
            proposed: safety.proposed(),
            entered: None,
            failures: 0,
            committed,
            blocks,
            wanted: BTreeSet::new(),
            certified: BTreeMap::new(),
            votes: BTreeMap::new(),
            timeouts: BTreeMap::new(),
            pending: Vec::new(),
            base: certified.clone(),
            tally: Tally::since(certified),
            shared: None,
            ending: None,
            handed: BTreeSet::new(),
            learned: Vec::new(),
            effects: Effects::default(),
        };
        for succession in eras {
            core.adopt(succession);
        }
        // The decided chain stops at a change, so only the last decided
        // block can hold one.
        core.ending = core
            .blocks
            .get(&core.committed.0)
            .and_then(|block| core.change_in(block));
        core
    }

    /// Moves on to the epoch that `succession` starts.
    fn adopt(&mut self, succession: Succession) {
        self.config.committee = succession.change().committee().clone();
        self.config.domain = succession.domain();
        self.eras.push(succession);
    }

    /// The first change in `block` that the committee can make, if any.
    fn change_in(&self, block: &Block) -> Option<ChangeCertificate> {
        block
            .commands()
            .iter()
            .find_map(|command| match Order::decode(command) {
                Ok(Order::Change(change)) => self.admits(&change).is_ok().then_some(change),
                Ok(_) | Err(_) => None,
            })
    }

    /// Moves on to `certificate`, a certified checkpoint whose state the
    /// replica took from the others instead of deciding the blocks up to
    /// it, and returns the records the journal needs, as
    /// [`Core::compact`] does. The blocks after it it decides as usual.
    pub(crate) fn rebase(&mut self, certificate: &CheckpointCertificate) -> Vec<Record> {
        self.committed = cmp::max_by_key(
            self.committed,
            (certificate.block(), certificate.view()),
            |(_, view)| *view,
        );
        self.certified = self
            .committed
            .1
            .next()
            .map(|next| self.certified.split_off(&next))
            .unwrap_or_default();
        let _newer = self.tally.adopt(certificate);
        self.compact(certificate)
    }

    /// Forgets the blocks that `certificate`, a certified checkpoint whose
    /// state the replica keeps, makes no longer matter, and returns the
    /// records that the journal needs to remember all it still knows:
    /// with these written, every record before them can go.
    pub(crate) fn compact(&mut self, certificate: &CheckpointCertificate) -> Vec<Record> {
        prune(&mut self.blocks, certificate.block(), certificate.view());
        self.base = Some(certificate.clone());
        // The epochs first, written again so that the journal can forget
        // the records that started them.
        let mut records: Vec<Record> = self.eras.iter().cloned().map(Record::Epoch).collect();
        records.extend(self.blocks.values().cloned().map(Record::Block));
        records.push(Record::Safety(self.safety()));
        records.push(Record::Committed(self.committed.0, self.committed.1));
        records.push(Record::Checkpoint(certificate.clone()));
        records
    }

    /// Starts the epoch that the change it decided begins, once the
    /// committee certified `certificate`, the checkpoint at the block that
    /// holds the change, and returns the records the journal needs, as
    /// [`Core::compact`] does. The commands it holds wait for the new
    /// committee. It returns nothing, and changes nothing, unless the
    /// checkpoint is that one.
    pub(crate) fn succeed(&mut self, certificate: &CheckpointCertificate) -> Option<Vec<Record>> {
        if certificate.block() != self.committed.0 {
            return None;
        }
        let change = self.ending.clone()?;
        if certificate.boundary() != Boundary::Ends(self.config.domain.next(&change)) {
            return None;
        }
        let succession = Succession::new(change, certificate.clone())?;
        self.begin(succession);
        // The checkpoint first: the state the new epoch starts from must
        // be on record before the epoch is.
        let mut records = vec![Record::Checkpoint(certificate.clone())];
        records.extend(self.compact(certificate));
        Some(records)
    }

    /// Starts the epoch `succession` starts: afresh, in the first view of
    /// a committee of its own, with only the commands it holds kept.
    fn begin(&mut self, succession: Succession) {
        self.tally = Tally::since(Some(succession.end().clone()));
        self.adopt(succession);
        self.view = View::GENESIS;
        self.voted = View::GENESIS;
        self.lock = Justify::Genesis;
        self.proposed = View::GENESIS;
        self.entered = None;
        self.failures = 0;
        self.committed = (BlockId::GENESIS, View::GENESIS);
        self.blocks.clear();
        self.wanted.clear();
        self.certified.clear();
        self.votes.clear();
        self.timeouts.clear();
        self.shared = None;
        self.ending = None;
        self.handed.clear();
    }

    /// Follows `successions` past the epoch it decides in, as a member of
    /// a later epoch sent them with `newest`, the newest checkpoint that
    /// member's committee certified: it checks each against the committee
    /// before it, starts every epoch but the last, and ends the one it is
    /// then in at the last one's change, as if it had decided it. It
    /// returns the state the replica must take before it moves on, or
    /// nothing when none of them is new.
    ///
    /// # Errors
    ///
    /// When one does not follow from the committee before it.
    pub(crate) fn follow(
        &mut self,
        successions: &[Succession],
        newest: Option<&CheckpointCertificate>,
    ) -> Result<Option<Following>, Rejection> {
        let epoch = self.epoch();
        let new: Vec<&Succession> = successions
            .iter()
            .filter(|succession| succession.epoch() > epoch)
            .collect();
        let Some((last, before)) = new.split_last() else {
            return Ok(None);
        };
        // Every one checked before any is taken on.
        let mut committee = self.config.committee.clone();
        let mut domain = self.config.domain;
        let mut at = epoch;
        for succession in &new {
            succession.verify(&committee, domain, at)?;
            committee = succession.change().committee().clone();
            domain = succession.domain();
            at = succession.epoch();
        }
        for succession in before {
            self.begin((*succession).clone());
        }
        self.ending = Some(last.change().clone());
        // Once the last committee certified a checkpoint of its own, the
        // state at the end may be gone: the replica takes the newer one.
        let newer = newest.filter(|certificate| {
            certificate.verify(&committee, domain).is_ok() && certificate.supersedes(last.end())
        });
        Ok(Some(newer.map_or_else(
            || Following::End(last.end().clone()),
            |certificate| Following::Past(Box::new((*last).clone()), certificate.clone()),
        )))
    }

    /// Starts the epoch `succession` starts, past its start: at
    /// `certificate`, a checkpoint its committee certified, whose state the
    /// replica took from that committee. It returns the records the
    /// journal needs, as [`Core::compact`] does.
    pub(crate) fn leap(
        &mut self,
        succession: Succession,
        certificate: &CheckpointCertificate,
    ) -> Vec<Record> {
        self.begin(succession);
        self.rebase(certificate)
    }

    /// Shows `to`, who sent what this epoch's committee cannot have
    /// signed, how its epochs succeeded and the newest checkpoint it
    /// knows, once a timeout at most: `to` may have missed the end of an
    /// epoch, or this replica may have, and `to` answers.
    fn hand_off(&mut self, to: NodeId) {
        if self.handed.insert(to) {
            self.effects.send(
                To::One(to),
                Message::Handoff(self.eras.clone(), self.base.clone()),
            );
        }
    }

    /// What it must not forget across a crash.
    fn safety(&self) -> Safety {
        Safety::new(self.voted, self.proposed, self.lock.clone())
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

    /// The epoch it decides in.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.eras.last().map_or(Epoch::FIRST, Succession::epoch)
    }

    /// The change it decided, which ends its epoch, if it did.
    #[cfg(test)]
    pub(crate) const fn ending(&self) -> Option<&ChangeCertificate> {
        self.ending.as_ref()
    }

    /// Checks that the committee can make `change`.
    ///
    /// # Errors
    ///
    /// As [`ChangeCertificate::verify`], for the committee of its epoch.
    pub fn admits(&self, change: &ChangeCertificate) -> Result<(), Rejection> {
        change.verify(&self.config.committee, self.config.domain, self.epoch())
    }

    /// Checks that the committee made `change`, a change it decided: the
    /// one that ends its epoch.
    ///
    /// # Errors
    ///
    /// As [`Core::admits`], or [`Rejection::Ended`] for a change decided
    /// after the one that ends the epoch.
    pub(crate) fn made(&self, change: &ChangeCertificate) -> Result<(), Rejection> {
        if self.ending.as_ref() == Some(change) {
            return Ok(());
        }
        self.admits(change)
            .and_then(|()| Err(Rejection::Ended(self.epoch())))
    }

    /// Handles `event`.
    pub fn handle(&mut self, event: Event) -> Step {
        let before = (self.voted, self.proposed, self.lock.view());
        let committed = self.committed;
        let sender = match &event {
            Event::Received { from, .. } => Some(*from),
            Event::Start | Event::Expired(_) | Event::Submitted(_) | Event::Reached(_) => None,
        };
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
            Event::Reached(checkpoint) => {
                let checkpoint = self.bound(checkpoint);
                let share = Share::sign(
                    &self.config.key,
                    self.config.domain,
                    self.node(),
                    checkpoint,
                );
                self.effects
                    .send(To::Others, Message::Checkpoint(share.clone()));
                self.shared = Some(share.clone());
                self.count(share);
                Ok(())
            }
        };
        if let Err(rejection) = handled {
            // Signed by its sender, but not in this epoch's domain.
            if let (Some(from), Rejection::Forged(signer) | Rejection::Stranger(signer)) =
                (sender, rejection)
                && signer == from
            {
                self.hand_off(from);
            }
            self.effects.reject(rejection);
        }
        // Ancestors first, so that any prefix of the records makes sense.
        let mut records: Vec<Record> = mem::take(&mut self.learned)
            .into_iter()
            .map(Record::Block)
            .collect();
        if before != (self.voted, self.proposed, self.lock.view()) {
            records.push(Record::Safety(self.safety()));
        }
        if committed != self.committed {
            records.push(Record::Committed(self.committed.0, self.committed.1));
        }
        Step::new(records, mem::take(&mut self.effects))
    }

    fn start(&mut self) {
        // Stopped at the end of its epoch: it takes the state there again,
        // and moves on if the committee certified it already.
        if self.ending.is_some() {
            self.effects.end(self.committed.0);
            if let Some(base) = self
                .base
                .clone()
                .filter(|base| base.block() == self.committed.0)
            {
                self.effects.certify(base);
            }
        }
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
                if from == self.node() {
                    return Ok(());
                }
                // A block it no longer holds may be one a checkpoint let
                // go: the asker can take the state at it instead.
                let answer = self.blocks.get(&id).map_or_else(
                    || self.base.clone().map(Message::Certified),
                    |block| Some(Message::Deliver(block.clone())),
                );
                if let Some(answer) = answer {
                    self.effects.send(To::One(from), answer);
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
            Message::Checkpoint(share) => {
                share.verify(&self.config.committee, self.config.domain)?;
                self.count(share);
                Ok(())
            }
            Message::Certified(certificate) => {
                certificate.verify(&self.config.committee, self.config.domain)?;
                if self.tally.adopt(&certificate) {
                    self.effects.certify(certificate);
                }
                Ok(())
            }
            // The replica follows the epochs a handoff shows: what is left
            // is to answer one from a replica further behind.
            Message::Handoff(successions, _) => {
                let theirs = successions.last().map_or(Epoch::FIRST, Succession::epoch);
                if theirs < self.epoch() {
                    self.hand_off(from);
                }
                Ok(())
            }
            // The state at a checkpoint is the replica's to send and take.
            Message::Want(..) | Message::Manifest(..) | Message::Chunk(..) => {
                Err(Rejection::Unwanted)
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

    /// `checkpoint`, saying whether the epoch ends at its block: it does
    /// at the block that holds the change the replica decided, and the
    /// next epoch decides in the domain that change leads to.
    fn bound(&self, checkpoint: Checkpoint) -> Checkpoint {
        match &self.ending {
            Some(change) if checkpoint.block() == self.committed.0 => {
                checkpoint.ending(self.config.domain.next(change))
            }
            _ => checkpoint,
        }
    }

    /// Counts a valid checkpoint share, and has the driver take in the
    /// certificate it completes.
    fn count(&mut self, share: Share) {
        if let Some(certificate) = self.tally.count(share, self.config.committee.quorum()) {
            self.effects.certify(certificate);
        }
    }

    // --- Views ---

    /// Moves to `view` and, when this replica leads it, proposes.
    fn enter(&mut self, view: View) {
        self.view = view;
        self.timeouts.clear();
        self.handed.clear();
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
        if self.view == View::GENESIS || self.ending.is_some() {
            // Not started: it enters its first view, and offers, then. Or
            // ended: the next committee decides the commands.
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
        // A handoff may have been lost, and a committee that needs the
        // replica it went to stays in its view.
        self.handed.clear();
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
        // No later checkpoint makes up for a lost share of the one that
        // ends the epoch. Its timeout shows the others its lock, which
        // decides the change for those that missed it.
        if let (Some(_), Some(share)) = (&self.ending, &self.shared) {
            self.effects
                .send(To::Others, Message::Checkpoint(share.clone()));
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
        drop(self.timeouts.insert(timeout.voter(), timeout));
        if self.timeouts.len() >= self.config.committee.quorum() {
            let timeouts = mem::take(&mut self.timeouts).into_values();
            self.timed_out(TimeoutCertificate::gather(view, timeouts));
        }
    }

    /// Leaves the view a timeout certificate ends.
    fn timed_out(&mut self, certificate: TimeoutCertificate) {
        // The newest certificate a quorum knew, so that it proposes on a
        // block every honest signer can vote for if it leads next.
        self.observe(certificate.high().clone());
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
            child.view().follows(parent).then(|| child.parent())
        });
        if let Some(target) = found {
            self.decide(target);
        }
    }

    /// Decides `target` and its undecided ancestors, oldest first, once it
    /// has all their bodies.
    /// A change ends the chain: it decides nothing after the first block
    /// that holds one the committee admits.
    fn decide(&mut self, target: BlockId) {
        if self.ending.is_some() {
            return;
        }
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
            id = block.parent();
            chain.push(block.clone());
        }
        let mut decided = Vec::new();
        for block in chain.into_iter().rev() {
            self.ending = self.change_in(&block);
            if self.ending.is_some() {
                self.effects.end(block.id());
                decided.push(block);
                break;
            }
            decided.push(block);
        }
        let Some(last) = decided.last() else {
            return;
        };
        self.committed = (last.id(), last.view());
        self.certified = last
            .view()
            .next()
            .map(|next| self.certified.split_off(&next))
            .unwrap_or_default();
        for block in decided {
            self.pending
                .retain(|command| !block.commands().contains(command));
            self.effects.decide(block);
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
        if self.proposed >= view || self.ending.is_some() {
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
        if self.ending.is_some() {
            return Err(Rejection::Ended(self.epoch()));
        }
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
#[cfg(test)]
mod trace;
