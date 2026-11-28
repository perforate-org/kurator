//! A replica of a shard as a node runs it: the [`Core`] driven by the
//! messages and timers of its [`Host`], each step written down by its
//! [`Journal`] before it acts, and the decided blocks applied by its
//! [`Ledger`]. It is the shard's [`Replicated`] replica for the layers
//! above consensus.
//!
//! Every replica takes writes: a command goes from the replica it was
//! given to on to the leader who proposes next, and returns once a block
//! that holds it is decided and applied. A committee with nothing to
//! decide proposes nothing, and its views time out. Reads see the
//! replica's own decided state ([`Reads::Decided`]).

use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::ops::Bound;
use std::time::Duration;

use futures_channel::oneshot;
use oleaph_consensus::{
    Duty, Error, Members, NodeId, Peer, Reads, Replicated, Result, Route, Transaction,
};
use oleaph_shard::{Applied, Shard};
use oleaph_storage::{Command, Draft, Finished, KeyValue, KvState, Outcome, Version};
use parking_lot::Mutex;

use crate::block::{Block, Order};
use crate::checkpoint::{
    Checkpoint, CheckpointCertificate, Height, Manifest, Own, Part, Succession, Verdict,
};
use crate::committee::ChangeCertificate;
use crate::core::{Core, Event};
use crate::message::Message;
use crate::step::{Act, Record, Timer, To};
use crate::view::Epoch;
use crate::view::View;
use crate::vote::BlockId;

use self::catchup::Catchup;
pub use self::journal::Journal;
pub use self::ledger::{Ledger, Settled};
use self::states::Snapshot;

mod catchup;
mod journal;
mod ledger;
mod states;

/// How long a view's timer runs before its first timeout. Each timeout
/// in a row doubles it, up to eight times as long.
const BASE_TIMEOUT: Duration = Duration::from_secs(1);

/// What a replica needs from the node it runs on.
///
/// Both calls are made while the replica handles an event, so they must
/// not block or call back into the replica: queue the message, arm the
/// timer, and return.
pub trait Host: Send + Sync {
    /// Sends `message` to `to`. Delivery may fail: the protocol asks again
    /// for what it needs.
    fn send(&self, to: NodeId, message: &Message);

    /// Calls [`Replica::expire`] with `view` once `after` has passed. A
    /// later call replaces the timer.
    fn wake(&self, view: View, after: Duration);
}

/// Whether a replica still runs.
#[derive(Debug)]
enum Health {
    Running,
    /// Its journal failed to write: it might forget what it would send,
    /// so it sends nothing until it is opened again.
    Stopped,
    /// Its state differs from the one the committee certified at the
    /// same height: it is not the shard's, and serves nothing more.
    Diverged(Checkpoint),
    /// The committee of the epoch has no place for it: it decides no
    /// more.
    Retired(Epoch),
}

impl Health {
    /// Why it no longer runs, if it does not.
    fn check(&self) -> Result<()> {
        match self {
            Self::Running => Ok(()),
            Self::Stopped => Err(Error::Failed(
                "the replica stopped: its journal failed to write".into(),
            )),
            Self::Diverged(mine) => Err(Error::Failed(
                format!(
                    "the replica stopped: its state at {} is not the one the committee certified",
                    mine.height()
                )
                .into(),
            )),
            Self::Retired(epoch) => Err(Error::Failed(
                format!("the replica retired: the committee of {epoch} leaves it out").into(),
            )),
        }
    }
}

#[derive(Debug)]
struct Inner {
    core: Core,
    journal: Journal,
    ledger: Ledger,
    /// The commands submitted here, by their bytes, with whoever waits for
    /// each to apply.
    waiting: BTreeMap<Vec<u8>, Vec<oneshot::Sender<Settled>>>,
    /// The checkpoints its state reached, for the core to share.
    /// The same checkpoint reached twice, by the interval and by the end
    /// of the epoch, is shared once.
    reached: BTreeSet<Checkpoint>,
    /// Those past the newest certified one, to judge against the
    /// certificates to come.
    own: Own,
    /// The newest checkpoint the committee certified.
    certified: Option<CheckpointCertificate>,
    /// The state at it, as the replica takes it from the others when the
    /// blocks up to it are gone.
    catchup: Option<Catchup>,
    health: Health,
}

impl Inner {
    /// Tells whoever waits for `command` how it settled. A waiter that gave
    /// up no longer listens.
    fn tell(&mut self, command: &[u8], settled: &Settled) {
        for waiter in self.waiting.remove(command).unwrap_or_default() {
            let _gone = waiter.send(settled.clone());
        }
    }

    /// Judges the replica's own checkpoint at the height of the certified
    /// one: it stops if its state differs, and forgets what came before
    /// if it agrees.
    fn judge(&mut self) {
        let Some(certified) = self.certified.clone() else {
            return;
        };
        match self.own.judge(&certified) {
            Verdict::Unknown => {}
            Verdict::Agrees => {
                let records = self.core.compact(&certified);
                self.settle(&certified, records);
            }
            Verdict::Differs(mine) => self.halt(Health::Diverged(mine)),
        }
    }

    /// Keeps `snapshot`, the state at a checkpoint the decided blocks
    /// reached, for the core to share and the committee to certify.
    fn reach(&mut self, snapshot: &Snapshot) {
        let Ok(checkpoint) = self.journal.keep(snapshot) else {
            self.halt(Health::Stopped);
            return;
        };
        self.own.take(checkpoint);
        let _new = self.reached.insert(checkpoint);
        self.judge();
    }

    /// What became of `command`, which `settled` as the ledger saw it: a
    /// change is made only if it is the one the core decided ends the
    /// epoch.
    fn verdict(&self, command: &[u8], settled: Settled) -> Settled {
        let Settled::Change(epoch) = settled else {
            return settled;
        };
        let Ok(Order::Change(change)) = Order::decode(command) else {
            return Settled::Garbled;
        };
        self.core
            .made(&change)
            .map_or_else(Settled::Refused, |()| Settled::Change(epoch))
    }

    /// The newest certified checkpoint, if the replica's state is behind
    /// it.
    fn behind(&self) -> Option<&CheckpointCertificate> {
        self.certified
            .as_ref()
            .filter(|certified| certified.height() > self.ledger.height())
    }

    /// Starts taking the state at the newest certified checkpoint from the
    /// others, if it is behind it and not taking it already: a member told
    /// it the blocks up to it are gone.
    fn catch_up(&mut self) {
        let Some(certified) = self.behind() else {
            return;
        };
        if self
            .catchup
            .as_ref()
            .is_none_or(|catchup| catchup.height() < certified.height())
        {
            self.catchup = Some(Catchup::new(certified.clone()));
        }
    }

    /// Asks again for what it is still taking, or stops taking it once its
    /// own blocks brought it as far.
    fn retry(&mut self) {
        if self.behind().is_none() {
            self.catchup = None;
        }
        if let Some(catchup) = self.catchup.as_mut() {
            catchup.again();
        }
    }

    /// What to ask the others for next, while it catches up.
    fn asks(&mut self) -> Vec<(NodeId, Message)> {
        if self.health.check().is_err() {
            return Vec::new();
        }
        let others = self.others();
        self.catchup
            .as_mut()
            .map(|catchup| catchup.asks(&others))
            .unwrap_or_default()
    }

    /// The other members of the committee.
    fn others(&self) -> Vec<NodeId> {
        let node = self.core.node();
        self.core
            .committee()
            .members()
            .filter(|member| *member != node)
            .collect()
    }

    /// Takes in the manifest of the state it catches up to.
    fn take_manifest(&mut self, height: Height, manifest: Manifest) {
        if let Some(catchup) = self.catchup.as_mut() {
            let _named = catchup.manifest(height, manifest);
        }
    }

    /// Takes in a chunk of the state it catches up to, and moves on to
    /// that state once it has every chunk.
    fn take_chunk(&mut self, height: Height, index: u32, bytes: Vec<u8>) {
        let Some(mut catchup) = self.catchup.take() else {
            return;
        };
        let _named = catchup.chunk(height, index, bytes);
        match catchup.finish() {
            Ok((certificate, encoded)) => self.install(&certificate, &encoded),
            Err(catchup) => self.catchup = Some(*catchup),
        }
    }

    /// Moves on to `encoded`, the state that `certificate` certifies,
    /// as if it had decided the blocks up to it. The commands that wait
    /// may or may not be among those blocks: their waiters learn the
    /// outcome is unknown.
    fn install(&mut self, certificate: &CheckpointCertificate, encoded: &[u8]) {
        // A state no further than its own, or one that does not decode: a
        // quorum of the committee is faulty, and the replica waits for
        // its own blocks.
        let Ok(snapshot) = self.ledger.restart(certificate, encoded) else {
            return;
        };
        let kept = self.journal.keep(&snapshot);
        if !kept.is_ok_and(|checkpoint| certificate.certifies(&checkpoint)) {
            self.halt(Health::Stopped);
            return;
        }
        let records = self.core.rebase(certificate);
        self.settle(certificate, records);
        let _judged = self.own.judge(certificate);
        self.waiting.clear();
    }

    /// Writes down that the replica keeps the state `certificate`
    /// certifies, with `records`, all the core still knows, and moves on
    /// to the next epoch if the checkpoint ends this one: it retires if
    /// the next committee leaves it out.
    fn settle(&mut self, certificate: &CheckpointCertificate, compacted: Vec<Record>) {
        let (records, succeeded) = match self.core.succeed(certificate) {
            Some(records) => (records, Some(self.core.epoch())),
            None => (compacted, None),
        };
        if self.journal.compact(certificate, &records).is_err() {
            self.halt(Health::Stopped);
            return;
        }
        let node = self.core.node();
        if let Some(epoch) = succeeded
            && self.core.committee().key(node).is_none()
        {
            self.halt(Health::Retired(epoch));
        }
    }

    /// Follows `successions`, sent by a member of a later epoch, and
    /// takes the state the last of them starts from, unless it holds it.
    fn follow(&mut self, successions: &[Succession]) {
        let Ok(Some(end)) = self.core.follow(successions) else {
            return;
        };
        self.certified = Some(end);
        // It decided up to the end itself, and only missed the
        // certificate, or it takes the state from the others.
        self.judge();
        self.catch_up();
    }

    /// Answers a replica catching up with `part` of the state kept at
    /// `height`, if it still keeps it.
    fn serve(&self, height: Height, part: Part) -> Option<Message> {
        match part {
            Part::Manifest => self
                .journal
                .manifest(height)
                .ok()
                .map(|manifest| Message::Manifest(height, manifest)),
            Part::Chunk(index) => self
                .journal
                .chunk(height, index)
                .ok()
                .flatten()
                .map(|bytes| Message::Chunk(height, index, bytes)),
        }
    }

    /// Stops the replica: whoever waits learns it stopped.
    fn halt(&mut self, health: Health) {
        self.health = health;
        self.waiting.clear();
    }

    /// The shard and the version of its decided state.
    fn position(&self) -> (Shard, Version) {
        (self.ledger.shard().clone(), self.ledger.kv().version())
    }
}

/// A replica carrying out its effects on its host.
struct Acting<'replica, H> {
    host: &'replica H,
    inner: &'replica mut Inner,
    /// The other members of the committee the step was taken in, which
    /// its messages are for even once the next committee takes over.
    others: Vec<NodeId>,
}

impl<H: Host> Act for Acting<'_, H> {
    fn decide(&mut self, block: &Block) {
        let outcomes = self.inner.ledger.apply(block);
        for (command, settled) in block.commands().iter().zip(outcomes) {
            let settled = self.inner.verdict(command, settled);
            self.inner.tell(command, &settled);
        }
        // A state that does not encode never took its keys in.
        if let Ok(Some(snapshot)) = self.inner.ledger.checkpoint() {
            self.inner.reach(&snapshot);
        }
    }

    fn end(&mut self, block: BlockId) {
        let Ok(snapshot) = self.inner.ledger.snapshot() else {
            return;
        };
        // The state is at the block that ends the epoch, decided first.
        if snapshot.checkpoint().block() != block {
            return;
        }
        self.inner.reach(&snapshot);
    }

    fn certify(&mut self, certificate: &CheckpointCertificate) {
        self.inner.certified = Some(certificate.clone());
        self.inner.judge();
    }

    fn send(&mut self, to: To, message: &Message) {
        match to {
            To::One(to) => self.host.send(to, message),
            To::Others => self
                .others
                .iter()
                .for_each(|member| self.host.send(*member, message)),
        }
    }

    fn wait(&mut self, timer: Timer) {
        let doubling = 1_u32 << timer.attempt().min(3);
        self.host
            .wake(timer.view(), BASE_TIMEOUT.saturating_mul(doubling));
    }
}

/// A replica of one shard, replicated by Byzantine fault tolerant
/// consensus.
#[derive(Debug)]
pub struct Replica<H> {
    host: H,
    peers: BTreeMap<NodeId, Peer>,
    inner: Mutex<Inner>,
}

impl<H: Host> Replica<H> {
    /// Starts `core`, which `journal` recovered, with `ledger` holding the
    /// state its decided blocks made, on `host`. `peers` are how the
    /// layers above reach the committee's members.
    pub fn start(
        core: Core,
        journal: Journal,
        ledger: Ledger,
        peers: BTreeMap<NodeId, Peer>,
        host: H,
    ) -> Self {
        let replica = Self {
            host,
            peers,
            inner: Mutex::new(Inner {
                core,
                journal,
                ledger,
                waiting: BTreeMap::new(),
                reached: BTreeSet::new(),
                own: Own::default(),
                certified: None,
                catchup: None,
                health: Health::Running,
            }),
        };
        replica.handle(&mut replica.inner.lock(), Event::Start);
        replica
    }

    /// Takes in `message`, which the transport authenticated as `from`'s.
    pub fn receive(&self, from: NodeId, message: Message) {
        self.take_in(&mut self.inner.lock(), from, message);
    }

    /// Gives up on `view` when the replica is still in it, and asks again
    /// for the state it catches up to.
    pub fn expire(&self, view: View) {
        let inner = &mut self.inner.lock();
        self.handle(inner, Event::Expired(view));
        inner.retry();
        self.ask(inner);
    }

    /// Takes in `message` from `from`: the parts of a checkpoint's state
    /// here, the rest in the core.
    fn take_in(&self, inner: &mut Inner, from: NodeId, message: Message) {
        if inner.health.check().is_err() {
            return;
        }
        match message {
            Message::Want(height, part) => {
                if let Some(answer) = inner.serve(height, part) {
                    self.host.send(from, &answer);
                }
            }
            Message::Manifest(height, manifest) => inner.take_manifest(height, manifest),
            Message::Chunk(height, index, bytes) => inner.take_chunk(height, index, bytes),
            // Sent for a block the member let go: what came before it is
            // to be taken as state.
            message @ Message::Certified(_) => {
                self.handle(inner, Event::Received { from, message });
                inner.catch_up();
            }
            Message::Handoff(successions) => inner.follow(&successions),
            message @ (Message::Proposal(_)
            | Message::Vote(_)
            | Message::Timeout(_)
            | Message::Fetch(_)
            | Message::Deliver(_)
            | Message::Skip(_)
            | Message::Forward(_)
            | Message::Checkpoint(_)) => self.handle(inner, Event::Received { from, message }),
        }
        self.ask(inner);
    }

    /// Sends the others what the replica asks them for while it catches
    /// up.
    fn ask(&self, inner: &mut Inner) {
        for (to, message) in inner.asks() {
            self.host.send(to, &message);
        }
    }

    /// Handles `event`, writes down what it changed, and only then acts.
    fn handle(&self, inner: &mut Inner, event: Event) {
        if inner.health.check().is_err() {
            return;
        }
        let step = inner.core.handle(event);
        if let Ok(effects) = inner.journal.persist(step) {
            let others = inner.others();
            effects.carry_out(&mut Acting {
                host: &self.host,
                inner,
                others,
            });
        } else {
            inner.halt(Health::Stopped);
        }
        // The core shares the checkpoints the decided blocks reached.
        for checkpoint in mem::take(&mut inner.reached) {
            self.handle(inner, Event::Reached(checkpoint));
        }
        // A new epoch starts in its first view.
        if inner.core.view() == View::GENESIS {
            self.handle(inner, Event::Start);
        }
    }

    /// The epoch it decides in.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.inner.lock().core.epoch()
    }

    /// The newest checkpoint the committee certified, if any.
    #[must_use]
    pub fn checkpoint(&self) -> Option<CheckpointCertificate> {
        self.inner.lock().certified.clone()
    }

    /// Submits `change`, which ends the epoch, and waits until a decided
    /// block holds it. The committee of the epoch it starts decides from
    /// the block after; this replica goes on in it, or retires if that
    /// committee leaves it out.
    ///
    /// # Errors
    ///
    /// When the committee cannot make the change, or decided another
    /// first, or as [`Replicated::propose`].
    pub async fn change(&self, change: ChangeCertificate) -> Result<Epoch> {
        let admitted = self.inner.lock().core.admits(&change);
        admitted.map_err(|rejection| Error::Failed(rejection.to_string().into()))?;
        match self.submit(Order::Change(change)).await? {
            Settled::Change(epoch) => Ok(epoch),
            Settled::Refused(rejection) => Err(Error::Failed(rejection.to_string().into())),
            other @ (Settled::Applied(_) | Settled::Garbled | Settled::Exhausted) => Err(
                Error::Failed(format!("the change settled as {other:?}").into()),
            ),
        }
    }

    /// Submits `order` and waits until a decided block applied it.
    async fn submit(&self, order: Order) -> Result<Settled> {
        let receiver = self.enqueue(&mut self.inner.lock(), order.encode())?;
        receiver.await.map_err(|_closed| {
            self.inner.lock().health.check().err().unwrap_or_else(|| {
                Error::Failed(
                    "the outcome is unknown: the replica took the state from the others".into(),
                )
            })
        })
    }

    /// Waits for `command` and hands it to the core.
    fn enqueue(&self, inner: &mut Inner, command: Vec<u8>) -> Result<oneshot::Receiver<Settled>> {
        inner.health.check()?;
        let (sender, receiver) = oneshot::channel();
        inner
            .waiting
            .entry(command.clone())
            .or_default()
            .push(sender);
        self.handle(inner, Event::Submitted(command));
        Ok(receiver)
    }
}

impl<H: Host> Replicated for Replica<H> {
    type Txn<'replica>
        = Txn<'replica, H>
    where
        Self: 'replica;

    const READS: Reads = Reads::Decided;

    fn node(&self) -> NodeId {
        self.inner.lock().core.node()
    }

    fn shard(&self) -> Shard {
        self.inner.lock().ledger.shard().clone()
    }

    /// Here: every replica takes writes.
    fn route(&self) -> Route {
        Route::Here
    }

    /// Mine while this replica leads the view it is in.
    fn duty(&self) -> Duty {
        let inner = self.inner.lock();
        if inner.core.leader() == Some(inner.core.node()) {
            Duty::Mine
        } else {
            Duty::Others
        }
    }

    /// The committee's members, all of them voters, fixed for the life of
    /// the shard until committees change.
    fn members(&self) -> Members {
        let voters = self.inner.lock().core.committee().members().collect();
        Members::new(voters, self.peers.clone(), 0)
    }

    async fn begin(&self) -> Result<Txn<'_, H>> {
        let (shard, read_at) = self.inner.lock().position();
        Ok(Txn {
            replica: self,
            shard,
            draft: Draft::new(read_at),
        })
    }

    async fn read<R: Send>(&self, read: impl FnOnce(&KvState) -> R + Send) -> Result<R> {
        Ok(read(self.inner.lock().ledger.kv()))
    }

    async fn propose(&self, command: Command) -> Result<Outcome> {
        let bytes = command.encode()?.payload().to_vec();
        match self.submit(Order::Command(bytes)).await? {
            Settled::Applied(Applied::Data(outcome)) => Ok(outcome),
            Settled::Applied(Applied::Misrouted(shard)) => Err(misrouted(shard)),
            other @ (Settled::Applied(_)
            | Settled::Garbled
            | Settled::Exhausted
            | Settled::Change(_)
            | Settled::Refused(_)) => Err(Error::Failed(
                format!("the command settled as {other:?}").into(),
            )),
        }
    }
}

/// A transaction on a [`Replica`]: it reads the replica's decided state at
/// one version and commits its writes as one command.
#[derive(Debug)]
pub struct Txn<'replica, H> {
    replica: &'replica Replica<H>,
    shard: Shard,
    draft: Draft,
}

impl<H: Host> Transaction for Txn<'_, H> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.shard.span().contains(key) {
            return Err(misrouted(self.shard.clone()));
        }
        let value = self.draft.get(self.replica.inner.lock().ledger.kv(), key);
        Ok(value.map_err(oleaph_storage::Error::from)?)
    }

    fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<KeyValue>> {
        let (start, end) = self.shard.span().clip(start, end);
        let entries = self
            .draft
            .range(self.replica.inner.lock().ledger.kv(), start, end);
        Ok(entries.map_err(oleaph_storage::Error::from)?)
    }

    fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.draft.put(key, value);
    }

    fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.draft.delete(key);
    }

    async fn commit(self) -> Result<Version> {
        let transaction = match self.draft.finish() {
            Finished::Writes(transaction) => transaction,
            Finished::Reads(read) => {
                let confirmed = self.replica.inner.lock().ledger.kv().confirm(&read);
                return Ok(confirmed.map_err(oleaph_storage::Error::from)?);
            }
        };
        match self.replica.propose(Command::Txn(transaction)).await? {
            Outcome::Committed(version) | Outcome::Prepared(version) => Ok(version),
            Outcome::Aborted(abort) => Err(oleaph_storage::Error::from(abort).into()),
            other => Err(Error::Failed(
                format!("a transaction settled as {other:?}").into(),
            )),
        }
    }
}

fn misrouted(shard: Shard) -> Error {
    Error::Misrouted {
        shard: Box::new(shard),
    }
}

#[cfg(test)]
mod tests;
