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

use std::collections::BTreeMap;
use std::mem;
use std::ops::Bound;
use std::time::Duration;

use futures_channel::oneshot;
use oleaph_consensus::{
    Duty, Error, Members, NodeId, Peer, Reads, Replicated, Result, Route, Transaction,
};
use oleaph_shard::{Applied, Shard};
use oleaph_storage::{Command, Draft, KeyValue, KvState, Outcome, Version};
use parking_lot::Mutex;

use crate::block::Block;
use crate::checkpoint::{Checkpoint, CheckpointCertificate, Own};
use crate::core::{Core, Event};
use crate::message::Message;
use crate::step::{Act, Timer, To};
use crate::view::View;

pub use self::journal::Journal;
pub use self::ledger::{Ledger, Settled};
pub use self::manifest::Manifest;

mod journal;
mod ledger;
mod manifest;

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
    reached: Vec<Checkpoint>,
    /// Those past the newest certified one, to judge against the
    /// certificates to come.
    own: Own,
    /// The newest checkpoint the committee certified.
    certified: Option<CheckpointCertificate>,
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

    /// Stops the replica if its own checkpoint differs from the certified
    /// one at the same height.
    fn judge(&mut self) {
        let diverged = self
            .certified
            .as_ref()
            .and_then(|certified| self.own.judge(certified));
        if let Some(mine) = diverged {
            self.halt(Health::Diverged(mine));
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
}

impl<H: Host> Act for Acting<'_, H> {
    fn decide(&mut self, block: &Block) {
        let outcomes = self.inner.ledger.apply(block);
        for (command, settled) in block.commands().iter().zip(outcomes) {
            self.inner.tell(command, &settled);
        }
        // A state that does not encode never took its keys in.
        if let Ok(Some(checkpoint)) = self.inner.ledger.checkpoint() {
            self.inner.own.take(checkpoint);
            self.inner.reached.push(checkpoint);
            self.inner.judge();
        }
    }

    fn certify(&mut self, certificate: &CheckpointCertificate) {
        self.inner.certified = Some(certificate.clone());
        self.inner.judge();
    }

    fn send(&mut self, to: To, message: &Message) {
        let core = &self.inner.core;
        match to {
            To::One(to) => self.host.send(to, message),
            To::Others => core
                .committee()
                .members()
                .filter(|member| *member != core.node())
                .for_each(|member| self.host.send(member, message)),
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
                reached: Vec::new(),
                own: Own::default(),
                certified: None,
                health: Health::Running,
            }),
        };
        replica.handle(&mut replica.inner.lock(), Event::Start);
        replica
    }

    /// Takes in `message`, which the transport authenticated as `from`'s.
    pub fn receive(&self, from: NodeId, message: Message) {
        self.handle(&mut self.inner.lock(), Event::Received { from, message });
    }

    /// Gives up on `view` when the replica is still in it.
    pub fn expire(&self, view: View) {
        self.handle(&mut self.inner.lock(), Event::Expired(view));
    }

    /// Handles `event`, writes down what it changed, and only then acts.
    fn handle(&self, inner: &mut Inner, event: Event) {
        if inner.health.check().is_err() {
            return;
        }
        let step = inner.core.handle(event);
        if let Ok(effects) = inner.journal.persist(step) {
            effects.carry_out(&mut Acting {
                host: &self.host,
                inner,
            });
        } else {
            inner.halt(Health::Stopped);
        }
        // The core shares the checkpoints the decided blocks reached.
        for checkpoint in mem::take(&mut inner.reached) {
            self.handle(inner, Event::Reached(checkpoint));
        }
    }

    /// The newest checkpoint the committee certified, if any.
    #[must_use]
    pub fn checkpoint(&self) -> Option<CheckpointCertificate> {
        self.inner.lock().certified.clone()
    }

    /// Submits `command` and waits until a decided block applied it.
    async fn submit(&self, command: Command) -> Result<Settled> {
        let bytes = command.encode()?.payload().to_vec();
        let receiver = self.enqueue(&mut self.inner.lock(), bytes)?;
        receiver.await.map_err(|_closed| {
            self.inner
                .lock()
                .health
                .check()
                .err()
                .unwrap_or_else(|| Error::Failed("the replica stopped".into()))
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
        match self.submit(command).await? {
            Settled::Applied(Applied::Data(outcome)) => Ok(outcome),
            Settled::Applied(Applied::Misrouted(shard)) => Err(misrouted(shard)),
            other @ (Settled::Applied(_) | Settled::Garbled | Settled::Exhausted) => Err(
                Error::Failed(format!("the command settled as {other:?}").into()),
            ),
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
        let read_at = self.draft.read_at();
        let Some(transaction) = self.draft.into_transaction() else {
            return Ok(read_at);
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
