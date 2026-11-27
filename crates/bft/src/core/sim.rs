//! A deterministic simulation of a shard's replicas, some of them faulty,
//! over a network that delays, reorders and drops messages until it
//! settles.
//!
//! Every run checks safety, that the honest replicas decide the same
//! blocks in the same order, and liveness, that every command submitted
//! to `f + 1` replicas is decided by every honest one once the network
//! settles.
//!
//! `KURATOR_SIM_SEEDS` sets how many seeds each test runs, and
//! `KURATOR_SIM_SEED` runs one seed alone.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use kurator_consensus::NodeId;
use kurator_crypto::{Algorithm, SigningKey};

use crate::block::{Block, Proposal};
use crate::committee::{Committee, Domain};
use crate::core::{Config, Core, Event};
use crate::message::Message;
use crate::step::Recovered;
use crate::step::{Effects, Outgoing, To};
use crate::view::View;
use crate::vote::{BlockId, Certificate, Justify, Vote};

/// When the network settles: no more drops, delays of a few ticks.
const SETTLED: u64 = 800;
/// The latest a run may take.
const DEADLINE: u64 = 10_000;
/// A view's timer before any timeout, in ticks.
const BASE_TIMEOUT: u64 = 40;
/// How many commands the clients submit.
const COMMANDS: u64 = 12;

/// `SplitMix64`, as `kurator-sim` uses.
#[derive(Debug)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next().checked_rem(bound).unwrap_or(0)
    }

    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}

/// How a replica behaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Behavior {
    Honest,
    /// Sends nothing at all.
    Silent,
    /// Proposes two blocks in each view it leads, one to each half of the
    /// others, and votes for every proposal it sees.
    Equivocate,
    /// Follows the protocol, and also sends votes in others' names,
    /// proposals in the leader's name and certificates of its own making.
    Forge,
    /// Follows the protocol, but now and then crashes before the step in
    /// hand is durable and restarts from what it wrote down. It counts as
    /// honest: it must never vote or propose twice in a view.
    Crash,
}

impl Behavior {
    const fn is_honest(self) -> bool {
        matches!(self, Self::Honest | Self::Crash)
    }
}

#[derive(Debug)]
enum Pending {
    Deliver {
        to: usize,
        from: NodeId,
        bytes: Vec<u8>,
    },
    Expire {
        node: usize,
        view: View,
        at: u64,
    },
    Submit {
        node: usize,
        command: Vec<u8>,
    },
}

#[derive(Debug)]
struct Replica {
    core: Core,
    key: SigningKey,
    behavior: Behavior,
    /// How many blocks it decided.
    decided: usize,
    /// The clients' commands among them.
    commands: BTreeSet<Vec<u8>>,
    timer: Option<(View, u64)>,
    /// What it wrote down, to restart from.
    durable: Recovered,
    /// The commands clients gave it, which they give again after a crash.
    submitted: BTreeSet<Vec<u8>>,
    /// The block it voted for, and the block it proposed, in each view.
    signed: BTreeMap<(View, Signed), BlockId>,
    config: (NodeId, Committee, Domain),
}

/// What an honest replica signs at most once in a view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Signed {
    Vote,
    Proposal,
}

#[derive(Debug)]
struct Sim {
    rng: Rng,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<(u64, u64, usize)>>,
    pending: BTreeMap<usize, Pending>,
    replicas: Vec<Replica>,
    committee: Committee,
    domain: Domain,
    /// The blocks decided so far, in order, as the first honest replica
    /// to decide each one decided it.
    log: Vec<BlockId>,
    /// Where two honest replicas decided differently.
    fork: Option<String>,
}

fn id(index: usize) -> NodeId {
    NodeId::new(u64::try_from(index).unwrap().saturating_add(1))
}

fn index(node: NodeId) -> usize {
    usize::try_from(node.get().saturating_sub(1)).unwrap()
}

impl Sim {
    fn new(seed: u64, behaviors: &[Behavior]) -> Self {
        let keys: Vec<SigningKey> = (0..behaviors.len())
            .map(|i| {
                let mut seed_bytes = [0; 32];
                seed_bytes[0] = u8::try_from(i).unwrap();
                seed_bytes[1] = 0xbf;
                SigningKey::from_seed(Algorithm::Ed25519, seed_bytes)
            })
            .collect();
        let members = keys
            .iter()
            .enumerate()
            .map(|(i, key)| (id(i), key.verifying_key()))
            .collect();
        let committee = Committee::new(members).unwrap();
        let domain = Domain::new([7; 32]);
        let replicas = keys
            .into_iter()
            .zip(behaviors)
            .enumerate()
            .map(|(i, (key, behavior))| Replica {
                core: Core::new(Config::new(id(i), key.clone(), committee.clone(), domain)),
                key,
                behavior: *behavior,
                decided: 0,
                commands: BTreeSet::new(),
                timer: None,
                durable: Recovered::default(),
                submitted: BTreeSet::new(),
                signed: BTreeMap::new(),
                config: (id(i), committee.clone(), domain),
            })
            .collect();
        Self {
            rng: Rng(seed),
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            pending: BTreeMap::new(),
            replicas,
            committee,
            domain,
            log: Vec::new(),
            fork: None,
        }
    }

    fn schedule(&mut self, at: u64, pending: Pending) {
        let seq = self.seq;
        self.seq = self.seq.saturating_add(1);
        let slot = usize::try_from(seq).unwrap();
        self.queue.push(Reverse((at, seq, slot)));
        let _none = self.pending.insert(slot, pending);
    }

    fn honest(&self) -> impl Iterator<Item = &Replica> {
        self.replicas
            .iter()
            .filter(|replica| replica.behavior.is_honest())
    }

    fn send(&mut self, from: usize, to: usize, message: &Message) {
        let settled = self.now >= SETTLED;
        if !settled && self.rng.chance(100) {
            return;
        }
        let spread = if settled { 4 } else { 30 };
        let delay = self.rng.below(spread).saturating_add(1);
        let bytes = message.encode().unwrap();
        self.schedule(
            self.now.saturating_add(delay),
            Pending::Deliver {
                to,
                from: id(from),
                bytes,
            },
        );
    }

    fn broadcast(&mut self, from: usize, message: &Message) {
        for to in 0..self.replicas.len() {
            if to != from {
                self.send(from, to, message);
            }
        }
    }

    fn apply(&mut self, node: usize, event: Event) {
        if let Event::Submitted(command) = &event {
            let _new: bool = self.replicas[node].submitted.insert(command.clone());
        }
        let step = self.replicas[node].core.handle(event);
        if self.replicas[node].behavior == Behavior::Crash
            && self.now < SETTLED
            && self.rng.chance(4)
        {
            self.restart(node);
            return;
        }
        let (records, effects) = step.into_parts();
        let durable = &mut self.replicas[node].durable;
        for record in records {
            durable.note(record);
        }
        if self.replicas[node].behavior.is_honest() {
            self.check_signed(node, &effects);
            self.record(node, effects.decided());
        }
        let replica = &mut self.replicas[node];
        if let Some(timer) = effects.timer() {
            let length = BASE_TIMEOUT.saturating_mul(1 << timer.attempt().min(3));
            let at = self.now.saturating_add(length);
            replica.timer = Some((timer.view(), at));
            self.schedule(
                at,
                Pending::Expire {
                    node,
                    view: timer.view(),
                    at,
                },
            );
        }
        match self.replicas[node].behavior {
            Behavior::Silent => {}
            Behavior::Honest | Behavior::Forge | Behavior::Crash => self.send_all(node, &effects),
            Behavior::Equivocate => self.equivocate(node, &effects),
        }
    }

    /// Crashes `node` before the step in hand is durable, so that the
    /// step is lost, and restarts it from what it wrote down. Its clients
    /// submit again what it has not decided.
    fn restart(&mut self, node: usize) {
        let replica = &mut self.replicas[node];
        let (me, committee, domain) = replica.config.clone();
        let config = Config::new(me, replica.key.clone(), committee, domain);
        replica.core = Core::recover(config, replica.durable.clone());
        replica.timer = None;
        let again: Vec<Vec<u8>> = replica
            .submitted
            .difference(&replica.commands)
            .cloned()
            .collect();
        self.apply(node, Event::Start);
        for command in again {
            self.apply(node, Event::Submitted(command));
        }
    }

    /// Checks that honest `node` signs one vote and one proposal in a view
    /// at most, across its restarts.
    fn check_signed(&mut self, node: usize, effects: &Effects) {
        let replica = &mut self.replicas[node];
        for outgoing in effects.messages() {
            let (view, kind, block) = match outgoing.message() {
                Message::Vote(vote) => (vote.view(), Signed::Vote, vote.block()),
                Message::Proposal(proposal) => (
                    proposal.block().view(),
                    Signed::Proposal,
                    proposal.block().id(),
                ),
                Message::Timeout(_)
                | Message::Fetch(_)
                | Message::Deliver(_)
                | Message::Skip(_) => continue,
            };
            let first = *replica.signed.entry((view, kind)).or_insert(block);
            if first != block {
                let fork = format!("{} signed two {kind:?}s in {view}", replica.core.node());
                self.fork = self.fork.take().or(Some(fork));
            }
        }
    }

    fn send_all(&mut self, node: usize, effects: &Effects) {
        for outgoing in effects.messages() {
            self.route(node, outgoing);
        }
    }

    fn route(&mut self, node: usize, outgoing: &Outgoing) {
        match outgoing.to() {
            To::Others => self.broadcast(node, outgoing.message()),
            To::One(to) => self.send(node, index(to), outgoing.message()),
        }
    }

    /// Sends each proposal it makes to half the others, and a second one,
    /// of other commands, to the other half.
    fn equivocate(&mut self, node: usize, effects: &Effects) {
        for outgoing in effects.messages() {
            let Message::Proposal(proposal) = outgoing.message() else {
                self.route(node, outgoing);
                continue;
            };
            let twin = self.twin(node, proposal);
            self.split(node, outgoing.message(), &twin);
        }
    }

    /// A proposal in the same view, on the same certificate, of other
    /// commands.
    fn twin(&self, node: usize, proposal: &Proposal) -> Message {
        let block = proposal.block();
        let evil = Block::new(
            block.view(),
            block.justify().clone(),
            vec![format!("evil {}", block.view()).into_bytes()],
        );
        Message::Proposal(Proposal::sign(
            &self.replicas[node].key,
            self.domain,
            id(node),
            evil,
            proposal.timeout().cloned(),
        ))
    }

    /// Sends `even` to the others with even indices and `odd` to the rest.
    fn split(&mut self, node: usize, even: &Message, odd: &Message) {
        for to in (0..self.replicas.len()).filter(|to| *to != node) {
            let half = if to.is_multiple_of(2) { even } else { odd };
            self.send(node, to, half);
        }
    }

    /// What a faulty replica sends on top of the protocol when `message`
    /// reaches it.
    fn mischief(&mut self, node: usize, message: &Message) {
        let Message::Proposal(proposal) = message else {
            return;
        };
        let view = proposal.block().view();
        let block = proposal.block().id();
        let Some(next) = view.next().and_then(|next| self.committee.leader(next)) else {
            return;
        };
        let key = self.replicas[node].key.clone();
        match self.replicas[node].behavior {
            Behavior::Equivocate => {
                // A vote for every proposal, whatever it voted before.
                let vote = Vote::sign(&key, self.domain, id(node), view, block);
                self.send(node, index(next), &Message::Vote(vote));
            }
            Behavior::Forge => self.forge(node, &key, proposal),
            Behavior::Honest | Behavior::Silent | Behavior::Crash => {}
        }
    }

    fn forge(&mut self, node: usize, key: &SigningKey, proposal: &Proposal) {
        let view = proposal.block().view();
        let n = self.replicas.len();
        let victim = id(node.saturating_add(1).checked_rem(n).unwrap_or(0));
        let Some(next) = view.next() else { return };
        let Some(next_leader) = self.committee.leader(next) else {
            return;
        };
        // A vote in someone else's name.
        let forged = Vote::sign(key, self.domain, victim, view, BlockId::new([9; 32]));
        self.send(node, index(next_leader), &Message::Vote(forged));
        // A proposal in the next leader's name, on a certificate of its
        // own making: its signature under every member's name.
        let signatures = (0..n)
            .map(|i| {
                let vote = Vote::sign(key, self.domain, id(i), view, proposal.block().id());
                (id(i), vote.into_signature())
            })
            .collect();
        let certificate = Certificate::new(view, proposal.block().id(), signatures);
        let block = Block::new(
            next,
            Justify::Certified(certificate),
            vec![b"forged".to_vec()],
        );
        let fake = Proposal::sign(key, self.domain, next_leader, block, None);
        self.broadcast(node, &Message::Proposal(fake));
    }

    /// Starts every replica, and has the clients submit each command to
    /// `f + 1` replicas in a row, so at least one honest replica holds it.
    fn start(&mut self) {
        for node in 0..self.replicas.len() {
            self.apply(node, Event::Start);
        }
        let n = self.replicas.len();
        let spread = self.committee.faults().saturating_add(1);
        for c in 0..COMMANDS {
            let at = self.rng.below(SETTLED);
            let first = usize::try_from(self.rng.below(u64::try_from(n).unwrap())).unwrap();
            for node in (first..first.saturating_add(spread)).map(|k| k.checked_rem(n).unwrap()) {
                let command = format!("command {c}").into_bytes();
                self.schedule(at, Pending::Submit { node, command });
            }
        }
    }

    fn run(&mut self) -> Result<(), String> {
        self.start();
        while let Some(Reverse((at, _, slot))) = self.queue.pop() {
            self.now = at;
            if self.now > DEADLINE {
                break;
            }
            if let Some(pending) = self.pending.remove(&slot) {
                self.step(pending);
            }
            if let Some(fork) = &self.fork {
                return Err(fork.clone());
            }
            if self.all_decided() {
                return Ok(());
            }
        }
        Err(format!("not every command decided by {}", self.now))
    }

    fn step(&mut self, pending: Pending) {
        match pending {
            Pending::Deliver { to, from, bytes } => {
                let message = Message::decode(&bytes).unwrap();
                self.mischief(to, &message);
                self.apply(to, Event::Received { from, message });
            }
            Pending::Expire { node, view, at } => {
                if self.replicas[node].timer == Some((view, at)) {
                    self.apply(node, Event::Expired(view));
                }
            }
            Pending::Submit { node, command } => self.apply(node, Event::Submitted(command)),
        }
    }

    /// Checks that honest `node` decides `blocks` where every other
    /// honest replica decided the same ones.
    fn record(&mut self, node: usize, blocks: &[Block]) {
        let replica = &mut self.replicas[node];
        for block in blocks {
            match self.log.get(replica.decided) {
                None => self.log.push(block.id()),
                Some(expected) if *expected == block.id() => {}
                Some(expected) => {
                    let fork = format!(
                        "{} decided {} where another decided {expected}",
                        replica.core.node(),
                        block.id()
                    );
                    self.fork = self.fork.take().or(Some(fork));
                }
            }
            replica.decided = replica.decided.saturating_add(1);
            let ours = block
                .commands()
                .iter()
                .filter(|command| command.starts_with(b"command "));
            replica.commands.extend(ours.cloned());
        }
    }

    fn all_decided(&self) -> bool {
        let count = usize::try_from(COMMANDS).unwrap();
        self.honest().all(|replica| replica.commands.len() == count)
    }
}

fn seeds() -> Vec<u64> {
    if let Ok(seed) = std::env::var("KURATOR_SIM_SEED") {
        return vec![seed.parse().unwrap()];
    }
    let count = std::env::var("KURATOR_SIM_SEEDS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(12);
    (0..count).collect()
}

fn check(behaviors: &[Behavior]) {
    for seed in seeds() {
        let mut sim = Sim::new(seed, behaviors);
        if let Err(failure) = sim.run() {
            panic!("seed {seed}, {behaviors:?}: {failure}");
        }
    }
}

mod tests {
    use super::Behavior::{Crash, Equivocate, Forge, Honest, Silent};
    use super::check;

    #[test]
    fn honest_replicas_decide_every_command() {
        check(&[Honest, Honest, Honest, Honest]);
    }

    #[test]
    fn a_silent_replica_holds_up_its_views_only() {
        check(&[Honest, Silent, Honest, Honest]);
    }

    #[test]
    fn an_equivocating_replica_splits_no_one() {
        check(&[Honest, Honest, Equivocate, Honest]);
    }

    #[test]
    fn forgeries_count_for_nothing() {
        check(&[Forge, Honest, Honest, Honest]);
    }

    #[test]
    fn crashed_replicas_never_sign_twice_in_a_view() {
        check(&[Crash, Crash, Crash, Crash]);
        check(&[Crash, Honest, Equivocate, Crash]);
    }

    #[test]
    fn seven_replicas_survive_two_faulty() {
        check(&[Honest, Equivocate, Honest, Honest, Silent, Honest, Honest]);
        check(&[Forge, Honest, Honest, Honest, Silent, Honest, Honest]);
        check(&[
            Equivocate, Honest, Honest, Equivocate, Honest, Honest, Honest,
        ]);
    }
}
