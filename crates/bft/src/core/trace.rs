//! Trace validation: replays runs of the Quint model `specs/bft/bft.qnt`
//! against [`Core`], to find where the model and the code part.
//!
//! A trace, in the Informal Trace Format, holds every state of one run of
//! the model and, for each step, the action taken and the choices it made.
//! The replay is the network and the faulty replicas. It keeps a `Core` for
//! each correct replica and hands it the event the action stands for; it
//! signs, with the members' keys, the votes, timeouts and proposals that
//! the model's message pools hold; and it gives every replica a client
//! command whenever it holds none, as the model takes clients to always
//! have more. After every step it checks that each replica's view, last
//! vote, lock, proposal, timeout certificate and decided blocks are the
//! model's, and that the proposals, votes and timeouts it sent are the
//! ones the model added.
//!
//! The traces are `specs/bft/traces/*.itf.json`, or those in the directory
//! `OLEAPH_BFT_TRACES` names. `specs/check.sh` makes new ones.

mod itf;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::{cmp, env, fs};

use oleaph_consensus::NodeId;
use serde_json::Value;

use super::{Config, Core, Event};
use crate::block::{Block as RealBlock, Order, Proposal as RealProposal};
use crate::fixture::{DOMAIN, change_to, committee, key};
use crate::message::Message;
use crate::step::{Recovered, To};
use crate::timeout::{Timeout as RealTimeout, TimeoutCertificate};
use crate::view::View;
use crate::vote::{BlockId, Certificate, Justify, Vote as RealVote};

/// A replica's name in the model: `n1` to `n4`, members 1 to 4.
type Name = String;

/// `QC`: a certificate of `block` from `view`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Qc {
    view: u64,
    block: u64,
}

/// `TC`: a quorum gave up on `view`, the newest of them knowing `high`,
/// which it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Tc {
    view: u64,
    high: Qc,
}

/// `Replica`: what a correct replica keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Replica {
    view: u64,
    voted: u64,
    lock: Qc,
    proposed: u64,
    entered: Option<Tc>,
    decided: BTreeSet<u64>,
}

/// `Block`, without what the model derives from its chain.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Block {
    view: u64,
    parent: u64,
    justify: u64,
    proposer: Name,
}

/// `Proposal`: a block and the timeout certificate passed on with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Proposal {
    block: u64,
    timeout: Option<Tc>,
}

/// `Vote`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Vote {
    voter: Name,
    view: u64,
    block: u64,
}

/// `Timeout`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Timeout {
    voter: Name,
    view: u64,
    high: Qc,
}

/// One state of a trace, with the action that led to it and the choices
/// the action made.
#[derive(Debug, Clone)]
struct State {
    replicas: BTreeMap<Name, Replica>,
    blocks: BTreeMap<u64, Block>,
    proposals: BTreeSet<Proposal>,
    votes: BTreeSet<Vote>,
    timeouts: BTreeSet<Timeout>,
    action: String,
    picks: BTreeMap<String, Value>,
}

/// A correct replica as the code runs it.
#[derive(Debug)]
struct Node {
    core: Core,
    /// What its journal holds, to restart from.
    durable: Recovered,
    /// Every block it decided, crashes or not.
    decided: BTreeSet<BlockId>,
}

/// What a replica sent in one step of the model.
#[derive(Debug, Default)]
struct Sent {
    proposals: Vec<RealProposal>,
    votes: BTreeSet<(View, BlockId)>,
    timeouts: BTreeSet<(View, View)>,
}

/// One trace being replayed.
#[derive(Debug)]
struct Replay {
    name: String,
    nodes: BTreeMap<Name, Node>,
    /// The code's block for each of the model's.
    blocks: BTreeMap<u64, RealBlock>,
    commands: u64,
    /// The step being replayed.
    step: usize,
}

fn member(name: &str) -> NodeId {
    NodeId::new(
        name.strip_prefix('n')
            .and_then(|digits| digits.parse().ok())
            .unwrap_or_else(|| panic!("not a member: {name}")),
    )
}

fn config(name: &str) -> Config {
    let node = member(name);
    Config::new(node, key(node.get()), committee(), DOMAIN)
}

/// The model's `Changes`, the blocks that hold a committee change: in
/// `n4f1`, the faulty n4's first block of view 2. Only a faulty leader's
/// blocks are the replay's to fill; `n4f0` has none.
const CHANGES: [u64; 1] = [4];

/// The view no step reaches: a timeout of it teaches a replica its high
/// certificate and counts for nothing.
const NOWHERE: View = View::new(u64::MAX);

impl Replay {
    fn new(name: String) -> Self {
        Self {
            name,
            nodes: BTreeMap::new(),
            blocks: BTreeMap::new(),
            commands: 0,
            step: 0,
        }
    }

    // --- What the model's pools hold, signed ---

    fn id(&self, block: u64) -> BlockId {
        if block == 0 {
            return BlockId::GENESIS;
        }
        self.blocks.get(&block).map_or_else(
            || panic!("{}: block {block} unknown", self.name),
            RealBlock::id,
        )
    }

    fn justify(&self, state: &State, qc: Qc) -> Justify {
        if qc.view == 0 {
            return Justify::Genesis;
        }
        let id = self.id(qc.block);
        let signatures = state
            .votes
            .iter()
            .filter(|vote| vote.view == qc.view && vote.block == qc.block)
            .map(|vote| {
                let voter = member(&vote.voter);
                let signed =
                    RealVote::sign(&key(voter.get()), DOMAIN, voter, View::new(qc.view), id);
                (voter, signed.into_signature())
            })
            .collect();
        Justify::Certified(Certificate::new(View::new(qc.view), id, signatures))
    }

    fn timeout(&self, state: &State, timeout: &Timeout) -> RealTimeout {
        let voter = member(&timeout.voter);
        let high = self.justify(state, timeout.high);
        RealTimeout::sign(
            &key(voter.get()),
            DOMAIN,
            voter,
            View::new(timeout.view),
            high,
        )
    }

    /// The timeouts that make `tc`: each signer's newest that knew no
    /// newer certificate than `tc.high`, the newest first.
    fn gave_up(state: &State, tc: Tc) -> Vec<&Timeout> {
        let mut newest: BTreeMap<&str, &Timeout> = BTreeMap::new();
        let counted = state
            .timeouts
            .iter()
            .filter(|timeout| timeout.view == tc.view && timeout.high.view <= tc.high.view);
        for timeout in counted {
            let kept = newest.entry(&timeout.voter).or_insert(timeout);
            *kept = cmp::max_by_key(*kept, timeout, |timeout| timeout.high.view);
        }
        let mut timeouts: Vec<&Timeout> = newest.into_values().collect();
        timeouts.sort_by_key(|timeout| cmp::Reverse(timeout.high.view));
        timeouts
    }

    fn certificate(&self, state: &State, tc: Tc) -> TimeoutCertificate {
        let timeouts = Self::gave_up(state, tc)
            .into_iter()
            .map(|timeout| self.timeout(state, timeout));
        TimeoutCertificate::gather(View::new(tc.view), timeouts)
    }

    /// What the code's timeout certificate shows of `tc`: its view, and
    /// its certificate's view and block.
    fn named(&self, tc: Tc) -> (u64, u64, BlockId) {
        (tc.view, tc.high.view, self.id(tc.high.block))
    }

    fn proposal(&self, state: &State, proposal: Proposal) -> (NodeId, RealProposal) {
        let block = self
            .blocks
            .get(&proposal.block)
            .unwrap_or_else(|| panic!("{}: block {} unknown", self.name, proposal.block))
            .clone();
        let proposer = member(&state.blocks[&proposal.block].proposer);
        let timeout = proposal.timeout.map(|tc| self.certificate(state, tc));
        let signed = RealProposal::sign(&key(proposer.get()), DOMAIN, proposer, block, timeout);
        (proposer, signed)
    }

    // --- Driving the code ---

    fn node(&mut self, name: &str) -> &mut Node {
        self.nodes
            .get_mut(name)
            .unwrap_or_else(|| panic!("{name} is not correct"))
    }

    /// Hands `event` to `name`, and every block it asks for after it, as
    /// the model takes every body to be at hand.
    fn run(&mut self, name: &str, event: Event, sent: &mut Sent) {
        let mut events = vec![event];
        while let Some(next) = events.pop() {
            let node = self.node(name);
            let (records, effects) = node.core.handle(next).into_parts();
            for record in records {
                node.durable.note(record);
            }
            node.decided
                .extend(effects.decided().iter().map(RealBlock::id));
            let answers: Vec<Event> = effects
                .messages()
                .iter()
                .filter_map(|outgoing| self.route(name, outgoing.to(), outgoing.message(), sent))
                .collect();
            events.extend(answers);
        }
    }

    /// Notes what `name` sent to `to`, and answers a request for a block.
    fn route(&self, name: &str, to: To, message: &Message, sent: &mut Sent) -> Option<Event> {
        match message {
            Message::Proposal(proposal) => sent.proposals.push(proposal.clone()),
            Message::Vote(vote) => {
                let _new: bool = sent.votes.insert((vote.view(), vote.block()));
            }
            Message::Timeout(timeout) if to == To::Others => {
                let _new: bool = sent
                    .timeouts
                    .insert((timeout.view(), timeout.high().view()));
            }
            Message::Fetch(id) => {
                let block = self.blocks.values().find(|block| block.id() == *id)?;
                return Some(Event::Received {
                    from: other(name),
                    message: Message::Deliver(block.clone()),
                });
            }
            Message::Timeout(_)
            | Message::Deliver(_)
            | Message::Skip(_)
            | Message::Forward(_)
            | Message::Checkpoint(_)
            | Message::Certified(_)
            | Message::Want(..)
            | Message::Manifest(..)
            | Message::Chunk(..)
            | Message::Handoff(_) => {}
        }
        None
    }

    fn deliver(&mut self, name: &str, from: NodeId, message: Message, sent: &mut Sent) {
        self.run(name, Event::Received { from, message }, sent);
    }

    /// Gives `name` a new command when it holds none.
    fn feed(&mut self, name: &str, sent: &mut Sent) {
        if self.node(name).core.pending.is_empty() {
            self.commands = self.commands.saturating_add(1);
            let command = format!("command {}", self.commands).into_bytes();
            self.run(name, Event::Submitted(command), sent);
        }
    }

    fn start(&mut self, name: &str, durable: Recovered, sent: &mut Sent) {
        let core = Core::recover(config(name), durable.clone());
        let decided = self
            .nodes
            .remove(name)
            .map(|node| node.decided)
            .unwrap_or_default();
        drop(self.nodes.insert(
            name.to_owned(),
            Node {
                core,
                durable,
                decided,
            },
        ));
        self.feed(name, sent);
        self.run(name, Event::Start, sent);
    }

    /// Takes the step that led from `before` to `after`.
    fn step(&mut self, before: &State, after: &State) -> BTreeMap<Name, Sent> {
        let mut sent: BTreeMap<Name, Sent> = after
            .replicas
            .keys()
            .map(|name| (name.clone(), Sent::default()))
            .collect();
        let names: Vec<Name> = after.replicas.keys().cloned().collect();
        for name in &names {
            let fed = sent.entry(name.clone()).or_default();
            self.feed(name, fed);
        }
        match after.action.as_str() {
            "receiveProposal" | "observeQC" | "timeout" | "collectTimeouts" | "receiveSkip"
            | "crash" => {
                let name = after.replica();
                let mut out = sent.remove(&name).unwrap_or_default();
                self.correct(before, after, &name, &mut out);
                drop(sent.insert(name, out));
            }
            // The faulty replicas' messages wait in the pools.
            "faultyVote" | "faultyTimeout" | "faultyPropose" | "relay" => {}
            other => panic!("{}: unknown action {other}", self.name),
        }
        sent
    }

    fn correct(&mut self, before: &State, after: &State, name: &str, sent: &mut Sent) {
        match after.action.as_str() {
            "receiveProposal" => {
                let (from, proposal) = self.proposal(before, after.proposal());
                self.deliver(name, from, Message::Proposal(proposal), sent);
            }
            "observeQC" => self.observe(before, name, after.qc(), sent),
            "timeout" => {
                let view = self.node(name).core.view();
                self.run(name, Event::Expired(view), sent);
            }
            // Its own timeout too: the code counts it when it gives up, but
            // forgets it in a crash, and the model's network, as a faulty
            // replica can, hands a replica back what it sent.
            "collectTimeouts" => {
                for timeout in Self::gave_up(before, after.tc()) {
                    let from = member(&timeout.voter);
                    let message = Message::Timeout(self.timeout(before, timeout));
                    self.deliver(name, from, message, sent);
                }
            }
            "receiveSkip" => {
                let certificate = self.certificate(before, after.tc());
                self.deliver(name, other(name), Message::Skip(certificate), sent);
            }
            _crash => {
                let durable = self.node(name).durable.clone();
                self.start(name, durable, sent);
            }
        }
    }

    /// Teaches `name` the certificate `qc`: by its votes when it leads the
    /// view after and is not past it, as the code collects them, or else in
    /// a timeout of a view nobody is in.
    fn observe(&mut self, state: &State, name: &str, qc: Qc, sent: &mut Sent) {
        let me = member(name);
        let next = View::new(qc.view).next();
        let core = &self.node(name).core;
        let collects =
            next.is_some_and(|next| committee().leader(next) == Some(me) && core.view() <= next);
        if collects {
            let id = self.id(qc.block);
            for vote in state
                .votes
                .iter()
                .filter(|vote| vote.view == qc.view && vote.block == qc.block)
            {
                // Its own too, which it forgets in a crash, as for timeouts.
                let voter = member(&vote.voter);
                let signed =
                    RealVote::sign(&key(voter.get()), DOMAIN, voter, View::new(qc.view), id);
                self.deliver(name, voter, Message::Vote(signed), sent);
            }
        } else {
            let high = self.justify(state, qc);
            let timeout = RealTimeout::sign(&key(me.get()), DOMAIN, me, NOWHERE, high);
            self.deliver(name, me, Message::Timeout(timeout), sent);
        }
    }

    // --- Comparing ---

    /// Where the replay is, for a failure to name.
    fn at(&self, action: &str) -> String {
        format!("{} step {} ({action})", self.name, self.step)
    }

    /// Takes in the blocks the step added: the code's own for a correct
    /// proposer, which must have sent it, or one made here for a faulty
    /// one.
    fn learn_blocks(&mut self, before: &State, after: &State, sent: &BTreeMap<Name, Sent>) {
        for (id, block) in &after.blocks {
            if *id == 0 || self.blocks.contains_key(id) {
                continue;
            }
            let real = sent.get(&block.proposer).map_or_else(
                || self.forged(before, *id, block),
                |out| self.proposed(after, *id, block, out),
            );
            drop(self.blocks.insert(*id, real));
        }
    }

    /// A faulty leader's block: commands that set it apart from any other,
    /// or a committee change the committee admits for a block in the
    /// model's `Changes`.
    fn forged(&self, state: &State, id: u64, block: &Block) -> RealBlock {
        let certified = Qc {
            view: block.justify,
            block: block.parent,
        };
        let command = if CHANGES.contains(&id) {
            Order::Change(change_to(1, &[1, 2, 3])).encode()
        } else {
            format!("faulty {id}").into_bytes()
        };
        let commands = vec![command];
        RealBlock::new(
            View::new(block.view),
            self.justify(state, certified),
            commands,
        )
    }

    /// The block `out` proposed as the model's block `id`.
    fn proposed(&self, after: &State, id: u64, block: &Block, out: &Sent) -> RealBlock {
        let at = self.at(&after.action);
        let found = out
            .proposals
            .iter()
            .find(|proposal| proposal.block().view() == View::new(block.view));
        let Some(proposal) = found else {
            panic!(
                "{at}: {} proposed block {id} in the model only",
                block.proposer
            );
        };
        let real = proposal.block();
        let entered = after
            .proposals
            .iter()
            .find(|model| model.block == id)
            .and_then(|model| model.timeout)
            .map(|tc| self.named(tc));
        let shown = proposal.timeout().map(shown);
        assert_eq!(
            real.justify().view().get(),
            block.justify,
            "{at}: justify of block {id}"
        );
        assert_eq!(
            real.parent(),
            self.id(block.parent),
            "{at}: parent of block {id}"
        );
        assert_eq!(shown, entered, "{at}: timeout certificate of block {id}");
        real.clone()
    }

    fn compare(&self, before: &State, after: &State, sent: &BTreeMap<Name, Sent>) {
        for (name, model) in &after.replicas {
            let at = format!("{}, {name}", self.at(&after.action));
            let node = &self.nodes[name];
            let core = &node.core;
            assert_eq!(core.view.get(), model.view, "{at}: view");
            assert_eq!(core.voted.get(), model.voted, "{at}: last vote");
            assert_eq!(core.lock.view().get(), model.lock.view, "{at}: lock's view");
            assert_eq!(
                core.lock.block(),
                self.id(model.lock.block),
                "{at}: lock's block"
            );
            assert_eq!(core.proposed.get(), model.proposed, "{at}: last proposal");
            let entered = core.entered.as_ref().map(shown);
            let expected = model.entered.map(|tc| self.named(tc));
            assert_eq!(
                entered, expected,
                "{at}: timeout certificate it entered with"
            );
            let decided: BTreeSet<BlockId> =
                model.decided.iter().map(|block| self.id(*block)).collect();
            assert_eq!(node.decided, decided, "{at}: decided blocks");
            if let Some(out) = sent.get(name) {
                self.compare_sent((before, after), name, out, &at);
            }
        }
    }

    /// Checks that what `name` sent in the step from `before` to `after` is
    /// what the model added.
    fn compare_sent(&self, (before, after): (&State, &State), name: &str, out: &Sent, at: &str) {
        let me = member(name);
        let cast: BTreeSet<(View, BlockId)> = after
            .votes
            .difference(&before.votes)
            .filter(|vote| vote.voter == name)
            .map(|vote| (View::new(vote.view), self.id(vote.block)))
            .collect();
        // A vote for the next view's leader stays with it when that is
        // this replica.
        let kept: BTreeSet<(View, BlockId)> = cast
            .iter()
            .filter(|(view, _)| view.next().and_then(|next| committee().leader(next)) == Some(me))
            .copied()
            .collect();
        let shown: BTreeSet<(View, BlockId)> = out.votes.union(&kept).copied().collect();
        assert_eq!(shown, cast, "{at}: votes");
        let gave_up: BTreeSet<(View, View)> = after
            .timeouts
            .difference(&before.timeouts)
            .filter(|timeout| timeout.voter == name)
            .map(|timeout| (View::new(timeout.view), View::new(timeout.high.view)))
            .collect();
        assert_eq!(out.timeouts, gave_up, "{at}: timeouts");
        let proposed = after
            .blocks
            .iter()
            .filter(|(id, block)| !before.blocks.contains_key(id) && block.proposer == name)
            .count();
        assert_eq!(out.proposals.len(), proposed, "{at}: proposals");
    }

    fn replay(mut self, states: &[State]) {
        let Some(first) = states.first() else {
            return;
        };
        let mut started = BTreeMap::new();
        for name in first.replicas.keys() {
            let mut out = Sent::default();
            self.start(name, Recovered::default(), &mut out);
            drop(started.insert(name.clone(), out));
        }
        let genesis = State {
            blocks: BTreeMap::new(),
            proposals: BTreeSet::new(),
            votes: BTreeSet::new(),
            timeouts: BTreeSet::new(),
            ..first.clone()
        };
        self.learn_blocks(&genesis, first, &started);
        self.compare(&genesis, first, &started);
        for pair in states.windows(2) {
            let (before, after) = (&pair[0], &pair[1]);
            self.step = self.step.saturating_add(1);
            let sent = self.step(before, after);
            self.learn_blocks(before, after, &sent);
            self.compare(before, after, &sent);
        }
    }
}

/// The view of a timeout certificate, and its certificate's view and
/// block.
fn shown(certificate: &TimeoutCertificate) -> (u64, u64, BlockId) {
    let high = certificate.high();
    (certificate.view().get(), high.view().get(), high.block())
}

/// Another member than `name`, for messages whose sender does not matter.
fn other(name: &str) -> NodeId {
    let me = member(name);
    (1..=4)
        .map(NodeId::new)
        .find(|node| *node != me)
        .unwrap_or(me)
}

fn traces() -> Vec<PathBuf> {
    let dir = env::var_os("OLEAPH_BFT_TRACES").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../specs/bft/traces"),
        PathBuf::from,
    );
    let mut paths: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("{}: {error}", dir.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.to_string_lossy().ends_with(".itf.json"))
        .collect();
    paths.sort();
    paths
}

#[test]
fn the_code_follows_the_model() {
    let paths = traces();
    assert!(!paths.is_empty(), "no traces to replay");
    for path in paths {
        let text = fs::read_to_string(&path).unwrap();
        let trace: Value = serde_json::from_str(&text).unwrap();
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        Replay::new(name).replay(&State::all(&trace));
    }
}
