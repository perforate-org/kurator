//! The rules of one replica, one event at a time.

use std::collections::{BTreeMap, VecDeque};

use kurator_consensus::NodeId;
use kurator_wal::Options;

use super::{Config, Core, Event};
use crate::block::{Block, Proposal};
use crate::checkpoint::{Checkpoint, Height, Share, StateRoot};
use crate::fixture::{DOMAIN, committee, key};
use crate::message::Message;
use crate::rejection::Rejection;
use crate::replica::Journal;
use crate::step::Recovered;
use crate::step::{Effects, Safety, To};
use crate::timeout::{Timeout, TimeoutCertificate};
use crate::view::View;
use crate::vote::{BlockId, Certificate, Justify, Vote};

fn leader(view: u64) -> NodeId {
    committee().leader(View::new(view)).unwrap()
}

/// A member that does not lead `views`.
fn follower(views: &[u64]) -> NodeId {
    (1..=4)
        .map(NodeId::new)
        .find(|node| views.iter().all(|view| leader(*view) != *node))
        .unwrap()
}

fn config(node: NodeId) -> Config {
    Config::new(node, key(node.get()), committee(), DOMAIN)
}

fn core(node: NodeId) -> Core {
    Core::new(config(node))
}

/// A replica holding a command, so that it proposes when it leads.
fn holding(node: NodeId) -> Core {
    let mut core = core(node);
    let _held = core
        .handle(Event::Submitted(b"command".to_vec()))
        .persisted();
    core
}

fn started(node: NodeId) -> Core {
    let mut core = holding(node);
    let _start = core.handle(Event::Start).persisted();
    core
}

fn proposal(view: u64, justify: Justify, timeout: Option<TimeoutCertificate>) -> Proposal {
    let view = View::new(view);
    let leader = committee().leader(view).unwrap();
    let block = Block::new(view, justify, vec![b"command".to_vec()]);
    Proposal::sign(&key(leader.get()), DOMAIN, leader, block, timeout)
}

/// A certificate for `block` in `view`, signed by nodes 1 to 3.
fn certify(view: u64, block: BlockId) -> Justify {
    let view = View::new(view);
    let signatures = (1..=3)
        .map(|node| {
            let vote = Vote::sign(&key(node), DOMAIN, NodeId::new(node), view, block);
            (NodeId::new(node), vote.into_signature())
        })
        .collect();
    Justify::Certified(Certificate::new(view, block, signatures))
}

/// A timeout certificate for `view`, by nodes 1 to 3 that each knew
/// `high` as their newest certificate.
fn timed_out(view: u64, high: &Justify) -> TimeoutCertificate {
    let entries = (1..=3)
        .map(|node| {
            let timeout = Timeout::sign(
                &key(node),
                DOMAIN,
                NodeId::new(node),
                View::new(view),
                high.clone(),
            );
            let (voter, seen, signature) = timeout.into_entry();
            (voter, (seen, signature))
        })
        .collect();
    TimeoutCertificate::new(View::new(view), entries)
}

fn receive(core: &mut Core, message: Message) -> Effects {
    let from = leader(1);
    core.handle(Event::Received { from, message }).persisted()
}

#[test]
fn the_first_leader_proposes_on_start() {
    let mut first = holding(leader(1));
    let step = first.handle(Event::Start);
    assert_eq!(
        step.safety().map(Safety::voted),
        Some(View::new(1)),
        "voted for its own block"
    );
    let effects = step.persisted();
    assert_eq!(first.view(), View::new(1), "in the first view");
    assert_eq!(
        effects.timer().map(|timer| timer.view()),
        Some(View::new(1)),
        "timer"
    );
    assert_eq!(
        effects.timer().map(|timer| timer.attempt()),
        Some(0),
        "first attempt"
    );
    let Message::Proposal(proposal) = effects.messages()[0].message() else {
        panic!("not a proposal: {effects:?}");
    };
    assert_eq!(effects.messages()[0].to(), To::Others, "to everyone");
    assert_eq!(proposal.block().justify(), &Justify::Genesis, "on genesis");
    assert_eq!(proposal.proposer(), leader(1), "signed by the leader");
    assert!(proposal.verify(&committee(), DOMAIN).is_ok(), "valid");
    assert_eq!(first.node(), leader(1), "node");
}

#[test]
fn others_wait_on_start() {
    let mut other = core(follower(&[1]));
    let step = other.handle(Event::Start);
    assert!(step.safety().is_none(), "nothing to persist");
    let effects = step.persisted();
    assert!(effects.messages().is_empty(), "nothing to say");
    assert_eq!(
        effects.timer().map(|timer| timer.view()),
        Some(View::new(1)),
        "timer"
    );
    let _again = other.handle(Event::Start).persisted();
    assert_eq!(other.view(), View::new(1), "starts once");
}

#[test]
fn a_replica_votes_once_in_a_view_for_its_leader() {
    let node = follower(&[1, 2]);
    let mut replica = started(node);
    let proposal = proposal(1, Justify::Genesis, None);
    let step = replica.handle(Event::Received {
        from: leader(1),
        message: Message::Proposal(proposal.clone()),
    });
    let safety = step.safety().cloned().unwrap();
    assert_eq!(safety.voted(), View::new(1), "voted");
    assert_eq!(safety.lock(), &Justify::Genesis, "lock");
    let effects = step.persisted();
    let [outgoing] = effects.messages() else {
        panic!("one vote: {effects:?}");
    };
    assert_eq!(outgoing.to(), To::One(leader(2)), "to the next leader");
    let Message::Vote(vote) = outgoing.message() else {
        panic!("not a vote");
    };
    assert_eq!(
        (vote.view(), vote.block(), vote.voter()),
        (View::new(1), proposal.block().id(), node),
        "its vote"
    );
    assert!(vote.verify(&committee(), DOMAIN).is_ok(), "signed");
    let again = receive(&mut replica, Message::Proposal(proposal));
    assert_eq!(
        again.rejected(),
        Some(Rejection::Voted(View::new(1))),
        "once"
    );
    assert!(again.messages().is_empty(), "no second vote");
}

#[test]
fn proposals_need_their_leaders_signature_and_a_quorum() {
    let mut replica = started(follower(&[1]));
    let block = Block::new(View::new(1), Justify::Genesis, Vec::new());
    let impostor = follower(&[1]);
    let not_leader = Proposal::sign(&key(impostor.get()), DOMAIN, impostor, block.clone(), None);
    let effects = receive(&mut replica, Message::Proposal(not_leader));
    assert_eq!(
        effects.rejected(),
        Some(Rejection::NotLeader(impostor, View::new(1))),
        "not the leader"
    );
    let forged = Proposal::sign(&key(impostor.get()), DOMAIN, leader(1), block, None);
    let second = receive(&mut replica, Message::Proposal(forged));
    assert_eq!(
        second.rejected(),
        Some(Rejection::Forged(leader(1))),
        "not its key"
    );
    let short = Justify::Certified(Certificate::new(
        View::new(1),
        BlockId::new([5; 32]),
        BTreeMap::new(),
    ));
    let third = receive(&mut replica, Message::Proposal(proposal(2, short, None)));
    assert_eq!(
        third.rejected(),
        Some(Rejection::Short { have: 0, need: 3 }),
        "no quorum"
    );
    assert_eq!(replica.view(), View::new(1), "still in the first view");
}

#[test]
fn strangers_sign_nothing() {
    let mut replica = started(follower(&[1]));
    let stranger = NodeId::new(9);
    let vote = Vote::sign(&key(9), DOMAIN, stranger, View::new(1), BlockId::GENESIS);
    let effects = receive(&mut replica, Message::Vote(vote));
    let expected = if leader(2) == replica.node() {
        Rejection::Stranger(stranger)
    } else {
        Rejection::Misdirected(View::new(1))
    };
    assert_eq!(effects.rejected(), Some(expected), "stranger");
    let timeout = Timeout::sign(&key(9), DOMAIN, stranger, View::new(1), Justify::Genesis);
    let second = receive(&mut replica, Message::Timeout(timeout));
    assert_eq!(
        second.rejected(),
        Some(Rejection::Stranger(stranger)),
        "stranger's timeout"
    );
}

/// The proposal `effects` send, if any.
fn proposal_in(effects: &Effects) -> Option<Proposal> {
    effects.messages().iter().find_map(|outgoing| {
        let Message::Proposal(proposal) = outgoing.message() else {
            return None;
        };
        Some(proposal.clone())
    })
}

#[test]
fn the_next_leader_certifies_a_quorum_of_votes_and_proposes() {
    let mut next = started(leader(2));
    let block = proposal(1, Justify::Genesis, None);
    let _vote = receive(&mut next, Message::Proposal(block.clone()));
    let id = block.block().id();
    let mut proposed = None;
    for voter in (1..=4).map(NodeId::new).filter(|voter| *voter != leader(2)) {
        let vote = Vote::sign(&key(voter.get()), DOMAIN, voter, View::new(1), id);
        let effects = receive(&mut next, Message::Vote(vote));
        assert_eq!(effects.rejected(), None, "a member's vote");
        proposed = proposed.or_else(|| proposal_in(&effects));
    }
    let proposal = proposed.unwrap();
    assert_eq!(proposal.block().view(), View::new(2), "in the next view");
    assert_eq!(proposal.block().parent(), id, "on the certified block");
    assert_eq!(next.view(), View::new(2), "moved on");
    let late = Vote::sign(
        &key(1),
        DOMAIN,
        NodeId::new(1),
        View::new(0),
        BlockId::GENESIS,
    );
    let second = receive(&mut next, Message::Vote(late));
    let expected = if leader(1) == next.node() {
        Rejection::OtherView(View::new(0), View::new(2))
    } else {
        Rejection::Misdirected(View::new(0))
    };
    assert_eq!(second.rejected(), Some(expected), "too late");
}

#[test]
fn votes_go_to_the_next_leader_only() {
    let node = follower(&[2]);
    let mut replica = started(node);
    let vote = Vote::sign(
        &key(1),
        DOMAIN,
        NodeId::new(1),
        View::new(1),
        BlockId::GENESIS,
    );
    let effects = receive(&mut replica, Message::Vote(vote));
    assert_eq!(
        effects.rejected(),
        Some(Rejection::Misdirected(View::new(1))),
        "misdirected"
    );
}

#[test]
fn a_view_without_a_certificate_needs_a_timeout_certificate() {
    let node = follower(&[1, 2, 3]);
    let mut replica = started(node);
    // Without a certificate for view 2, view 3 cannot follow genesis.
    let skip = Message::Skip(timed_out(2, &Justify::Genesis));
    let effects = receive(&mut replica, skip);
    assert_eq!(effects.rejected(), None, "a valid timeout certificate");
    assert_eq!(replica.view(), View::new(3), "skipped to view 3");
    assert_eq!(
        effects.timer().map(|timer| timer.attempt()),
        Some(1),
        "one view timed out"
    );
    let bare = receive(
        &mut replica,
        Message::Proposal(proposal(3, Justify::Genesis, None)),
    );
    assert_eq!(
        bare.rejected(),
        Some(Rejection::Unjustified(View::new(3))),
        "no certificate"
    );
    let wrong = receive(
        &mut replica,
        Message::Proposal(proposal(
            3,
            Justify::Genesis,
            Some(timed_out(1, &Justify::Genesis)),
        )),
    );
    assert_eq!(
        wrong.rejected(),
        Some(Rejection::Unjustified(View::new(3))),
        "another view's"
    );
    let shown = receive(
        &mut replica,
        Message::Proposal(proposal(
            3,
            Justify::Genesis,
            Some(timed_out(2, &Justify::Genesis)),
        )),
    );
    assert_eq!(shown.rejected(), None, "justified by the timeout");
    assert!(
        matches!(shown.messages()[0].message(), Message::Vote(_)),
        "votes"
    );
}

#[test]
fn a_timeout_certificate_cannot_hide_a_newer_certificate() {
    let node = follower(&[1, 2, 3]);
    let mut replica = started(node);
    let first = proposal(1, Justify::Genesis, None);
    let certified = certify(1, first.block().id());
    let skip = Message::Skip(timed_out(2, &certified));
    let _skip = receive(&mut replica, skip);
    let stale = timed_out(2, &certified);
    let effects = receive(
        &mut replica,
        Message::Proposal(proposal(3, Justify::Genesis, Some(stale))),
    );
    assert_eq!(
        effects.rejected(),
        Some(Rejection::Unjustified(View::new(3))),
        "older than a signer's"
    );
}

#[test]
fn a_replica_never_votes_against_its_lock() {
    let node = follower(&[1, 2, 3]);
    let mut replica = started(node);
    let first = proposal(1, Justify::Genesis, None);
    let certified = certify(1, first.block().id());
    // It learns the certificate from a timeout, then a timeout
    // certificate whose signers knew none hides it from the leader.
    let timeout = Timeout::sign(&key(1), DOMAIN, NodeId::new(1), View::new(2), certified);
    let _learn = receive(&mut replica, Message::Timeout(timeout));
    assert_eq!(replica.view(), View::new(2), "the certificate moves it on");
    let _skip = receive(&mut replica, Message::Skip(timed_out(2, &Justify::Genesis)));
    let effects = receive(
        &mut replica,
        Message::Proposal(proposal(
            3,
            Justify::Genesis,
            Some(timed_out(2, &Justify::Genesis)),
        )),
    );
    assert_eq!(
        effects.rejected(),
        Some(Rejection::Unsafe(View::GENESIS, View::new(1))),
        "unsafe"
    );
}

#[test]
fn proposals_for_other_views_get_no_vote() {
    let node = follower(&[1, 2, 3]);
    let mut replica = started(node);
    let _skip = receive(&mut replica, Message::Skip(timed_out(2, &Justify::Genesis)));
    let effects = receive(
        &mut replica,
        Message::Proposal(proposal(1, Justify::Genesis, None)),
    );
    assert_eq!(
        effects.rejected(),
        Some(Rejection::OtherView(View::new(1), View::new(3))),
        "behind"
    );
}

#[test]
fn an_expired_timer_gives_up_on_its_view_only() {
    let node = follower(&[1]);
    let mut replica = started(node);
    let other = replica.handle(Event::Expired(View::new(4)));
    assert!(other.safety().is_none(), "another view's timer");
    assert!(other.persisted().messages().is_empty(), "says nothing");
    let step = replica.handle(Event::Expired(View::new(1)));
    assert_eq!(
        step.safety().map(Safety::voted),
        Some(View::new(1)),
        "no more votes in it"
    );
    let effects = step.persisted();
    let Message::Timeout(timeout) = effects.messages()[0].message() else {
        panic!("not a timeout: {effects:?}");
    };
    assert_eq!(
        (timeout.view(), timeout.voter()),
        (View::new(1), node),
        "its timeout"
    );
    assert_eq!(
        timeout.high(),
        &Justify::Genesis,
        "with its newest certificate"
    );
    assert_eq!(
        effects.timer().map(|timer| timer.attempt()),
        Some(1),
        "tries again later"
    );
    let late = receive(
        &mut replica,
        Message::Proposal(proposal(1, Justify::Genesis, None)),
    );
    assert_eq!(
        late.rejected(),
        Some(Rejection::Voted(View::new(1))),
        "gave up"
    );
}

#[test]
fn a_quorum_of_timeouts_moves_every_replica_on() {
    let node = follower(&[1, 2]);
    let mut replica = started(node);
    let others: Vec<NodeId> = (1..=4)
        .map(NodeId::new)
        .filter(|other| *other != node)
        .collect();
    for (count, other) in others.iter().enumerate() {
        let timeout = Timeout::sign(
            &key(other.get()),
            DOMAIN,
            *other,
            View::new(1),
            Justify::Genesis,
        );
        let effects = receive(&mut replica, Message::Timeout(timeout));
        assert_eq!(effects.rejected(), None, "a member's timeout");
        let expected = if count < 2 { 1 } else { 2 };
        assert_eq!(
            replica.view(),
            View::new(expected),
            "after {} timeouts",
            count + 1
        );
    }
    let late = Timeout::sign(
        &key(others[0].get()),
        DOMAIN,
        others[0],
        View::new(1),
        Justify::Genesis,
    );
    let second = receive(&mut replica, Message::Timeout(late));
    assert_eq!(
        second.rejected(),
        Some(Rejection::OtherView(View::new(1), View::new(2))),
        "too late"
    );
    // Its next timeout shows the certificate it moved on with.
    let third = replica.handle(Event::Expired(View::new(2))).persisted();
    assert!(
        third.messages().iter().any(|outgoing| matches!(outgoing.message(), Message::Skip(certificate) if certificate.view() == View::new(1))),
        "shows the timeout certificate: {third:?}"
    );
}

#[test]
fn blocks_are_delivered_only_to_who_asked() {
    let node = follower(&[1]);
    let mut replica = started(node);
    let block = Block::new(View::new(1), Justify::Genesis, Vec::new());
    let effects = receive(&mut replica, Message::Deliver(block));
    assert_eq!(effects.rejected(), Some(Rejection::Unwanted), "unasked");
    let first = proposal(1, Justify::Genesis, None);
    let _vote = receive(&mut replica, Message::Proposal(first.clone()));
    let asked = receive(&mut replica, Message::Fetch(first.block().id()));
    assert_eq!(
        asked.messages()[0].message(),
        &Message::Deliver(first.block().clone()),
        "delivered"
    );
    assert_eq!(asked.messages()[0].to(), To::One(leader(1)), "to who asked");
    let unknown = receive(&mut replica, Message::Fetch(BlockId::new([3; 32])));
    assert!(unknown.messages().is_empty(), "nothing to deliver");
}

/// A network that delivers every message, in the order sent.
#[derive(Default)]
struct Lossless {
    queue: VecDeque<(NodeId, NodeId, Message)>,
    decided: BTreeMap<NodeId, Vec<Block>>,
}

impl Lossless {
    fn take(&mut self, from: NodeId, effects: &Effects, nodes: &[NodeId]) {
        self.decided
            .entry(from)
            .or_default()
            .extend(effects.decided().iter().cloned());
        for outgoing in effects.messages() {
            let message = outgoing.message();
            let targets: Vec<NodeId> = match outgoing.to() {
                To::Others => nodes.iter().copied().filter(|to| *to != from).collect(),
                To::One(to) => vec![to],
            };
            self.queue
                .extend(targets.into_iter().map(|to| (from, to, message.clone())));
        }
    }
}

/// Runs `cores`, each with a command of its own, for `rounds` messages.
fn run(cores: &mut BTreeMap<NodeId, Core>, rounds: usize) -> BTreeMap<NodeId, Vec<Block>> {
    let mut network = Lossless::default();
    let nodes: Vec<NodeId> = cores.keys().copied().collect();
    for (node, core) in cores.iter_mut() {
        let submitted = core
            .handle(Event::Submitted(format!("from {node}").into_bytes()))
            .persisted();
        network.take(*node, &submitted, &nodes);
        let started = core.handle(Event::Start).persisted();
        network.take(*node, &started, &nodes);
    }
    for _ in 0..rounds {
        let Some((from, to, message)) = network.queue.pop_front() else {
            break;
        };
        let received = cores
            .get_mut(&to)
            .unwrap()
            .handle(Event::Received { from, message })
            .persisted();
        assert_eq!(received.rejected(), None, "honest messages");
        network.take(to, &received, &nodes);
    }
    network.decided
}

#[test]
fn a_block_commits_once_its_child_in_the_next_view_is_certified() {
    let mut cores: BTreeMap<NodeId, Core> = (1..=4)
        .map(|node| (NodeId::new(node), core(NodeId::new(node))))
        .collect();
    let decided = run(&mut cores, 400);
    let first = &decided[&NodeId::new(1)];
    assert!(!first.is_empty(), "decides");
    assert_eq!(first[0].view(), View::new(1), "the first block first");
    for blocks in decided.values() {
        let common = blocks.len().min(first.len());
        assert_eq!(blocks[..common], first[..common], "the same blocks");
        for pair in blocks.windows(2) {
            assert_eq!(pair[1].parent(), pair[0].id(), "a chain");
        }
    }
    let commands: Vec<&Vec<u8>> = first.iter().flat_map(Block::commands).collect();
    for node in 1..=4 {
        let from = format!("from {}", NodeId::new(node)).into_bytes();
        assert_eq!(
            commands.iter().filter(|command| ***command == from).count(),
            1,
            "{node}'s command once"
        );
    }
    let core = &cores[&NodeId::new(1)];
    assert_eq!(
        core.committed(),
        first.last().unwrap().id(),
        "the last decided"
    );
}

#[test]
fn a_leader_of_two_views_in_a_row_counts_its_own_vote() {
    let view = (2..100)
        .find(|view| leader(*view) == leader(view.saturating_add(1)))
        .unwrap();
    let node = leader(view);
    let mut replica = started(node);
    let effects = receive(
        &mut replica,
        Message::Skip(timed_out(view.saturating_sub(1), &Justify::Genesis)),
    );
    let proposed = proposal_in(&effects).unwrap();
    assert!(
        effects
            .messages()
            .iter()
            .all(|outgoing| !matches!(outgoing.message(), Message::Vote(_))),
        "keeps its vote"
    );
    let block = proposed.block().id();
    let others = (1..=4).map(NodeId::new).filter(|other| *other != node);
    let mut next = None;
    for other in others.take(2) {
        let vote = Vote::sign(&key(other.get()), DOMAIN, other, View::new(view), block);
        next = proposal_in(&receive(&mut replica, Message::Vote(vote)));
    }
    assert_eq!(
        next.map(|next| next.block().view()),
        Some(View::new(view.saturating_add(1))),
        "two more votes make a quorum"
    );
}

#[test]
fn a_replica_remembers_its_votes_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node = leader(1);
    let (mut journal, fresh) = Journal::open(dir.path(), Options::default()).unwrap();
    assert_eq!(fresh, Recovered::default(), "nothing yet");
    let mut replica = Core::recover(config(node), fresh);
    let _held = replica.handle(Event::Submitted(b"command".to_vec()));
    let first = journal.persist(replica.handle(Event::Start)).unwrap();
    let proposed = proposal_in(&first).unwrap();
    drop(journal);

    let (_journal, recovered) = Journal::open(dir.path(), Options::default()).unwrap();
    let mut restarted = Core::recover(config(node), recovered);
    let _held_again = restarted.handle(Event::Submitted(b"command".to_vec()));
    let again = restarted.handle(Event::Start).persisted();
    assert_eq!(restarted.view(), View::new(1), "back in its view");
    assert!(proposal_in(&again).is_none(), "proposes once a view");
    let twin = Proposal::sign(
        &key(node.get()),
        DOMAIN,
        node,
        Block::new(View::new(1), Justify::Genesis, vec![b"twin".to_vec()]),
        None,
    );
    assert_ne!(twin.block().id(), proposed.block().id(), "another block");
    let effects = receive(&mut restarted, Message::Proposal(twin));
    assert_eq!(
        effects.rejected(),
        Some(Rejection::Voted(View::new(1))),
        "votes once a view"
    );
}

#[test]
fn an_idle_leader_proposes_nothing_and_waits_out_its_view() {
    let mut first = core(leader(1));
    let effects = first.handle(Event::Start).persisted();
    assert_eq!(first.view(), View::new(1), "in the first view");
    assert!(proposal_in(&effects).is_none(), "nothing to decide");
    assert_eq!(
        effects.timer().map(|timer| timer.view()),
        Some(View::new(1)),
        "the view times out"
    );

    // A command forwarded to it is proposed at once.
    let command = b"forwarded".to_vec();
    let forwarded = receive(&mut first, Message::Forward(vec![command.clone()]));
    let proposed = proposal_in(&forwarded).unwrap();
    assert_eq!(proposed.block().commands(), [command], "proposes it");
}

#[test]
fn a_command_goes_to_the_leader_of_the_view() {
    let node = follower(&[1]);
    let mut replica = core(node);
    let _start = replica.handle(Event::Start).persisted();
    let command = b"submitted".to_vec();
    let effects = replica
        .handle(Event::Submitted(command.clone()))
        .persisted();
    let [outgoing] = effects.messages() else {
        panic!("one message: {:?}", effects.messages());
    };
    assert_eq!(outgoing.to(), To::One(leader(1)), "to the leader");
    assert_eq!(
        outgoing.message(),
        &Message::Forward(vec![command]),
        "the command"
    );

    // Forwarded commands go no further.
    let other = b"other".to_vec();
    let kept = receive(&mut replica, Message::Forward(vec![other]));
    assert!(kept.messages().is_empty(), "kept for later");
}

/// The votes of two members other than `node` for `block` in `view`.
fn two_votes(node: NodeId, view: u64, block: BlockId) -> impl Iterator<Item = Message> {
    (1..=4)
        .map(NodeId::new)
        .filter(move |other| *other != node)
        .take(2)
        .map(move |other| {
            let vote = Vote::sign(&key(other.get()), DOMAIN, other, View::new(view), block);
            Message::Vote(vote)
        })
}

#[test]
fn a_leader_proposes_after_commands_until_they_decide_then_idles() {
    let first = proposal(1, Justify::Genesis, None);
    let commands = first.block().id();

    // The leader of view 2 certifies the block of commands, and proposes
    // an empty block after it: only that one's certificate commits it.
    let node = leader(2);
    let mut replica = core(node);
    let _start = replica.handle(Event::Start).persisted();
    let mut effects = receive(&mut replica, Message::Proposal(first.clone()));
    for vote in two_votes(node, 1, commands) {
        effects = receive(&mut replica, vote);
    }
    let after = proposal_in(&effects).unwrap();
    assert_eq!(after.block().view(), View::new(2), "in the next view");
    assert!(after.block().commands().is_empty(), "empty");

    // The leader of view 3 decides the commands, and proposes once more,
    // to show the others the certificate that decides them.
    let (shown, decided) = next_leader(&[&first, &after], 3);
    assert_eq!(decided, [commands], "decides the commands");
    assert!(shown.block().commands().is_empty(), "empty");

    // The leader of view 4 has nothing more to show.
    let (idle, _decided) = next_leader(&[&first, &after, &shown], 4);
    assert!(idle.block().view() < View::new(4), "idles");
}

/// The leader of `view`, a fresh replica, once it saw `chain` and the
/// votes that certify its last block in the view before: what it proposes
/// next, the last of the chain if nothing, and what it decided.
fn next_leader(chain: &[&Proposal], view: u64) -> (Proposal, Vec<BlockId>) {
    let node = leader(view);
    let mut replica = core(node);
    let _start = replica.handle(Event::Start).persisted();
    for proposal in chain {
        let _voted = receive(&mut replica, Message::Proposal((*proposal).clone()));
    }
    let last = chain.last().unwrap();
    let mut decided = Vec::new();
    let mut proposed = None;
    for vote in two_votes(node, view.saturating_sub(1), last.block().id()) {
        let effects = receive(&mut replica, vote);
        decided.extend(effects.decided().iter().map(Block::id));
        proposed = proposed.or_else(|| proposal_in(&effects));
    }
    assert_eq!(replica.view(), View::new(view), "in view {view}");
    (proposed.unwrap_or_else(|| (*last).clone()), decided)
}

#[test]
fn the_commands_a_block_leaves_out_go_with_the_vote() {
    let node = follower(&[1, 2]);
    let mut replica = started(node);
    let other = Proposal::sign(
        &key(leader(1).get()),
        DOMAIN,
        leader(1),
        Block::new(View::new(1), Justify::Genesis, vec![b"other".to_vec()]),
        None,
    );
    let effects = receive(&mut replica, Message::Proposal(other));
    let sent: Vec<(To, &Message)> = effects
        .messages()
        .iter()
        .map(|outgoing| (outgoing.to(), outgoing.message()))
        .collect();
    let forward = Message::Forward(vec![b"command".to_vec()]);
    assert_eq!(sent.len(), 2, "a vote and its commands: {sent:?}");
    assert!(
        matches!(sent[0], (To::One(to), Message::Vote(_)) if to == leader(2)),
        "the vote"
    );
    assert_eq!(sent[1], (To::One(leader(2)), &forward), "the commands");
}

#[test]
fn a_leader_that_proposed_hands_later_commands_on() {
    let node = leader(1);
    assert_ne!(leader(2), node, "another leader next");
    let mut first = started(node);
    let late = b"late".to_vec();
    let effects = first.handle(Event::Submitted(late.clone())).persisted();
    let [outgoing] = effects.messages() else {
        panic!("one message: {:?}", effects.messages());
    };
    assert_eq!(outgoing.to(), To::One(leader(2)), "to the next leader");
    let Message::Forward(commands) = outgoing.message() else {
        panic!("forwards: {outgoing:?}");
    };
    // Its own block is not certified yet: its commands go along too.
    assert!(commands.contains(&late), "the command: {commands:?}");
}

#[test]
fn a_quorum_of_shares_certifies_a_checkpoint() {
    let checkpoint = Checkpoint::new(
        Height::new(1000),
        BlockId::new([5; 32]),
        StateRoot::new([6; 32]),
    );
    let mut ours = core(NodeId::new(1));
    let reached = ours.handle(Event::Reached(checkpoint)).persisted();
    let [sent] = reached.messages() else {
        panic!("one share: {:?}", reached.messages());
    };
    assert_eq!(sent.to(), To::Others, "to the others");
    let Message::Checkpoint(share) = sent.message() else {
        panic!("a share: {:?}", sent.message());
    };
    assert_eq!(share.verify(&committee(), DOMAIN), Ok(()), "signed");
    assert!(reached.certified().is_none(), "one of three");

    // A share signed with another's key counts for nothing.
    let forged = Share::sign(&key(3), DOMAIN, NodeId::new(2), checkpoint);
    let refused = receive(&mut ours, Message::Checkpoint(forged));
    assert_eq!(
        refused.rejected(),
        Some(Rejection::Forged(NodeId::new(2))),
        "forged"
    );

    let second = Share::sign(&key(2), DOMAIN, NodeId::new(2), checkpoint);
    assert!(
        receive(&mut ours, Message::Checkpoint(second))
            .certified()
            .is_none(),
        "two"
    );
    let third = Share::sign(&key(3), DOMAIN, NodeId::new(3), checkpoint);
    let certified = receive(&mut ours, Message::Checkpoint(third));
    let certificate = certified.certified().unwrap();
    assert_eq!(certificate.checkpoint(), &checkpoint, "certified");
    assert_eq!(
        certificate.verify(&committee(), DOMAIN),
        Ok(()),
        "by a quorum"
    );
}
