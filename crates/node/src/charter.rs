//! A BFT shard's charter: the static file that names the shard and the
//! committee that decides it, the same on every member.
//!
//! ```text
//! # a comment
//! shard <id>
//! member <id> <addr> <transport key> <consensus key> <operator>
//! ```
//!
//! Keys are as `oleaph key` prints them. An operator is one word, and no
//! operator runs two members: a committee tolerates a third of its
//! members failing only when they fail apart.
//!
//! Until committees change in consensus, a charter is all a node knows of
//! its BFT shard: change it on every member at once, or not at all.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use oleaph_bft::{Committee, Config, Core, Domain, Journal, Ledger};
use oleaph_crypto::{KeyPurpose, NodeSecret, VerifyingKey};
use oleaph_net::{BftReplica, Quic, Wiring};
use oleaph_raft::{NodeId, Peer};
use oleaph_shard::{Shard, ShardId, ShardState};
use oleaph_storage::KvState;

use crate::NodeError;
use crate::args::{addr, key, node_id};

/// A BFT shard and its committee, as its charter names them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Charter {
    shard: ShardId,
    committee: Committee,
    peers: BTreeMap<NodeId, Peer>,
}

/// What is wrong with a charter.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CharterError {
    /// One line is wrong.
    #[error("line {line}: {problem}")]
    Line {
        /// Its number, from 1.
        line: usize,
        /// What is wrong with it.
        problem: Problem,
    },
    /// No line names the shard.
    #[error("no `shard <id>` line")]
    NoShard,
    /// No line names a member.
    #[error("no `member` line")]
    NoMembers,
    /// The node is not a member.
    #[error("{0} is not a member of the committee")]
    Absent(NodeId),
    /// The node's secret derives other keys than the charter lists for it.
    #[error("the charter lists other keys for {0} than its secret derives")]
    OtherKeys(NodeId),
}

/// What is wrong with one line of a charter.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Problem {
    /// A line that is neither a shard's nor a member's.
    #[error(
        "expected `shard <id>` or `member <id> <addr> <transport key> <consensus key> <operator>`"
    )]
    Unknown,
    /// A shard or member line whose fields do not read.
    #[error("cannot read it")]
    Unreadable,
    /// A second shard line.
    #[error("a charter names one shard")]
    SecondShard,
    /// A member named twice.
    #[error("{0} is a member already")]
    SameNode(NodeId),
    /// An operator that runs a member already.
    #[error("{0} runs a member already")]
    SameOperator(String),
}

/// A member line's fields.
struct Line {
    node: NodeId,
    peer: Peer,
    consensus: VerifyingKey,
    operator: String,
}

/// The lines read so far.
#[derive(Default)]
struct Draft {
    shard: Option<ShardId>,
    voters: BTreeMap<NodeId, VerifyingKey>,
    peers: BTreeMap<NodeId, Peer>,
    operators: BTreeSet<String>,
}

impl Draft {
    /// Takes in one line.
    fn line(&mut self, words: &[&str]) -> Result<(), Problem> {
        match words {
            [] => Ok(()),
            ["shard", id] => {
                let id = id.parse().map_err(|_| Problem::Unreadable)?;
                match self.shard.replace(ShardId::new(id)) {
                    Some(_) => Err(Problem::SecondShard),
                    None => Ok(()),
                }
            }
            ["member", fields @ ..] => self.seat(member(fields).ok_or(Problem::Unreadable)?),
            _ => Err(Problem::Unknown),
        }
    }

    /// The charter the lines make.
    fn finish(self) -> Result<Charter, CharterError> {
        Ok(Charter {
            shard: self.shard.ok_or(CharterError::NoShard)?,
            committee: Committee::new(self.voters).ok_or(CharterError::NoMembers)?,
            peers: self.peers,
        })
    }

    fn seat(&mut self, seat: Line) -> Result<(), Problem> {
        if self.peers.contains_key(&seat.node) {
            return Err(Problem::SameNode(seat.node));
        }
        if !self.operators.insert(seat.operator.clone()) {
            return Err(Problem::SameOperator(seat.operator));
        }
        drop(self.voters.insert(seat.node, seat.consensus));
        drop(self.peers.insert(seat.node, seat.peer));
        Ok(())
    }
}

/// `<id> <addr> <transport key> <consensus key> <operator>`.
fn member(fields: &[&str]) -> Option<Line> {
    let [id, at, transport, consensus, operator] = fields else {
        return None;
    };
    Some(Line {
        node: node_id(id)?,
        peer: Peer::new(addr(at)?, key(transport)?),
        consensus: key(consensus)?,
        operator: (*operator).to_owned(),
    })
}

impl Charter {
    /// Reads a charter.
    ///
    /// # Errors
    ///
    /// When `text` is not a valid charter.
    pub fn parse(text: &str) -> Result<Self, CharterError> {
        let mut draft = Draft::default();
        for (index, line) in text.lines().enumerate() {
            let content = line.split('#').next().unwrap_or_default();
            let words: Vec<&str> = content.split_whitespace().collect();
            draft.line(&words).map_err(|problem| CharterError::Line {
                line: index.saturating_add(1),
                problem,
            })?;
        }
        draft.finish()
    }

    /// The shard.
    #[must_use]
    pub const fn shard(&self) -> ShardId {
        self.shard
    }

    /// The committee that decides it.
    #[must_use]
    pub const fn committee(&self) -> &Committee {
        &self.committee
    }

    /// Where each member is reached, and its transport key.
    #[must_use]
    pub const fn peers(&self) -> &BTreeMap<NodeId, Peer> {
        &self.peers
    }

    /// Seats node `node`, which `secret` signs for, in the committee.
    ///
    /// # Errors
    ///
    /// When the node is not a member under the keys `secret` derives.
    pub fn seat(self, node: NodeId, secret: &NodeSecret) -> Result<Seat<'_>, CharterError> {
        let (Some(consensus), Some(peer)) = (self.committee.key(node), self.peers.get(&node))
        else {
            return Err(CharterError::Absent(node));
        };
        let mine = *consensus == secret.public_key(KeyPurpose::Consensus)
            && *peer.key() == secret.public_key(KeyPurpose::Transport);
        if mine {
            Ok(Seat {
                charter: self,
                node,
                secret,
            })
        } else {
            Err(CharterError::OtherKeys(node))
        }
    }
}

/// A node seated in a charter's committee under its own keys.
#[derive(Debug)]
pub struct Seat<'secret> {
    charter: Charter,
    node: NodeId,
    secret: &'secret NodeSecret,
}

impl Seat<'_> {
    /// Starts the node's replica of the shard, with its journal in `dir`,
    /// reaching the others through `quic`. A replica that ran before goes
    /// on from what its journal holds.
    ///
    /// # Errors
    ///
    /// When the journal cannot be opened, or there is no Tokio runtime.
    pub fn start(self, dir: &Path, quic: Quic) -> Result<Arc<BftReplica>, NodeError> {
        let Charter {
            shard,
            committee,
            peers,
        } = self.charter;
        let (journal, recovered) = Journal::open(dir, oleaph_wal::Options::default())?;
        let state = ShardState::new(Shard::whole(shard), KvState::default());
        let ledger = Ledger::replay(state, &recovered);
        let domain = Domain::of(shard, &committee);
        let key = self.secret.signing_key(KeyPurpose::Consensus);
        let core = Core::recover(Config::new(self.node, key, committee, domain), recovered);
        Ok(Wiring::new(quic, shard)?.start(core, journal, ledger, peers))
    }
}

#[cfg(test)]
mod tests {
    use oleaph_crypto::{KeyPurpose, NodeSecret};
    use oleaph_raft::NodeId;
    use oleaph_shard::ShardId;

    use super::{Charter, CharterError, Problem};

    fn secret(id: u8) -> NodeSecret {
        NodeSecret::from_seed([id; 32])
    }

    /// The member line of node `id`, run by `operator`.
    fn member(id: u8, operator: &str) -> String {
        let transport = secret(id).public_key(KeyPurpose::Transport);
        let consensus = secret(id).public_key(KeyPurpose::Consensus);
        format!("member {id} 127.0.0.1:700{id} {transport} {consensus} {operator}")
    }

    #[test]
    fn a_charter_names_its_shard_and_committee() {
        let text = format!(
            "# shard forty\nshard 40\n\n{}\n{}  # the second\n",
            member(1, "alice"),
            member(2, "bob")
        );
        let charter = Charter::parse(&text).unwrap();
        assert_eq!(charter.shard(), ShardId::new(40), "shard");
        assert_eq!(charter.committee().size(), 2, "members");
        let peer = &charter.peers()[&NodeId::new(2)];
        assert_eq!(peer.addr().port(), 7002, "addr");
        assert_eq!(
            peer.key(),
            &secret(2).public_key(KeyPurpose::Transport),
            "transport key"
        );
        assert_eq!(
            charter.committee().key(NodeId::new(1)),
            Some(&secret(1).public_key(KeyPurpose::Consensus)),
            "consensus key"
        );
        let seated =
            |node, secret: &NodeSecret| charter.clone().seat(NodeId::new(node), secret).map(|_| ());
        assert_eq!(seated(1, &secret(1)), Ok(()), "seated");
        assert_eq!(
            seated(1, &secret(2)),
            Err(CharterError::OtherKeys(NodeId::new(1))),
            "another's secret"
        );
        assert_eq!(
            seated(3, &secret(3)),
            Err(CharterError::Absent(NodeId::new(3))),
            "not a member"
        );
        // One key of its own is not enough.
        let transport = secret(1).public_key(KeyPurpose::Transport);
        let consensus = secret(2).public_key(KeyPurpose::Consensus);
        let mixed = format!("shard 1\nmember 1 127.0.0.1:1 {transport} {consensus} a");
        assert_eq!(
            Charter::parse(&mixed)
                .unwrap()
                .seat(NodeId::new(1), &secret(1))
                .map(|_| ()),
            Err(CharterError::OtherKeys(NodeId::new(1))),
            "another's consensus key"
        );
    }

    #[test]
    fn a_bad_charter_says_what_is_wrong() {
        let line = |line, problem| Err(CharterError::Line { line, problem });
        let cases = [
            ("shard x".to_owned(), line(1, Problem::Unreadable)),
            ("shard 1\nshard 2".to_owned(), line(2, Problem::SecondShard)),
            ("keeper 1".to_owned(), line(1, Problem::Unknown)),
            ("member 1 nowhere".to_owned(), line(1, Problem::Unreadable)),
            (
                format!("{}\n{}", member(1, "a"), member(1, "b")),
                line(2, Problem::SameNode(NodeId::new(1))),
            ),
            (
                format!("{}\n{}", member(1, "a"), member(2, "a")),
                line(2, Problem::SameOperator("a".to_owned())),
            ),
            (member(1, "a"), Err(CharterError::NoShard)),
            ("shard 1".to_owned(), Err(CharterError::NoMembers)),
        ];
        for (text, expected) in cases {
            assert_eq!(Charter::parse(&text), expected, "{text}");
        }
    }
}
