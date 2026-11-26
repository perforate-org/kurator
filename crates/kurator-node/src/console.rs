//! The console a replica reads commands from, one per line.
//!
//! ```text
//! put <key> <value>        write through this replica, which must lead
//! del <key>
//! get <key>                a linearizable read through the leader
//! status                   role, term, leader and log positions
//! members                  voters and learners with their peers
//! add <id> <addr> <key>    add a learner
//! promote <id>             make a learner that caught up a voter
//! remove <id>              take a node out
//! quit
//! ```

use kurator_net::Replica;
use kurator_raft::{Membership, NodeId, Peer};
use kurator_storage::WriteBatch;

use crate::args::{addr, key, node_id};

/// One console command.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    /// Writes a value.
    Put {
        /// The key.
        key: String,
        /// The value.
        value: String,
    },
    /// Deletes a key.
    Delete(String),
    /// Reads a key.
    Get(String),
    /// Shows where the replica stands.
    Status,
    /// Shows the membership.
    Members,
    /// Adds a learner.
    Add(NodeId, Peer),
    /// Promotes a learner.
    Promote(NodeId),
    /// Removes a node.
    Remove(NodeId),
    /// Stops the replica.
    Quit,
}

/// What came of a line.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply {
    /// Show this and read the next line.
    Show(String),
    /// Stop.
    Quit,
}

const HELP: &str = "commands: put <k> <v> | del <k> | get <k> | status | members \
                    | add <id> <addr> <key> | promote <id> | remove <id> | quit";

impl Command {
    /// Reads one line; `None` when it is not a command.
    #[must_use]
    pub fn parse(line: &str) -> Option<Self> {
        let words: Vec<&str> = line.split_whitespace().collect();
        let command = match words.as_slice() {
            ["put", key, value] => Self::Put {
                key: (*key).to_owned(),
                value: (*value).to_owned(),
            },
            ["del", key] => Self::Delete((*key).to_owned()),
            ["get", key] => Self::Get((*key).to_owned()),
            ["status"] => Self::Status,
            ["members"] => Self::Members,
            ["add", id, at, public] => Self::Add(node_id(id)?, Peer::new(addr(at)?, key(public)?)),
            ["promote", id] => Self::Promote(node_id(id)?),
            ["remove", id] => Self::Remove(node_id(id)?),
            ["quit"] => Self::Quit,
            _ => return None,
        };
        Some(command)
    }
}

/// Runs one line against `replica`.
pub async fn execute(replica: &Replica, line: &str) -> Reply {
    let Some(command) = Command::parse(line) else {
        return Reply::Show(HELP.to_owned());
    };
    let shown = match command {
        Command::Put { key, value } => {
            logged(replica.write(WriteBatch::new().put(key, value)).await)
        }
        Command::Delete(key) => logged(replica.write(WriteBatch::new().delete(key)).await),
        Command::Get(key) => {
            let read = replica
                .read(|state| {
                    state
                        .get(key.as_bytes())
                        .map(|value| String::from_utf8_lossy(value).into_owned())
                })
                .await;
            match read {
                Ok(Some(value)) => value,
                Ok(None) => "(none)".to_owned(),
                Err(error) => format!("error: {error}"),
            }
        }
        Command::Status => format!("{:?}", replica.status()),
        Command::Members => members(&replica.membership()),
        Command::Add(id, peer) => logged(replica.add_learner(id, peer).await),
        Command::Promote(id) => logged(replica.promote(id).await),
        Command::Remove(id) => logged(replica.remove(id).await),
        Command::Quit => return Reply::Quit,
    };
    Reply::Show(shown)
}

fn logged<T: std::fmt::Display>(result: kurator_raft::Result<T>) -> String {
    match result {
        Ok(log_id) => format!("ok at {log_id}"),
        Err(error) => format!("error: {error}"),
    }
}

/// One line per node: `voter` or `learner`, its id and its peer.
fn members(membership: &Membership) -> String {
    let voters: Vec<NodeId> = membership.voter_ids().collect();
    let lines: Vec<String> = membership
        .nodes()
        .map(|(id, peer)| {
            let role = if voters.contains(id) {
                "voter"
            } else {
                "learner"
            };
            format!("{role} {} {peer}", id.get())
        })
        .collect();
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use kurator_raft::{KeyPurpose, Membership, NodeId, NodeSecret, Peer};

    use super::{Command, members};

    fn peer(id: u8) -> Peer {
        let key = NodeSecret::from_seed([id; 32]).public_key(KeyPurpose::Transport);
        Peer::new(format!("127.0.0.1:700{id}").parse().unwrap(), key)
    }

    #[test]
    fn lines_parse_into_commands() {
        let key = peer(4).key().to_string();
        for (line, command) in [
            (
                "put k v",
                Some(Command::Put {
                    key: "k".to_owned(),
                    value: "v".to_owned(),
                }),
            ),
            ("del k", Some(Command::Delete("k".to_owned()))),
            ("  get   k ", Some(Command::Get("k".to_owned()))),
            ("status", Some(Command::Status)),
            ("members", Some(Command::Members)),
            (
                &format!("add 4 127.0.0.1:7004 {key}"),
                Some(Command::Add(NodeId::new(4), peer(4))),
            ),
            ("promote 4", Some(Command::Promote(NodeId::new(4)))),
            ("remove 2", Some(Command::Remove(NodeId::new(2)))),
            ("quit", Some(Command::Quit)),
            ("put k", None),
            ("promote four", None),
            ("add 4 nowhere key", None),
            ("", None),
        ] {
            assert_eq!(Command::parse(line), command, "{line:?}");
        }
    }

    #[test]
    fn members_show_roles_and_peers() {
        let membership = Membership::new(
            vec![BTreeSet::from([NodeId::new(1)])],
            BTreeMap::from([(NodeId::new(1), peer(1)), (NodeId::new(2), peer(2))]),
        )
        .unwrap();
        assert_eq!(
            members(&membership),
            format!("voter 1 {}\nlearner 2 {}", peer(1), peer(2)),
            "listing"
        );
    }
}
