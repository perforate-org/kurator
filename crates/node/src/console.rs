//! The console a node reads commands from, one per line.
//!
//! Reads and writes go to the node's replica of the shard that holds the
//! key; membership changes go to every shard the node holds.
//!
//! ```text
//! put <key> <value>        write through this node, which must lead the key's shard
//! del <key>
//! get <key>                a linearizable read through the leader
//! split <key> <id>         split the shard that holds <key> there; the new shard is <id>
//! shards                   the shards this node holds, with their spans and epochs
//! status                   role, term, leader and log positions, by shard
//! members                  voters and learners with their peers, by shard
//! add <id> <addr> <key>    add a learner to every shard
//! promote <id>             make a learner that caught up a voter in every shard
//! remove <id>              take a node out of every shard
//! quit
//! ```

use std::sync::Arc;

use kurator_net::{Replica, Store};
use kurator_raft::{Membership, NodeId, Peer};
use kurator_shard::ShardId;
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
    /// Splits the shard that holds a key there.
    Split {
        /// Where the new shard starts.
        at: String,
        /// The new shard; it must name no shard that ever existed.
        right: ShardId,
    },
    /// Lists the shards.
    Shards,
    /// Shows where each replica stands.
    Status,
    /// Shows the membership.
    Members,
    /// Adds a learner.
    Add(NodeId, Peer),
    /// Promotes a learner.
    Promote(NodeId),
    /// Removes a node.
    Remove(NodeId),
    /// Stops the node.
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

const HELP: &str = "commands: put <k> <v> | del <k> | get <k> | split <k> <id> | shards \
                    | status | members | add <id> <addr> <key> | promote <id> \
                    | remove <id> | quit";

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
            ["split", at, right] => Self::Split {
                at: (*at).to_owned(),
                right: ShardId::new(right.parse().ok()?),
            },
            ["shards"] => Self::Shards,
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

/// Runs one line against `store`.
pub async fn execute(store: &Store, line: &str) -> Reply {
    let Some(command) = Command::parse(line) else {
        return Reply::Show(HELP.to_owned());
    };
    let shown = match command {
        Command::Put { key, value } => {
            let batch = WriteBatch::new().put(key.clone(), value);
            match holder(store, &key) {
                Some(replica) => logged(replica.write(batch).await),
                None => unheld(),
            }
        }
        Command::Delete(key) => match holder(store, &key) {
            Some(replica) => logged(replica.write(WriteBatch::new().delete(key)).await),
            None => unheld(),
        },
        Command::Get(key) => match holder(store, &key) {
            Some(replica) => get(&replica, &key).await,
            None => unheld(),
        },
        Command::Split { at, right } => split(store, &at, right).await,
        Command::Shards => by_shard(store, |_| String::new()),
        Command::Status => by_shard(store, |replica| format!(": {:?}", replica.status())),
        Command::Members => by_shard(store, |replica| members(&replica.membership())),
        Command::Add(id, peer) => {
            let mut outcomes = Vec::new();
            for replica in held(store) {
                outcomes.push((
                    replica.shard_id(),
                    replica.add_learner(id, peer.clone()).await,
                ));
            }
            everywhere(outcomes)
        }
        Command::Promote(id) => {
            let mut outcomes = Vec::new();
            for replica in held(store) {
                outcomes.push((replica.shard_id(), replica.promote(id).await));
            }
            everywhere(outcomes)
        }
        Command::Remove(id) => {
            let mut outcomes = Vec::new();
            for replica in held(store) {
                outcomes.push((replica.shard_id(), replica.remove(id).await));
            }
            everywhere(outcomes)
        }
        Command::Quit => return Reply::Quit,
    };
    Reply::Show(shown)
}

/// The replicas of `store` that hold a state, by shard.
fn held(store: &Store) -> impl Iterator<Item = Arc<Replica>> {
    store
        .replicas()
        .into_iter()
        .filter(|replica| replica.is_initialized())
}

/// The replica of the shard that holds `key`, as this node knows it.
fn holder(store: &Store, key: &str) -> Option<Arc<Replica>> {
    held(store).find(|replica| replica.shard().span().contains(key.as_bytes()))
}

fn unheld() -> String {
    "error: no shard on this node holds the key".to_owned()
}

async fn get(replica: &Replica, key: &str) -> String {
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

async fn split(store: &Store, at: &str, right: ShardId) -> String {
    let Some(replica) = holder(store, at) else {
        return unheld();
    };
    match store.split(replica.shard_id(), at.as_bytes(), right).await {
        Ok(halves) => format!("ok: {} | {}", halves.left(), halves.right()),
        Err(error) => format!("error: {error}"),
    }
}

/// One block per shard: the shard, then what `show` says of its replica.
fn by_shard(store: &Store, show: impl Fn(&Replica) -> String) -> String {
    let lines: Vec<String> = held(store)
        .map(|replica| format!("{}{}", replica.shard(), show(&replica)))
        .collect();
    lines.join("\n")
}

/// `ok` with every shard that took a change, or the first refusal.
fn everywhere<T>(outcomes: Vec<(ShardId, kurator_raft::Result<T>)>) -> String {
    let mut shards = Vec::new();
    for (shard, outcome) in outcomes {
        match outcome {
            Ok(_) => shards.push(shard.to_string()),
            Err(error) => return format!("error on {shard}: {error}"),
        }
    }
    format!("ok on {}", shards.join(", "))
}

fn logged<T: std::fmt::Display>(result: kurator_raft::Result<T>) -> String {
    match result {
        Ok(log_id) => format!("ok at {log_id}"),
        Err(error) => format!("error: {error}"),
    }
}

/// One line per node, each after a line break: `voter` or `learner`, its
/// id and its peer.
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
            format!("\n{role} {} {peer}", id.get())
        })
        .collect();
    lines.concat()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use kurator_raft::{KeyPurpose, Membership, NodeId, NodeSecret, Peer};

    use kurator_shard::ShardId;

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
            (
                "split m 2",
                Some(Command::Split {
                    at: "m".to_owned(),
                    right: ShardId::new(2),
                }),
            ),
            ("shards", Some(Command::Shards)),
            ("split m two", None),
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
            format!("\nvoter 1 {}\nlearner 2 {}", peer(1), peer(2)),
            "listing"
        );
    }
}
