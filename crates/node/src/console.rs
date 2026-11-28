//! The console a node reads commands from, one per line.
//!
//! Reads and writes go to the node's replica of the shard that holds the
//! key; membership changes go to every shard the node holds, the meta
//! shard among them. A split, `shards`, the namespace commands and
//! `in <namespace>` ask the meta shard's leader, found through this node's
//! replica of the meta shard, wherever it is.
//!
//! A key is a key of the default namespace, or of the one `in` names.
//!
//! ```text
//! put <key> <value> [in <namespace>]   write through this node, which must lead the key's shard
//! del <key> [in <namespace>]
//! get <key> [in <namespace>]           a linearizable read through the leader
//! split <key> [in <namespace>]         split the shard that holds <key> there, under an id the meta shard allocates
//! shards                   every shard the meta shard records, with its span, epoch, namespace and replicas
//! namespace <name> cft     make a namespace whose shards are crash fault tolerant, on the meta shard's voters
//! namespaces               every namespace by name, with its id and assurance
//! status                   role, term, leader and log positions, by shard
//! members                  voters and learners with their peers, by shard
//! add <id> <addr> <key>    add a learner to every shard
//! promote <id>             make a learner that caught up a voter in every shard
//! remove <id>              take a node out of every shard
//! collect                  forget the decisions on transactions across shards long past their deadline
//! backup <dir>             write every shard this node holds to <dir>, which must not exist; see `run --restore`
//! quit
//! ```
//!
//! The node that leads the meta shard also collects on its own, every
//! [`sweep::EVERY`](crate::sweep::EVERY), and every node splits the shards
//! it leads once they pass their ceiling, every
//! [`split::EVERY`](crate::split::EVERY).

use std::path::PathBuf;
use std::sync::Arc;

use oleaph_client::Collected;
use oleaph_consensus::{Members, NodeId, Peer, Replicated as _};
use oleaph_meta::{Assurance, Meta, NamespaceName};
use oleaph_net::{Quic, Replica, Store};
use oleaph_shard::{NamespaceId, ShardId};
use oleaph_storage::WriteBatch;

use crate::args::{addr, key, node_id};
use crate::{namespace, report};

/// A key the console names: the key, and the namespace it is a key of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    namespace: NamespaceName,
    key: String,
}

impl Key {
    /// `key` of the namespace called `namespace`.
    #[must_use]
    pub const fn new(namespace: NamespaceName, key: String) -> Self {
        Self { namespace, key }
    }

    /// Reads `key`, then `in <namespace>` or nothing for the default
    /// namespace.
    fn parse(key: &str, rest: &[&str]) -> Option<Self> {
        let namespace = match rest {
            [] => NamespaceName::default_name(),
            ["in", name] => name.parse().ok()?,
            _ => return None,
        };
        Some(Self::new(namespace, key.to_owned()))
    }
}

/// One console command.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    /// Writes a value.
    Put {
        /// The key.
        key: Key,
        /// The value.
        value: String,
    },
    /// Deletes a key.
    Delete(Key),
    /// Reads a key.
    Get(Key),
    /// Splits the shard that holds a key there.
    Split(Key),
    /// Lists the shards the registry records.
    Shards,
    /// Makes a namespace.
    Namespace(NamespaceName, Assurance),
    /// Lists the namespaces the registry records.
    Namespaces,
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
    /// Forgets old decisions on transactions across shards.
    Collect,
    /// Backs up every shard the node holds to a new directory.
    Backup(PathBuf),
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

const HELP: &str = "commands: put <k> <v> | del <k> | get <k> | split <k>, each [in <namespace>] \
                    | shards | namespace <name> cft | namespaces \
                    | status | members | add <id> <addr> <key> | promote <id> \
                    | remove <id> | collect | backup <dir> | quit";

impl Command {
    /// Reads one line; `None` when it is not a command.
    #[must_use]
    pub fn parse(line: &str) -> Option<Self> {
        let words: Vec<&str> = line.split_whitespace().collect();
        let command = match words.as_slice() {
            ["put", key, value, rest @ ..] => Self::Put {
                key: Key::parse(key, rest)?,
                value: (*value).to_owned(),
            },
            ["del", key, rest @ ..] => Self::Delete(Key::parse(key, rest)?),
            ["get", key, rest @ ..] => Self::Get(Key::parse(key, rest)?),
            ["split", at, rest @ ..] => Self::Split(Key::parse(at, rest)?),
            ["shards"] => Self::Shards,
            ["namespace", name, assurance] => {
                Self::Namespace(name.parse().ok()?, assurance.parse().ok()?)
            }
            ["namespaces"] => Self::Namespaces,
            ["status"] => Self::Status,
            ["members"] => Self::Members,
            ["add", id, at, public] => Self::Add(node_id(id)?, Peer::new(addr(at)?, key(public)?)),
            ["promote", id] => Self::Promote(node_id(id)?),
            ["remove", id] => Self::Remove(node_id(id)?),
            ["collect"] => Self::Collect,
            ["backup", dir] => Self::Backup(PathBuf::from(dir)),
            ["quit"] => Self::Quit,
            _ => return None,
        };
        Some(command)
    }
}

/// Runs one line against `store`, asking other nodes through `ask`.
pub async fn execute(store: &Store, ask: &Quic, line: &str) -> Reply {
    let Some(command) = Command::parse(line) else {
        return Reply::Show(HELP.to_owned());
    };
    let shown = match command {
        Command::Put { key, value } => match holder(store, ask, &key).await {
            Ok(replica) => logged(replica.write(WriteBatch::new().put(key.key, value)).await),
            Err(error) => error,
        },
        Command::Delete(key) => match holder(store, ask, &key).await {
            Ok(replica) => logged(replica.write(WriteBatch::new().delete(key.key)).await),
            Err(error) => error,
        },
        Command::Get(key) => match holder(store, ask, &key).await {
            Ok(replica) => get(&replica, &key.key).await,
            Err(error) => error,
        },
        Command::Split(at) => split(store, ask, &at).await,
        Command::Shards => shards(store, ask).await,
        Command::Namespace(name, assurance) => {
            match Box::pin(namespace::create(store, ask, &name, assurance)).await {
                Ok(shard) => format!("ok: {name} starts as {shard}"),
                Err(error) => format!("error: {error}"),
            }
        }
        Command::Namespaces => namespaces(store, ask).await,
        Command::Status => by_shard(store, |replica| format!(": {:?}", replica.status())),
        Command::Members => by_shard(store, |replica| members(&replica.members())),
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
        Command::Collect => match crate::sweep::collect(store, ask).await {
            Collected::Forgotten => "ok: forgotten".to_owned(),
            Collected::Unfinished | _ => "error: a shard did not answer; try again".to_owned(),
        },
        Command::Backup(dir) => {
            let backup = store.back_up();
            let shards = backup.shards().count();
            match backup.write(&dir) {
                Ok(()) => format!("ok: {shards} shards in {}", dir.display()),
                Err(error) => format!("error: {error}"),
            }
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

/// The replica of the shard that holds `key`, as this node knows it, or
/// what to show instead. The meta shard holds none of the cluster's keys.
async fn holder(store: &Store, ask: &Quic, key: &Key) -> Result<Arc<Replica>, String> {
    // Boxed: the meta shard's calls nest deeper than the console's
    // future can lay out.
    let namespace = Box::pin(namespace::resolve(store, ask, &key.namespace))
        .await
        .map_err(|error| format!("error: {error}"))?;
    held_in(store, namespace, &key.key).ok_or_else(unheld)
}

/// The replica of the shard of `namespace` that holds `key`, if this node
/// holds one.
fn held_in(store: &Store, namespace: NamespaceId, key: &str) -> Option<Arc<Replica>> {
    held(store).find(|replica| {
        let shard = replica.shard();
        replica.shard_id() != ShardId::META
            && shard.namespace() == namespace
            && shard.span().contains(key.as_bytes())
    })
}

/// This node's replica of the meta shard.
fn meta(store: &Store) -> Option<Arc<Replica>> {
    store.inbox().replica(ShardId::META)
}

fn no_meta() -> String {
    "error: this node holds no replica of the meta shard".to_owned()
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

/// Splits the shard that holds `at` there; see [`crate::split::split`].
async fn split(store: &Store, ask: &Quic, at: &Key) -> String {
    let replica = match holder(store, ask, at).await {
        Ok(replica) => replica,
        Err(error) => return error,
    };
    match crate::split::split(store, ask, replica.shard_id(), at.key.as_bytes()).await {
        Ok(halves) => format!("ok: {} | {}", halves.left(), halves.right()),
        Err(error) => format!("error: {error}"),
    }
}

/// Reports every shard this node leads to the meta shard, then lists what
/// the meta shard records, one shard a line.
async fn shards(store: &Store, ask: &Quic) -> String {
    let Some(meta) = meta(store) else {
        return no_meta();
    };
    if let Err(error) = report::heartbeat(store, ask).await {
        return format!("error: {error}");
    }
    match Meta::new(&meta, ask).registry().await {
        Ok(registry) => {
            let lines: Vec<String> = registry.placements().map(ToString::to_string).collect();
            lines.join("\n")
        }
        Err(error) => format!("error: {error}"),
    }
}

/// Lists the namespaces the meta shard records, one a line.
async fn namespaces(store: &Store, ask: &Quic) -> String {
    let Some(meta) = meta(store) else {
        return no_meta();
    };
    match Meta::new(&meta, ask).registry().await {
        Ok(registry) => {
            let lines: Vec<String> = registry
                .namespaces()
                .map(|(name, namespace)| format!("{name} {namespace}"))
                .collect();
            lines.join("\n")
        }
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
fn everywhere<T>(outcomes: Vec<(ShardId, oleaph_raft::Result<T>)>) -> String {
    let mut shards = Vec::new();
    for (shard, outcome) in outcomes {
        match outcome {
            Ok(_) => shards.push(shard.to_string()),
            Err(error) => return format!("error on {shard}: {error}"),
        }
    }
    format!("ok on {}", shards.join(", "))
}

fn logged<T: std::fmt::Display>(result: oleaph_raft::Result<T>) -> String {
    match result {
        Ok(log_id) => format!("ok at {log_id}"),
        Err(error) => format!("error: {error}"),
    }
}

/// One line per node, each after a line break: `voter` or `learner`, its
/// id and its peer.
fn members(members: &Members) -> String {
    let lines: Vec<String> = members
        .peers()
        .map(|(id, peer)| {
            let role = if members.votes(id) {
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

    use oleaph_consensus::{Members, NodeId, Peer};
    use oleaph_crypto::{KeyPurpose, NodeSecret};

    use oleaph_meta::{Assurance, NamespaceName};

    use super::{Command, Key, members};

    /// `key` of the default namespace.
    fn default(key: &str) -> Key {
        Key::new(NamespaceName::default_name(), key.to_owned())
    }

    fn peer(id: u8) -> Peer {
        let key = NodeSecret::from_seed([id; 32]).public_key(KeyPurpose::Transport);
        Peer::new(format!("127.0.0.1:700{id}").parse().unwrap(), key)
    }

    #[test]
    fn lines_parse_into_commands() {
        let key = peer(4).key().to_string();
        let payments: NamespaceName = "payments".parse().unwrap();
        for (line, command) in [
            (
                "put k v",
                Some(Command::Put {
                    key: default("k"),
                    value: "v".to_owned(),
                }),
            ),
            (
                "put k v in payments",
                Some(Command::Put {
                    key: Key::new(payments.clone(), "k".to_owned()),
                    value: "v".to_owned(),
                }),
            ),
            ("del k", Some(Command::Delete(default("k")))),
            ("split m", Some(Command::Split(default("m")))),
            (
                "get k in payments",
                Some(Command::Get(Key::new(payments.clone(), "k".to_owned()))),
            ),
            ("get k in Payments", None),
            ("get k payments", None),
            ("get k in", None),
            ("shards", Some(Command::Shards)),
            (
                "namespace payments cft",
                Some(Command::Namespace(payments, Assurance::Cft)),
            ),
            ("namespace payments raft", None),
            ("namespace payments", None),
            ("namespaces", Some(Command::Namespaces)),
            ("split m 2", None),
            ("  get   k ", Some(Command::Get(default("k")))),
            ("status", Some(Command::Status)),
            ("members", Some(Command::Members)),
            (
                &format!("add 4 127.0.0.1:7004 {key}"),
                Some(Command::Add(NodeId::new(4), peer(4))),
            ),
            ("promote 4", Some(Command::Promote(NodeId::new(4)))),
            ("remove 2", Some(Command::Remove(NodeId::new(2)))),
            ("quit", Some(Command::Quit)),
            ("backup b", Some(Command::Backup("b".into()))),
            ("backup", None),
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
        let membership = Members::new(
            BTreeSet::from([NodeId::new(1)]),
            BTreeMap::from([(NodeId::new(1), peer(1)), (NodeId::new(2), peer(2))]),
            0,
        );
        assert_eq!(
            members(&membership),
            format!("\nvoter 1 {}\nlearner 2 {}", peer(1), peer(2)),
            "listing"
        );
    }
}
