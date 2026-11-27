//! The command line.
//!
//! ```text
//! kurator key <dir>
//! kurator run --id <n> --dir <dir> --listen <addr> [--peer <n>=<addr>,<key>]... [--client <key>]... [--split-above <bytes>] [--metrics <addr>] [--found | --restore <backup>]
//! kurator client --dir <dir> --peer <n>=<addr>,<key> [--peer ...]... (get <key> | put <key> <value> | del <key>)...
//! ```
//!
//! A client's operations run as one transaction, across every shard they
//! touch. A node splits a shard it leads once the shard's state takes more
//! than `--split-above` bytes, [`Ceiling::DEFAULT`] unless given.
//!
//! `--metrics` serves the node's metrics over HTTP, at `/metrics`.
//!
//! `--restore` founds a new cluster from a backup the console's `backup`
//! wrote: the node holds every shard of it, as their only voter, and
//! other nodes join it as learners.

use std::net::SocketAddr;
use std::path::PathBuf;

use kurator_crypto::VerifyingKey;
use kurator_raft::{NodeId, Peer};
use kurator_shard::Ceiling;

/// What the process was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Invocation {
    /// Create the node secret in a directory, if there is none, and print
    /// the transport key.
    Key(PathBuf),
    /// Run a replica.
    Run(Settings),
    /// Run operations on a cluster as a client.
    Client(Session),
}

/// What a client does: who it is, where it first asks, and the
/// operations of its transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    dir: PathBuf,
    peers: Vec<(NodeId, Peer)>,
    ops: Vec<Op>,
}

impl Session {
    /// Where the client keeps its secret.
    #[must_use]
    pub const fn dir(&self) -> &PathBuf {
        &self.dir
    }

    /// The nodes it asks first where the shards are.
    #[must_use]
    pub fn peers(&self) -> &[(NodeId, Peer)] {
        &self.peers
    }

    /// The operations, in order.
    #[must_use]
    pub fn ops(&self) -> &[Op] {
        &self.ops
    }
}

/// One operation of a client's transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Op {
    /// Read a key.
    Get(Vec<u8>),
    /// Set a key.
    Put(Vec<u8>, Vec<u8>),
    /// Remove a key.
    Delete(Vec<u8>),
}

/// How a replica runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    id: NodeId,
    dir: PathBuf,
    listen: SocketAddr,
    peers: Vec<(NodeId, Peer)>,
    clients: Vec<VerifyingKey>,
    ceiling: Ceiling,
    metrics: Option<SocketAddr>,
    start: Start,
}

/// How a replica comes into its cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Start {
    /// Wait: the cluster exists, or another node founds it, and a leader
    /// adds this one. Also how a replica restarts.
    Wait,
    /// Found the cluster, with this node and its peers as voters.
    Found,
    /// Found the cluster from the backup in this directory, with this node
    /// as the only voter.
    Restore(PathBuf),
}

impl Settings {
    /// The replica's id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Where it keeps its secret, log and snapshot.
    #[must_use]
    pub const fn dir(&self) -> &PathBuf {
        &self.dir
    }

    /// Where it listens.
    #[must_use]
    pub const fn listen(&self) -> SocketAddr {
        self.listen
    }

    /// The peers it trusts before it knows a membership.
    #[must_use]
    pub fn peers(&self) -> &[(NodeId, Peer)] {
        &self.peers
    }

    /// The keys of the clients it serves.
    #[must_use]
    pub fn clients(&self) -> &[VerifyingKey] {
        &self.clients
    }

    /// How large a shard it leads grows before it splits it.
    #[must_use]
    pub const fn ceiling(&self) -> Ceiling {
        self.ceiling
    }

    /// Where it serves its metrics, if anywhere.
    #[must_use]
    pub const fn metrics(&self) -> Option<SocketAddr> {
        self.metrics
    }

    /// How it comes into its cluster.
    #[must_use]
    pub const fn start(&self) -> &Start {
        &self.start
    }
}

/// What is wrong with the command line.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum UsageError {
    /// No command, or one this build does not know.
    #[error(
        "expected `key <dir>`, `run --id <n> --dir <dir> --listen <addr> ...` or `client --dir <dir> --peer ... <op>...`"
    )]
    Command,
    /// A client with nothing to do.
    #[error("a client needs at least one operation: get <key>, put <key> <value> or del <key>")]
    Nothing,
    /// An option this build does not know.
    #[error("unknown option {0}")]
    Option(String),
    /// An option without its value.
    #[error("{0} needs a value")]
    Missing(&'static str),
    /// A value that does not parse.
    #[error("{option}: cannot read {value:?}")]
    Value {
        /// The option.
        option: &'static str,
        /// The value given.
        value: String,
    },
}

/// Reads the arguments after the program name.
///
/// # Errors
///
/// When they are not a valid command line.
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Invocation, UsageError> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("key") => {
            let dir = args.next().ok_or(UsageError::Missing("key"))?;
            args.next().map_or_else(
                || Ok(Invocation::Key(dir.into())),
                |extra| Err(UsageError::Option(extra)),
            )
        }
        Some("run") => run(args).map(Invocation::Run),
        Some("client") => client(args).map(Invocation::Client),
        Some(_) | None => Err(UsageError::Command),
    }
}

fn run(mut args: impl Iterator<Item = String>) -> Result<Settings, UsageError> {
    let mut id = None;
    let mut dir = None;
    let mut listen = None;
    let mut peers = Vec::new();
    let mut clients = Vec::new();
    let mut ceiling = Ceiling::DEFAULT;
    let mut metrics = None;
    let mut start = Start::Wait;
    while let Some(arg) = args.next() {
        let mut value = |option: &'static str| args.next().ok_or(UsageError::Missing(option));
        match arg.as_str() {
            "--id" => id = Some(read("--id", &value("--id")?, node_id)?),
            "--dir" => dir = Some(PathBuf::from(value("--dir")?)),
            "--listen" => listen = Some(read("--listen", &value("--listen")?, addr)?),
            "--peer" => peers.push(read("--peer", &value("--peer")?, peer)?),
            "--client" => clients.push(read("--client", &value("--client")?, key)?),
            "--split-above" => {
                ceiling = read("--split-above", &value("--split-above")?, bytes)?;
            }
            "--metrics" => metrics = Some(read("--metrics", &value("--metrics")?, addr)?),
            "--found" => start = Start::Found,
            "--restore" => start = Start::Restore(PathBuf::from(value("--restore")?)),
            _ => return Err(UsageError::Option(arg)),
        }
    }
    Ok(Settings {
        id: id.ok_or(UsageError::Missing("--id"))?,
        dir: dir.ok_or(UsageError::Missing("--dir"))?,
        listen: listen.ok_or(UsageError::Missing("--listen"))?,
        peers,
        clients,
        ceiling,
        metrics,
        start,
    })
}

fn client(mut args: impl Iterator<Item = String>) -> Result<Session, UsageError> {
    let mut dir = None;
    let mut peers = Vec::new();
    let mut ops = Vec::new();
    while let Some(arg) = args.next() {
        let mut value = |option: &'static str| args.next().ok_or(UsageError::Missing(option));
        match arg.as_str() {
            "--dir" => dir = Some(PathBuf::from(value("--dir")?)),
            "--peer" => peers.push(read("--peer", &value("--peer")?, peer)?),
            "get" => ops.push(Op::Get(value("get")?.into_bytes())),
            "put" => {
                let key = value("put")?.into_bytes();
                ops.push(Op::Put(key, value("put")?.into_bytes()));
            }
            "del" => ops.push(Op::Delete(value("del")?.into_bytes())),
            _ => return Err(UsageError::Option(arg)),
        }
    }
    if peers.is_empty() {
        return Err(UsageError::Missing("--peer"));
    }
    if ops.is_empty() {
        return Err(UsageError::Nothing);
    }
    Ok(Session {
        dir: dir.ok_or(UsageError::Missing("--dir"))?,
        peers,
        ops,
    })
}

fn read<T>(
    option: &'static str,
    value: &str,
    parse: impl FnOnce(&str) -> Option<T>,
) -> Result<T, UsageError> {
    parse(value).ok_or_else(|| UsageError::Value {
        option,
        value: value.to_owned(),
    })
}

/// A node id: a positive integer.
pub(crate) fn node_id(text: &str) -> Option<NodeId> {
    text.parse().ok().map(NodeId::new)
}

pub(crate) fn addr(text: &str) -> Option<SocketAddr> {
    text.parse().ok()
}

/// A ceiling: a positive number of bytes.
fn bytes(text: &str) -> Option<Ceiling> {
    text.parse().ok().map(Ceiling::new)
}

/// `<id>=<addr>,<key>`.
fn peer(text: &str) -> Option<(NodeId, Peer)> {
    let (id, rest) = text.split_once('=')?;
    let (addr_text, key_text) = rest.split_once(',')?;
    Some((node_id(id)?, Peer::new(addr(addr_text)?, key(key_text)?)))
}

/// A transport key as `kurator key` prints it: `ed25519:` and 64 hex
/// digits.
pub(crate) fn key(text: &str) -> Option<VerifyingKey> {
    text.parse().ok()
}

#[cfg(test)]
mod tests {
    use kurator_crypto::{KeyPurpose, NodeSecret};
    use kurator_raft::NodeId;

    use kurator_shard::Ceiling;

    use super::{Invocation, Op, Start, UsageError, key, parse};

    fn args(line: &str) -> Vec<String> {
        line.split_whitespace().map(str::to_owned).collect()
    }

    #[test]
    fn keys_read_back_as_they_print() {
        let public = NodeSecret::from_seed([3; 32]).public_key(KeyPurpose::Transport);
        assert_eq!(key(&public.to_string()), Some(public.clone()), "roundtrip");
        let text = public.to_string();
        let short: String = text.chars().take(62).collect();
        assert_eq!(key(&short), None, "short");
        assert_eq!(key(&format!("{text}0")), None, "long");
        assert_eq!(key(&format!("{short}zz")), None, "not hex");
        assert_eq!(
            key(&format!("02{}", "0".repeat(62))),
            None,
            "not a curve point"
        );
    }

    #[test]
    fn a_run_line_parses() {
        let peer_key = NodeSecret::from_seed([2; 32]).public_key(KeyPurpose::Transport);
        let client_key = NodeSecret::from_seed([5; 32]).public_key(KeyPurpose::Transport);
        let line = format!(
            "run --id 1 --dir n1 --listen 127.0.0.1:7001 --peer 2=127.0.0.1:7002,{peer_key} --client {client_key} --split-above 4096 --metrics 127.0.0.1:9100 --found"
        );
        let Ok(Invocation::Run(settings)) = parse(args(&line)) else {
            panic!("parses");
        };
        assert_eq!(settings.id(), NodeId::new(1), "id");
        assert_eq!(settings.dir().to_str(), Some("n1"), "dir");
        assert_eq!(settings.listen().port(), 7001, "listen");
        assert_eq!(settings.start(), &Start::Found, "found");
        let [(id, peer)] = settings.peers() else {
            panic!("one peer");
        };
        assert_eq!(*id, NodeId::new(2), "peer id");
        assert_eq!(peer.addr().port(), 7002, "peer addr");
        assert_eq!(peer.key(), &peer_key, "peer key");
        assert_eq!(settings.clients(), [client_key], "client");
        assert_eq!(settings.ceiling().bytes().get(), 4096, "ceiling");
        assert_eq!(
            settings.metrics().map(|addr| addr.port()),
            Some(9100),
            "metrics"
        );
        let Ok(Invocation::Run(waiting)) = parse(args("run --id 4 --dir d --listen [::1]:1"))
        else {
            panic!("parses");
        };
        assert_eq!(waiting.start(), &Start::Wait, "waits by default");
        let Ok(Invocation::Run(restoring)) =
            parse(args("run --id 4 --dir d --listen [::1]:1 --restore b"))
        else {
            panic!("parses");
        };
        assert_eq!(
            restoring.start(),
            &Start::Restore("b".into()),
            "restores a backup"
        );
        assert!(waiting.peers().is_empty(), "no peers");
        assert!(waiting.clients().is_empty(), "no clients");
        assert_eq!(waiting.ceiling(), Ceiling::DEFAULT, "the default ceiling");
        assert_eq!(waiting.metrics(), None, "no metrics");
    }

    #[test]
    fn a_client_line_parses() {
        let peer_key = NodeSecret::from_seed([2; 32]).public_key(KeyPurpose::Transport);
        let line = format!("client --dir c --peer 2=127.0.0.1:7002,{peer_key} get a put b 1 del c");
        let Ok(Invocation::Client(session)) = parse(args(&line)) else {
            panic!("parses");
        };
        assert_eq!(session.dir().to_str(), Some("c"), "dir");
        assert_eq!(session.peers().len(), 1, "peer");
        let ops = [
            Op::Get(b"a".to_vec()),
            Op::Put(b"b".to_vec(), b"1".to_vec()),
            Op::Delete(b"c".to_vec()),
        ];
        assert_eq!(session.ops(), ops, "ops in order");
        let peer = format!("--peer 2=127.0.0.1:7002,{peer_key}");
        for (bad, error) in [
            (format!("client --dir c {peer}"), UsageError::Nothing),
            (
                "client --dir c get a".to_owned(),
                UsageError::Missing("--peer"),
            ),
            (format!("client {peer} get a"), UsageError::Missing("--dir")),
            (
                format!("client --dir c {peer} put a"),
                UsageError::Missing("put"),
            ),
            (
                format!("client --dir c {peer} get"),
                UsageError::Missing("get"),
            ),
            (
                format!("client --dir c {peer} del"),
                UsageError::Missing("del"),
            ),
            (
                format!("client --dir c {peer} scan"),
                UsageError::Option("scan".to_owned()),
            ),
        ] {
            assert_eq!(parse(args(&bad)), Err(error), "{bad:?}");
        }
    }

    #[test]
    fn bad_lines_say_what_is_wrong() {
        for (line, error) in [
            ("", UsageError::Command),
            ("serve", UsageError::Command),
            ("key", UsageError::Missing("key")),
            ("key a b", UsageError::Option("b".to_owned())),
            (
                "run --dir d --listen 127.0.0.1:1",
                UsageError::Missing("--id"),
            ),
            (
                "run --id 1 --listen 127.0.0.1:1",
                UsageError::Missing("--dir"),
            ),
            ("run --id 1 --dir d", UsageError::Missing("--listen")),
            ("run --restore", UsageError::Missing("--restore")),
            ("run --id", UsageError::Missing("--id")),
            (
                "run --id 1 --verbose",
                UsageError::Option("--verbose".to_owned()),
            ),
            (
                "run --id one",
                UsageError::Value {
                    option: "--id",
                    value: "one".to_owned(),
                },
            ),
            (
                "run --peer 2=127.0.0.1:1",
                UsageError::Value {
                    option: "--peer",
                    value: "2=127.0.0.1:1".to_owned(),
                },
            ),
            (
                "run --split-above 0",
                UsageError::Value {
                    option: "--split-above",
                    value: "0".to_owned(),
                },
            ),
            (
                "run --client ed25519:00",
                UsageError::Value {
                    option: "--client",
                    value: "ed25519:00".to_owned(),
                },
            ),
        ] {
            assert_eq!(parse(args(line)), Err(error), "{line:?}");
        }
        assert_eq!(
            parse(args("key n1")),
            Ok(Invocation::Key("n1".into())),
            "key"
        );
    }
}
