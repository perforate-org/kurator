//! The command line.
//!
//! ```text
//! kurator key <dir>
//! kurator run --id <n> --dir <dir> --listen <addr> [--peer <n>=<addr>,<key>]... [--client <key>]... [--found]
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;

use kurator_crypto::VerifyingKey;
use kurator_raft::{NodeId, Peer};

/// What the process was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Invocation {
    /// Create the node secret in a directory, if there is none, and print
    /// the transport key.
    Key(PathBuf),
    /// Run a replica.
    Run(Settings),
}

/// How a replica runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    id: NodeId,
    dir: PathBuf,
    listen: SocketAddr,
    peers: Vec<(NodeId, Peer)>,
    clients: Vec<VerifyingKey>,
    start: Start,
}

/// How a replica comes into its cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Start {
    /// Wait: the cluster exists, or another node founds it, and a leader
    /// adds this one. Also how a replica restarts.
    Wait,
    /// Found the cluster, with this node and its peers as voters.
    Found,
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

    /// How it comes into its cluster.
    #[must_use]
    pub const fn start(&self) -> Start {
        self.start
    }
}

/// What is wrong with the command line.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum UsageError {
    /// No command, or one this build does not know.
    #[error("expected `key <dir>` or `run --id <n> --dir <dir> --listen <addr> ...`")]
    Command,
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
        Some(_) | None => Err(UsageError::Command),
    }
}

fn run(mut args: impl Iterator<Item = String>) -> Result<Settings, UsageError> {
    let mut id = None;
    let mut dir = None;
    let mut listen = None;
    let mut peers = Vec::new();
    let mut clients = Vec::new();
    let mut start = Start::Wait;
    while let Some(arg) = args.next() {
        let mut value = |option: &'static str| args.next().ok_or(UsageError::Missing(option));
        match arg.as_str() {
            "--id" => id = Some(read("--id", &value("--id")?, node_id)?),
            "--dir" => dir = Some(PathBuf::from(value("--dir")?)),
            "--listen" => listen = Some(read("--listen", &value("--listen")?, addr)?),
            "--peer" => peers.push(read("--peer", &value("--peer")?, peer)?),
            "--client" => clients.push(read("--client", &value("--client")?, key)?),
            "--found" => start = Start::Found,
            _ => return Err(UsageError::Option(arg)),
        }
    }
    Ok(Settings {
        id: id.ok_or(UsageError::Missing("--id"))?,
        dir: dir.ok_or(UsageError::Missing("--dir"))?,
        listen: listen.ok_or(UsageError::Missing("--listen"))?,
        peers,
        clients,
        start,
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

    use super::{Invocation, Start, UsageError, key, parse};

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
            "run --id 1 --dir n1 --listen 127.0.0.1:7001 --peer 2=127.0.0.1:7002,{peer_key} --client {client_key} --found"
        );
        let Ok(Invocation::Run(settings)) = parse(args(&line)) else {
            panic!("parses");
        };
        assert_eq!(settings.id(), NodeId::new(1), "id");
        assert_eq!(settings.dir().to_str(), Some("n1"), "dir");
        assert_eq!(settings.listen().port(), 7001, "listen");
        assert_eq!(settings.start(), Start::Found, "found");
        let [(id, peer)] = settings.peers() else {
            panic!("one peer");
        };
        assert_eq!(*id, NodeId::new(2), "peer id");
        assert_eq!(peer.addr().port(), 7002, "peer addr");
        assert_eq!(peer.key(), &peer_key, "peer key");
        assert_eq!(settings.clients(), [client_key], "client");
        let Ok(Invocation::Run(waiting)) = parse(args("run --id 4 --dir d --listen [::1]:1"))
        else {
            panic!("parses");
        };
        assert_eq!(waiting.start(), Start::Wait, "waits by default");
        assert!(waiting.peers().is_empty(), "no peers");
        assert!(waiting.clients().is_empty(), "no clients");
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
