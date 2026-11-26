//! A Kurator replica as a process: [`run`] opens the replica in a
//! directory, serves it over QUIC, and takes commands from a console.
//!
//! ```text
//! <dir>/node.secret   the node secret every key is derived from
//! <dir>/log/          the signed Raft log
//! <dir>/snapshot/     the latest snapshot
//! ```

pub mod args;
pub mod console;
pub mod secret;

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use kurator_net::{Endpoint, NetError, Replica};
use kurator_raft::{KeyPurpose, Member, Network, NodeSecret};
use kurator_wal::VerifyingKey;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

use crate::args::{Settings, Start};
use crate::console::{Reply, execute};

/// Why a replica stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NodeError {
    /// The console or the directory failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The endpoint could not start.
    #[error(transparent)]
    Net(#[from] NetError),
    /// The replica failed.
    #[error(transparent)]
    Raft(#[from] kurator_raft::Error),
    /// The Raft configuration is invalid.
    #[error(transparent)]
    Config(#[from] kurator_raft::openraft::ConfigError),
}

/// The transport key of the node whose secret is in `dir`, creating the
/// secret when there is none.
///
/// # Errors
///
/// When the secret cannot be read or written.
pub fn key(dir: &std::path::Path) -> io::Result<VerifyingKey> {
    let (secret, _) = secret::load_or_create(dir)?;
    Ok(secret.public_key(KeyPurpose::Transport))
}

/// Runs the replica `settings` describe until the console says `quit` or
/// closes.
///
/// # Errors
///
/// When the replica cannot start, or the console fails.
pub async fn run(settings: Settings) -> Result<(), NodeError> {
    let (secret, _) = secret::load_or_create(settings.dir())?;
    let replica = open(&settings, secret).await?;
    let mut out = tokio::io::stdout();
    out.write_all(b"ready; type a command, or anything else for help\n")
        .await?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let (replica, _server) = replica;
    while let Some(line) = lines.next_line().await? {
        match execute(&replica, &line).await {
            Reply::Show(text) => {
                out.write_all(text.as_bytes()).await?;
                out.write_all(b"\n").await?;
            }
            Reply::Quit => break,
        }
    }
    replica.shutdown().await?;
    Ok(())
}

async fn open(
    settings: &Settings,
    secret: NodeSecret,
) -> Result<(Replica, kurator_net::Server), NodeError> {
    let member = Member::new(settings.id(), secret);
    let bootstrap: BTreeMap<_, _> = settings
        .peers()
        .iter()
        .map(|(id, peer)| (*id, peer.key().clone()))
        .collect();
    let endpoint = Endpoint::bind(settings.listen(), member.secret(), bootstrap)?;
    let me = member.peer(endpoint.local_addr()?);
    let network = Network::new(endpoint.transport());
    let config = Arc::new(kurator_raft::openraft::Config::default().validate()?);
    let replica = Replica::open(member, settings.dir(), config, network).await?;
    let server = endpoint.serve(replica.inbox())?;
    match settings.start() {
        Start::Found => {
            let voters = settings
                .peers()
                .iter()
                .cloned()
                .chain([(settings.id(), me)]);
            match replica.initialize(voters).await {
                Ok(()) | Err(kurator_raft::Error::Initialized) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Start::Wait => {}
    }
    Ok((replica, server))
}
