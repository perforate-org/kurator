//! A Kurator node as a process: [`run`] opens the node's store in a
//! directory, serves it over QUIC, and takes commands from a console.
//!
//! ```text
//! <dir>/node.secret              the node secret every key is derived from
//! <dir>/shards/<id>/log/         the signed Raft log of each shard's replica
//! <dir>/shards/<id>/snapshot/    its latest snapshot
//! <dir>/seeds/<id>/              a shard a split or a restore made, until it is planted
//! ```

pub mod args;
pub mod console;
pub mod report;
pub mod secret;
pub mod session;
pub mod split;
pub mod sweep;

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use kurator_crypto::{KeyPurpose, NodeSecret, VerifyingKey};
use kurator_net::{Endpoint, NetError, Quic, Server, Store};
use kurator_raft::{Backup, Member, NodeId, Peer};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

use crate::args::{Settings, Start};
use crate::console::{Reply, execute};

/// Why a node stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NodeError {
    /// The console or the directory failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The endpoint could not start.
    #[error(transparent)]
    Net(#[from] NetError),
    /// A replica failed.
    #[error(transparent)]
    Raft(#[from] kurator_raft::Error),
    /// The Raft configuration is invalid.
    #[error(transparent)]
    Config(#[from] kurator_raft::openraft::ConfigError),
    /// No node told the client where the shards are.
    #[error("no node answered")]
    Unreached,
    /// A client's operation failed.
    #[error(transparent)]
    Client(#[from] kurator_client::Error),
}

/// The transport key of the node whose secret is in `dir`, creating the
/// secret when there is none.
///
/// # Errors
///
/// When the secret cannot be read or written.
pub fn key(dir: &Path) -> io::Result<VerifyingKey> {
    let (secret, _) = secret::load_or_create(dir)?;
    Ok(secret.public_key(KeyPurpose::Transport))
}

/// Runs the node `settings` describe until the console says `quit` or
/// closes.
///
/// # Errors
///
/// When the node cannot start, or the console fails.
pub async fn run(settings: Settings) -> Result<(), NodeError> {
    let (secret, _) = secret::load_or_create(settings.dir())?;
    let (store, quic, _server) = open(&settings, secret).await?;
    let mut out = tokio::io::stdout();
    out.write_all(b"ready; type a command, or anything else for help\n")
        .await?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut beats = tokio::time::interval(report::EVERY);
    let mut sweeps = tokio::time::interval(sweep::EVERY);
    let mut checks = tokio::time::interval(split::EVERY);
    loop {
        let line = tokio::select! {
            line = lines.next_line() => line?,
            _ = beats.tick() => {
                // A beat that failed is made again on the next one.
                drop(report::heartbeat(&store, &quic).await);
                continue;
            }
            _ = sweeps.tick() => {
                // A sweep that did not finish is made again on the next one.
                let _swept: Option<kurator_client::Collected> = sweep::sweep(&store, &quic).await;
                continue;
            }
            _ = checks.tick() => {
                // A shard left too large is split on the next check.
                drop(split::grow(&store, &quic, settings.ceiling()).await);
                continue;
            }
        };
        let Some(line) = line else {
            break;
        };
        match execute(&store, &quic, &line).await {
            Reply::Show(text) => {
                out.write_all(text.as_bytes()).await?;
                out.write_all(b"\n").await?;
            }
            Reply::Quit => break,
        }
    }
    store.shutdown().await?;
    Ok(())
}

async fn open(settings: &Settings, secret: NodeSecret) -> Result<(Store, Quic, Server), NodeError> {
    let member = Member::new(settings.id(), secret);
    let bootstrap: BTreeMap<_, _> = settings
        .peers()
        .iter()
        .map(|(id, peer)| (*id, peer.key().clone()))
        .collect();
    let endpoint = Endpoint::bind(settings.listen(), member.secret(), bootstrap)?
        .with_clients(settings.clients().to_vec());
    let me = member.peer(endpoint.local_addr()?);
    let config = Arc::new(kurator_raft::openraft::Config::default().validate()?);
    if let Start::Restore(from) = settings.start() {
        restore(from, settings.dir(), settings.id(), me.clone())?;
    }
    let store = Store::open(member, settings.dir(), config, endpoint.transport()).await?;
    let quic = endpoint.transport();
    let server = endpoint.serve(store.inbox())?;
    match settings.start() {
        Start::Found => {
            let voters = settings
                .peers()
                .iter()
                .cloned()
                .chain([(settings.id(), me)]);
            // A node that founded the cluster before holds a shard already.
            match store.found(voters).await {
                Ok(()) | Err(kurator_raft::Error::Initialized) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Start::Wait | Start::Restore(_) => {}
    }
    Ok((store, quic, server))
}

/// Plants the backup in `from` in the node directory `dir`, with `node`,
/// reached as `peer`, as the only voter of every shard. A node that
/// restored before holds its shards already, and goes on from them.
fn restore(from: &Path, dir: &Path, node: NodeId, peer: Peer) -> Result<(), NodeError> {
    match Backup::read(from)?.plant(dir, node, peer) {
        Ok(()) | Err(kurator_raft::Error::Initialized) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
