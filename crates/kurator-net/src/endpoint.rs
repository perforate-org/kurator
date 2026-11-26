//! The server side, and the endpoint both sides share.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kurator_raft::{Inbox, NodeId, NodeSecret, Refusal, Request, Response};
use kurator_wal::VerifyingKey;
use quinn::crypto::rustls::QuicServerConfig;

use crate::Tokio;
use crate::connection::{MAX_MESSAGE, Quic};
use crate::roster::Roster;
use crate::tls::{Identity, presented_key};

/// Why an endpoint could not start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NetError {
    /// The socket could not be bound.
    #[error("cannot bind the endpoint: {0}")]
    Io(#[from] io::Error),
    /// The TLS configuration is invalid.
    #[error("TLS configuration: {0}")]
    Tls(#[from] rustls::Error),
    /// The TLS configuration does not suit QUIC.
    #[error("QUIC configuration: {0}")]
    Quic(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
}

/// How often an idle connection is kept alive.
const KEEP_ALIVE: Duration = Duration::from_secs(2);

/// How long a connection may stay silent before it counts as broken.
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// A replica's QUIC endpoint: it connects to peers from the moment it is
/// bound, and listens once it serves a replica, authenticated by the
/// replica's transport key.
#[derive(Debug)]
pub struct Endpoint {
    socket: quinn::Endpoint,
    identity: Identity,
    bootstrap: BTreeMap<NodeId, VerifyingKey>,
    transport: Arc<quinn::TransportConfig>,
}

impl Endpoint {
    /// Binds to `addr` as the replica holding `secret`. Until the replica
    /// it serves knows a membership, it accepts the peers in `bootstrap`.
    ///
    /// # Errors
    ///
    /// When the socket cannot be bound or the key cannot be loaded.
    pub fn bind(
        addr: SocketAddr,
        secret: &NodeSecret,
        bootstrap: BTreeMap<NodeId, VerifyingKey>,
    ) -> Result<Self, NetError> {
        let identity = Identity::new(secret)?;
        let mut transport = quinn::TransportConfig::default();
        let _tuned = transport
            .keep_alive_interval(Some(KEEP_ALIVE))
            .max_idle_timeout(Some(
                quinn::IdleTimeout::try_from(IDLE_TIMEOUT).map_err(io::Error::other)?,
            ));
        Ok(Self {
            socket: quinn::Endpoint::client(addr)?,
            identity,
            bootstrap,
            transport: Arc::new(transport),
        })
    }

    /// Where it listens.
    ///
    /// # Errors
    ///
    /// When the socket is gone.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// The transport a replica sends through.
    #[must_use]
    pub fn transport(&self) -> Quic {
        Quic::new(
            self.socket.clone(),
            self.identity.clone(),
            Arc::clone(&self.transport),
        )
    }

    /// Starts accepting peers and delivering their requests to `inbox`.
    /// Serving stops, and the endpoint closes, when the returned
    /// [`Server`] drops.
    ///
    /// # Errors
    ///
    /// When the TLS configuration cannot be built.
    pub fn serve(self, inbox: Inbox<Tokio>) -> Result<Server, NetError> {
        let roster = Arc::new(Roster::new(self.bootstrap, inbox.clone()));
        let tls = self.identity.server(Arc::clone(&roster))?;
        let mut server =
            quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
        let _configured = server.transport_config(self.transport);
        self.socket.set_server_config(Some(server));
        let task = tokio::spawn(accept(self.socket.clone(), roster, inbox));
        Ok(Server {
            socket: self.socket,
            task,
        })
    }
}

/// A running server. Dropping it closes the endpoint, so the replica's
/// own outgoing connections close too.
#[derive(Debug)]
pub struct Server {
    socket: quinn::Endpoint,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
        self.socket.close(quinn::VarInt::from_u32(0), b"shutdown");
    }
}

/// Accepts connections until the endpoint closes.
async fn accept(endpoint: quinn::Endpoint, roster: Arc<Roster>, inbox: Inbox<Tokio>) {
    while let Some(incoming) = endpoint.accept().await {
        drop(tokio::spawn(connection(
            incoming,
            Arc::clone(&roster),
            inbox.clone(),
        )));
    }
}

/// Serves one connection: one request per stream, each in its own task.
async fn connection(incoming: quinn::Incoming, roster: Arc<Roster>, inbox: Inbox<Tokio>) {
    let Ok(connection) = incoming.await else {
        return;
    };
    let Some(key) = peer_key(&connection) else {
        connection.close(quinn::VarInt::from_u32(1), b"no key");
        return;
    };
    while let Ok((send, recv)) = connection.accept_bi().await {
        // A node that left the membership is cut off on its next request.
        let Some(sender) = roster.identify(&key) else {
            connection.close(quinn::VarInt::from_u32(2), b"not a member");
            return;
        };
        drop(tokio::spawn(stream(sender, send, recv, inbox.clone())));
    }
}

/// The transport key the peer of `connection` authenticated with.
fn peer_key(connection: &quinn::Connection) -> Option<VerifyingKey> {
    let identity = connection.peer_identity()?;
    let certificates = identity
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .ok()?;
    presented_key(certificates.first()?)
}

/// Reads one request, handles it and writes the answer.
async fn stream(
    sender: NodeId,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    inbox: Inbox<Tokio>,
) {
    let Ok(bytes) = recv.read_to_end(MAX_MESSAGE).await else {
        return;
    };
    let response = match Request::decode(&bytes) {
        Ok(request) if request.sender() == sender => inbox.handle(request).await,
        Ok(_) => Response::Refused(Refusal::Forged),
        Err(_) => Response::Refused(Refusal::Malformed),
    };
    if send.write_all(&response.encode()).await.is_ok() {
        drop(send.finish());
    }
}
