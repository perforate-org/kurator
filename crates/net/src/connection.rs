//! The client side: one persistent QUIC connection per peer, one
//! bidirectional stream per request.
//!
//! A request is written whole and the stream finished; the answer is read
//! to the end of the stream. Its first byte says what it is: a Raft
//! message for a replica, or a call on the meta shard. QUIC multiplexes the streams, so a large
//! snapshot does not hold up heartbeats, and one connection carries every
//! request to a peer for as long as it stays up. A broken connection is
//! dropped and the next request opens a new one.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kurator_client::{Reach, Reply, Request};
use kurator_meta::{Answer, Ask, Call};
use kurator_raft::{Connection, Failure, Message, NodeId, Peer, Response, Transport};
use quinn::crypto::rustls::QuicClientConfig;

use crate::Tokio;
use crate::tls::Identity;

/// The largest message either side reads, a whole snapshot included.
pub(crate) const MAX_MESSAGE: usize = 1 << 30;

/// What a stream carries, as its first byte says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    /// A [`Message`] for a replica, answered by a [`Response`].
    Raft,
    /// A [`Call`] on the meta shard, answered by an [`Answer`].
    Meta,
    /// A client's [`Request`], answered by a [`Reply`].
    Client,
}

impl Channel {
    const RAFT: u8 = 1;
    const META: u8 = 2;
    const CLIENT: u8 = 3;

    const fn tag(self) -> u8 {
        match self {
            Self::Raft => Self::RAFT,
            Self::Meta => Self::META,
            Self::Client => Self::CLIENT,
        }
    }

    /// The channel a stream's `bytes` are on, and the rest of them.
    pub(crate) fn split(bytes: &[u8]) -> Option<(Self, &[u8])> {
        let (tag, rest) = bytes.split_first()?;
        let channel = match *tag {
            Self::RAFT => Self::Raft,
            Self::META => Self::Meta,
            Self::CLIENT => Self::Client,
            _ => return None,
        };
        Some((channel, rest))
    }
}

/// How long a call on the meta shard may take, the leader's commit
/// included.
const ASK_TIMEOUT: Duration = Duration::from_secs(10);

/// The server name sent in the handshake. Peers are known by key, not
/// name, so it is the same for all.
const SERVER_NAME: &str = "kurator";

/// Opens [`QuicLink`]s from one endpoint.
#[derive(Debug, Clone)]
pub struct Quic {
    endpoint: quinn::Endpoint,
    identity: Identity,
    transport: Arc<quinn::TransportConfig>,
}

impl Quic {
    pub(crate) const fn new(
        endpoint: quinn::Endpoint,
        identity: Identity,
        transport: Arc<quinn::TransportConfig>,
    ) -> Self {
        Self {
            endpoint,
            identity,
            transport,
        }
    }

    fn client_config(&self, peer: &Peer) -> io::Result<quinn::ClientConfig> {
        let tls = self.identity.client(peer.key()).map_err(io::Error::other)?;
        let quic = QuicClientConfig::try_from(tls).map_err(io::Error::other)?;
        let mut config = quinn::ClientConfig::new(Arc::new(quic));
        let _configured = config.transport_config(Arc::clone(&self.transport));
        Ok(config)
    }
}

impl Transport<Tokio> for Quic {
    type Connection = QuicLink;

    fn connect(&self, target: NodeId, peer: &Peer) -> QuicLink {
        QuicLink {
            target,
            endpoint: self.endpoint.clone(),
            addr: peer.addr(),
            config: self.client_config(peer).map_err(Arc::new),
            connection: None,
        }
    }
}

/// The persistent connection to one peer.
#[derive(Debug)]
pub struct QuicLink {
    target: NodeId,
    endpoint: quinn::Endpoint,
    addr: SocketAddr,
    /// The TLS configuration that expects the peer's key, or why there is
    /// none.
    config: Result<quinn::ClientConfig, Arc<io::Error>>,
    connection: Option<quinn::Connection>,
}

impl QuicLink {
    /// The open connection, or a new one.
    async fn connection(&mut self) -> io::Result<quinn::Connection> {
        if let Some(open) = &self.connection
            && open.close_reason().is_none()
        {
            return Ok(open.clone());
        }
        let config = self
            .config
            .as_ref()
            .map_err(|error| io::Error::other(error.to_string()))?
            .clone();
        let connecting = self
            .endpoint
            .connect_with(config, self.addr, SERVER_NAME)
            .map_err(io::Error::other)?;
        let connection = connecting.await.map_err(io::Error::other)?;
        self.connection = Some(connection.clone());
        Ok(connection)
    }

    /// Sends `request` on `channel` on a new stream and reads the answer.
    async fn exchange(&mut self, channel: Channel, request: &[u8]) -> io::Result<Vec<u8>> {
        let connection = self.connection().await?;
        let (mut send, mut recv) = connection.open_bi().await.map_err(io::Error::other)?;
        send.write_all(&[channel.tag()])
            .await
            .map_err(io::Error::other)?;
        send.write_all(request).await.map_err(io::Error::other)?;
        send.finish().map_err(io::Error::other)?;
        recv.read_to_end(MAX_MESSAGE)
            .await
            .map_err(io::Error::other)
    }
}

impl Connection<Tokio> for QuicLink {
    async fn call(
        &mut self,
        message: Message<Tokio>,
        deadline: Duration,
    ) -> Result<Response<Tokio>, Failure> {
        let bytes = message.encode().map_err(Failure::Unreachable)?;
        let reply = match tokio::time::timeout(deadline, self.exchange(Channel::Raft, &bytes)).await
        {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => {
                self.connection = None;
                return Err(Failure::Unreachable(io::Error::other(format!(
                    "{} at {}: {error}",
                    self.target, self.addr
                ))));
            }
            Err(_) => return Err(Failure::Timeout),
        };
        Response::decode(&reply).map_err(|error| Failure::Unreachable(io::Error::other(error)))
    }
}

impl Ask for Quic {
    /// Opens a connection of its own for the call: calls on the meta shard
    /// are rare next to Raft's traffic.
    async fn ask(&self, node: NodeId, peer: &Peer, call: &Call) -> io::Result<Answer> {
        let request = call.encode()?;
        let mut link = self.connect(node, peer);
        let reply = tokio::time::timeout(ASK_TIMEOUT, link.exchange(Channel::Meta, &request))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
        Answer::decode(&reply).map_err(io::Error::other)
    }
}

impl Reach for Quic {
    /// Opens a connection of its own for the request, as [`Quic::ask`]
    /// does.
    async fn reach(&self, node: NodeId, peer: &Peer, request: &Request) -> io::Result<Reply> {
        let request = request.encode()?;
        let mut link = self.connect(node, peer);
        let reply = tokio::time::timeout(ASK_TIMEOUT, link.exchange(Channel::Client, &request))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
        Reply::decode(&reply).map_err(io::Error::other)
    }
}
