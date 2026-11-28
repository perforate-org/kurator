//! The server side, and the endpoint both sides share.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use oleaph_client::{Reply, Request};
use oleaph_crypto::{NodeSecret, VerifyingKey};
use oleaph_meta::{Answer, Call, Meta};
use oleaph_raft::{Message, NodeId, Refusal, Response};
use oleaph_shard::ShardId;
use quinn::crypto::rustls::QuicServerConfig;

#[cfg(feature = "bft")]
use crate::bft::{BftReplica, Committees};
use crate::connection::{Channel, MAX_MESSAGE, Quic};
use crate::roster::{Caller, Members, Roster};
use crate::tls::{Identity, presented_key};
use crate::{Inbox, Tokio};

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
    clients: Vec<VerifyingKey>,
    transport: Arc<quinn::TransportConfig>,
    #[cfg(feature = "bft")]
    committees: Committees,
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
            clients: Vec::new(),
            transport: Arc::new(transport),
            #[cfg(feature = "bft")]
            committees: Committees::default(),
        })
    }

    /// Serves the clients that hold `clients` too, as clients: they may
    /// read and write, but not take part in replication.
    #[must_use]
    pub fn with_clients(self, clients: Vec<VerifyingKey>) -> Self {
        Self { clients, ..self }
    }

    /// Serves the BFT `replicas` too, by shard, and trusts the members of
    /// their committees with their messages.
    #[cfg(feature = "bft")]
    #[must_use]
    pub fn with_bft(self, replicas: BTreeMap<ShardId, Arc<BftReplica>>) -> Self {
        Self {
            committees: Committees::new(replicas),
            ..self
        }
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
    pub fn serve(self, inbox: Inbox) -> Result<Server, NetError> {
        let quic = self.transport();
        // A build without BFT holds no BFT shard.
        #[cfg(feature = "bft")]
        let committees: Option<Box<dyn Members>> = Some(Box::new(self.committees.clone()));
        #[cfg(not(feature = "bft"))]
        let committees: Option<Box<dyn Members>> = None;
        let roster = Arc::new(Roster::new(
            self.bootstrap,
            inbox.memberships(),
            committees,
            self.clients,
        ));
        let tls = self.identity.server(Arc::clone(&roster))?;
        let mut server =
            quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
        let _configured = server.transport_config(self.transport);
        self.socket.set_server_config(Some(server));
        let serving = Serving {
            inbox,
            quic,
            #[cfg(feature = "bft")]
            committees: self.committees,
        };
        let task = tokio::spawn(accept(self.socket.clone(), roster, serving));
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

/// What serving a request needs: the replicas, and the transport to ask
/// other nodes through.
#[derive(Debug, Clone)]
struct Serving {
    inbox: Inbox,
    quic: Quic,
    #[cfg(feature = "bft")]
    committees: Committees,
}

/// Accepts connections until the endpoint closes.
async fn accept(endpoint: quinn::Endpoint, roster: Arc<Roster>, serving: Serving) {
    while let Some(incoming) = endpoint.accept().await {
        drop(tokio::spawn(connection(
            incoming,
            Arc::clone(&roster),
            serving.clone(),
        )));
    }
}

/// Serves one connection: one request per stream, each in its own task.
async fn connection(incoming: quinn::Incoming, roster: Arc<Roster>, serving: Serving) {
    let Ok(connection) = incoming.await else {
        return;
    };
    let Some(key) = peer_key(&connection) else {
        connection.close(quinn::VarInt::from_u32(1), b"no key");
        return;
    };
    while let Ok((send, recv)) = connection.accept_bi().await {
        // A node that left the membership is cut off on its next request.
        let Some(caller) = roster.identify(&key) else {
            connection.close(quinn::VarInt::from_u32(2), b"not a member");
            return;
        };
        drop(tokio::spawn(stream(caller, send, recv, serving.clone())));
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

/// Reads one request, handles it and writes the answer. A client may use
/// the client channel only, and a member of a BFT committee the BFT
/// channel as well.
async fn stream(
    caller: Caller,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    serving: Serving,
) {
    let Ok(bytes) = recv.read_to_end(MAX_MESSAGE).await else {
        return;
    };
    let answer = match (Channel::split(&bytes), caller) {
        (Some((Channel::Raft, message)), Caller::Node(sender)) => {
            raft(sender, message, &serving.inbox).await
        }
        (Some((Channel::Meta, call)), Caller::Node(_)) => meta(call, &serving.inbox).await,
        (Some((Channel::Client, request)), _) => client(request, &serving).await,
        #[cfg(feature = "bft")]
        (Some((Channel::Bft, frame)), Caller::Node(sender) | Caller::Member(sender)) => {
            serving.committees.deliver(sender, frame).await;
            Vec::new()
        }
        // A build without BFT holds no BFT replica to hand a message to.
        #[cfg(not(feature = "bft"))]
        (Some((Channel::Bft, _)), Caller::Node(_) | Caller::Member(_)) => Vec::new(),
        (Some((Channel::Raft | Channel::Meta, _)), Caller::Client | Caller::Member(_))
        | (Some((Channel::Bft, _)), Caller::Client) => {
            Response::<Tokio>::Refused(Refusal::Forged).encode()
        }
        (None, _) => Response::<Tokio>::Refused(Refusal::Malformed).encode(),
    };
    if send.write_all(&answer).await.is_ok() {
        drop(send.finish());
    }
}

/// Delivers the Raft message `bytes` from `sender` to its replica, and
/// encodes the response.
async fn raft(sender: NodeId, bytes: &[u8], inbox: &Inbox) -> Vec<u8> {
    let response = match Message::decode(bytes) {
        Ok(message) if message.sender() == sender => inbox.handle(message).await,
        Ok(_) => Response::Refused(Refusal::Forged),
        Err(_) => Response::Refused(Refusal::Malformed),
    };
    response.encode()
}

/// Runs the call on the meta shard `bytes` on this node's replica of it,
/// and encodes the answer. An answer too long to encode is none.
async fn meta(bytes: &[u8], inbox: &Inbox) -> Vec<u8> {
    let answer = match (Call::decode(bytes), inbox.replica(ShardId::META)) {
        (Ok(call), Some(replica)) => oleaph_meta::answer(&replica, &call).await,
        (Ok(_), None) => Answer::Failed("no replica of the meta shard here".to_owned()),
        (Err(error), _) => Answer::Failed(format!("the call does not decode: {error}")),
    };
    answer.encode().unwrap_or_default()
}

/// Answers the client request `bytes` from this node's replicas, and
/// encodes the reply. A reply too long to encode is none.
async fn client(bytes: &[u8], serving: &Serving) -> Vec<u8> {
    let reply = match Request::decode(bytes) {
        Ok(Request::Map) => map(serving).await,
        Ok(Request::Shard(shard, call)) => match serving.inbox.replica(shard) {
            Some(replica) => oleaph_client::answer(&replica, call).await,
            #[cfg(feature = "bft")]
            None => serving
                .committees
                .answer(shard, call)
                .await
                .unwrap_or(Reply::NoReplica),
            #[cfg(not(feature = "bft"))]
            None => Reply::NoReplica,
        },
        Ok(_) => Reply::Failed("a request this node does not know".to_owned()),
        Err(error) => Reply::Failed(format!("the request does not decode: {error}")),
    };
    reply.encode().unwrap_or_default()
}

/// The registry, from the meta shard's leader wherever it is, and the
/// nodes this node knows of.
async fn map(serving: &Serving) -> Reply {
    let Some(replica) = serving.inbox.replica(ShardId::META) else {
        return Reply::NoReplica;
    };
    match Meta::new(&replica, &serving.quic).registry().await {
        Ok(registry) => Reply::Map {
            registry,
            peers: serving.inbox.memberships().nodes(),
        },
        Err(error) => Reply::Failed(error.to_string()),
    }
}
