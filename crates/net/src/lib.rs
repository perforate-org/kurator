//! QUIC transport for Kurator nodes.
//!
//! Each node runs one [`Endpoint`] that both listens and connects, for
//! every replica of its [`Store`].
//!
//! - **Persistent connections, binary RPC.** A replica keeps one QUIC
//!   connection per peer and sends each request on a stream of its own,
//!   in the binary encoding of [`kurator_raft::Message`], which names the
//!   shard it is for. Entries travel in
//!   the batches `OpenRaft` forms; a snapshot travels whole on its own
//!   stream, beside the heartbeats.
//! - **Meta calls on the same transport.** The first byte of a stream
//!   names its channel: Raft messages, or calls on the meta shard
//!   ([`kurator_meta::Call`]), which a node answers from its own meta
//!   replica. [`Quic`] implements [`kurator_meta::Ask`], so any node can
//!   reach the meta shard's leader.
//! - **Mutual authentication by Ed25519 key.** TLS 1.3 with raw public
//!   keys (RFC 7250), no certificates: each side proves it holds the
//!   transport key derived from its [`NodeSecret`](kurator_raft::NodeSecret)
//!   ([`KeyPurpose::Transport`](kurator_raft::KeyPurpose::Transport),
//!   separate from the key that signs its WAL). The client expects the
//!   key the membership lists for the peer; the server accepts the keys
//!   of its replicas' memberships, and before it has one, the peers it was started
//!   with.
//! - **No impersonation.** A request names its sender in its vote; the
//!   server refuses a request whose sender is not the node that
//!   authenticated the connection, and drops a connection whose node left
//!   every membership.
//!
//! The transport plugs into a store through the same
//! [`kurator_raft::Transport`] boundary as the deterministic simulator.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::collections::BTreeMap;
//! use std::sync::Arc;
//! use kurator_net::{Endpoint, Store};
//! use kurator_raft::{Member, NodeId, NodeSecret};
//!
//! let member = Member::new(NodeId::new(1), NodeSecret::from_seed([1; 32]));
//! let endpoint = Endpoint::bind("127.0.0.1:7001".parse()?, member.secret(), BTreeMap::new())?;
//! let config = Arc::new(kurator_raft::openraft::Config::default().validate()?);
//! let store = Store::open(member, "n1".as_ref(), config, endpoint.transport()).await?;
//! let _server = endpoint.serve(store.inbox())?;
//! # Ok(())
//! # }
//! ```

mod connection;
mod der;
mod endpoint;
mod roster;
mod tls;

pub use connection::{Quic, QuicLink};
pub use endpoint::{Endpoint, NetError, Server};

kurator_raft::declare_config!(
    /// Replicas on Tokio, as the QUIC transport runs them.
    pub Tokio, openraft_rt_tokio::TokioRuntime
);

/// A replica on Tokio.
pub type Replica = kurator_raft::Replica<Tokio>;

/// A node's replicas, reaching their peers over QUIC.
pub type Store = kurator_raft::Store<Tokio, Quic>;

/// Where an [`Endpoint`] delivers what peers send to a [`Store`].
pub type Inbox = kurator_raft::Inbox<Tokio, Quic>;
