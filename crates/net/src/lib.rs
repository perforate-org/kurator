//! QUIC transport for OleaphDB nodes.
//!
//! Each node runs one [`Endpoint`] that both listens and connects, for
//! every replica of its [`Store`].
//!
//! - **Persistent connections, binary RPC.** A replica keeps one QUIC
//!   connection per peer and sends each request on a stream of its own,
//!   in the binary encoding of [`oleaph_raft::Message`], which names the
//!   shard it is for. Entries travel in
//!   the batches `OpenRaft` forms; a snapshot travels whole on its own
//!   stream, beside the heartbeats.
//! - **Meta calls on the same transport.** The first byte of a stream
//!   names its channel: Raft messages, or calls on the meta shard
//!   ([`oleaph_meta::Call`]), which a node answers from its own meta
//!   replica. [`Quic`] implements [`oleaph_meta::Ask`], so any node can
//!   reach the meta shard's leader. A third channel carries client
//!   requests ([`oleaph_client::Request`]): the shard map, and versioned
//!   reads and writes on a replica this node holds. [`Quic`] implements
//!   [`oleaph_client::Reach`], and [`Wall`] implements
//!   [`oleaph_client::Time`], so a [`oleaph_client::Client`] runs over
//!   them.
//! - **Mutual authentication by transport key.** TLS 1.3 with raw public
//!   keys (RFC 7250), no certificates, signed with the key's own algorithm
//!   (Ed25519 today), and a post-quantum key exchange only
//!   (X25519MLKEM768, so recorded traffic stays secret): each side proves it holds the
//!   transport key derived from its [`NodeSecret`](oleaph_crypto::NodeSecret)
//!   ([`KeyPurpose::Transport`](oleaph_crypto::KeyPurpose::Transport),
//!   separate from the key that signs its WAL). The client expects the
//!   key the membership lists for the peer; the server accepts the keys
//!   of its replicas' memberships, and before it has one, the peers it was started
//!   with, and the client keys it was given
//!   ([`Endpoint::with_clients`]). A client may use the client channel
//!   only.
//! - **No impersonation.** A request names its sender in its vote; the
//!   server refuses a request whose sender is not the node that
//!   authenticated the connection, and drops a connection whose node left
//!   every membership.
//!
//! The transport plugs into a store through the same
//! [`oleaph_raft::Transport`] boundary as the deterministic simulator.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::collections::BTreeMap;
//! use std::sync::Arc;
//! use oleaph_net::{Endpoint, Store};
//! use oleaph_crypto::NodeSecret;
//! use oleaph_raft::{Member, NodeId};
//!
//! let member = Member::new(NodeId::new(1), NodeSecret::from_seed([1; 32]));
//! let endpoint = Endpoint::bind("127.0.0.1:7001".parse()?, member.secret(), BTreeMap::new())?;
//! let config = Arc::new(oleaph_raft::openraft::Config::default().validate()?);
//! let store = Store::open(member, "n1".as_ref(), config, endpoint.transport()).await?;
//! let _server = endpoint.serve(store.inbox())?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "bft")]
mod bft;
mod connection;
mod endpoint;
mod roster;
mod tls;
mod wall;

#[cfg(feature = "bft")]
pub use bft::{BftReplica, Wire, Wiring};
pub use connection::{Quic, QuicLink};
pub use endpoint::{Endpoint, NetError, Server};
pub use wall::Wall;

oleaph_raft::declare_config!(
    /// Replicas on Tokio, as the QUIC transport runs them.
    pub Tokio, openraft_rt_tokio::TokioRuntime
);

/// A replica on Tokio.
pub type Replica = oleaph_raft::Replica<Tokio>;

/// A node's replicas, reaching their peers over QUIC.
pub type Store = oleaph_raft::Store<Tokio, Quic>;

/// Where an [`Endpoint`] delivers what peers send to a [`Store`].
pub type Inbox = oleaph_raft::Inbox<Tokio, Quic>;
