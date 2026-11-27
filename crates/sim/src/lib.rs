//! Deterministic simulation of Kurator clusters, in the manner of
//! `FoundationDB` and `TigerBeetle`.
//!
//! A whole cluster runs on one thread under [`openraft_rt_sim::SimRuntime`]:
//! tasks run in a fixed order, time is virtual and jumps to the next timer,
//! and every random choice, Raft's election timeouts as well as the
//! network's losses, delays and partitions, comes from one seed. A failing
//! seed therefore fails the same way every time it runs.
//!
//! Replicas are real: each node's store writes its replicas' signed logs and
//! snapshots to a temporary directory, and a crash throws away everything
//! but those files. Shards split while the faults go on, and the client
//! follows them as a real one would, by the refusals of replicas that no
//! longer hold a key.
//!
//! ```
//! use kurator_sim::{Cluster, Faults, Outcome};
//! use kurator_raft::openraft::AsyncRuntime as _;
//! use kurator_storage::WriteBatch;
//! use openraft_rt_sim::SimRuntime;
//!
//! SimRuntime::with_seed(7).block_on(async {
//!     let mut cluster = Cluster::start(7, 3, Faults::default()).await?;
//!     cluster.settle().await?;
//!     let outcome = cluster.write(WriteBatch::new().put("k", "v")).await;
//!     assert_eq!(outcome, Outcome::Acked);
//!     assert_eq!(cluster.split(b"m").await, Outcome::Acked);
//!     assert_eq!(cluster.shards().len(), 2);
//!     let moved = cluster.write(WriteBatch::new().put("melon", "v")).await;
//!     assert_eq!(moved, Outcome::Acked);
//!     cluster.shutdown().await
//! })?;
//! # Ok::<(), kurator_sim::SimError>(())
//! ```

mod cluster;
mod holdings;
mod network;
mod rng;

pub use cluster::{Across, Cluster, Coordinator, Outcome, SimError, Transacted};
pub use holdings::Holdings;
pub use network::{Endpoint, Faults, Garbled, Network, Wire};
pub use rng::Rng;

kurator_raft::declare_config!(
    /// Replicas on the deterministic simulator.
    pub Sim, openraft_rt_sim::SimRuntime
);
