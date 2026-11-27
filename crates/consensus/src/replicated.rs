//! What a replica of a shard offers, whatever protocol replicates it.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use std::ops::Bound;

use kurator_shard::Shard;
use kurator_storage::{Command, KeyValue, KvState, Outcome, Version};

use crate::error::Result;
use crate::node::{NodeId, Peer};

/// What a replica is doing in its shard's consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// Receives what is decided but takes no part in deciding it.
    Learner,
    /// Takes part in deciding and follows a leader.
    Follower,
    /// Seeks to lead.
    Candidate,
    /// Proposes what is decided: writes go to it.
    Leader,
    /// Shut down.
    Stopped,
}

/// The nodes that hold a shard's replicas, each with the [`Peer`] it is
/// reached at, and where in the shard's history that membership was set:
/// a later change has a later position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Members {
    voters: BTreeSet<NodeId>,
    peers: BTreeMap<NodeId, Peer>,
    position: u64,
}

impl Members {
    /// `peers`, of which `voters` take part in deciding, set at
    /// `position`.
    #[must_use]
    pub const fn new(
        voters: BTreeSet<NodeId>,
        peers: BTreeMap<NodeId, Peer>,
        position: u64,
    ) -> Self {
        Self {
            voters,
            peers,
            position,
        }
    }

    /// Every node, voter or learner, with its peer.
    pub fn peers(&self) -> impl Iterator<Item = (NodeId, &Peer)> {
        self.peers.iter().map(|(id, peer)| (*id, peer))
    }

    /// `node`'s peer, if it is a member.
    #[must_use]
    pub fn peer(&self, node: NodeId) -> Option<&Peer> {
        self.peers.get(&node)
    }

    /// Whether `node` takes part in deciding.
    #[must_use]
    pub fn votes(&self, node: NodeId) -> bool {
        self.voters.contains(&node)
    }

    /// Where in the shard's history the membership was set.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }
}

/// A replica of one shard, as the layers above consensus see it.
pub trait Replicated: Send + Sync {
    /// A transaction on the shard's state.
    type Txn<'replica>: Transaction + Send
    where
        Self: 'replica;

    /// The node this replica runs on.
    fn node(&self) -> NodeId;

    /// The shard, as this replica holds it now.
    fn shard(&self) -> Shard;

    /// What this replica is doing.
    fn role(&self) -> Role;

    /// The leader this replica knows of.
    fn leader(&self) -> Option<NodeId>;

    /// The members this replica knows of, decided or not: who may send to
    /// it.
    fn members(&self) -> Members;

    /// Starts a transaction that reads one version of the shard's state,
    /// as of every write that returned before, and commits its writes as
    /// one decided command.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`](crate::Error::NotLeader) when this replica
    /// does not lead.
    fn begin(&self) -> impl Future<Output = Result<Self::Txn<'_>>> + Send;

    /// Runs `read` against a state that reflects every write that returned
    /// before this call.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`](crate::Error::NotLeader) when this replica
    /// does not lead.
    fn read<R: Send>(
        &self,
        read: impl FnOnce(&KvState) -> R + Send,
    ) -> impl Future<Output = Result<R>> + Send;

    /// Replicates `command` and applies it, and returns what applying it
    /// did. An abort is an [`Outcome::Aborted`], not an error: the
    /// command ordered and applied, writing nothing.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`](crate::Error::NotLeader) when this replica
    /// does not lead, [`Error::Discarded`](crate::Error::Discarded) when
    /// a new leader dropped it, and
    /// [`Error::Misrouted`](crate::Error::Misrouted) when the shard does
    /// not hold what it touches.
    fn propose(&self, command: Command) -> impl Future<Output = Result<Outcome>> + Send;
}

/// A transaction on a replicated shard: it reads one version and holds
/// its writes back until it commits. Dropping it writes nothing.
pub trait Transaction {
    /// The value of `key`, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// [`Error::Misrouted`](crate::Error::Misrouted) when the shard does
    /// not hold `key`, and [`Error::Storage`](crate::Error::Storage) when
    /// the version it reads is forgotten.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// The entries between `start` and `end` that the shard holds, in key
    /// order, with the transaction's own writes applied.
    ///
    /// # Errors
    ///
    /// As [`Transaction::get`] when the version is forgotten.
    fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<KeyValue>>;

    /// Sets `key` to `value` when the transaction commits.
    fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>);

    /// Removes `key` when the transaction commits.
    fn delete(&mut self, key: impl Into<Vec<u8>>);

    /// Commits the writes as one command and returns the version they
    /// made, once it is decided and this replica applied it.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`](crate::Error::Storage) with
    /// [`kurator_storage::Error::Aborted`] when a write after its read
    /// version touched what it read: running it again may succeed.
    fn commit(self) -> impl Future<Output = Result<Version>> + Send;
}

/// A shared replica is the replica.
impl<T: Replicated + ?Sized> Replicated for Arc<T> {
    type Txn<'replica>
        = T::Txn<'replica>
    where
        Self: 'replica;

    fn node(&self) -> NodeId {
        T::node(self)
    }

    fn shard(&self) -> Shard {
        T::shard(self)
    }

    fn role(&self) -> Role {
        T::role(self)
    }

    fn leader(&self) -> Option<NodeId> {
        T::leader(self)
    }

    fn members(&self) -> Members {
        T::members(self)
    }

    fn begin(&self) -> impl Future<Output = Result<Self::Txn<'_>>> + Send {
        T::begin(self)
    }

    fn read<R: Send>(
        &self,
        read: impl FnOnce(&KvState) -> R + Send,
    ) -> impl Future<Output = Result<R>> + Send {
        T::read(self, read)
    }

    fn propose(&self, command: Command) -> impl Future<Output = Result<Outcome>> + Send {
        T::propose(self, command)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::ops::Bound;
    use std::sync::Arc;

    use futures_util::FutureExt as _;
    use kurator_crypto::{Algorithm, SigningKey};
    use kurator_shard::Shard;
    use kurator_storage::{Command, KeyValue, KvState, Outcome, Version, WriteBatch};

    use super::{Members, Replicated, Role, Transaction};
    use crate::error::{Error, Result};
    use crate::node::{NodeId, Peer};

    fn peer(id: u8) -> Peer {
        let key = SigningKey::from_seed(Algorithm::Ed25519, [id; 32]).verifying_key();
        Peer::new(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 7000), key)
    }

    /// Node 3, a voter, and node 4, a learner, set at position 5.
    fn members() -> Members {
        let voters = BTreeSet::from([NodeId::new(3)]);
        let peers = BTreeMap::from([(NodeId::new(3), peer(3)), (NodeId::new(4), peer(4))]);
        Members::new(voters, peers, 5)
    }

    /// A replica on node 3 that leads the whole key space.
    struct Fixed;

    /// A transaction that reads nothing and commits at version 7.
    struct Empty;

    impl Transaction for Empty {
        fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        fn range(&mut self, _start: Bound<&[u8]>, _end: Bound<&[u8]>) -> Result<Vec<KeyValue>> {
            Ok(Vec::new())
        }

        fn put(&mut self, _key: impl Into<Vec<u8>>, _value: impl Into<Vec<u8>>) {}

        fn delete(&mut self, _key: impl Into<Vec<u8>>) {}

        async fn commit(self) -> Result<Version> {
            Ok(Version::new(7))
        }
    }

    impl Replicated for Fixed {
        type Txn<'replica> = Empty;

        fn node(&self) -> NodeId {
            NodeId::new(3)
        }

        fn shard(&self) -> Shard {
            Shard::first()
        }

        fn role(&self) -> Role {
            Role::Leader
        }

        fn leader(&self) -> Option<NodeId> {
            Some(NodeId::new(3))
        }

        fn members(&self) -> Members {
            members()
        }

        async fn begin(&self) -> Result<Empty> {
            Ok(Empty)
        }

        async fn propose(&self, _command: Command) -> Result<Outcome> {
            Ok(Outcome::Committed(Version::new(9)))
        }

        async fn read<R: Send>(&self, read: impl FnOnce(&KvState) -> R + Send) -> Result<R> {
            Ok(read(&KvState::default()))
        }
    }

    #[test]
    fn members_tell_voters_from_learners() {
        let members = members();
        assert!(members.votes(NodeId::new(3)), "a voter");
        assert!(!members.votes(NodeId::new(4)), "a learner");
        assert_eq!(members.peer(NodeId::new(4)), Some(&peer(4)), "its peer");
        assert_eq!(members.peer(NodeId::new(5)), None, "a stranger");
        let ids: Vec<u64> = members.peers().map(|(id, _)| id.get()).collect();
        assert_eq!(ids, [3, 4], "every member");
        assert_eq!(members.position(), 5, "where it was set");
    }

    #[test]
    fn a_shared_replica_is_the_replica() {
        let shared = Arc::new(Fixed);
        assert_eq!(shared.node(), NodeId::new(3), "node");
        assert_eq!(shared.shard(), Shard::first(), "shard");
        assert_eq!(shared.role(), Role::Leader, "role");
        assert_eq!(shared.leader(), Some(NodeId::new(3)), "leader");
        assert_eq!(shared.members(), members(), "members");
        let read = shared.read(KvState::version).now_or_never().unwrap();
        assert_eq!(read.unwrap(), Version::ZERO, "read");
        let proposed = shared
            .propose(Command::Kv(WriteBatch::new().put("k", "v")))
            .now_or_never()
            .unwrap();
        assert_eq!(
            proposed.unwrap(),
            Outcome::Committed(Version::new(9)),
            "propose"
        );
        let mut txn = shared.begin().now_or_never().unwrap().unwrap();
        assert_eq!(txn.get(b"k").unwrap(), None, "get");
        assert!(
            txn.range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .is_empty(),
            "range"
        );
        txn.put("k", "v");
        txn.delete("k");
        let committed = txn.commit().now_or_never().unwrap();
        assert_eq!(committed.unwrap(), Version::new(7), "commit");
    }

    #[test]
    fn errors_name_the_leader_or_an_abort() {
        let leader = Some(NodeId::new(2));
        let not_leader = Error::NotLeader { leader };
        assert_eq!(not_leader.redirect(), Some(leader), "not the leader");
        let discarded = Error::Discarded { leader: None };
        assert_eq!(discarded.redirect(), Some(None), "discarded");
        let failed = Error::Failed("stopped".into());
        assert_eq!(failed.redirect(), None, "no redirect");
        assert!(!failed.is_aborted(), "not an abort");
        let abort = kurator_storage::Abort::RolledBack(kurator_storage::TxnId::new(1));
        let aborted = Error::Storage(kurator_storage::Error::Aborted(abort));
        assert!(aborted.is_aborted(), "an abort");
        assert_eq!(aborted.redirect(), None, "no redirect either");
    }
}
