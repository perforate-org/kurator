//! Byzantine fault tolerant consensus for OleaphDB's shards.
//!
//! A shard whose replicas are run by independent operators cannot trust
//! any one of them, as Raft trusts its leader. Here, as on the Internet
//! Computer's subnets, `n = 3f + 1` replicas decide every block, and up to
//! `f` of them may lie, stay silent or sign two things at once without the
//! others deciding differently.
//!
//! The protocol is `HotStuff` with a two-chain commit, as `HotStuff-2` and
//! Jolteon run it:
//!
//! - each [`View`] has its own leader, the [`Committee`]'s members in turn,
//!   who proposes one [`Block`] on top of the newest certified block;
//! - replicas send their [`Vote`]s to the next view's leader, whose
//!   quorum of them is a [`Certificate`] that starts the next view;
//! - a replica that waits too long sends a [`Timeout`], and a quorum of
//!   those is a [`TimeoutCertificate`] that starts the next view instead;
//! - a block commits once its child from the very next view is certified.
//!
//! A replica is a [`Core`]: a deterministic state machine that takes an
//! [`Event`] and returns a [`Step`]. It reads no clock and does no I/O, so
//! a simulation drives exactly the code a node runs. A step's messages
//! may include a vote, which binds the replica never to vote again in that
//! view, so they are handed over only once what the event changed is
//! durable: a [`Journal`] writes the step's records to a write-ahead log
//! and only then hands over its [`Effects`]. After a crash,
//! [`Journal::open`] reads them back into a [`Recovered`] replica, which
//! [`Core::recover`] restarts without voting twice in a view, and a
//! [`Ledger`] applies the decided blocks' commands to the shard's state.
//!
//! A [`Replica`] puts these together on a node: its [`Host`] carries the
//! messages and timers, and it is the shard's
//! [`Replicated`](oleaph_consensus::Replicated) replica, so the layers
//! above see a BFT shard as they see a Raft one.
//!
//! Every [`Interval`] of decided blocks, each replica names its state by a
//! [`StateRoot`], the root of its [`Manifest`], and shares a signed
//! [`Share`] of that [`Checkpoint`]. A quorum of equal shares is a
//! [`CheckpointCertificate`]: the committee's word for the shard's state at
//! that [`Height`]. A replica whose own checkpoint differs has applied the
//! same blocks to another state, and stops serving; one whose checkpoint
//! agrees keeps that state and lets its [`Journal`] forget every record
//! before it, so that it restarts from the certified state.
//!
//! Signatures are [`oleaph_crypto`]'s, tagged with their algorithm, so a
//! certificate is a set of signatures that can move to a post-quantum
//! algorithm as a whole.

mod block;
mod checkpoint;
mod committee;
mod core;
mod message;
mod rejection;
mod replica;
mod step;
mod timeout;
mod view;
mod vote;
mod wire;

pub use block::{Block, Order, Proposal};
pub use checkpoint::{
    Checkpoint, CheckpointCertificate, Height, Interval, Manifest, Share, StateRoot,
};
pub use committee::{Change, ChangeCertificate, Committee, Domain, Endorsement};
pub use core::{Config, Core, Event};
pub use message::Message;
pub use rejection::Rejection;
pub use replica::{Host, Journal, Ledger, Replica, Settled, Txn};
pub use step::{Act, Effects, Recovered, Safety, Step, Timer, To};
pub use timeout::{Timeout, TimeoutCertificate};
pub use view::{Epoch, View};
pub use vote::{BlockId, Certificate, Justify, Vote};

/// What the tests share: four members with keys from their ids, in one
/// domain.
#[cfg(test)]
mod fixture {
    use oleaph_consensus::NodeId;
    use oleaph_crypto::{Algorithm, SigningKey};

    use crate::checkpoint::{Checkpoint, CheckpointCertificate, Share, Tally};
    use crate::committee::{Change, ChangeCertificate, Committee, Domain, Endorsement};
    use crate::view::Epoch;

    /// The domain every test signs in.
    pub(crate) const DOMAIN: Domain = Domain::new([1; 32]);

    /// The key of `node`, seeded by its id.
    pub(crate) fn key(node: u64) -> SigningKey {
        SigningKey::from_seed(Algorithm::Ed25519, [u8::try_from(node).unwrap(); 32])
    }

    /// Nodes 1 to 4.
    pub(crate) fn committee() -> Committee {
        let members = (1..=4)
            .map(|node| (NodeId::new(node), key(node).verifying_key()))
            .collect();
        Committee::new(members).unwrap()
    }

    /// The change to a committee of `members` in `epoch`, which nodes 1
    /// to 3 endorse in [`DOMAIN`].
    pub(crate) fn change_to(epoch: u64, members: &[u64]) -> ChangeCertificate {
        let keys = members
            .iter()
            .map(|node| (NodeId::new(*node), key(*node).verifying_key()))
            .collect();
        let change = Change::new(Epoch::new(epoch), Committee::new(keys).unwrap());
        let endorsements: Vec<_> = (1..=3)
            .map(|node| Endorsement::sign(&key(node), DOMAIN, NodeId::new(node), &change))
            .collect();
        ChangeCertificate::new(change, endorsements)
    }

    /// A certificate for `checkpoint`, signed by nodes 1 to 3.
    pub(crate) fn certify(checkpoint: Checkpoint) -> CheckpointCertificate {
        let mut tally = Tally::default();
        (1..=3)
            .find_map(|node| {
                let share = Share::sign(&key(node), DOMAIN, NodeId::new(node), checkpoint);
                tally.count(share, 3)
            })
            .unwrap()
    }
}
