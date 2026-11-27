//! Byzantine fault tolerant consensus for Kurator's shards.
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
//! Signatures are [`kurator_crypto`]'s, tagged with their algorithm, so a
//! certificate is a set of signatures that can move to a post-quantum
//! algorithm as a whole.

mod block;
mod committee;
mod core;
#[cfg(test)]
mod fixture;
mod journal;
mod ledger;
mod message;
mod rejection;
mod step;
mod timeout;
mod view;
mod vote;
mod wire;

pub use block::{Block, Proposal};
pub use committee::{Committee, Domain};
pub use core::{Config, Core, Event};
pub use journal::{Journal, Recovered};
pub use ledger::{Ledger, Settled};
pub use message::Message;
pub use rejection::Rejection;
pub use step::{Effects, Outgoing, Safety, Step, Timer, To};
pub use timeout::{Timeout, TimeoutCertificate};
pub use view::View;
pub use vote::{BlockId, Certificate, Justify, Vote};
