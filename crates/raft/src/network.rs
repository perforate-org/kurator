//! The boundary between a replica and whatever carries its messages.
//!
//! A [`Transport`] opens a [`Connection`] to a peer, and a connection
//! turns one [`Message`] into one [`Response`]. [`Network`] adapts any
//! transport to the network `OpenRaft` drives for one shard, so the QUIC
//! transport and the deterministic simulator plug into a store the same
//! way and differ only in how bytes travel.

use std::fmt;
use std::future::Future;
use std::io;
use std::time::Duration;

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};

use crate::config::Config;
use crate::rpc::{Message, Refusal, Request, Response};
use crate::snapshot::{Snapshot, SnapshotBytes};
use crate::types::{NodeId, Peer, Vote};
use kurator_shard::ShardId;

/// Why a request got no answer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Failure {
    /// The peer could not be reached, or the connection broke: try again
    /// later.
    Unreachable(io::Error),
    /// No answer came within the deadline.
    Timeout,
}

/// Opens connections to peers. Clones open connections from the same
/// endpoint.
pub trait Transport<C: Config>: Clone + Send + Sync + 'static {
    /// A connection to one peer.
    type Connection: Connection<C>;

    /// A connection to `target`, reached and authenticated as `peer`
    /// describes. It may connect lazily, on its first request.
    fn connect(&self, target: NodeId, peer: &Peer) -> Self::Connection;
}

/// Carries messages to one peer and brings back its answers.
pub trait Connection<C: Config>: Send + Sync + 'static {
    /// Sends `message` and waits at most `deadline` for the response.
    fn call(
        &mut self,
        message: Message<C>,
        deadline: Duration,
    ) -> impl Future<Output = Result<Response<C>, Failure>> + Send;
}

/// The network one shard's replica sends through: `transport`, adapted to
/// `OpenRaft`, with every request addressed to the shard.
#[derive(Debug, Clone)]
pub(crate) struct Network<T> {
    transport: T,
    shard: ShardId,
}

impl<T> Network<T> {
    /// Sends `shard`'s requests through `transport`.
    pub(crate) const fn new(transport: T, shard: ShardId) -> Self {
        Self { transport, shard }
    }
}

/// The link to one peer's replica of a shard.
#[derive(Debug)]
pub struct Link<K> {
    target: NodeId,
    shard: ShardId,
    connection: K,
}

impl<C: Config, T: Transport<C>> RaftNetworkFactory<C> for Network<T> {
    type Network = Link<T::Connection>;

    async fn new_client(&mut self, target: NodeId, node: &Peer) -> Self::Network {
        Link {
            target,
            shard: self.shard,
            connection: self.transport.connect(target, node),
        }
    }
}

/// A response that answers another request than the one sent.
#[derive(Debug)]
struct Mismatched {
    target: NodeId,
}

impl fmt::Display for Mismatched {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} answered another request", self.target)
    }
}

impl std::error::Error for Mismatched {}

/// A request the peer refused.
#[derive(Debug)]
struct Refused {
    target: NodeId,
    refusal: Refusal,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} refused the request: {}", self.target, self.refusal)
    }
}

impl std::error::Error for Refused {}

impl<K> Link<K> {
    /// Sends `request` and picks the expected answer out of the response.
    async fn exchange<C, R>(
        &mut self,
        request: Request<C>,
        deadline: Duration,
        expect: impl FnOnce(Response<C>) -> Option<R>,
    ) -> Result<R, RPCError<C>>
    where
        C: Config,
        K: Connection<C>,
    {
        let response = self
            .connection
            .call(Message::new(self.shard, request), deadline)
            .await
            .map_err(|failure| match failure {
                Failure::Unreachable(error) => RPCError::Unreachable(Unreachable::from_string(
                    format!("{} is unreachable: {error}", self.target),
                )),
                Failure::Timeout => RPCError::Unreachable(Unreachable::from_string(format!(
                    "{} did not answer within {deadline:?}",
                    self.target
                ))),
            })?;
        let target = self.target;
        match response {
            Response::Refused(refusal) => Err(RPCError::Unreachable(Unreachable::new(&Refused {
                target,
                refusal,
            }))),
            answer @ (Response::AppendEntries(_)
            | Response::Vote(_)
            | Response::PreVote(_)
            | Response::InstallSnapshot(_)) => expect(answer)
                .ok_or_else(|| RPCError::Network(NetworkError::new(&Mismatched { target }))),
        }
    }
}

impl<C: Config, K: Connection<C>> RaftNetworkV2<C> for Link<K> {
    type SnapshotData = SnapshotBytes;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendResult<C>, RPCError<C>> {
        self.exchange(Request::AppendEntries(rpc), option.hard_ttl(), |response| {
            if let Response::AppendEntries(result) = response {
                Some(result)
            } else {
                None
            }
        })
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        self.exchange(Request::Vote(rpc), option.hard_ttl(), |response| {
            if let Response::Vote(vote) = response {
                Some(vote)
            } else {
                None
            }
        })
        .await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        self.exchange(Request::PreVote(rpc), option.hard_ttl(), |response| {
            if let Response::PreVote(vote) = response {
                Some(vote)
            } else {
                None
            }
        })
        .await
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let request = Request::InstallSnapshot { vote, snapshot };
        let installed = self
            .exchange(request, option.hard_ttl(), |response| {
                if let Response::InstallSnapshot(installed) = response {
                    Some(installed)
                } else {
                    None
                }
            })
            .await;
        installed.map_err(|error| match error {
            RPCError::Unreachable(unreachable) => StreamingError::Unreachable(unreachable),
            RPCError::Network(network) => StreamingError::Network(network),
            other @ (RPCError::Timeout(_) | RPCError::RemoteError(_)) => {
                StreamingError::Network(NetworkError::new(&other))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

    use openraft::AsyncRuntime as _;
    use openraft::error::RPCError;
    use openraft::network::{RPCOption, RaftNetworkFactory as _, v2::RaftNetworkV2 as _};
    use openraft::raft::{SnapshotResponse, VoteRequest};
    use openraft_rt_sim::SimRuntime;

    use kurator_shard::ShardId;

    use super::{Connection, Failure, Network, Transport};
    use crate::rpc::{Message, Refusal, Response};
    use crate::types::testing::peer;
    use crate::types::{NodeId, Peer, Vote};

    crate::declare_config!(Test, SimRuntime);

    /// Answers every request with a fixed outcome.
    #[derive(Debug, Clone, Copy)]
    enum Canned {
        Refuse,
        Mismatch,
        Fail,
        Slow,
    }

    impl Transport<Test> for Canned {
        type Connection = Self;

        fn connect(&self, _target: NodeId, _peer: &Peer) -> Self {
            *self
        }
    }

    impl Connection<Test> for Canned {
        async fn call(
            &mut self,
            _message: Message<Test>,
            _deadline: Duration,
        ) -> Result<Response<Test>, Failure> {
            match self {
                Self::Refuse => Ok(Response::Refused(Refusal::Forged)),
                Self::Mismatch => Ok(Response::InstallSnapshot(SnapshotResponse::new(Vote::new(
                    1,
                    NodeId::new(1),
                )))),
                Self::Fail => Err(Failure::Unreachable(io::Error::other("down"))),
                Self::Slow => Err(Failure::Timeout),
            }
        }
    }

    #[test]
    fn failures_and_wrong_answers_become_errors() {
        SimRuntime::with_seed(0).block_on(async {
            for (canned, unreachable) in [
                (Canned::Refuse, true),
                (Canned::Mismatch, false),
                (Canned::Fail, true),
                (Canned::Slow, true),
            ] {
                let mut network = Network::new(canned, ShardId::FIRST);
                let mut link = network.new_client(NodeId::new(2), &peer(2)).await;
                let request = VoteRequest::new(Vote::new(1, NodeId::new(1)), None);
                let option = RPCOption::new(Duration::from_secs(1));
                let error = link.vote(request, option).await.unwrap_err();
                assert_eq!(
                    matches!(error, RPCError::Unreachable(_)),
                    unreachable,
                    "{canned:?}: {error}"
                );
                assert!(
                    error.to_string().contains("n2"),
                    "{canned:?} names the peer: {error}"
                );
            }
        });
    }
}
