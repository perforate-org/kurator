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
use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{Either, select};

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};

use crate::config::Config;
use crate::rpc::{Message, Refusal, Request, Response};
use crate::snapshot::{Snapshot, SnapshotBytes, SnapshotChunk, chunks};
use crate::types::{NodeId, Peer, Vote};
use oleaph_shard::ShardId;

/// The size of a snapshot chunk when `OpenRaft` names none: its own
/// default.
const CHUNK: NonZeroUsize = match NonZeroUsize::new(3 << 20) {
    Some(size) => size,
    None => NonZeroUsize::MIN,
};

/// How fast a replica is taken to install a snapshot it has received:
/// check, decode, write and sync it, in bytes a second. The last chunk's
/// deadline grows by the time the whole snapshot takes at this pace, so
/// that a large shard is not refused for the work after its transfer.
const INSTALL_PACE: u64 = 32 << 20;

/// How long to wait for the answer to `chunk` when a request may take
/// `deadline`: the last chunk also waits for the install.
fn chunk_deadline(chunk: &SnapshotChunk, deadline: Duration) -> Duration {
    if chunk.is_last() {
        let install = chunk.total().saturating_mul(1000).div_ceil(INSTALL_PACE);
        deadline.saturating_add(Duration::from_millis(install))
    } else {
        deadline
    }
}

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
            | Response::InstallSnapshot(_)
            | Response::Received) => expect(answer)
                .ok_or_else(|| RPCError::Network(NetworkError::new(&Mismatched { target }))),
        }
    }

    /// Sends `snapshot`'s chunks of `size` bytes in order, waiting at
    /// most `deadline` for each answer, and returns the answer to the last
    /// one, or the first that names a higher vote.
    async fn send_chunks<C>(
        &mut self,
        vote: Vote,
        snapshot: &Snapshot,
        size: NonZeroUsize,
        deadline: Duration,
    ) -> Result<SnapshotResponse<C>, RPCError<C>>
    where
        C: Config,
        K: Connection<C>,
    {
        let target = self.target;
        for chunk in chunks(snapshot, size) {
            let last = chunk.is_last();
            let wait = chunk_deadline(&chunk, deadline);
            let request = Request::InstallSnapshot { vote, chunk };
            let answer = self
                .exchange(request, wait, |response| match response {
                    Response::InstallSnapshot(installed) => Some(Answer::Done(installed)),
                    Response::Received if !last => Some(Answer::Kept),
                    Response::Received
                    | Response::Refused(_)
                    | Response::AppendEntries(_)
                    | Response::Vote(_)
                    | Response::PreVote(_) => None,
                })
                .await?;
            if let Answer::Done(installed) = answer {
                return Ok(installed);
            }
        }
        Err(RPCError::Network(NetworkError::new(&Mismatched { target })))
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

    /// Sends `snapshot` in chunks of the size `option` names, each
    /// within `option`'s deadline, and stops when `cancel` resolves.
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let size = option
            .snapshot_chunk_size()
            .and_then(NonZeroUsize::new)
            .unwrap_or(CHUNK);
        let sending = pin!(self.send_chunks(vote, &snapshot, size, option.hard_ttl()));
        match select(pin!(cancel), sending).await {
            Either::Left((closed, _)) => Err(StreamingError::Closed(closed)),
            Either::Right((sent, _)) => sent.map_err(|error| match error {
                RPCError::Unreachable(unreachable) => StreamingError::Unreachable(unreachable),
                RPCError::Network(network) => StreamingError::Network(network),
                other @ (RPCError::Timeout(_) | RPCError::RemoteError(_)) => {
                    StreamingError::Network(NetworkError::new(&other))
                }
            }),
        }
    }
}

/// What the receiver said to a chunk.
enum Answer<C: openraft::RaftTypeConfig> {
    /// It kept the chunk: send the next.
    Kept,
    /// It installed the snapshot, or holds a higher vote: stop.
    Done(SnapshotResponse<C>),
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

    use oleaph_shard::ShardId;

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use openraft::error::{ReplicationClosed, StreamingError};
    use parking_lot::Mutex;

    use super::{Connection, Failure, Network, Transport};
    use crate::rpc::{Message, Refusal, Request, Response};
    use crate::snapshot::{Snapshot, SnapshotBytes, SnapshotChunk};
    use crate::types::testing::peer;
    use crate::types::{NodeId, Peer, SnapshotMeta, Vote};

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

    /// Takes snapshot chunks, noting each one's deadline and whether it
    /// was the last, and answers as `answer` says for the nth chunk.
    #[derive(Clone)]
    struct Taker {
        seen: Arc<Mutex<Vec<(bool, Duration)>>>,
        answer: fn(usize, &SnapshotChunk) -> Response<Test>,
    }

    impl Transport<Test> for Taker {
        type Connection = Self;

        fn connect(&self, _target: NodeId, _peer: &Peer) -> Self {
            self.clone()
        }
    }

    impl Connection<Test> for Taker {
        async fn call(
            &mut self,
            message: Message<Test>,
            deadline: Duration,
        ) -> Result<Response<Test>, Failure> {
            let Request::InstallSnapshot { chunk, .. } = message.into_request() else {
                return Ok(Response::Refused(Refusal::Malformed));
            };
            let mut seen = self.seen.lock();
            seen.push((chunk.is_last(), deadline));
            Ok((self.answer)(seen.len(), &chunk))
        }
    }

    fn installed(_nth: usize, chunk: &SnapshotChunk) -> Response<Test> {
        if chunk.is_last() {
            Response::InstallSnapshot(SnapshotResponse::new(leader()))
        } else {
            Response::Received
        }
    }

    fn deposed(_nth: usize, _chunk: &SnapshotChunk) -> Response<Test> {
        Response::InstallSnapshot(SnapshotResponse::new(Vote::new_committed(
            9,
            NodeId::new(3),
        )))
    }

    fn lost(nth: usize, chunk: &SnapshotChunk) -> Response<Test> {
        if nth == 2 {
            Response::Refused(Refusal::OutOfOrder)
        } else {
            installed(nth, chunk)
        }
    }

    fn early(_nth: usize, _chunk: &SnapshotChunk) -> Response<Test> {
        Response::InstallSnapshot(SnapshotResponse::new(leader()))
    }

    /// What came of sending a snapshot, and what the receiver saw of each
    /// chunk: whether it was the last, and its deadline.
    type Sent = (
        Result<SnapshotResponse<Test>, StreamingError<Test>>,
        Vec<(bool, Duration)>,
    );

    /// When the replication that sends a snapshot is closed.
    #[derive(Debug, Clone, Copy)]
    enum Cancel {
        Never,
        AtOnce,
    }

    fn leader() -> Vote {
        Vote::new_committed(2, NodeId::new(1))
    }

    /// Sends a snapshot of `len` bytes to a [`Taker`] answering with
    /// `answer`, and returns the outcome and what the taker saw.
    fn send(
        len: usize,
        answer: fn(usize, &SnapshotChunk) -> Response<Test>,
        cancel: Cancel,
    ) -> Sent {
        SimRuntime::with_seed(0).block_on(async move {
            let taker = Taker {
                seen: Arc::default(),
                answer,
            };
            let mut network = Network::new(taker.clone(), ShardId::FIRST);
            let mut link = network.new_client(NodeId::new(2), &peer(2)).await;
            let snapshot = Snapshot {
                meta: SnapshotMeta::default(),
                snapshot: SnapshotBytes::from_vec(vec![7; len]),
            };
            let option = RPCOption::new(Duration::from_millis(200));
            let wait: Pin<Box<dyn Future<Output = ()> + Send>> = match cancel {
                Cancel::Never => Box::pin(std::future::pending()),
                Cancel::AtOnce => Box::pin(std::future::ready(())),
            };
            let closing = async move {
                wait.await;
                ReplicationClosed::new("cancelled")
            };
            let outcome = link
                .full_snapshot(leader(), snapshot, closing, option)
                .await;
            let seen = taker.seen.lock().clone();
            (outcome, seen)
        })
    }

    #[test]
    fn a_snapshot_goes_in_chunks_and_the_last_waits_for_the_install() {
        let (outcome, seen) = send((7 << 20) + 1, installed, Cancel::Never);
        assert_eq!(outcome.unwrap().vote, leader(), "installed");
        let ttl = Duration::from_millis(200);
        // 7 MiB at 32 MiB a second: 219 ms more for the install.
        assert_eq!(
            seen,
            [
                (false, ttl),
                (false, ttl),
                (true, ttl + Duration::from_millis(219))
            ],
            "three chunks of at most 3 MiB"
        );
    }

    #[test]
    fn sending_stops_at_a_higher_vote() {
        let (outcome, seen) = send(7 << 20, deposed, Cancel::Never);
        assert_eq!(outcome.unwrap().vote.leader_id.term, 9, "higher vote");
        assert_eq!(seen.len(), 1, "stopped after the first chunk");
    }

    #[test]
    fn sending_stops_at_a_refusal() {
        let (outcome, seen) = send(7 << 20, lost, Cancel::Never);
        assert!(
            matches!(outcome, Err(StreamingError::Unreachable(_))),
            "{outcome:?}"
        );
        assert_eq!(seen.len(), 2, "stopped at the refusal");
    }

    #[test]
    fn sending_stops_when_cancelled() {
        let (outcome, seen) = send(7 << 20, installed, Cancel::AtOnce);
        assert!(
            matches!(outcome, Err(StreamingError::Closed(_))),
            "{outcome:?}"
        );
        assert!(seen.is_empty(), "nothing sent");
    }

    #[test]
    fn a_kept_last_chunk_answers_another_request() {
        let (outcome, seen) = send(1, |_, _| Response::Received, Cancel::Never);
        assert!(
            matches!(outcome, Err(StreamingError::Network(_))),
            "{outcome:?}"
        );
        assert_eq!(seen, [(true, Duration::from_millis(201))], "one chunk");
    }

    #[test]
    fn an_empty_snapshot_is_one_chunk() {
        let (outcome, seen) = send(0, early, Cancel::Never);
        assert_eq!(outcome.unwrap().vote, leader(), "answered");
        assert_eq!(seen.len(), 1, "one chunk");
    }
}
