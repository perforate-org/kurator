//! What a client asks a node, how both are encoded, and how a node answers
//! from its replica of a shard.
//!
//! ```text
//! request = 1 map | 2 shard u64 call
//! call    = 1 begin | 2 get version bytes(key) | 3 scan version span
//!         | 4 propose bytes(command)
//! reply   = 1 map registry count u32 (node u64 peer)*
//!         | 2 begun shard version | 3 value option(bytes)
//!         | 4 entries count u32 (bytes bytes)* | 5 applied outcome
//!         | 6 not-leader option(u64) | 7 discarded option(u64)
//!         | 8 misrouted shard | 9 refused abort | 10 no-replica
//!         | 11 failed bytes(utf-8)
//! option(x) = 0 | 1 x
//! ```
//!
//! `registry`, `peer`, `shard`, `span`, `command`, `outcome` and `abort`
//! are as [`Registry::encode`], [`Peer::encode`], [`Shard::encode`],
//! [`Span::encode`], [`Command::encode`], [`Outcome::encode`] and
//! [`Abort::encode`] write them.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::ops::Bound;

use oleaph_codec::{DecodeError, Reader, put_bytes};
use oleaph_consensus::{NodeId, Peer, Replicated};
use oleaph_meta::Registry;
use oleaph_shard::{Shard, ShardId};
use oleaph_storage::{Abort, Command, End, KeyValue, Outcome, Span, Unreadable, Version};

const MAP: u8 = 1;
const SHARD: u8 = 2;

const BEGIN: u8 = 1;
const GET: u8 = 2;
const SCAN: u8 = 3;
const PROPOSE: u8 = 4;

const MAPPED: u8 = 1;
const BEGUN: u8 = 2;
const VALUE: u8 = 3;
const ENTRIES: u8 = 4;
const APPLIED: u8 = 5;
const NOT_LEADER: u8 = 6;
const DISCARDED: u8 = 7;
const MISROUTED: u8 = 8;
const REFUSED: u8 = 9;
const NO_REPLICA: u8 = 10;
const FAILED: u8 = 11;

const NONE: u8 = 0;
const SOME: u8 = 1;

/// What a client asks a node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Request {
    /// Where the shards are: the registry, as the meta shard's leader
    /// holds it, and how to reach the nodes this node knows of.
    Map,
    /// A call on this node's replica of a shard.
    Shard(ShardId, Call),
}

/// A call on a replica of one shard, which its leader answers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Call {
    /// The shard as it is now, and the newest version it applied: where a
    /// transaction starts reading.
    Begin,
    /// The value of a key at a version.
    Get {
        /// The version to read.
        at: Version,
        /// The key.
        key: Vec<u8>,
    },
    /// The entries in a span at a version, in key order.
    Scan {
        /// The version to read.
        at: Version,
        /// The keys to read.
        span: Span,
    },
    /// Replicate and apply a command.
    Propose(Command),
}

/// How a node answers a [`Request`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply {
    /// The answer to [`Request::Map`].
    Map {
        /// The shards and their replicas.
        registry: Registry,
        /// How to reach each node.
        peers: BTreeMap<NodeId, Peer>,
    },
    /// The answer to [`Call::Begin`].
    Begun {
        /// The shard.
        shard: Shard,
        /// The newest version it applied.
        at: Version,
    },
    /// The answer to [`Call::Get`].
    Value(Option<Vec<u8>>),
    /// The answer to [`Call::Scan`].
    Entries(Vec<KeyValue>),
    /// What [`Call::Propose`] did, an abort among them.
    Applied(Outcome),
    /// This replica does not lead; this one may.
    NotLeader(Option<NodeId>),
    /// The command may or may not apply: a new leader, this one if known,
    /// dropped it from this replica's log. Proposing it again may apply it
    /// twice.
    Discarded(Option<NodeId>),
    /// The shard does not hold what the call touches: it split. This is
    /// the shard now.
    Misrouted(Shard),
    /// The read cannot be served at its version.
    Refused(Abort),
    /// This node holds no replica of the shard.
    NoReplica,
    /// Something else went wrong.
    Failed(String),
}

impl Request {
    /// Encodes the request.
    ///
    /// # Errors
    ///
    /// When a key or a command is too long to encode, or a command writes
    /// nothing.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        match self {
            Self::Map => out.push(MAP),
            Self::Shard(shard, call) => {
                out.push(SHARD);
                out.extend_from_slice(&shard.get().to_le_bytes());
                call.encode(&mut out)?;
            }
        }
        Ok(out)
    }

    /// Decodes what [`Request::encode`] wrote, all of `bytes`.
    ///
    /// # Errors
    ///
    /// When `bytes` is not one whole request.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let request = match reader.read::<u8>()? {
            MAP => Self::Map,
            SHARD => Self::Shard(ShardId::new(reader.read()?), Call::decode(&mut reader)?),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(request)
    }
}

impl Call {
    fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        match self {
            Self::Begin => out.push(BEGIN),
            Self::Get { at, key } => {
                out.push(GET);
                out.extend_from_slice(&at.get().to_le_bytes());
                put_bytes(out, key)?;
            }
            Self::Scan { at, span } => {
                out.push(SCAN);
                out.extend_from_slice(&at.get().to_le_bytes());
                span.encode(out)?;
            }
            Self::Propose(command) => {
                out.push(PROPOSE);
                let encoded = command.clone().encode().map_err(io::Error::other)?;
                put_bytes(out, encoded.payload())?;
            }
        }
        Ok(())
    }

    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match reader.read::<u8>()? {
            BEGIN => Ok(Self::Begin),
            GET => Ok(Self::Get {
                at: Version::new(reader.read()?),
                key: reader.bytes()?.to_vec(),
            }),
            SCAN => Ok(Self::Scan {
                at: Version::new(reader.read()?),
                span: Span::decode(reader)?,
            }),
            PROPOSE => Ok(Self::Propose(Command::decode(reader.bytes()?)?)),
            other => Err(DecodeError::Tag(other)),
        }
    }
}

impl Reply {
    /// Where to ask instead, when this reply is not the answer: `Some`
    /// with the leader the node named, if it named one, when the node
    /// does not lead, dropped the command for a new leader, or holds no
    /// replica of the shard; `None` when this is the answer.
    #[must_use]
    pub const fn redirect(&self) -> Option<Option<NodeId>> {
        match self {
            Self::NotLeader(leader) | Self::Discarded(leader) => Some(*leader),
            Self::NoReplica => Some(None),
            Self::Map { .. }
            | Self::Begun { .. }
            | Self::Value(_)
            | Self::Entries(_)
            | Self::Applied(_)
            | Self::Misrouted(_)
            | Self::Refused(_)
            | Self::Failed(_) => None,
        }
    }

    /// Encodes the reply.
    ///
    /// # Errors
    ///
    /// When a key or a value is too long to encode.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        match self {
            Self::Map { registry, peers } => {
                out.push(MAPPED);
                registry.encode(&mut out)?;
                put_count(&mut out, peers.len())?;
                for (node, peer) in peers {
                    out.extend_from_slice(&node.get().to_le_bytes());
                    peer.encode(&mut out);
                }
            }
            Self::Begun { shard, at } => {
                out.push(BEGUN);
                shard.encode(&mut out)?;
                out.extend_from_slice(&at.get().to_le_bytes());
            }
            Self::Value(value) => {
                out.push(VALUE);
                match value {
                    None => out.push(NONE),
                    Some(value) => {
                        out.push(SOME);
                        put_bytes(&mut out, value)?;
                    }
                }
            }
            Self::Entries(entries) => {
                out.push(ENTRIES);
                put_count(&mut out, entries.len())?;
                for (key, value) in entries {
                    put_bytes(&mut out, key)?;
                    put_bytes(&mut out, value)?;
                }
            }
            Self::Applied(outcome) => {
                out.push(APPLIED);
                outcome.encode(&mut out).map_err(io::Error::other)?;
            }
            Self::NotLeader(leader) => put_node(&mut out, NOT_LEADER, *leader),
            Self::Discarded(leader) => put_node(&mut out, DISCARDED, *leader),
            Self::Misrouted(shard) => {
                out.push(MISROUTED);
                shard.encode(&mut out)?;
            }
            Self::Refused(abort) => {
                out.push(REFUSED);
                abort.encode(&mut out).map_err(io::Error::other)?;
            }
            Self::NoReplica => out.push(NO_REPLICA),
            Self::Failed(reason) => {
                out.push(FAILED);
                put_bytes(&mut out, reason.as_bytes())?;
            }
        }
        Ok(out)
    }

    /// Decodes what [`Reply::encode`] wrote, all of `bytes`.
    ///
    /// # Errors
    ///
    /// When `bytes` is not one whole reply.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let reply = match reader.read::<u8>()? {
            MAPPED => Self::Map {
                registry: Registry::decode(&mut reader)?,
                peers: read_peers(&mut reader)?,
            },
            BEGUN => Self::Begun {
                shard: Shard::decode(&mut reader)?,
                at: Version::new(reader.read()?),
            },
            VALUE => Self::Value(match reader.read::<u8>()? {
                NONE => None,
                SOME => Some(reader.bytes()?.to_vec()),
                other => return Err(DecodeError::Tag(other)),
            }),
            ENTRIES => {
                let mut entries = Vec::new();
                for _ in 0..reader.read::<u32>()? {
                    entries.push((reader.bytes()?.to_vec(), reader.bytes()?.to_vec()));
                }
                Self::Entries(entries)
            }
            APPLIED => Self::Applied(Outcome::decode(&mut reader)?),
            NOT_LEADER => Self::NotLeader(read_node(&mut reader)?),
            DISCARDED => Self::Discarded(read_node(&mut reader)?),
            MISROUTED => Self::Misrouted(Shard::decode(&mut reader)?),
            REFUSED => Self::Refused(Abort::decode(&mut reader)?),
            NO_REPLICA => Self::NoReplica,
            FAILED => Self::Failed(
                String::from_utf8(reader.bytes()?.to_vec()).map_err(|_| DecodeError::Invalid)?,
            ),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(reply)
    }
}

/// Reads `count u32 (node u64 peer)*`, each node once.
fn read_peers(reader: &mut Reader<'_>) -> Result<BTreeMap<NodeId, Peer>, DecodeError> {
    let mut peers = BTreeMap::new();
    for _ in 0..reader.read::<u32>()? {
        let node = NodeId::new(reader.read()?);
        if peers.insert(node, Peer::decode(reader)?).is_some() {
            return Err(DecodeError::Invalid);
        }
    }
    Ok(peers)
}

fn put_count(out: &mut Vec<u8>, len: usize) -> io::Result<()> {
    let count = u32::try_from(len).map_err(io::Error::other)?;
    out.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn put_node(out: &mut Vec<u8>, tag: u8, node: Option<NodeId>) {
    out.push(tag);
    match node {
        None => out.push(NONE),
        Some(node) => {
            out.push(SOME);
            out.extend_from_slice(&node.get().to_le_bytes());
        }
    }
}

fn read_node(reader: &mut Reader<'_>) -> Result<Option<NodeId>, DecodeError> {
    match reader.read::<u8>()? {
        NONE => Ok(None),
        SOME => Ok(Some(NodeId::new(reader.read()?))),
        other => Err(DecodeError::Tag(other)),
    }
}

/// Sends a [`Request`] to a node and reads its [`Reply`]: the transport's
/// side of a client.
pub trait Reach: Sync {
    /// Sends `request` to `node`, reached as `peer`, which answers it.
    fn reach(
        &self,
        node: NodeId,
        peer: &Peer,
        request: &Request,
    ) -> impl Future<Output = io::Result<Reply>> + Send;
}

/// Runs `call` on `replica`, this node's replica of the shard, as its
/// leader would: what a node answers a client.
///
/// A read checks the shard's span after it reads. A shard's span only
/// shrinks, so a key still in it afterwards was in it while it was read.
pub async fn answer(replica: &impl Replicated, call: Call) -> Reply {
    match call {
        Call::Begin => match replica.read(oleaph_storage::KvState::version).await {
            Ok(at) => Reply::Begun {
                shard: replica.shard(),
                at,
            },
            Err(error) => failed(error),
        },
        Call::Get { at, key } => {
            let read = replica
                .read(|kv| Ok::<_, Unreadable>(kv.view(at)?.get(&key).map(<[u8]>::to_vec)))
                .await;
            held(replica, |span| span.contains(&key), read, Reply::Value)
        }
        Call::Scan { at, span } => {
            let (start, end) = bounds(&span);
            let read = replica
                .read(|kv| {
                    Ok::<_, Unreadable>(
                        kv.view(at)?
                            .range(start, end)
                            .map(|(key, value)| (key.to_vec(), value.to_vec()))
                            .collect(),
                    )
                })
                .await;
            held(replica, |held| held.holds(start, end), read, Reply::Entries)
        }
        Call::Propose(command) => match replica.propose(command).await {
            Ok(outcome) => Reply::Applied(outcome),
            Err(error) => failed(error),
        },
    }
}

/// The reply to a read that came out as `read`, if the shard still
/// `holds` what it read.
fn held<T>(
    replica: &impl Replicated,
    holds: impl FnOnce(&Span) -> bool,
    read: oleaph_consensus::Result<Result<T, Unreadable>>,
    reply: impl FnOnce(T) -> Reply,
) -> Reply {
    match read {
        Ok(Ok(value)) => {
            let shard = replica.shard();
            if holds(shard.span()) {
                reply(value)
            } else {
                Reply::Misrouted(shard)
            }
        }
        Ok(Err(unreadable)) => Reply::Refused(Abort::Unreadable(unreadable)),
        Err(error) => failed(error),
    }
}

/// The keys of `span` as range bounds.
fn bounds(span: &Span) -> (Bound<&[u8]>, Bound<&[u8]>) {
    let end = match span.end() {
        End::Before(end) => Bound::Excluded(end.as_slice()),
        End::Unbounded | _ => Bound::Unbounded,
    };
    (Bound::Included(span.start()), end)
}

/// The reply for a call that failed with `error`.
fn failed(error: oleaph_consensus::Error) -> Reply {
    match error {
        oleaph_consensus::Error::NotLeader { leader } => Reply::NotLeader(leader),
        oleaph_consensus::Error::Discarded { leader } => Reply::Discarded(leader),
        oleaph_consensus::Error::Misrouted { shard } => Reply::Misrouted(*shard),
        other @ (oleaph_consensus::Error::Storage(_) | oleaph_consensus::Error::Failed(_) | _) => {
            Reply::Failed(other.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::ops::Bound;

    use futures_util::FutureExt as _;
    use oleaph_consensus::{Duty, Members, NodeId, Peer, Reads, Replicated, Route, Transaction};
    use oleaph_crypto::{Algorithm, SigningKey};
    use oleaph_meta::Registry;
    use oleaph_shard::{Epoch, NamespaceId, Shard, ShardId};
    use oleaph_storage::{
        Abort, Command, End, KeyValue, KvState, Outcome, Span, Unreadable, Version, WriteBatch,
    };
    use proptest::prelude::*;
    use std::sync::Mutex;

    use super::{Call, Reply, Request, answer};

    /// A replica of the keys before `m`, led by node 1 or not.
    #[derive(Debug)]
    struct Fake {
        state: Mutex<KvState>,
        route: Route,
    }

    /// No transactions: a client builds its own.
    #[derive(Debug)]
    struct None_;

    impl Transaction for None_ {
        fn get(&mut self, _key: &[u8]) -> oleaph_consensus::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        fn range(
            &mut self,
            _start: Bound<&[u8]>,
            _end: Bound<&[u8]>,
        ) -> oleaph_consensus::Result<Vec<KeyValue>> {
            Ok(Vec::new())
        }

        fn put(&mut self, _key: impl Into<Vec<u8>>, _value: impl Into<Vec<u8>>) {}

        fn delete(&mut self, _key: impl Into<Vec<u8>>) {}

        async fn commit(self) -> oleaph_consensus::Result<Version> {
            Ok(Version::ZERO)
        }
    }

    fn left() -> Shard {
        let span = Span::new("", End::Before(b"m".to_vec())).unwrap();
        Shard::new(NamespaceId::DEFAULT, ShardId::new(5), span, Epoch::new(1))
    }

    impl Fake {
        fn new(route: Route) -> Self {
            Self {
                state: Mutex::new(KvState::default()),
                route,
            }
        }

        fn leads(&self) -> oleaph_consensus::Result<()> {
            match self.route {
                Route::Here => Ok(()),
                Route::Leader(leader) => Err(oleaph_consensus::Error::NotLeader {
                    leader: Some(leader),
                }),
                Route::Unknown | _ => Err(oleaph_consensus::Error::NotLeader { leader: None }),
            }
        }
    }

    impl Replicated for Fake {
        type Txn<'replica> = None_;

        fn node(&self) -> NodeId {
            NodeId::new(1)
        }

        fn shard(&self) -> Shard {
            left()
        }

        const READS: Reads = Reads::Linearizable;

        fn route(&self) -> Route {
            self.route
        }

        fn duty(&self) -> Duty {
            Duty::Others
        }

        fn members(&self) -> Members {
            Members::new(BTreeSet::default(), BTreeMap::new(), 0)
        }

        async fn begin(&self) -> oleaph_consensus::Result<None_> {
            Ok(None_)
        }

        async fn read<R: Send>(
            &self,
            read: impl FnOnce(&KvState) -> R + Send,
        ) -> oleaph_consensus::Result<R> {
            self.leads()?;
            Ok(read(&self.state.lock().unwrap()))
        }

        async fn propose(&self, command: Command) -> oleaph_consensus::Result<Outcome> {
            self.leads()?;
            if !command.fits(left().span()) {
                return Err(oleaph_consensus::Error::Misrouted {
                    shard: Box::new(left()),
                });
            }
            let mut state = self.state.lock().unwrap();
            let next = Version::new(state.version().get().checked_add(1).unwrap());
            Ok(state.apply(next, command))
        }
    }

    fn put(key: &str, value: &str) -> Call {
        Call::Propose(Command::Kv(WriteBatch::new().put(key, value)))
    }

    fn ask(replica: &Fake, call: Call) -> Reply {
        answer(replica, call).now_or_never().unwrap()
    }

    #[test]
    fn a_leader_writes_and_reads_at_the_version_asked() {
        let replica = Fake::new(Route::Here);
        assert_eq!(
            ask(&replica, put("a", "1")),
            Reply::Applied(Outcome::Committed(Version::new(1))),
            "first write"
        );
        assert_eq!(
            ask(&replica, put("a", "2")),
            Reply::Applied(Outcome::Committed(Version::new(2))),
            "second write"
        );
        assert_eq!(
            ask(&replica, Call::Begin),
            Reply::Begun {
                shard: left(),
                at: Version::new(2)
            },
            "begin"
        );
        let get = |at| Call::Get {
            at: Version::new(at),
            key: b"a".to_vec(),
        };
        assert_eq!(
            ask(&replica, get(1)),
            Reply::Value(Some(b"1".to_vec())),
            "old"
        );
        assert_eq!(
            ask(&replica, get(2)),
            Reply::Value(Some(b"2".to_vec())),
            "new"
        );
        assert_eq!(
            ask(&replica, get(3)),
            Reply::Refused(Abort::Unreadable(Unreadable::Ahead {
                at: Version::new(3),
                newest: Version::new(2)
            })),
            "not applied yet"
        );
        let scan = Call::Scan {
            at: Version::new(1),
            span: Span::new("", End::Before(b"b".to_vec())).unwrap(),
        };
        assert_eq!(
            ask(&replica, scan),
            Reply::Entries(vec![(b"a".to_vec(), b"1".to_vec())]),
            "scan"
        );
    }

    #[test]
    fn what_the_shard_does_not_hold_is_misrouted() {
        let replica = Fake::new(Route::Here);
        let get = Call::Get {
            at: Version::ZERO,
            key: b"x".to_vec(),
        };
        assert_eq!(ask(&replica, get), Reply::Misrouted(left()), "get");
        let scan = Call::Scan {
            at: Version::ZERO,
            span: Span::FULL,
        };
        assert_eq!(ask(&replica, scan), Reply::Misrouted(left()), "scan");
        assert_eq!(
            ask(&replica, put("x", "1")),
            Reply::Misrouted(left()),
            "write"
        );
    }

    #[test]
    fn a_follower_names_the_leader() {
        let replica = Fake::new(Route::Leader(NodeId::new(2)));
        for call in [Call::Begin, put("a", "1")] {
            assert_eq!(
                ask(&replica, call),
                Reply::NotLeader(Some(NodeId::new(2))),
                "not leader"
            );
        }
    }

    fn peer(seed: u8) -> Peer {
        let key = SigningKey::from_seed(Algorithm::Ed25519, [seed; 32]).verifying_key();
        Peer::new(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 7000), key)
    }

    fn bytes() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..6)
    }

    fn call() -> impl Strategy<Value = Call> {
        prop_oneof![
            Just(Call::Begin),
            (any::<u64>(), bytes()).prop_map(|(at, key)| Call::Get {
                at: Version::new(at),
                key
            }),
            any::<u64>().prop_map(|at| Call::Scan {
                at: Version::new(at),
                span: left().span().clone()
            }),
            (bytes(), bytes()).prop_map(|(key, value)| {
                Call::Propose(Command::Kv(WriteBatch::new().put(key, value)))
            }),
        ]
    }

    fn reply() -> impl Strategy<Value = Reply> {
        let node = proptest::option::of(any::<u64>().prop_map(NodeId::new));
        prop_oneof![
            any::<u8>().prop_map(|seed| Reply::Map {
                registry: Registry::read(&KvState::default()).unwrap(),
                peers: BTreeMap::from([(NodeId::new(1), peer(seed))]),
            }),
            any::<u64>().prop_map(|at| Reply::Begun {
                shard: left(),
                at: Version::new(at)
            }),
            proptest::option::of(bytes()).prop_map(Reply::Value),
            proptest::collection::vec((bytes(), bytes()), 0..3).prop_map(Reply::Entries),
            any::<u64>().prop_map(|at| Reply::Applied(Outcome::Prepared(Version::new(at)))),
            node.clone().prop_map(Reply::NotLeader),
            node.prop_map(Reply::Discarded),
            Just(Reply::Misrouted(left())),
            any::<u128>()
                .prop_map(|id| Reply::Refused(Abort::RolledBack(oleaph_storage::TxnId::new(id)))),
            Just(Reply::NoReplica),
            ".{0,8}".prop_map(Reply::Failed),
        ]
    }

    #[test]
    fn requests_have_a_fixed_encoding() {
        assert_eq!(Request::Map.encode().unwrap(), [1], "map");
        let begin = Request::Shard(ShardId::new(3), Call::Begin);
        assert_eq!(
            begin.encode().unwrap(),
            [2, 3, 0, 0, 0, 0, 0, 0, 0, 1],
            "begin on shard 3"
        );
        assert_eq!(Reply::NoReplica.encode().unwrap(), [10], "no replica");
        assert_eq!(
            Reply::NotLeader(None).encode().unwrap(),
            [6, 0],
            "no leader known"
        );
    }

    proptest! {
        #[test]
        fn requests_roundtrip(shard in any::<u64>(), call in call()) {
            let request = Request::Shard(ShardId::new(shard), call);
            prop_assert_eq!(Request::decode(&request.encode().unwrap()), Ok(request));
        }

        #[test]
        fn replies_roundtrip(reply in reply()) {
            prop_assert_eq!(Reply::decode(&reply.encode().unwrap()), Ok(reply));
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            drop(Request::decode(&bytes));
            drop(Reply::decode(&bytes));
        }
    }
}
