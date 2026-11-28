//! The meta shard asked from any node: the calls a node sends the meta
//! shard's leader, their encoding, how the leader answers them, and how a
//! node finds the leader.
//!
//! A node that does not lead the meta shard cannot run [`allocate`],
//! [`report`] or [`registry`] on its own replica. [`Meta`] runs them on
//! whichever node leads: on this node's replica when it leads, and
//! otherwise as a [`Call`] sent through an [`Ask`], which the leader
//! answers with [`answer`]. A follower that is asked names the leader it
//! knows of, and the call follows it.
//!
//! ```text
//! call   = 1 allocate | 2 report bytes(placement) | 3 registry
//!        | 4 create bytes(name) membership u64 nodes
//!        | 5 charter bytes(name) namespace u32 shard u64 nodes
//! answer = 1 allocated u64 | 2 reported u8 | 3 registry registry
//!        | 4 not-leader option(u64) | 5 failed bytes(utf-8)
//!        | 6 created created
//! nodes    = count u32 u64*, strictly ascending
//! reported = 1 recorded | 2 stale
//! created  = 1 founded shard | 2 taken bytes(namespace) | 3 spent
//! ```
//!
//! `placement`, `registry`, `shard` and `namespace` are as
//! [`Placement::encode`], [`Registry::encode`], [`Shard::encode`] and
//! [`Namespace::encode`] write them.

use std::collections::BTreeSet;
use std::future::Future;
use std::io;

use oleaph_codec::{DecodeError, Reader, put_bytes};
use oleaph_consensus::{NodeId, Peer, Replicated};
use oleaph_shard::{NamespaceId, Shard, ShardId};

use crate::client::{Created, allocate, charter, create, registry, report};
use crate::error::{MetaError, Result};
use crate::namespace::{Namespace, NamespaceName};
use crate::placement::{Placement, Reported};
use crate::registry::Registry;

const ALLOCATE: u8 = 1;
const REPORT: u8 = 2;
const REGISTRY: u8 = 3;
const CREATE: u8 = 4;
const CHARTER: u8 = 5;

const ALLOCATED: u8 = 1;
const REPORTED: u8 = 2;
const REGISTERED: u8 = 3;
const NOT_LEADER: u8 = 4;
const FAILED: u8 = 5;
const CREATED: u8 = 6;

const RECORDED: u8 = 1;
const STALE: u8 = 2;

const FOUNDED: u8 = 1;
const TAKEN: u8 = 2;
const SPENT: u8 = 3;

const NONE: u8 = 0;
const SOME: u8 = 1;

/// How many leaders a call follows before it gives up: leadership that
/// keeps moving.
const HOPS: usize = 4;

/// An operation on the meta shard, for its leader to run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Call {
    /// [`allocate`] an id.
    Allocate,
    /// [`report`] a placement.
    Report(Placement),
    /// Read the [`registry`].
    Registry,
    /// [`create`] a namespace.
    Create(Creation),
    /// Record the namespace a [`charter`] names.
    Charter(Chartered),
}

/// What [`create`] is asked to make: a crash fault tolerant namespace's
/// name, and where its first shard is to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Creation {
    name: NamespaceName,
    replicas: BTreeSet<NodeId>,
    membership: u64,
}

impl Creation {
    /// A namespace called `name`, whose first shard is on `replicas` at
    /// membership `membership`.
    #[must_use]
    pub const fn new(name: NamespaceName, replicas: BTreeSet<NodeId>, membership: u64) -> Self {
        Self {
            name,
            replicas,
            membership,
        }
    }

    /// Writes it as the module documentation lays a create call out,
    /// after its tag.
    fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        put_bytes(out, self.name.as_str().as_bytes())?;
        out.extend_from_slice(&self.membership.to_le_bytes());
        put_nodes(out, &self.replicas)
    }

    /// Reads what [`Creation::encode`] wrote off the front of `reader`.
    fn decode(reader: &mut Reader<'_>) -> std::result::Result<Self, DecodeError> {
        let name = NamespaceName::from_bytes(reader.bytes()?)?;
        let membership = reader.read()?;
        Ok(Self::new(name, read_nodes(reader)?, membership))
    }
}

/// What [`charter`] is asked to record: a Byzantine fault tolerant
/// namespace as its committee's charter names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chartered {
    name: NamespaceName,
    namespace: NamespaceId,
    shard: ShardId,
    members: BTreeSet<NodeId>,
}

impl Chartered {
    /// The namespace called `name`, with id `namespace`, whose one shard
    /// `shard` the committee of `members` decides.
    #[must_use]
    pub const fn new(
        name: NamespaceName,
        namespace: NamespaceId,
        shard: ShardId,
        members: BTreeSet<NodeId>,
    ) -> Self {
        Self {
            name,
            namespace,
            shard,
            members,
        }
    }

    /// Its name.
    #[must_use]
    pub const fn name(&self) -> &NamespaceName {
        &self.name
    }

    /// Writes it as the module documentation lays a charter call out,
    /// after its tag.
    fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        put_bytes(out, self.name.as_str().as_bytes())?;
        out.extend_from_slice(&self.namespace.get().to_le_bytes());
        out.extend_from_slice(&self.shard.get().to_le_bytes());
        put_nodes(out, &self.members)
    }

    /// Reads what [`Chartered::encode`] wrote off the front of `reader`.
    fn decode(reader: &mut Reader<'_>) -> std::result::Result<Self, DecodeError> {
        let name = NamespaceName::from_bytes(reader.bytes()?)?;
        let namespace = NamespaceId::new(reader.read()?);
        let shard = ShardId::new(reader.read()?);
        Ok(Self::new(name, namespace, shard, read_nodes(reader)?))
    }
}

/// Writes `nodes` as the module documentation lays `nodes` out.
fn put_nodes(out: &mut Vec<u8>, nodes: &BTreeSet<NodeId>) -> io::Result<()> {
    let count = u32::try_from(nodes.len()).map_err(io::Error::other)?;
    out.extend_from_slice(&count.to_le_bytes());
    for id in nodes {
        out.extend_from_slice(&id.get().to_le_bytes());
    }
    Ok(())
}

/// Reads what [`put_nodes`] wrote off the front of `reader`.
fn read_nodes(reader: &mut Reader<'_>) -> std::result::Result<BTreeSet<NodeId>, DecodeError> {
    let mut nodes = BTreeSet::new();
    for _ in 0..reader.read::<u32>()? {
        let id = NodeId::new(reader.read()?);
        // Strictly ascending, so no id comes twice.
        if nodes.last().is_some_and(|last| *last >= id) {
            return Err(DecodeError::Invalid);
        }
        let _new: bool = nodes.insert(id);
    }
    Ok(nodes)
}

/// How the node a [`Call`] was sent to answered it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Answer {
    /// The id [`Call::Allocate`] took.
    Allocated(ShardId),
    /// What [`Call::Report`] did.
    Reported(Reported),
    /// The registry [`Call::Registry`] read.
    Registry(Registry),
    /// The node does not lead the meta shard; this one does, if it knows.
    NotLeader(Option<NodeId>),
    /// The call failed on the leader, for this reason.
    Failed(String),
    /// What [`Call::Create`] did.
    Created(Created),
}

impl Call {
    /// The call's bytes, as the module documentation lays them out.
    ///
    /// # Errors
    ///
    /// When a placement is too long for its prefix.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        match self {
            Self::Allocate => out.push(ALLOCATE),
            Self::Report(placement) => {
                out.push(REPORT);
                let mut encoded = Vec::new();
                placement.encode(&mut encoded)?;
                put_bytes(&mut out, &encoded)?;
            }
            Self::Registry => out.push(REGISTRY),
            Self::Create(creation) => {
                out.push(CREATE);
                creation.encode(&mut out)?;
            }
            Self::Charter(chartered) => {
                out.push(CHARTER);
                chartered.encode(&mut out)?;
            }
        }
        Ok(out)
    }

    /// Decodes what [`Call::encode`] wrote, all of `bytes`.
    ///
    /// # Errors
    ///
    /// When `bytes` is not one whole call.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let call = match reader.read::<u8>()? {
            ALLOCATE => Self::Allocate,
            REPORT => Self::Report(Placement::decode(reader.bytes()?)?),
            REGISTRY => Self::Registry,
            CREATE => Self::Create(Creation::decode(&mut reader)?),
            CHARTER => Self::Charter(Chartered::decode(&mut reader)?),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(call)
    }
}

impl Answer {
    /// The answer's bytes, as the module documentation lays them out.
    ///
    /// # Errors
    ///
    /// When something is too long for its prefix.
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        match self {
            Self::Allocated(id) => {
                out.push(ALLOCATED);
                out.extend_from_slice(&id.get().to_le_bytes());
            }
            Self::Reported(reported) => {
                out.push(REPORTED);
                out.push(match reported {
                    Reported::Recorded => RECORDED,
                    Reported::Stale => STALE,
                });
            }
            Self::Registry(registry) => {
                out.push(REGISTERED);
                registry.encode(&mut out)?;
            }
            Self::NotLeader(leader) => {
                out.push(NOT_LEADER);
                match leader {
                    Some(id) => {
                        out.push(SOME);
                        out.extend_from_slice(&id.get().to_le_bytes());
                    }
                    None => out.push(NONE),
                }
            }
            Self::Failed(reason) => {
                out.push(FAILED);
                put_bytes(&mut out, reason.as_bytes())?;
            }
            Self::Created(Created::Founded(shard)) => {
                out.extend_from_slice(&[CREATED, FOUNDED]);
                shard.encode(&mut out)?;
            }
            Self::Created(Created::Taken(namespace)) => {
                out.extend_from_slice(&[CREATED, TAKEN]);
                let mut encoded = Vec::new();
                namespace.encode(&mut encoded)?;
                put_bytes(&mut out, &encoded)?;
            }
            Self::Created(Created::Spent) => out.extend_from_slice(&[CREATED, SPENT]),
        }
        Ok(out)
    }

    /// Decodes what [`Answer::encode`] wrote, all of `bytes`.
    ///
    /// # Errors
    ///
    /// When `bytes` is not one whole answer.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let answer = match reader.read::<u8>()? {
            ALLOCATED => Self::Allocated(ShardId::new(reader.read()?)),
            REPORTED => Self::Reported(match reader.read::<u8>()? {
                RECORDED => Reported::Recorded,
                STALE => Reported::Stale,
                other => return Err(DecodeError::Tag(other)),
            }),
            REGISTERED => Self::Registry(Registry::decode(&mut reader)?),
            NOT_LEADER => Self::NotLeader(match reader.read::<u8>()? {
                NONE => None,
                SOME => Some(NodeId::new(reader.read()?)),
                other => return Err(DecodeError::Tag(other)),
            }),
            FAILED => Self::Failed(
                String::from_utf8(reader.bytes()?.to_vec()).map_err(|_| DecodeError::Invalid)?,
            ),
            CREATED => Self::Created(match reader.read::<u8>()? {
                FOUNDED => Created::Founded(Shard::decode(&mut reader)?),
                TAKEN => Created::Taken(Namespace::decode(reader.bytes()?)?),
                SPENT => Created::Spent,
                other => return Err(DecodeError::Tag(other)),
            }),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(answer)
    }
}

/// Runs `call` on `meta`, this node's replica of the meta shard, as its
/// leader would: what a node answers a call from another.
pub async fn answer(meta: &impl Replicated, call: &Call) -> Answer {
    let answered = match call {
        Call::Allocate => allocate(meta).await.map(Answer::Allocated),
        Call::Report(placement) => report(meta, placement).await.map(Answer::Reported),
        Call::Registry => registry(meta).await.map(Answer::Registry),
        Call::Create(creation) => create(
            meta,
            &creation.name,
            &creation.replicas,
            creation.membership,
        )
        .await
        .map(Answer::Created),
        Call::Charter(chartered) => charter(
            meta,
            &chartered.name,
            chartered.namespace,
            chartered.shard,
            &chartered.members,
        )
        .await
        .map(Answer::Created),
    };
    answered.unwrap_or_else(|error| {
        if let MetaError::Consensus(oleaph_consensus::Error::NotLeader { leader }) = &error {
            return Answer::NotLeader(*leader);
        }
        Answer::Failed(error.to_string())
    })
}

/// Sends a [`Call`] to another node and reads its [`Answer`]: the
/// transport's side of [`Meta`].
pub trait Ask: Sync {
    /// Sends `call` to `node`, reached as `peer`, which answers it with
    /// [`answer`].
    fn ask(
        &self,
        node: NodeId,
        peer: &Peer,
        call: &Call,
    ) -> impl Future<Output = io::Result<Answer>> + Send;
}

/// The meta shard as a node sees it: its own replica of the meta shard,
/// and an [`Ask`] to reach the others.
#[derive(Debug)]
pub struct Meta<'node, R, A> {
    replica: &'node R,
    ask: &'node A,
}

impl<'node, R: Replicated, A: Ask> Meta<'node, R, A> {
    /// The meta shard through `replica`, this node's replica of it,
    /// asking other nodes through `ask`.
    #[must_use]
    pub const fn new(replica: &'node R, ask: &'node A) -> Self {
        Self { replica, ask }
    }

    /// [`allocate`] on the meta shard's leader.
    ///
    /// # Errors
    ///
    /// As [`allocate`], and as [`Meta::call`].
    pub async fn allocate(&self) -> Result<ShardId> {
        let answer = self.call(Call::Allocate).await?;
        let Answer::Allocated(id) = answer else {
            return Err(unanswered(answer));
        };
        Ok(id)
    }

    /// [`report`] on the meta shard's leader.
    ///
    /// # Errors
    ///
    /// As [`report`], and as [`Meta::call`].
    pub async fn report(&self, placement: &Placement) -> Result<Reported> {
        let answer = self.call(Call::Report(placement.clone())).await?;
        let Answer::Reported(reported) = answer else {
            return Err(unanswered(answer));
        };
        Ok(reported)
    }

    /// [`registry`] on the meta shard's leader.
    ///
    /// # Errors
    ///
    /// As [`registry`], and as [`Meta::call`].
    pub async fn registry(&self) -> Result<Registry> {
        let answer = self.call(Call::Registry).await?;
        let Answer::Registry(registry) = answer else {
            return Err(unanswered(answer));
        };
        Ok(registry)
    }

    /// [`create`] on the meta shard's leader.
    ///
    /// # Errors
    ///
    /// As [`create`], and as [`Meta::call`].
    pub async fn create(&self, creation: Creation) -> Result<Created> {
        self.created(Call::Create(creation)).await
    }

    /// [`charter`] on the meta shard's leader.
    ///
    /// # Errors
    ///
    /// As [`charter`], and as [`Meta::call`].
    pub async fn charter(&self, chartered: Chartered) -> Result<Created> {
        self.created(Call::Charter(chartered)).await
    }

    /// Runs `call`, which makes a namespace, and returns what it made.
    async fn created(&self, call: Call) -> Result<Created> {
        let answer = self.call(call).await?;
        let Answer::Created(created) = answer else {
            return Err(unanswered(answer));
        };
        Ok(created)
    }

    /// Runs `call` where this node's replica routes writes, following the
    /// leader each node it asks names, and returns the last answer.
    ///
    /// # Errors
    ///
    /// [`MetaError::Unreachable`] when a node cannot be asked, or is not in
    /// the meta shard's membership.
    pub async fn call(&self, call: Call) -> Result<Answer> {
        follow(self.replica.route().node(self.replica.node()), |node| {
            self.call_on(node, &call)
        })
        .await
    }

    /// Runs `call` on `node`: on this node's replica, or by asking.
    async fn call_on(&self, node: NodeId, call: &Call) -> Result<Answer> {
        if node == self.replica.node() {
            return Ok(answer(self.replica, call).await);
        }
        let unreachable = |source| MetaError::Unreachable { node, source };
        let members = self.replica.members();
        let peer = members
            .peer(node)
            .ok_or_else(|| unreachable(io::Error::other("not in the meta shard's membership")))?;
        self.ask.ask(node, peer, call).await.map_err(unreachable)
    }
}

/// Asks `leader`, then each leader a node that does not lead names, for at
/// most [`HOPS`] asks, and returns the last answer: `NotLeader` when no
/// node asked leads, or when a node names itself or no one.
async fn follow<F, Fut>(mut leader: Option<NodeId>, mut ask: F) -> Result<Answer>
where
    F: FnMut(NodeId) -> Fut,
    Fut: Future<Output = Result<Answer>>,
{
    let mut answer = Answer::NotLeader(leader);
    for _ in 0..HOPS {
        let Some(target) = leader else {
            break;
        };
        answer = ask(target).await?;
        let Answer::NotLeader(next) = answer else {
            break;
        };
        if next == Some(target) {
            break;
        }
        leader = next;
    }
    Ok(answer)
}

/// The error for an answer that is not the one the call expects.
fn unanswered(answer: Answer) -> MetaError {
    match answer {
        Answer::NotLeader(_) => MetaError::NoLeader,
        Answer::Failed(reason) => MetaError::Remote(reason),
        Answer::Allocated(_) | Answer::Reported(_) | Answer::Registry(_) | Answer::Created(_) => {
            MetaError::Mismatch
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use futures_util::FutureExt as _;
    use oleaph_codec::{DecodeError, Reader, put_bytes};
    use oleaph_consensus::NodeId;
    use oleaph_shard::ShardId;
    use proptest::prelude::*;

    use oleaph_shard::NamespaceId;

    use super::{Answer, Call, Chartered, Creation, HOPS, follow};
    use crate::client::Created;
    use crate::namespace::tests::{names, namespaces};
    use crate::placement::Reported;
    use crate::placement::tests::{placements, shards};
    use crate::registry::Registry;

    /// A registry of `placements`, one per shard but the meta shard, and
    /// `next`.
    fn registry(next: u64, placements: Vec<crate::Placement>) -> Registry {
        let by_id: BTreeMap<_, _> = placements
            .into_iter()
            .filter(|placement| placement.id() != ShardId::META)
            .map(|placement| (placement.id(), placement))
            .collect();
        let mut out = next.to_le_bytes().to_vec();
        out.extend_from_slice(&u32::try_from(by_id.len()).unwrap().to_le_bytes());
        for placement in by_id.values() {
            let mut encoded = Vec::new();
            placement.encode(&mut encoded).unwrap();
            put_bytes(&mut out, &encoded).unwrap();
        }
        out.extend_from_slice(&0_u32.to_le_bytes());
        Registry::decode(&mut Reader::new(&out)).unwrap()
    }

    fn creations() -> impl Strategy<Value = Creation> {
        (
            names(),
            proptest::collection::btree_set(any::<u64>(), 0..4),
            any::<u64>(),
        )
            .prop_map(|(name, replicas, membership)| {
                let replicas = replicas.into_iter().map(NodeId::new).collect();
                Creation::new(name, replicas, membership)
            })
    }

    fn charters() -> impl Strategy<Value = Chartered> {
        (
            names(),
            any::<u32>(),
            any::<u64>(),
            proptest::collection::btree_set(any::<u64>(), 0..4),
        )
            .prop_map(|(name, namespace, shard, members)| {
                let members = members.into_iter().map(NodeId::new).collect();
                Chartered::new(
                    name,
                    NamespaceId::new(namespace),
                    ShardId::new(shard),
                    members,
                )
            })
    }

    fn calls() -> impl Strategy<Value = Call> {
        prop_oneof![
            Just(Call::Allocate),
            Just(Call::Registry),
            placements().prop_map(Call::Report),
            creations().prop_map(Call::Create),
            charters().prop_map(Call::Charter),
        ]
    }

    fn answers() -> impl Strategy<Value = Answer> {
        prop_oneof![
            any::<u64>().prop_map(|id| Answer::Allocated(ShardId::new(id))),
            Just(Answer::Reported(Reported::Recorded)),
            Just(Answer::Reported(Reported::Stale)),
            (any::<u64>(), proptest::collection::vec(placements(), 0..3))
                .prop_map(|(next, placements)| Answer::Registry(registry(next, placements))),
            proptest::option::of(any::<u64>())
                .prop_map(|leader| Answer::NotLeader(leader.map(NodeId::new))),
            ".{0,8}".prop_map(Answer::Failed),
            shards().prop_map(|shard| Answer::Created(Created::Founded(shard))),
            namespaces().prop_map(|namespace| Answer::Created(Created::Taken(namespace))),
            Just(Answer::Created(Created::Spent)),
        ]
    }

    /// Runs [`follow`] from `leader` against nodes that answer as
    /// `answers` says, and returns the answer and the nodes asked in turn.
    fn followed(leader: Option<u64>, answers: impl Fn(NodeId) -> Answer) -> (Answer, Vec<u64>) {
        let mut asked = Vec::new();
        let answer = follow(leader.map(NodeId::new), |node| {
            asked.push(node.get());
            std::future::ready(Ok(answers(node)))
        })
        .now_or_never()
        .unwrap()
        .unwrap();
        (answer, asked)
    }

    #[test]
    fn calls_follow_the_leader_each_node_names() {
        let (answer, asked) = followed(Some(1), |node| match node.get() {
            3 => Answer::Allocated(ShardId::new(7)),
            other => Answer::NotLeader(Some(NodeId::new(other.saturating_add(1)))),
        });
        assert_eq!(
            answer,
            Answer::Allocated(ShardId::new(7)),
            "the leader answers"
        );
        assert_eq!(asked, [1, 2, 3], "each hint followed");
    }

    #[test]
    fn calls_stop_where_no_leader_is_known() {
        let none = Answer::NotLeader(None);
        assert_eq!(
            followed(None, |_| Answer::Allocated(ShardId::new(7))),
            (none.clone(), vec![]),
            "no one to ask"
        );
        assert_eq!(
            followed(Some(1), |_| Answer::NotLeader(None)),
            (none, vec![1]),
            "no one named"
        );
        assert_eq!(
            followed(Some(1), |node| Answer::NotLeader(Some(node))),
            (Answer::NotLeader(Some(NodeId::new(1))), vec![1]),
            "names itself, and is not asked again"
        );
        let (_, asked) = followed(Some(1), |node| {
            Answer::NotLeader(Some(NodeId::new(node.get().saturating_add(1))))
        });
        assert_eq!(asked.len(), HOPS, "at most {HOPS} asks");
    }

    #[test]
    fn unknown_tags_and_a_registry_out_of_order_are_rejected() {
        assert_eq!(Call::decode(&[9]), Err(DecodeError::Tag(9)), "call");
        assert_eq!(Answer::decode(&[9]), Err(DecodeError::Tag(9)), "answer");
        assert_eq!(
            Answer::decode(&[2, 9]),
            Err(DecodeError::Tag(9)),
            "reported"
        );
        assert_eq!(Answer::decode(&[4, 9]), Err(DecodeError::Tag(9)), "leader");
        assert_eq!(Answer::decode(&[6, 9]), Err(DecodeError::Tag(9)), "created");
        let mut twice = vec![4, 1, 0, 0, 0, b'a'];
        twice.extend_from_slice(&0_u64.to_le_bytes());
        twice.extend_from_slice(&2_u32.to_le_bytes());
        twice.extend_from_slice(&[7_u64.to_le_bytes(), 7_u64.to_le_bytes()].concat());
        assert_eq!(
            Call::decode(&twice),
            Err(DecodeError::Invalid),
            "a replica twice"
        );
        assert_eq!(
            Answer::decode(&[5, 1, 0, 0, 0, 0xFF]),
            Err(DecodeError::Invalid),
            "not utf-8"
        );
        assert_eq!(Call::decode(&[1, 0]), Err(DecodeError::Trailing), "call");
        let Answer::Registry(registry) = Answer::decode(&{
            let mut out = vec![3];
            registry(5, Vec::new()).encode(&mut out).unwrap();
            out
        })
        .unwrap() else {
            panic!("a registry");
        };
        assert_eq!(registry.next(), ShardId::new(5), "next");
        let mut placement = Vec::new();
        crate::Placement::new(oleaph_shard::Shard::first(), BTreeSet::new(), 0)
            .encode(&mut placement)
            .unwrap();
        let mut out = 5_u64.to_le_bytes().to_vec();
        out.extend_from_slice(&2_u32.to_le_bytes());
        for _ in 0..2 {
            put_bytes(&mut out, &placement).unwrap();
        }
        out.extend_from_slice(&0_u32.to_le_bytes());
        assert_eq!(
            Registry::decode(&mut Reader::new(&out)),
            Err(DecodeError::Invalid),
            "the same shard twice"
        );
    }

    proptest! {
        #[test]
        fn calls_roundtrip(call in calls()) {
            prop_assert_eq!(Call::decode(&call.encode().unwrap()), Ok(call));
        }

        #[test]
        fn answers_roundtrip(answer in answers()) {
            prop_assert_eq!(Answer::decode(&answer.encode().unwrap()), Ok(answer));
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..48)) {
            drop(Call::decode(&bytes));
            drop(Answer::decode(&bytes));
        }
    }
}
