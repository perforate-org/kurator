//! Checkpoints: the state a shard's decided blocks made, every so many
//! blocks, and the committee's word for it.
//!
//! Every honest replica decides the same blocks and applies them alike,
//! so at every [`Interval`] of decided blocks each holds the same state.
//! Each signs a [`Checkpoint`] naming that state by its [`StateRoot`], and
//! a quorum of those [`Share`]s is a [`CheckpointCertificate`]: the
//! committee's word that the shard's state at that height has that root.
//! A replica whose own root differs from a certified one has diverged:
//! its state is not the shard's, and it must stop.
//!
//! A vote certifies a block before anyone applies it, so it cannot speak
//! for the state after it. Checkpoints speak for the state, apart from the
//! votes, every so often.
//!
//! A root names the state as [`ShardState::encode`] writes it, which every
//! replica holding the same state writes byte for byte alike, cut into
//! chunks of [`CHUNK`] bytes. Each chunk is named by its hash, and the
//! state by the root over its length and its chunks' hashes, its
//! [`Manifest`], so that a replica catching up can take each chunk from
//! anyone and check it.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::num::NonZeroU64;

use oleaph_codec::{DecodeError, Reader};
use oleaph_consensus::NodeId;
use oleaph_crypto::{Signature, SigningKey};
use oleaph_shard::ShardState;

use crate::committee::{Committee, Domain};
use crate::rejection::Rejection;
use crate::view::View;
use crate::vote::{BlockId, check};
use crate::wire::{
    put_count, put_node, put_signature, put_view, read_count, read_node, read_signature, read_view,
};

/// What a checkpoint share's signature is over, after the domain.
const SHARE: &[u8] = b"oleaph-bft checkpoint";

/// How many heights past the last certified one a replica keeps each
/// member's shares for, the lowest first: a faulty member cannot fill its
/// memory with shares for heights the shard will not reach for long, and
/// an honest one is never that far ahead.
const AHEAD: usize = 4;

/// How many blocks a shard has decided.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Height(u64);

impl Height {
    /// No block decided yet.
    pub const ZERO: Self = Self(0);

    /// The height `height`.
    #[must_use]
    pub const fn new(height: u64) -> Self {
        Self(height)
    }

    /// The number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The height one more block makes, unless there is none.
    #[must_use]
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for Height {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "height {}", self.0)
    }
}

/// How many decided blocks apart checkpoints are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval(NonZeroU64);

impl Interval {
    /// A thousand blocks: often enough that a replica that falls behind
    /// has little to replay, seldom enough that hashing the state costs
    /// little next to deciding the blocks.
    pub const DEFAULT: Self = Self(NonZeroU64::new(1000).unwrap());

    /// Every `blocks` blocks.
    #[must_use]
    pub const fn new(blocks: NonZeroU64) -> Self {
        Self(blocks)
    }

    /// Whether a checkpoint falls at `height`.
    #[must_use]
    pub const fn at(self, height: Height) -> bool {
        height.0 != 0 && height.0.is_multiple_of(self.0.get())
    }
}

/// Names a shard's state: a hash of all of it, as
/// [`Manifest`] computes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateRoot([u8; 32]);

impl StateRoot {
    /// Wraps the 32 bytes of a hash.
    #[must_use]
    pub const fn new(hash: [u8; 32]) -> Self {
        Self(hash)
    }

    /// The 32 bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The state of a shard once it decided `block`, of `view`, its block at
/// `height`: the state named by `root`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Checkpoint {
    height: Height,
    block: BlockId,
    view: View,
    root: StateRoot,
}

impl Checkpoint {
    /// The state named by `root`, after `block` of `view`, decided at
    /// `height`.
    #[must_use]
    pub const fn new(height: Height, block: BlockId, view: View, root: StateRoot) -> Self {
        Self {
            height,
            block,
            view,
            root,
        }
    }

    /// How many blocks were decided.
    #[must_use]
    pub const fn height(&self) -> Height {
        self.height
    }

    /// The last of them.
    #[must_use]
    pub const fn block(&self) -> BlockId {
        self.block
    }

    /// The view of the last of them.
    #[must_use]
    pub const fn view(&self) -> View {
        self.view
    }

    /// The state they made.
    #[must_use]
    pub const fn root(&self) -> StateRoot {
        self.root
    }

    /// The bytes a share of it signs.
    fn statement(&self, domain: Domain) -> Vec<u8> {
        [
            domain.as_bytes().as_slice(),
            SHARE,
            &self.height.0.to_le_bytes(),
            self.block.as_bytes(),
            &self.view.get().to_le_bytes(),
            &self.root.0,
        ]
        .concat()
    }

    fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        out.write_all(&self.height.0.to_le_bytes())?;
        self.block.encode(out)?;
        put_view(out, self.view)?;
        out.write_all(&self.root.0)
    }

    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let height = Height(reader.read()?);
        let block = BlockId::decode(reader)?;
        let view = read_view(reader)?;
        let root = StateRoot(reader.read()?);
        Ok(Self::new(height, block, view, root))
    }
}

/// One member's word for a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Share {
    checkpoint: Checkpoint,
    signer: NodeId,
    signature: Signature,
}

impl Share {
    /// `signer`'s share of `checkpoint`, signed with `key` in `domain`.
    #[must_use]
    pub fn sign(key: &SigningKey, domain: Domain, signer: NodeId, checkpoint: Checkpoint) -> Self {
        Self {
            checkpoint,
            signer,
            signature: key.sign(&checkpoint.statement(domain)),
        }
    }

    /// The checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Who signed it.
    #[must_use]
    pub const fn signer(&self) -> NodeId {
        self.signer
    }

    /// Checks that a member of `committee` signed it in `domain`.
    ///
    /// # Errors
    ///
    /// When the signer is not a member or the signature is not theirs.
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        let statement = self.checkpoint.statement(domain);
        check(committee, self.signer, &statement, &self.signature)
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        self.checkpoint.encode(out)?;
        put_node(out, self.signer)?;
        put_signature(out, &self.signature)
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            checkpoint: Checkpoint::decode(reader)?,
            signer: read_node(reader)?,
            signature: read_signature(reader)?,
        })
    }
}

/// A quorum's word for a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCertificate {
    checkpoint: Checkpoint,
    signatures: BTreeMap<NodeId, Signature>,
}

impl CheckpointCertificate {
    /// The checkpoint `signatures` sign.
    #[must_use]
    pub const fn new(checkpoint: Checkpoint, signatures: BTreeMap<NodeId, Signature>) -> Self {
        Self {
            checkpoint,
            signatures,
        }
    }

    /// The checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// The height of the checkpoint.
    #[must_use]
    pub const fn height(&self) -> Height {
        self.checkpoint.height
    }

    /// The block after which it certifies the state.
    #[must_use]
    pub const fn block(&self) -> BlockId {
        self.checkpoint.block
    }

    /// The view of that block.
    #[must_use]
    pub const fn view(&self) -> View {
        self.checkpoint.view
    }

    /// The root of the state it certifies.
    #[must_use]
    pub const fn root(&self) -> StateRoot {
        self.checkpoint.root
    }

    /// Whether it certifies `checkpoint`.
    #[must_use]
    pub fn certifies(&self, checkpoint: &Checkpoint) -> bool {
        self.checkpoint == *checkpoint
    }

    /// Whether it certifies a later checkpoint than `older` does.
    #[must_use]
    pub fn supersedes(&self, older: &Self) -> bool {
        self.height() > older.height()
    }

    /// Checks that a quorum of `committee` signed it in `domain`.
    ///
    /// # Errors
    ///
    /// When it has fewer signatures than a quorum, or one is not its
    /// signer's.
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        let need = committee.quorum();
        let have = self.signatures.len();
        if have < need {
            return Err(Rejection::Short { have, need });
        }
        let statement = self.checkpoint.statement(domain);
        self.signatures
            .iter()
            .try_for_each(|(signer, signature)| check(committee, *signer, &statement, signature))
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        self.checkpoint.encode(out)?;
        put_count(out, self.signatures.len())?;
        self.signatures.iter().try_for_each(|(signer, signature)| {
            put_node(out, *signer)?;
            put_signature(out, signature)
        })
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let checkpoint = Checkpoint::decode(reader)?;
        let count = read_count(reader)?;
        let mut signatures = BTreeMap::new();
        for _ in 0..count {
            let signer = read_node(reader)?;
            if signatures.insert(signer, read_signature(reader)?).is_some() {
                return Err(DecodeError::Invalid);
            }
        }
        Ok(Self::new(checkpoint, signatures))
    }
}

/// A replica's own checkpoints, until the committee certifies one at
/// their height.
#[derive(Debug, Clone, Default)]
pub(crate) struct Own(BTreeMap<Height, Checkpoint>);

impl Own {
    /// Keeps `checkpoint`, which the replica's state reached.
    pub(crate) fn take(&mut self, checkpoint: Checkpoint) {
        let _replaced = self.0.insert(checkpoint.height, checkpoint);
    }

    /// Forgets the checkpoints up to `certificate`'s height, and says how
    /// the one at its height compares.
    pub(crate) fn judge(&mut self, certificate: &CheckpointCertificate) -> Verdict {
        let at = certificate.height();
        let mine = self.0.get(&at).copied();
        self.0.retain(|height, _| *height > at);
        match mine {
            None => Verdict::Unknown,
            Some(mine) if certificate.certifies(&mine) => Verdict::Agrees,
            Some(mine) => Verdict::Differs(mine),
        }
    }
}

/// How a replica's own checkpoint compares with a certified one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// It has none at that height, or judged it already.
    Unknown,
    /// Its state is the one certified.
    Agrees,
    /// Its state is another: this one.
    Differs(Checkpoint),
}

/// The shares a replica collected, until a quorum certifies a checkpoint.
#[derive(Debug, Clone, Default)]
pub(crate) struct Tally {
    /// The newest certified checkpoint.
    certified: Option<CheckpointCertificate>,
    /// The first share of each member at each height past it.
    shares: BTreeMap<Height, BTreeMap<NodeId, Share>>,
}

impl Tally {
    /// A tally that counts only shares past `certified`.
    pub(crate) const fn since(certified: Option<CheckpointCertificate>) -> Self {
        Self {
            certified,
            shares: BTreeMap::new(),
        }
    }

    /// Takes in `certificate`, already verified, when it is newer than any
    /// it knows, and says whether it was.
    pub(crate) fn adopt(&mut self, certificate: &CheckpointCertificate) -> bool {
        if certificate.height() <= self.last() {
            return false;
        }
        self.shares = self.shares.split_off(&certificate.height());
        let _done = self.shares.remove(&certificate.height());
        self.certified = Some(certificate.clone());
        true
    }

    /// The height of the newest certified checkpoint.
    fn last(&self) -> Height {
        self.certified
            .as_ref()
            .map_or(Height::ZERO, |certified| certified.checkpoint.height)
    }

    /// Counts `share`, already verified, and returns the certificate it
    /// completes, if it does. A member's second share at a height counts
    /// for nothing, as do shares for heights already certified, or above
    /// the member's [`AHEAD`] lowest.
    pub(crate) fn count(&mut self, share: Share, quorum: usize) -> Option<CheckpointCertificate> {
        let checkpoint = share.checkpoint;
        let height = checkpoint.height;
        if height <= self.last() || !self.room(share.signer, height) {
            return None;
        }
        let at = self.shares.entry(height).or_default();
        let _first = at.entry(share.signer).or_insert(share);
        let signatures: BTreeMap<NodeId, Signature> = at
            .values()
            .filter(|other| other.checkpoint == checkpoint)
            .map(|other| (other.signer, other.signature.clone()))
            .collect();
        if signatures.len() < quorum {
            return None;
        }
        let certified = CheckpointCertificate::new(checkpoint, signatures);
        self.adopt(&certified).then_some(certified)
    }

    /// Whether `signer` may have a share at `height` counted: it has fewer
    /// than [`AHEAD`] heights waiting, or one higher, which then goes.
    fn room(&mut self, signer: NodeId, height: Height) -> bool {
        let mut waiting: Vec<Height> = self
            .shares
            .iter()
            .filter(|(_, shares)| shares.contains_key(&signer))
            .map(|(at, _)| *at)
            .collect();
        if waiting.contains(&height) {
            return true;
        }
        waiting.push(height);
        waiting.sort_unstable();
        match waiting.get(AHEAD) {
            None => true,
            Some(dropped) if *dropped == height => false,
            Some(dropped) => {
                if let Some(shares) = self.shares.get_mut(dropped) {
                    let _evicted = shares.remove(&signer);
                }
                true
            }
        }
    }
}

/// How large a chunk is, the last one aside.
pub(crate) const CHUNK: usize = 1 << 20;

/// The key derivation context of chunk hashes. Fixed forever.
const CHUNK_CONTEXT: &str = "oleaph 2026-11 bft state chunk";

/// The key derivation context of state roots. Fixed forever.
const ROOT_CONTEXT: &str = "oleaph 2026-11 bft state root";

/// A part of the state at a checkpoint, as a replica catching up asks for
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Part {
    /// Its manifest.
    Manifest,
    /// Its chunk of this index, from 0.
    Chunk(u32),
}

/// The hash of one chunk of an encoded state.
fn chunk_hash(chunk: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(CHUNK_CONTEXT)
        .update(chunk)
        .finalize()
        .as_bytes()
}

/// The hash of each chunk of a shard's encoded state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    length: u64,
    chunks: Vec<[u8; 32]>,
}

impl Manifest {
    /// The manifest of `state`.
    ///
    /// # Errors
    ///
    /// When a key or value is too long for its length prefix, which a
    /// state that took it in never holds.
    pub fn of(state: &ShardState) -> io::Result<Self> {
        let mut encoded = Vec::new();
        state.encode(&mut encoded)?;
        Ok(Self::over(&encoded))
    }

    /// The manifest of the encoded state `bytes`.
    pub(crate) fn over(bytes: &[u8]) -> Self {
        let chunks = bytes.chunks(CHUNK).map(chunk_hash).collect();
        Self {
            length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            chunks,
        }
    }

    /// The hash of each chunk, in order.
    #[must_use]
    pub fn chunks(&self) -> &[[u8; 32]] {
        &self.chunks
    }

    /// How many chunks the state takes.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.chunks.len()
    }

    /// Whether `bytes` are chunk `index` of the state.
    #[must_use]
    pub fn holds(&self, index: u32, bytes: &[u8]) -> bool {
        usize::try_from(index)
            .ok()
            .and_then(|index| self.chunks.get(index))
            .is_some_and(|hash| *hash == chunk_hash(bytes))
    }

    /// Where chunk `index` lies in the encoded state, if it is one.
    pub(crate) fn span(&self, index: u32) -> Option<std::ops::Range<u64>> {
        let chunk = u64::try_from(CHUNK).ok()?;
        let start = u64::from(index).checked_mul(chunk)?;
        (start < self.length).then(|| start..start.saturating_add(chunk).min(self.length))
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        out.write_all(&self.length.to_le_bytes())?;
        put_count(out, self.chunks.len())?;
        self.chunks.iter().try_for_each(|hash| out.write_all(hash))
    }

    /// Reads a manifest, which must have as many chunks as its length
    /// takes.
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let length: u64 = reader.read()?;
        let count = read_count(reader)?;
        let chunk = u64::try_from(CHUNK).map_err(|_| DecodeError::Invalid)?;
        if u64::from(count) != length.div_ceil(chunk) {
            return Err(DecodeError::Invalid);
        }
        let chunks = (0..count)
            .map(|_| reader.read::<[u8; 32]>())
            .collect::<Result<_, _>>()?;
        Ok(Self { length, chunks })
    }

    /// The root that names the state.
    #[must_use]
    pub fn root(&self) -> StateRoot {
        let mut hasher = blake3::Hasher::new_derive_key(ROOT_CONTEXT);
        let _length = hasher.update(&self.length.to_le_bytes());
        for chunk in &self.chunks {
            let _chunk = hasher.update(chunk);
        }
        StateRoot::new(*hasher.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use oleaph_codec::Reader;
    use oleaph_consensus::NodeId;

    use oleaph_shard::ShardState;
    use oleaph_storage::{Version, WriteBatch};

    use super::{
        CHUNK, Checkpoint, CheckpointCertificate, Height, Interval, Manifest, Own, Share,
        StateRoot, Tally, Verdict,
    };
    use crate::committee::Domain;
    use crate::fixture::{DOMAIN, certify, committee, key};
    use crate::rejection::Rejection;
    use crate::step::Effects;
    use crate::view::View;
    use crate::vote::BlockId;

    fn checkpoint(height: u64, root: u8) -> Checkpoint {
        Checkpoint::new(
            Height::new(height),
            BlockId::new([7; 32]),
            View::new(3),
            StateRoot::new([root; 32]),
        )
    }

    fn share(node: u64, checkpoint: Checkpoint) -> Share {
        Share::sign(&key(node), DOMAIN, NodeId::new(node), checkpoint)
    }

    fn every(blocks: u64) -> Interval {
        Interval::new(NonZeroU64::new(blocks).unwrap())
    }

    #[test]
    fn a_share_is_its_signers_in_its_domain() {
        let signed = share(1, checkpoint(10, 1));
        assert_eq!(signed.verify(&committee(), DOMAIN), Ok(()), "valid");
        assert_eq!(
            signed.verify(&committee(), Domain::new([2; 32])),
            Err(Rejection::Forged(NodeId::new(1))),
            "another domain"
        );
        let stranger = Share::sign(&key(9), DOMAIN, NodeId::new(9), checkpoint(10, 1));
        assert_eq!(
            stranger.verify(&committee(), DOMAIN),
            Err(Rejection::Stranger(NodeId::new(9))),
            "not a member"
        );
        let mut bytes = Vec::new();
        signed.encode(&mut bytes).unwrap();
        let mut reader = Reader::new(&bytes);
        assert_eq!(Share::decode(&mut reader).unwrap(), signed, "round trip");
    }

    #[test]
    fn a_quorum_of_the_same_checkpoint_certifies_it() {
        let mut tally = Tally::default();
        let ours = checkpoint(10, 1);
        assert!(tally.count(share(1, ours), 3).is_none(), "one");
        assert!(
            tally.count(share(2, checkpoint(10, 2)), 3).is_none(),
            "another root counts apart"
        );
        assert!(
            tally.count(share(1, checkpoint(10, 3)), 3).is_none(),
            "a second share"
        );
        assert!(tally.count(share(3, ours), 3).is_none(), "two");
        let certified = tally.count(share(4, ours), 3).unwrap();
        assert_eq!(certified.checkpoint(), &ours, "certified");
        assert_eq!(certified.verify(&committee(), DOMAIN), Ok(()), "a quorum's");
        assert_eq!(certified.signatures.len(), 3, "the three that agree");

        let mut bytes = Vec::new();
        certified.encode(&mut bytes).unwrap();
        let mut reader = Reader::new(&bytes);
        assert_eq!(
            CheckpointCertificate::decode(&mut reader).unwrap(),
            certified,
            "round trip"
        );
        assert!(
            tally.count(share(2, ours), 1).is_none(),
            "certified already"
        );
    }

    #[test]
    fn a_member_has_shares_waiting_at_few_heights() {
        let mut tally = Tally::default();
        for height in [50, 60, 70, 80] {
            assert!(
                tally.count(share(1, checkpoint(height, 1)), 2).is_none(),
                "{height}"
            );
        }
        // Full: a higher height is refused, a lower one takes the place of
        // the highest.
        assert!(
            tally.count(share(1, checkpoint(90, 1)), 2).is_none(),
            "full"
        );
        assert!(
            tally.count(share(2, checkpoint(90, 1)), 2).is_none(),
            "not counted"
        );
        assert!(
            tally.count(share(1, checkpoint(20, 1)), 2).is_none(),
            "lower"
        );
        assert!(
            tally.count(share(2, checkpoint(80, 1)), 2).is_none(),
            "evicted"
        );
        let certified = tally.count(share(2, checkpoint(20, 1)), 2);
        assert_eq!(
            certified.map(|certified| certified.checkpoint().height()),
            Some(Height::new(20)),
            "the lower one counts"
        );
        // Below it, nothing waits any more.
        assert!(
            tally.count(share(2, checkpoint(50, 1)), 2).is_some(),
            "50 still waits"
        );
    }

    #[test]
    fn a_short_certificate_is_refused() {
        let ours = checkpoint(10, 1);
        let signatures = [1, 2]
            .into_iter()
            .map(|node| {
                let Share { signature, .. } = share(node, ours);
                (NodeId::new(node), signature)
            })
            .collect();
        assert_eq!(
            CheckpointCertificate::new(ours, signatures).verify(&committee(), DOMAIN),
            Err(Rejection::Short { have: 2, need: 3 }),
            "short"
        );
    }

    #[test]
    fn heights_and_intervals_count_blocks() {
        let ten = every(10);
        assert!(!ten.at(Height::ZERO), "nothing at zero");
        assert!(ten.at(Height::new(10)), "at ten");
        assert!(!ten.at(Height::new(11)), "not at eleven");
        assert_eq!(Height::new(4).next(), Some(Height::new(5)), "next");
        assert_eq!(Height::new(u64::MAX).next(), None, "the last");
        assert_eq!(Height::new(42).get(), 42, "counts blocks");
        assert_eq!(Height::new(3).to_string(), "height 3", "shown");
        assert_eq!(Interval::DEFAULT, every(1000), "a thousand");
    }

    #[test]
    fn a_certificate_judges_the_replicas_own_checkpoints() {
        let mut own = Own::default();
        for height in [10, 20, 30] {
            own.take(checkpoint(height, 1));
        }
        let judge =
            |judged: &mut Own, height, root| judged.judge(&certify(checkpoint(height, root)));
        assert_eq!(judge(&mut own, 20, 1), Verdict::Agrees, "agrees");
        assert_eq!(judge(&mut own, 10, 2), Verdict::Unknown, "forgotten");
        assert_eq!(
            judge(&mut own, 30, 2),
            Verdict::Differs(checkpoint(30, 1)),
            "differs"
        );
        assert_eq!(judge(&mut own, 30, 2), Verdict::Unknown, "once");
        assert_eq!(judge(&mut own, 40, 2), Verdict::Unknown, "not reached");
    }

    #[test]
    fn a_step_hands_on_its_newest_certificate() {
        let older = certify(checkpoint(10, 1));
        let newer = certify(checkpoint(20, 1));
        assert!(newer.supersedes(&older), "newer");
        assert!(!older.supersedes(&newer), "older");
        assert!(!older.supersedes(&older), "the same");
        assert_eq!(older.height(), Height::new(10), "height");
        assert!(older.certifies(&checkpoint(10, 1)), "its own");
        assert!(!older.certifies(&checkpoint(10, 2)), "another");
        let mut first = Effects::default();
        first.certify(newer.clone());
        first.certify(older.clone());
        assert_eq!(first.certified(), Some(&newer), "keeps the newer");
        let mut last = Effects::default();
        last.certify(older);
        last.certify(newer.clone());
        assert_eq!(last.certified(), Some(&newer), "takes the newer");
    }

    #[test]
    fn the_root_names_the_state() {
        let empty = Manifest::of(&ShardState::default()).unwrap();
        assert_eq!(
            empty,
            Manifest::of(&ShardState::default()).unwrap(),
            "fixed"
        );
        assert_eq!(empty.chunks().len(), 1, "one short chunk");
        let mut state = ShardState::default();
        let _applied = state.apply(
            Version::new(1),
            oleaph_storage::Command::Kv(WriteBatch::new().put("a", "1")),
        );
        let written = Manifest::of(&state).unwrap();
        assert_ne!(written.root(), empty.root(), "another state");
    }

    #[test]
    fn large_states_take_many_chunks() {
        let bytes = vec![7_u8; CHUNK.saturating_mul(2).saturating_add(1)];
        let manifest = Manifest::over(&bytes);
        assert_eq!(manifest.chunks().len(), 3, "two whole and one byte");
        assert_eq!(manifest.chunks()[0], manifest.chunks()[1], "same bytes");
        assert_ne!(manifest.chunks()[1], manifest.chunks()[2], "one byte");
        let shorter = Manifest::over(&bytes[..CHUNK.saturating_mul(2)]);
        assert_ne!(shorter.root(), manifest.root(), "the length counts");
        let reordered = Manifest {
            length: manifest.length,
            chunks: manifest.chunks.iter().rev().copied().collect(),
        };
        assert_ne!(reordered.root(), manifest.root(), "the order counts");
    }
}
