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

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::num::NonZeroU64;

use kurator_codec::{DecodeError, Reader};
use kurator_consensus::NodeId;
use kurator_crypto::{Signature, SigningKey};

use crate::committee::{Committee, Domain};
use crate::rejection::Rejection;
use crate::vote::{BlockId, check};
use crate::wire::{put_count, put_node, put_signature, read_count, read_node, read_signature};

/// What a checkpoint share's signature is over, after the domain.
const SHARE: &[u8] = b"kurator-bft checkpoint";

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
/// [`Manifest`](crate::Manifest) computes it.
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

/// The state of a shard once it decided `block`, its block at `height`:
/// the state named by `root`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Checkpoint {
    height: Height,
    block: BlockId,
    root: StateRoot,
}

impl Checkpoint {
    /// The state named by `root`, after `block`, decided at `height`.
    #[must_use]
    pub const fn new(height: Height, block: BlockId, root: StateRoot) -> Self {
        Self {
            height,
            block,
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
            &self.root.0,
        ]
        .concat()
    }

    fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        out.write_all(&self.height.0.to_le_bytes())?;
        self.block.encode(out)?;
        out.write_all(&self.root.0)
    }

    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let height = Height(reader.read()?);
        let block = BlockId::decode(reader)?;
        let root = StateRoot(reader.read()?);
        Ok(Self::new(height, block, root))
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

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the journal keeps certificates from P4a-2")
    )]
    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        self.checkpoint.encode(out)?;
        put_count(out, self.signatures.len())?;
        self.signatures.iter().try_for_each(|(signer, signature)| {
            put_node(out, *signer)?;
            put_signature(out, signature)
        })
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the journal keeps certificates from P4a-2")
    )]
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

    /// Forgets the checkpoints up to `certificate`'s height, and returns
    /// the one at its height if the committee certified another there.
    pub(crate) fn judge(&mut self, certificate: &CheckpointCertificate) -> Option<Checkpoint> {
        let at = certificate.height();
        let mine = self.0.get(&at).copied();
        self.0.retain(|height, _| *height > at);
        mine.filter(|mine| !certificate.certifies(mine))
    }
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
        let mut later = self.shares.split_off(&height);
        let _done = later.remove(&height);
        self.shares = later;
        let certified = CheckpointCertificate::new(checkpoint, signatures);
        self.certified = Some(certified.clone());
        Some(certified)
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use kurator_codec::Reader;
    use kurator_consensus::NodeId;

    use super::{
        Checkpoint, CheckpointCertificate, Height, Interval, Own, Share, StateRoot, Tally,
    };
    use crate::committee::Domain;
    use crate::fixture::{DOMAIN, committee, key};
    use crate::rejection::Rejection;
    use crate::step::Effects;
    use crate::vote::BlockId;

    fn checkpoint(height: u64, root: u8) -> Checkpoint {
        Checkpoint::new(
            Height::new(height),
            BlockId::new([7; 32]),
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
        assert_eq!(Height::new(3).to_string(), "height 3", "shown");
        assert_eq!(Interval::DEFAULT, every(1000), "a thousand");
    }

    /// A certificate for `checkpoint`, by nodes 1 to 3.
    fn certificate(checkpoint: Checkpoint) -> CheckpointCertificate {
        let mut tally = Tally::default();
        (1..=3)
            .find_map(|node| tally.count(share(node, checkpoint), 3))
            .unwrap()
    }

    #[test]
    fn a_certificate_judges_the_replicas_own_checkpoints() {
        let mut own = Own::default();
        for height in [10, 20, 30] {
            own.take(checkpoint(height, 1));
        }
        assert_eq!(own.judge(&certificate(checkpoint(20, 1))), None, "agrees");
        assert_eq!(
            own.judge(&certificate(checkpoint(10, 2))),
            None,
            "forgotten"
        );
        assert_eq!(
            own.judge(&certificate(checkpoint(30, 2))),
            Some(checkpoint(30, 1)),
            "differs"
        );
        assert_eq!(own.judge(&certificate(checkpoint(30, 2))), None, "once");
        assert_eq!(
            own.judge(&certificate(checkpoint(40, 2))),
            None,
            "not reached"
        );
    }

    #[test]
    fn a_step_hands_on_its_newest_certificate() {
        let older = certificate(checkpoint(10, 1));
        let newer = certificate(checkpoint(20, 1));
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
}
