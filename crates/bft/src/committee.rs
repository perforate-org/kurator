//! Who decides: the replicas of a shard, their voting keys, how many of
//! them must agree, and who leads each view; and how a committee hands
//! the shard on to the next.
//!
//! A shard's committees follow one another in epochs. The charter names
//! the first, in epoch 0. A [`ChangeCertificate`], a quorum of one epoch's
//! committee signing the next one's, ends the epoch at the block that
//! carries it.

use std::collections::BTreeMap;
use std::io::{self, Write};

use oleaph_codec::{DecodeError, Reader, put_bytes};
use oleaph_consensus::NodeId;
use oleaph_crypto::{Algorithm, Signature, SigningKey, VerifyingKey};
use oleaph_shard::ShardId;

use crate::rejection::Rejection;
use crate::view::{Epoch, View};
use crate::wire::{put_count, put_node, put_signature, read_count, read_node, read_signature};

/// What every signature in one shard's consensus is bound to, so that a
/// vote in one shard, or one test, never counts in another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Domain([u8; 32]);

impl Domain {
    /// The domain named by `id`: the same 32 bytes on every replica of
    /// the shard, different for every shard.
    #[must_use]
    pub const fn new(id: [u8; 32]) -> Self {
        Self(id)
    }

    /// The 32 bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The domain of shard `shard` decided by `committee`: a hash of the
    /// shard's id and every member's id and key, so that a signature
    /// counts neither in another shard nor under another committee.
    #[must_use]
    pub fn of(shard: ShardId, committee: &Committee) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key(DOMAIN_CONTEXT);
        let _shard = hasher.update(&shard.get().to_le_bytes());
        for (node, key) in &committee.members {
            let _member = hasher
                .update(&node.get().to_le_bytes())
                .update(&[key.algorithm().id()])
                .update(key.as_bytes());
        }
        Self(*hasher.finalize().as_bytes())
    }

    /// The domain of the epoch that `change` starts, after this one's: a
    /// hash of this domain and the change, so that the domains of a
    /// shard's epochs are all different, even when a committee returns.
    #[must_use]
    pub fn next(&self, change: &ChangeCertificate) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key(NEXT_DOMAIN_CONTEXT);
        let _domain = hasher.update(&self.0);
        let _change = hasher.update(&change.change.statement());
        Self(*hasher.finalize().as_bytes())
    }
}

/// The key derivation context of the domains after the first. Fixed
/// forever.
const NEXT_DOMAIN_CONTEXT: &str = "oleaph 2026-11 bft next domain";

/// The key derivation context of shard domains. Fixed forever.
const DOMAIN_CONTEXT: &str = "oleaph 2026-11 bft shard domain";

/// The replicas that decide a shard, each with the public key it signs
/// votes with.
///
/// Of `n` replicas, up to `f = (n - 1) / 3` may be faulty in any way, and
/// a quorum is `n - f`: any two quorums share at least `f + 1` replicas,
/// so at least one honest replica, which never votes for two blocks in a
/// view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committee {
    members: BTreeMap<NodeId, VerifyingKey>,
}

impl Committee {
    /// The committee of `members`, or `None` when there are none.
    #[must_use]
    pub fn new(members: BTreeMap<NodeId, VerifyingKey>) -> Option<Self> {
        (!members.is_empty()).then_some(Self { members })
    }

    /// How many replicas decide.
    #[must_use]
    pub fn size(&self) -> usize {
        self.members.len()
    }

    /// How many faulty replicas the committee tolerates.
    #[must_use]
    pub fn faults(&self) -> usize {
        faults(self.size())
    }

    /// How many replicas must sign a certificate.
    #[must_use]
    pub fn quorum(&self) -> usize {
        quorum(self.size())
    }

    /// The key `node` votes with, if it is a member.
    #[must_use]
    pub fn key(&self, node: NodeId) -> Option<&VerifyingKey> {
        self.members.get(&node)
    }

    /// Every member, in id order.
    pub fn members(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.members.keys().copied()
    }

    /// Checks that `signature` is `signer`'s, on `message`.
    ///
    /// # Errors
    ///
    /// When `signer` is not a member, or the signature is not its.
    pub(crate) fn check(
        &self,
        signer: NodeId,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), Rejection> {
        let key = self.key(signer).ok_or(Rejection::Stranger(signer))?;
        if key.verify(message, signature) {
            Ok(())
        } else {
            Err(Rejection::Forged(signer))
        }
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        put_count(out, self.members.len())?;
        self.members.iter().try_for_each(|(node, key)| {
            put_node(out, *node)?;
            out.write_all(&[key.algorithm().id()])?;
            put_bytes(out, key.as_bytes())
        })
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let count = read_count(reader)?;
        let mut members = BTreeMap::new();
        for _ in 0..count {
            let node = read_node(reader)?;
            if members
                .last_key_value()
                .is_some_and(|(last, _): (&NodeId, _)| *last >= node)
            {
                return Err(DecodeError::Invalid);
            }
            let id = reader.read::<u8>()?;
            let algorithm = Algorithm::from_id(id).ok_or(DecodeError::Tag(id))?;
            let key =
                VerifyingKey::from_parts(algorithm, reader.bytes()?).ok_or(DecodeError::Invalid)?;
            drop(members.insert(node, key));
        }
        Self::new(members).ok_or(DecodeError::Invalid)
    }

    /// The leader of `view`, drawn from the members by a hash of the
    /// view, so that a leader that stalls or censors holds up one view
    /// only, and no member always follows the same one.
    ///
    /// A fixed turn would not do: a block commits once the leaders of the
    /// next two views certify it and its child, so a member always
    /// followed by a silent one would never see its blocks commit.
    #[must_use]
    pub fn leader(&self, view: View) -> Option<NodeId> {
        let size = u64::try_from(self.size()).ok()?;
        let hash = blake3::derive_key(LEADER_CONTEXT, &view.get().to_le_bytes());
        let draw = u64::from_le_bytes(hash.first_chunk::<8>().copied()?);
        let turn = usize::try_from(draw.checked_rem(size)?).ok()?;
        self.members.keys().nth(turn).copied()
    }
}

/// The key derivation context of the leader schedule. Fixed forever.
const LEADER_CONTEXT: &str = "oleaph 2026-11 bft leader schedule";

/// What a signature on a change is over, after the domain.
const CHANGE: &[u8] = b"oleaph-bft change";

/// The committee that decides a shard from `epoch` on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    epoch: Epoch,
    committee: Committee,
}

impl Change {
    /// `committee` decides from `epoch` on.
    #[must_use]
    pub const fn new(epoch: Epoch, committee: Committee) -> Self {
        Self { epoch, committee }
    }

    /// The epoch it starts.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The committee of that epoch.
    #[must_use]
    pub const fn committee(&self) -> &Committee {
        &self.committee
    }

    /// The bytes that name it, after the domain and [`CHANGE`].
    fn statement(&self) -> Vec<u8> {
        let mut bytes = self.epoch.get().to_le_bytes().to_vec();
        // Writing to a vector does not fail, and a committee holds fewer
        // members than a `u32` counts.
        let _written = self.committee.encode(&mut bytes);
        bytes
    }

    /// What a member of the committee before signs, in `domain`.
    fn signed(&self, domain: Domain) -> Vec<u8> {
        [domain.as_bytes().as_slice(), CHANGE, &self.statement()].concat()
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        out.write_all(&self.epoch.get().to_le_bytes())?;
        self.committee.encode(out)
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            epoch: Epoch::new(reader.read()?),
            committee: Committee::decode(reader)?,
        })
    }
}

/// One member's consent to a [`Change`]: its signature, in the domain of
/// the epoch the change ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endorsement {
    signer: NodeId,
    signature: Signature,
}

impl Endorsement {
    /// `signer`, which signs with `key`, consents to `change` in `domain`.
    #[must_use]
    pub fn sign(key: &SigningKey, domain: Domain, signer: NodeId, change: &Change) -> Self {
        Self {
            signer,
            signature: key.sign(&change.signed(domain)),
        }
    }

    /// Who signed it.
    #[must_use]
    pub const fn signer(&self) -> NodeId {
        self.signer
    }
}

/// A [`Change`] with the consent of a quorum of the committee it follows:
/// what it takes to end an epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeCertificate {
    change: Change,
    signatures: BTreeMap<NodeId, Signature>,
}

impl ChangeCertificate {
    /// `change`, with `endorsements`.
    #[must_use]
    pub fn new(change: Change, endorsements: impl IntoIterator<Item = Endorsement>) -> Self {
        let signatures = endorsements
            .into_iter()
            .map(|endorsement| (endorsement.signer, endorsement.signature))
            .collect();
        Self { change, signatures }
    }

    /// The change.
    #[must_use]
    pub const fn change(&self) -> &Change {
        &self.change
    }

    /// The epoch it starts.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.change.epoch
    }

    /// The committee of that epoch.
    #[must_use]
    pub const fn committee(&self) -> &Committee {
        &self.change.committee
    }

    /// Checks that a quorum of `committee`, which decides `epoch` in
    /// `domain`, consents to the change, and that it starts the epoch
    /// after.
    ///
    /// # Errors
    ///
    /// When it starts another epoch, a signer is not a member, a
    /// signature is not its signer's, or the signers are fewer than a
    /// quorum.
    pub fn verify(
        &self,
        committee: &Committee,
        domain: Domain,
        epoch: Epoch,
    ) -> Result<(), Rejection> {
        if epoch.next() != Some(self.change.epoch) {
            return Err(Rejection::OtherEpoch(self.change.epoch, epoch));
        }
        let need = committee.quorum();
        let have = self.signatures.len();
        if have < need {
            return Err(Rejection::Short { have, need });
        }
        let signed = self.change.signed(domain);
        self.signatures
            .iter()
            .try_for_each(|(signer, signature)| committee.check(*signer, &signed, signature))
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        self.change.encode(out)?;
        put_count(out, self.signatures.len())?;
        self.signatures.iter().try_for_each(|(signer, signature)| {
            put_node(out, *signer)?;
            put_signature(out, signature)
        })
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let change = Change::decode(reader)?;
        let count = read_count(reader)?;
        let mut signatures = BTreeMap::new();
        for _ in 0..count {
            let signer = read_node(reader)?;
            if signatures
                .last_key_value()
                .is_some_and(|(last, _): (&NodeId, _)| *last >= signer)
            {
                return Err(DecodeError::Invalid);
            }
            drop(signatures.insert(signer, read_signature(reader)?));
        }
        Ok(Self { change, signatures })
    }
}

/// How many of `n` replicas may be faulty: the most `f` with `3f + 1 <= n`.
const fn faults(n: usize) -> usize {
    match n.saturating_sub(1).checked_div(3) {
        Some(f) => f,
        None => 0,
    }
}

/// How many of `n` replicas make a quorum: all but the faulty ones.
const fn quorum(n: usize) -> usize {
    n.saturating_sub(faults(n))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use oleaph_consensus::NodeId;
    use oleaph_crypto::{Algorithm, SigningKey};

    use oleaph_shard::ShardId;

    use oleaph_codec::{DecodeError, Reader, put_bytes};

    use super::{ChangeCertificate, Committee, Domain, Endorsement, faults, quorum};
    use crate::fixture::{self, DOMAIN, change_to, key};
    use crate::rejection::Rejection;
    use crate::view::{Epoch, View};
    use crate::wire::{put_count, put_node};

    fn committee(size: u64) -> Committee {
        let members = (1..=size)
            .map(|id| {
                let seed = [u8::try_from(id).unwrap(); 32];
                let key = SigningKey::from_seed(Algorithm::Ed25519, seed).verifying_key();
                (NodeId::new(id.saturating_mul(10)), key)
            })
            .collect::<BTreeMap<_, _>>();
        Committee::new(members).unwrap()
    }

    #[test]
    fn sizes_tolerate_a_third() {
        let cases = [
            (1, 0, 1),
            (3, 0, 3),
            (4, 1, 3),
            (6, 1, 5),
            (7, 2, 5),
            (10, 3, 7),
        ];
        for (n, f, q) in cases {
            assert_eq!(faults(n), f, "faults of {n}");
            assert_eq!(quorum(n), q, "quorum of {n}");
        }
        assert_eq!(faults(0), 0, "none");
        let four = committee(4);
        assert_eq!(four.size(), 4, "size");
        assert_eq!(four.faults(), 1, "faults");
        assert_eq!(four.quorum(), 3, "quorum");
        assert_eq!(committee(7).faults(), 2, "faults of seven");
    }

    #[test]
    fn every_member_leads_and_no_one_always_follows_another() {
        let four = committee(4);
        let leaders: Vec<u64> = (0..400)
            .map(|view| four.leader(View::new(view)).unwrap().get())
            .collect();
        assert_eq!(
            &leaders[..8],
            [40, 20, 40, 20, 40, 20, 20, 10],
            "fixed schedule"
        );
        for member in [10, 20, 30, 40] {
            let led = leaders.iter().filter(|leader| **leader == member).count();
            assert!((70..130).contains(&led), "{member} leads {led} of 400");
            let next: std::collections::BTreeSet<u64> = leaders
                .windows(2)
                .filter(|pair| pair[0] == member)
                .map(|pair| pair[1])
                .collect();
            assert_eq!(next.len(), 4, "{member} is followed by anyone");
        }
        assert!(four.key(NodeId::new(20)).is_some(), "a member");
        assert!(four.key(NodeId::new(25)).is_none(), "a stranger");
        assert_eq!(four.members().count(), 4, "members");
        assert!(Committee::new(BTreeMap::new()).is_none(), "nobody");
    }

    #[test]
    fn a_change_needs_a_quorum_of_the_committee_it_follows() {
        let four = fixture::committee();
        let change = change_to(1, &[1, 2, 3]);
        assert_eq!(change.verify(&four, DOMAIN, Epoch::FIRST), Ok(()), "valid");
        assert_eq!(
            change.verify(&four, DOMAIN, Epoch::new(1)),
            Err(Rejection::OtherEpoch(Epoch::new(1), Epoch::new(1))),
            "not the epoch after"
        );
        assert_eq!(
            change.verify(&four, Domain::new([7; 32]), Epoch::FIRST),
            Err(Rejection::Forged(NodeId::new(1))),
            "another domain"
        );
        let two: Vec<_> = (1..=2)
            .map(|node| Endorsement::sign(&key(node), DOMAIN, NodeId::new(node), change.change()))
            .collect();
        let short = ChangeCertificate::new(change.change().clone(), two);
        assert_eq!(
            short.verify(&four, DOMAIN, Epoch::FIRST),
            Err(Rejection::Short { have: 2, need: 3 }),
            "two of four"
        );
        let mut endorsements: Vec<_> = (1..=2)
            .map(|node| Endorsement::sign(&key(node), DOMAIN, NodeId::new(node), change.change()))
            .collect();
        endorsements.push(Endorsement::sign(
            &key(9),
            DOMAIN,
            NodeId::new(9),
            change.change(),
        ));
        assert_eq!(endorsements[2].signer(), NodeId::new(9), "its signer");
        let stranger = ChangeCertificate::new(change.change().clone(), endorsements);
        assert_eq!(
            stranger.verify(&four, DOMAIN, Epoch::FIRST),
            Err(Rejection::Stranger(NodeId::new(9))),
            "a stranger"
        );
        assert_eq!(change.committee().size(), 3, "three");
        assert_eq!(Epoch::new(u64::MAX).next(), None, "the last epoch");
        assert_eq!(Epoch::new(4).to_string(), "epoch 4", "named");
    }

    #[test]
    fn every_epoch_signs_in_a_domain_of_its_own() {
        let one = change_to(1, &[1, 2, 3]);
        let other = change_to(1, &[1, 2, 4]);
        let next = DOMAIN.next(&one);
        assert_ne!(next, DOMAIN, "not the one before");
        assert_ne!(next, DOMAIN.next(&other), "another committee");
        assert_ne!(next, next.next(&one), "the same change again");
    }

    #[test]
    fn changes_round_trip() {
        let change = change_to(1, &[1, 2, 3]);
        let mut bytes = Vec::new();
        change.encode(&mut bytes).unwrap();
        let mut reader = Reader::new(&bytes);
        assert_eq!(ChangeCertificate::decode(&mut reader), Ok(change), "same");
        assert_eq!(reader.finish(), Ok(()), "all of it");
        let mut empty = Vec::new();
        put_count(&mut empty, 0).unwrap();
        assert_eq!(
            Committee::decode(&mut Reader::new(&empty)),
            Err(DecodeError::Invalid),
            "nobody"
        );
        let mut twice = Vec::new();
        put_count(&mut twice, 2).unwrap();
        for _ in 0..2 {
            put_node(&mut twice, NodeId::new(1)).unwrap();
            twice.push(key(1).verifying_key().algorithm().id());
            put_bytes(&mut twice, key(1).verifying_key().as_bytes()).unwrap();
        }
        assert_eq!(
            Committee::decode(&mut Reader::new(&twice)),
            Err(DecodeError::Invalid),
            "a member twice"
        );
    }

    #[test]
    fn domains_are_their_bytes() {
        assert_eq!(Domain::new([3; 32]).as_bytes(), &[3; 32], "bytes");
    }

    #[test]
    fn a_domain_names_its_shard_and_its_committee() {
        let four = committee(4);
        let first = Domain::of(ShardId::new(1), &four);
        assert_eq!(first, Domain::of(ShardId::new(1), &committee(4)), "fixed");
        assert_ne!(first, Domain::of(ShardId::new(2), &four), "another shard");
        assert_ne!(
            first,
            Domain::of(ShardId::new(1), &committee(5)),
            "another committee"
        );
    }
}

#[cfg(kani)]
mod proofs {
    use super::{faults, quorum};

    /// Any two quorums of `n` replicas share at least `f + 1` of them, so
    /// at least one honest one, and the honest replicas alone make a
    /// quorum.
    #[kani::proof]
    fn quorums_intersect_in_an_honest_replica() {
        let n: usize = kani::any_where(|n: &usize| (1..=1 << 20).contains(n));
        let f = faults(n);
        let q = quorum(n);
        assert!(3 * f < n, "at most a third is faulty");
        assert!(q + q >= n + f + 1, "two quorums share f + 1");
        assert!(q <= n - f, "the honest make a quorum");
    }
}
