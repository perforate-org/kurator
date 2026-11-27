//! Who decides: the replicas of a shard, their voting keys, how many of
//! them must agree, and who leads each view.

use std::collections::BTreeMap;

use kurator_consensus::NodeId;
use kurator_crypto::VerifyingKey;

use crate::view::View;

/// What every signature in one shard's consensus is bound to, so that a
/// vote in one shard, or one test, never counts in another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
}

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
const LEADER_CONTEXT: &str = "kurator 2026-11 bft leader schedule";

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

    use kurator_consensus::NodeId;
    use kurator_crypto::{Algorithm, SigningKey};

    use super::{Committee, Domain, faults, quorum};
    use crate::view::View;

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
            [10, 30, 40, 30, 30, 40, 10, 20],
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
    fn domains_are_their_bytes() {
        assert_eq!(Domain::new([3; 32]).as_bytes(), &[3; 32], "bytes");
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
