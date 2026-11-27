//! Giving up on a view, and the certificate a quorum of give-ups makes.

use std::collections::BTreeMap;
use std::io::{self, Write};

use kurator_codec::{DecodeError, Reader};
use kurator_consensus::NodeId;
use kurator_crypto::{Signature, SigningKey};

use crate::committee::{Committee, Domain};
use crate::rejection::Rejection;
use crate::view::View;
use crate::vote::{Justify, check};
use crate::wire::{
    put_count, put_node, put_signature, put_view, read_count, read_node, read_signature, read_view,
};

/// What a timeout's signature is over, after the domain.
const TIMEOUT: &[u8] = b"kurator-bft timeout";

/// The bytes a timeout in `view`, by a replica whose newest certificate is
/// from `high`, signs.
fn statement(domain: Domain, view: View, high: View) -> Vec<u8> {
    [
        domain.as_bytes().as_slice(),
        TIMEOUT,
        &view.get().to_le_bytes(),
        &high.get().to_le_bytes(),
    ]
    .concat()
}

/// A replica gives up on a view: it will not vote in it any more. It
/// sends along the newest certificate it knows, so the next leader can
/// extend a block every honest replica will vote for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timeout {
    view: View,
    high: Justify,
    voter: NodeId,
    signature: Signature,
}

impl Timeout {
    /// `voter` gives up on `view`, knowing `high` as its newest
    /// certificate.
    #[must_use]
    pub fn sign(
        key: &SigningKey,
        domain: Domain,
        voter: NodeId,
        view: View,
        high: Justify,
    ) -> Self {
        let signature = key.sign(&statement(domain, view, high.view()));
        Self {
            view,
            high,
            voter,
            signature,
        }
    }

    /// The view given up on.
    #[must_use]
    pub const fn view(&self) -> View {
        self.view
    }

    /// The newest certificate the voter knew.
    #[must_use]
    pub const fn high(&self) -> &Justify {
        &self.high
    }

    /// Who gave up.
    #[must_use]
    pub const fn voter(&self) -> NodeId {
        self.voter
    }

    /// Checks that a member signed it, and its certificate.
    ///
    /// # Errors
    ///
    /// As [`Justify::verify`], or when the signer is not a member or the
    /// signature not theirs.
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        let message = statement(domain, self.view, self.high.view());
        check(committee, self.voter, &message, &self.signature)?;
        self.high.verify(committee, domain)
    }

    /// What it adds to a timeout certificate.
    pub(crate) fn into_entry(self) -> (NodeId, View, Signature) {
        (self.voter, self.high.view(), self.signature)
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        put_view(out, self.view)?;
        self.high.encode(out)?;
        put_node(out, self.voter)?;
        put_signature(out, &self.signature)
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            view: read_view(reader)?,
            high: Justify::decode(reader)?,
            voter: read_node(reader)?,
            signature: read_signature(reader)?,
        })
    }
}

/// Proof that a quorum gave up on a view, each with the view of the
/// newest certificate it knew.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutCertificate {
    view: View,
    entries: BTreeMap<NodeId, (View, Signature)>,
}

impl TimeoutCertificate {
    /// The certificate that `entries`, each a signer's newest certificate
    /// view and timeout signature, make for `view`.
    #[must_use]
    pub const fn new(view: View, entries: BTreeMap<NodeId, (View, Signature)>) -> Self {
        Self { view, entries }
    }

    /// The view given up on.
    #[must_use]
    pub const fn view(&self) -> View {
        self.view
    }

    /// The newest certificate any signer knew of. A leader that proposes
    /// after this certificate must extend a block at least this new, so
    /// that every honest signer can vote for it.
    #[must_use]
    pub fn high(&self) -> View {
        self.entries
            .values()
            .map(|(high, _)| *high)
            .max()
            .unwrap_or(View::GENESIS)
    }

    /// Checks that a quorum of members signed it.
    ///
    /// # Errors
    ///
    /// When a signer is not a member, a signature is not its signer's, or
    /// the signers are fewer than a quorum.
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        let need = committee.quorum();
        let have = self.entries.len();
        if have < need {
            return Err(Rejection::Short { have, need });
        }
        self.entries
            .iter()
            .try_for_each(|(signer, (high, signature))| {
                check(
                    committee,
                    *signer,
                    &statement(domain, self.view, *high),
                    signature,
                )
            })
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        put_view(out, self.view)?;
        put_count(out, self.entries.len())?;
        self.entries
            .iter()
            .try_for_each(|(signer, (high, signature))| {
                put_node(out, *signer)?;
                put_view(out, *high)?;
                put_signature(out, signature)
            })
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let view = read_view(reader)?;
        let count = read_count(reader)?;
        let mut entries = BTreeMap::new();
        for _ in 0..count {
            let signer = read_node(reader)?;
            if entries
                .last_key_value()
                .is_some_and(|(last, _)| *last >= signer)
            {
                return Err(DecodeError::Invalid);
            }
            let high = read_view(reader)?;
            drop(entries.insert(signer, (high, read_signature(reader)?)));
        }
        Ok(Self { view, entries })
    }
}

#[cfg(test)]
mod tests {
    use crate::fixture::{DOMAIN, committee, key};
    use std::collections::BTreeMap;

    use kurator_consensus::NodeId;

    use super::{Timeout, TimeoutCertificate};
    use crate::committee::Domain;
    use crate::rejection::Rejection;
    use crate::view::View;
    use crate::vote::{BlockId, Certificate, Justify};

    /// A certificate for view 9 from `signers`, each with its own high.
    fn certificate(signers: &[(u64, u64)]) -> TimeoutCertificate {
        let entries = signers
            .iter()
            .map(|(node, high)| {
                let high = Justify::Certified(Certificate::new(
                    View::new(*high),
                    BlockId::GENESIS,
                    BTreeMap::new(),
                ));
                let timeout =
                    Timeout::sign(&key(*node), DOMAIN, NodeId::new(*node), View::new(9), high);
                let (voter, seen, signature) = timeout.into_entry();
                (voter, (seen, signature))
            })
            .collect();
        TimeoutCertificate::new(View::new(9), entries)
    }

    #[test]
    fn a_certificate_shows_the_newest_lock_of_a_quorum() {
        let full = certificate(&[(1, 3), (2, 7), (4, 5)]);
        assert_eq!(full.verify(&committee(), DOMAIN), Ok(()), "a quorum");
        assert_eq!(full.high(), View::new(7), "the newest");
        assert_eq!(full.view(), View::new(9), "view");
        let short = certificate(&[(1, 3), (2, 7)]);
        assert_eq!(
            short.verify(&committee(), DOMAIN),
            Err(Rejection::Short { have: 2, need: 3 }),
            "two"
        );
        assert_eq!(certificate(&[]).high(), View::GENESIS, "none");
        let stranger = certificate(&[(1, 3), (2, 7), (5, 1)]);
        assert_eq!(
            stranger.verify(&committee(), DOMAIN),
            Err(Rejection::Stranger(NodeId::new(5))),
            "stranger"
        );
        let other_domain = full.verify(&committee(), Domain::new([5; 32]));
        assert_eq!(
            other_domain,
            Err(Rejection::Forged(NodeId::new(1))),
            "another shard's"
        );
    }

    #[test]
    fn a_signer_cannot_claim_a_newer_lock_than_it_signed() {
        let honest = certificate(&[(1, 3), (2, 7), (4, 5)]);
        let mut entries = honest.entries;
        let (_, signature) = entries.remove(&NodeId::new(1)).unwrap();
        let _replaced = entries.insert(NodeId::new(1), (View::new(8), signature));
        let lying = TimeoutCertificate::new(View::new(9), entries);
        assert_eq!(
            lying.verify(&committee(), DOMAIN),
            Err(Rejection::Forged(NodeId::new(1))),
            "forged"
        );
    }

    #[test]
    fn a_timeout_checks_its_certificate_too() {
        let short = Justify::Certified(Certificate::new(
            View::new(2),
            BlockId::GENESIS,
            BTreeMap::new(),
        ));
        let timeout = Timeout::sign(&key(1), DOMAIN, NodeId::new(1), View::new(3), short);
        assert_eq!(
            timeout.verify(&committee(), DOMAIN),
            Err(Rejection::Short { have: 0, need: 3 }),
            "short"
        );
        let second = Timeout::sign(
            &key(1),
            DOMAIN,
            NodeId::new(2),
            View::new(3),
            Justify::Genesis,
        );
        assert_eq!(
            second.verify(&committee(), DOMAIN),
            Err(Rejection::Forged(NodeId::new(2))),
            "forged"
        );
    }
}
