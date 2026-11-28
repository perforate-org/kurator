//! Votes for blocks, and the certificates a quorum of them makes.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};

use oleaph_codec::{DecodeError, Reader};
use oleaph_consensus::NodeId;
use oleaph_crypto::{Signature, SigningKey};

use crate::committee::{Committee, Domain};
use crate::rejection::Rejection;
use crate::view::View;
use crate::wire::{
    put_count, put_node, put_signature, put_view, read_count, read_node, read_signature, read_view,
};

/// What a vote's signature is over, after the domain.
const VOTE: &[u8] = b"oleaph-bft vote";

/// Names a block: the BLAKE3 hash of its view, its certificate's view and
/// block, and its commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockId([u8; 32]);

impl BlockId {
    /// The genesis block, which every chain starts from and nobody
    /// proposes.
    pub const GENESIS: Self = Self([0; 32]);

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

    pub(crate) fn encode(self, out: &mut dyn Write) -> io::Result<()> {
        out.write_all(&self.0)
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self(reader.read()?))
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.iter().take(4) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// The bytes a vote for `block` in `view` signs.
fn statement(domain: Domain, view: View, block: BlockId) -> Vec<u8> {
    [
        domain.as_bytes().as_slice(),
        VOTE,
        &view.get().to_le_bytes(),
        block.as_bytes(),
    ]
    .concat()
}

/// A replica's vote for a block in a view. An honest replica votes once
/// in a view at most.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Vote {
    view: View,
    block: BlockId,
    voter: NodeId,
    signature: Signature,
}

impl Vote {
    /// `voter`'s vote for `block` in `view`, signed with `key`.
    #[must_use]
    pub fn sign(
        key: &SigningKey,
        domain: Domain,
        voter: NodeId,
        view: View,
        block: BlockId,
    ) -> Self {
        Self {
            view,
            block,
            voter,
            signature: key.sign(&statement(domain, view, block)),
        }
    }

    /// The view it is cast in.
    #[must_use]
    pub const fn view(&self) -> View {
        self.view
    }

    /// The block it is for.
    #[must_use]
    pub const fn block(&self) -> BlockId {
        self.block
    }

    /// Who cast it.
    #[must_use]
    pub const fn voter(&self) -> NodeId {
        self.voter
    }

    /// Checks that a member cast it.
    ///
    /// # Errors
    ///
    /// When the voter is not a member or the signature is not theirs.
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        let message = statement(domain, self.view, self.block);
        committee.check(self.voter, &message, &self.signature)
    }

    pub(crate) fn into_signature(self) -> Signature {
        self.signature
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        put_view(out, self.view)?;
        self.block.encode(out)?;
        put_node(out, self.voter)?;
        put_signature(out, &self.signature)
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            view: read_view(reader)?,
            block: BlockId::decode(reader)?,
            voter: read_node(reader)?,
            signature: read_signature(reader)?,
        })
    }
}

/// Proof that a quorum voted for a block in a view: a quorum certificate.
///
/// Since any two quorums share an honest replica, and an honest replica
/// votes once in a view, a view certifies at most one block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Certificate {
    view: View,
    block: BlockId,
    signatures: BTreeMap<NodeId, Signature>,
}

impl Certificate {
    /// The certificate for `block` in `view` that `signatures`, each a
    /// vote's, make.
    #[must_use]
    pub const fn new(view: View, block: BlockId, signatures: BTreeMap<NodeId, Signature>) -> Self {
        Self {
            view,
            block,
            signatures,
        }
    }

    /// Checks that a quorum of members signed it.
    ///
    /// # Errors
    ///
    /// When a signer is not a member, a signature is not its signer's, or
    /// the signers are fewer than a quorum.
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        let need = committee.quorum();
        let have = self.signatures.len();
        if have < need {
            return Err(Rejection::Short { have, need });
        }
        let message = statement(domain, self.view, self.block);
        self.signatures
            .iter()
            .try_for_each(|(signer, signature)| committee.check(*signer, &message, signature))
    }
}

/// What a block extends: the certificate of its parent.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Justify {
    /// The genesis block, certified by definition.
    Genesis,
    /// A block a quorum voted for.
    Certified(Certificate),
}

impl Justify {
    /// The view of the certified block.
    #[must_use]
    pub const fn view(&self) -> View {
        match self {
            Self::Genesis => View::GENESIS,
            Self::Certified(certificate) => certificate.view,
        }
    }

    /// The certified block.
    #[must_use]
    pub const fn block(&self) -> BlockId {
        match self {
            Self::Genesis => BlockId::GENESIS,
            Self::Certified(certificate) => certificate.block,
        }
    }

    /// Checks the certificate, as [`Certificate::verify`] does. Genesis
    /// needs no signatures.
    ///
    /// # Errors
    ///
    /// As [`Certificate::verify`].
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        match self {
            Self::Genesis => Ok(()),
            Self::Certified(certificate) => certificate.verify(committee, domain),
        }
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        match self {
            Self::Genesis => out.write_all(&[GENESIS]),
            Self::Certified(certificate) => {
                out.write_all(&[CERTIFIED])?;
                put_view(out, certificate.view)?;
                certificate.block.encode(out)?;
                put_count(out, certificate.signatures.len())?;
                certificate
                    .signatures
                    .iter()
                    .try_for_each(|(signer, signature)| {
                        put_node(out, *signer)?;
                        put_signature(out, signature)
                    })
            }
        }
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match reader.read::<u8>()? {
            GENESIS => Ok(Self::Genesis),
            CERTIFIED => {
                let view = read_view(reader)?;
                let block = BlockId::decode(reader)?;
                let signatures = read_signatures(reader)?;
                Ok(Self::Certified(Certificate::new(view, block, signatures)))
            }
            other => Err(DecodeError::Tag(other)),
        }
    }
}

const GENESIS: u8 = 0;
const CERTIFIED: u8 = 1;

/// Reads `count u32 | (node | signature)*`, in strictly increasing node
/// order, so that every set has one encoding.
fn read_signatures(reader: &mut Reader<'_>) -> Result<BTreeMap<NodeId, Signature>, DecodeError> {
    let count = read_count(reader)?;
    let mut signatures = BTreeMap::new();
    for _ in 0..count {
        let signer = read_node(reader)?;
        if signatures
            .last_key_value()
            .is_some_and(|(last, _)| *last >= signer)
        {
            return Err(DecodeError::Invalid);
        }
        drop(signatures.insert(signer, read_signature(reader)?));
    }
    Ok(signatures)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use oleaph_consensus::NodeId;

    use super::{BlockId, Certificate, Vote};
    use crate::committee::Domain;
    use crate::fixture::{DOMAIN, committee, key};
    use crate::rejection::Rejection;
    use crate::view::View;

    fn vote(node: u64, view: u64, block: u8) -> Vote {
        Vote::sign(
            &key(node),
            DOMAIN,
            NodeId::new(node),
            View::new(view),
            BlockId::new([block; 32]),
        )
    }

    #[test]
    fn a_vote_is_its_voters_in_its_domain() {
        let committee = committee();
        assert_eq!(vote(1, 3, 7).verify(&committee, DOMAIN), Ok(()), "valid");
        assert_eq!(
            vote(1, 3, 7).verify(&committee, Domain::new([2; 32])),
            Err(Rejection::Forged(NodeId::new(1))),
            "other domain"
        );
        let stolen = Vote {
            voter: NodeId::new(2),
            ..vote(1, 3, 7)
        };
        assert_eq!(
            stolen.verify(&committee, DOMAIN),
            Err(Rejection::Forged(NodeId::new(2))),
            "another's name"
        );
        let stranger = Vote::sign(
            &key(9),
            DOMAIN,
            NodeId::new(9),
            View::new(3),
            BlockId::GENESIS,
        );
        assert_eq!(
            stranger.verify(&committee, DOMAIN),
            Err(Rejection::Stranger(NodeId::new(9))),
            "stranger"
        );
    }

    #[test]
    fn a_signature_covers_its_view_and_block() {
        let committee = committee();
        let signatures = |view, block| -> BTreeMap<NodeId, _> {
            (1..=3)
                .map(|node| (NodeId::new(node), vote(node, view, block).into_signature()))
                .collect()
        };
        let certificate = |view, block, signed: BTreeMap<_, _>| {
            Certificate::new(View::new(view), BlockId::new([block; 32]), signed)
        };
        assert_eq!(
            certificate(3, 7, signatures(3, 7)).verify(&committee, DOMAIN),
            Ok(()),
            "valid"
        );
        let forged = Err(Rejection::Forged(NodeId::new(1)));
        assert_eq!(
            certificate(4, 7, signatures(3, 7)).verify(&committee, DOMAIN),
            forged,
            "other view"
        );
        assert_eq!(
            certificate(3, 8, signatures(3, 7)).verify(&committee, DOMAIN),
            forged,
            "other block"
        );
    }
}
