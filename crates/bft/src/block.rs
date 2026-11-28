//! Blocks of commands, and a leader's proposal of one.

use std::collections::BTreeMap;
use std::io::{self, Write};

use kurator_codec::{DecodeError, Reader};
use kurator_consensus::NodeId;
use kurator_crypto::{Signature, SigningKey};

use crate::committee::{Committee, Domain};
use crate::rejection::Rejection;
use crate::timeout::TimeoutCertificate;
use crate::view::View;
use crate::vote::{BlockId, Justify, check};
use crate::wire::{
    put_commands, put_node, put_signature, put_view, read_commands, read_node, read_signature,
    read_view,
};

/// Forgets the blocks of `blocks` that can no longer matter once `through`
/// is decided: those of its view or earlier, other than `through` itself,
/// where a chain walk back now stops. Its ancestors are decided, and
/// every other block of those views lost. Knowing nothing of `through`,
/// it forgets nothing.
pub(crate) fn prune(blocks: &mut BTreeMap<BlockId, Block>, through: BlockId) {
    let Some(view) = blocks.get(&through).map(Block::view) else {
        return;
    };
    blocks.retain(|id, block| *id == through || block.view() > view);
}

/// The key derivation context of block ids. Fixed forever.
const ID_CONTEXT: &str = "kurator 2026-11 bft block id";

/// What a proposal's signature is over, after the domain.
const PROPOSAL: &[u8] = b"kurator-bft proposal";

/// Commands proposed in a view, on top of the block its certificate
/// certifies: the parent. Blocks chain by their parents back to genesis.
///
/// The commands are opaque here: the shard above decides what they mean,
/// and applies them in the order their blocks commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    view: View,
    justify: Justify,
    commands: Vec<Vec<u8>>,
    id: BlockId,
}

impl Block {
    /// The block of `commands` in `view`, extending the block `justify`
    /// certifies.
    #[must_use]
    pub fn new(view: View, justify: Justify, commands: Vec<Vec<u8>>) -> Self {
        let id = id(view, &justify, &commands);
        Self {
            view,
            justify,
            commands,
            id,
        }
    }

    /// Its id.
    #[must_use]
    pub const fn id(&self) -> BlockId {
        self.id
    }

    /// The view it is proposed in.
    #[must_use]
    pub const fn view(&self) -> View {
        self.view
    }

    /// The certificate of its parent.
    #[must_use]
    pub const fn justify(&self) -> &Justify {
        &self.justify
    }

    /// Its parent's id.
    #[must_use]
    pub const fn parent(&self) -> BlockId {
        self.justify.block()
    }

    /// Its commands, in order.
    #[must_use]
    pub fn commands(&self) -> &[Vec<u8>] {
        &self.commands
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        put_view(out, self.view)?;
        self.justify.encode(out)?;
        put_commands(out, &self.commands)
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let view = read_view(reader)?;
        let justify = Justify::decode(reader)?;
        Ok(Self::new(view, justify, read_commands(reader)?))
    }
}

/// The id of a block: a hash over every field, each length-prefixed so
/// that no two blocks share an encoding. The certificate counts by its
/// view and block alone, since every certificate of one view certifies the
/// same block.
fn id(view: View, justify: &Justify, commands: &[Vec<u8>]) -> BlockId {
    let mut hasher = blake3::Hasher::new_derive_key(ID_CONTEXT);
    let _header = hasher
        .update(&view.get().to_le_bytes())
        .update(&justify.view().get().to_le_bytes())
        .update(justify.block().as_bytes())
        .update(&len(commands.len()).to_le_bytes());
    for command in commands {
        let _command = hasher
            .update(&len(command.len()).to_le_bytes())
            .update(command);
    }
    BlockId::new(*hasher.finalize().as_bytes())
}

/// A length as the hash counts it.
fn len(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

/// The bytes a proposal of `block` signs.
fn statement(domain: Domain, block: BlockId) -> Vec<u8> {
    [domain.as_bytes().as_slice(), PROPOSAL, block.as_bytes()].concat()
}

/// A leader proposes a block in its view. When the view before ended in a
/// timeout, it shows the timeout certificate that let it start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    block: Block,
    timeout: Option<TimeoutCertificate>,
    proposer: NodeId,
    signature: Signature,
}

impl Proposal {
    /// `proposer`'s proposal of `block`, after `timeout` when the view
    /// before timed out.
    #[must_use]
    pub fn sign(
        key: &SigningKey,
        domain: Domain,
        proposer: NodeId,
        block: Block,
        timeout: Option<TimeoutCertificate>,
    ) -> Self {
        let signature = key.sign(&statement(domain, block.id()));
        Self {
            block,
            timeout,
            proposer,
            signature,
        }
    }

    /// The block proposed.
    #[must_use]
    pub const fn block(&self) -> &Block {
        &self.block
    }

    /// The timeout certificate of the view before, if it timed out.
    #[must_use]
    pub const fn timeout(&self) -> Option<&TimeoutCertificate> {
        self.timeout.as_ref()
    }

    /// Who proposed it.
    #[must_use]
    pub const fn proposer(&self) -> NodeId {
        self.proposer
    }

    /// Checks that the leader of the block's view signed it, that every
    /// certificate it carries checks out, and that they justify the view:
    /// the block extends a certificate from the view right before, or the
    /// view before timed out and the block extends a certificate at least
    /// as new as any signer of the timeout knew.
    ///
    /// # Errors
    ///
    /// When the proposer does not lead the view, the signature is not
    /// theirs, a certificate does not check out, or they do not justify
    /// the view.
    pub fn verify(&self, committee: &Committee, domain: Domain) -> Result<(), Rejection> {
        let view = self.block.view;
        if committee.leader(view) != Some(self.proposer) {
            return Err(Rejection::NotLeader(self.proposer, view));
        }
        let message = statement(domain, self.block.id);
        check(committee, self.proposer, &message, &self.signature)?;
        let extends = self.block.justify.view();
        let after_timeout = self
            .timeout
            .as_ref()
            .is_some_and(|timeout| view.follows(timeout.view()) && extends >= timeout.high());
        if !view.follows(extends) && !after_timeout {
            return Err(Rejection::Unjustified(view));
        }
        self.block.justify.verify(committee, domain)?;
        self.timeout
            .as_ref()
            .map_or(Ok(()), |timeout| timeout.verify(committee, domain))
    }

    /// The block and the timeout certificate, once [`Proposal::verify`]
    /// finds them valid.
    ///
    /// # Errors
    ///
    /// As [`Proposal::verify`].
    pub fn open(
        self,
        committee: &Committee,
        domain: Domain,
    ) -> Result<(Block, Option<TimeoutCertificate>), Rejection> {
        self.verify(committee, domain)?;
        Ok((self.block, self.timeout))
    }

    pub(crate) fn encode(&self, out: &mut dyn Write) -> io::Result<()> {
        self.block.encode(out)?;
        match &self.timeout {
            None => out.write_all(&[0])?,
            Some(timeout) => {
                out.write_all(&[1])?;
                timeout.encode(out)?;
            }
        }
        put_node(out, self.proposer)?;
        put_signature(out, &self.signature)
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let block = Block::decode(reader)?;
        let timeout = match reader.read::<u8>()? {
            0 => None,
            1 => Some(TimeoutCertificate::decode(reader)?),
            other => return Err(DecodeError::Tag(other)),
        };
        Ok(Self {
            block,
            timeout,
            proposer: read_node(reader)?,
            signature: read_signature(reader)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::fixture::{DOMAIN, committee, key};
    use std::collections::BTreeMap;

    use super::{Block, Proposal};
    use crate::committee::Domain;
    use crate::rejection::Rejection;
    use crate::timeout::TimeoutCertificate;
    use crate::view::View;
    use crate::vote::{BlockId, Certificate, Justify};

    fn certified(view: u64, block: u8) -> Justify {
        Justify::Certified(Certificate::new(
            View::new(view),
            BlockId::new([block; 32]),
            BTreeMap::new(),
        ))
    }

    #[test]
    fn every_field_counts_in_the_id() {
        let commands = vec![b"ab".to_vec(), b"c".to_vec()];
        let block = Block::new(View::new(5), certified(4, 1), commands.clone());
        let same = Block::new(View::new(5), certified(4, 1), commands.clone());
        assert_eq!(block.id(), same.id(), "deterministic");
        let others = [
            Block::new(View::new(6), certified(4, 1), commands.clone()),
            Block::new(View::new(5), certified(3, 1), commands.clone()),
            Block::new(View::new(5), certified(4, 2), commands.clone()),
            Block::new(
                View::new(5),
                certified(4, 1),
                vec![b"a".to_vec(), b"bc".to_vec()],
            ),
            Block::new(View::new(5), certified(4, 1), vec![b"abc".to_vec()]),
            Block::new(View::new(5), certified(4, 1), Vec::new()),
        ];
        for other in others {
            assert_ne!(block.id(), other.id(), "{other:?}");
        }
        let empty_last = Block::new(
            View::new(5),
            certified(4, 1),
            vec![b"a".to_vec(), Vec::new()],
        );
        for length in [0_u64, 1] {
            let mut padded = b"a".to_vec();
            padded.extend_from_slice(&length.to_le_bytes());
            let one = Block::new(View::new(5), certified(4, 1), vec![padded]);
            assert_ne!(empty_last.id(), one.id(), "lengths count: {length}");
        }
        assert_ne!(block.id(), BlockId::GENESIS, "not genesis");
        assert_eq!(block.parent(), BlockId::new([1; 32]), "parent");
        assert_eq!(block.commands(), commands, "commands");
        assert_eq!(block.id().to_string().len(), 8, "short display");
        assert_eq!(BlockId::new([0xab; 32]).to_string(), "abababab", "hex");
    }

    #[test]
    fn a_proposal_checks_what_it_carries() {
        let leader = committee().leader(View::new(5)).unwrap();
        let block = Block::new(View::new(5), Justify::Genesis, Vec::new());
        let short = TimeoutCertificate::new(View::new(4), BTreeMap::new());
        let proposal = Proposal::sign(
            &key(leader.get()),
            DOMAIN,
            leader,
            block.clone(),
            Some(short.clone()),
        );
        assert_eq!(proposal.timeout(), Some(&short), "its timeout");
        assert_eq!(
            proposal.verify(&committee(), DOMAIN),
            Err(Rejection::Short { have: 0, need: 3 }),
            "timeout"
        );
        let bad_justify = Block::new(View::new(5), certified(4, 1), Vec::new());
        let second = Proposal::sign(&key(leader.get()), DOMAIN, leader, bad_justify, None);
        assert_eq!(
            second.verify(&committee(), DOMAIN),
            Err(Rejection::Short { have: 0, need: 3 }),
            "justify"
        );
        let third = Proposal::sign(&key(leader.get()), DOMAIN, leader, block, None);
        assert_eq!(
            third.verify(&committee(), DOMAIN),
            Err(Rejection::Unjustified(View::new(5))),
            "skips views"
        );
        let first = committee().leader(View::new(1)).unwrap();
        let valid = Block::new(View::new(1), Justify::Genesis, Vec::new());
        let fourth = Proposal::sign(&key(first.get()), DOMAIN, first, valid.clone(), None);
        assert_eq!(fourth.verify(&committee(), DOMAIN), Ok(()), "valid");
        assert_eq!(fourth.timeout(), None, "no timeout");
        assert_eq!(
            fourth.verify(&committee(), Domain::new([9; 32])),
            Err(Rejection::Forged(first)),
            "another shard's"
        );
        assert_eq!(
            fourth.open(&committee(), DOMAIN),
            Ok((valid, None)),
            "opened"
        );
    }
}
