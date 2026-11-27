//! What the tests share: four members with keys from their ids, in one
//! domain.

use kurator_consensus::NodeId;
use kurator_crypto::{Algorithm, SigningKey};

use crate::committee::{Committee, Domain};

/// The domain every test signs in.
pub(crate) const DOMAIN: Domain = Domain::new([1; 32]);

/// The key of `node`, seeded by its id.
pub(crate) fn key(node: u64) -> SigningKey {
    SigningKey::from_seed(Algorithm::Ed25519, [u8::try_from(node).unwrap(); 32])
}

/// Nodes 1 to 4.
pub(crate) fn committee() -> Committee {
    let members = (1..=4)
        .map(|node| (NodeId::new(node), key(node).verifying_key()))
        .collect();
    Committee::new(members).unwrap()
}
