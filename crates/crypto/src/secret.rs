//! A node's identity: one secret, and a key for each thing the node signs,
//! derived from it.
//!
//! ICP gives a node separate keys for separate purposes, so that a key
//! leaked or misused in one protocol proves nothing in another. Kurator
//! does the same, but derives every key from one 32-byte [`NodeSecret`]
//! with BLAKE3's key derivation, so an operator keeps one file. Each
//! [`KeyPurpose`] has its own derivation context for each algorithm, and
//! contexts never change: changing one changes the key.

use std::fmt;

use crate::{Algorithm, SigningKey, VerifyingKey};

/// The one secret a node holds. Every key it uses is derived from it.
///
/// `Debug` shows nothing of it.
#[derive(Clone, PartialEq, Eq)]
pub struct NodeSecret([u8; 32]);

/// What a derived key is for. Keys for different purposes are unrelated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KeyPurpose {
    /// Signs the node's own Raft logs (`kurator_wal::Integrity::Signed`).
    WalSigning,
    /// Authenticates the node to other nodes in the transport's handshake.
    /// Its public half is the key in the node's `Peer`.
    Transport,
    /// Signs the node's proposals, votes and timeouts in Byzantine fault
    /// tolerant shards (`kurator_bft`). Its public half is what the
    /// committee checks them with.
    Consensus,
}

impl KeyPurpose {
    /// The algorithm of this purpose's keys.
    ///
    /// Each purpose moves to another algorithm on its own, when everything
    /// that checks its signatures can: a log can switch as soon as this
    /// build reads the new algorithm, the transport only once TLS stacks
    /// and every peer speak it.
    #[must_use]
    pub const fn algorithm(self) -> Algorithm {
        match self {
            Self::WalSigning | Self::Transport | Self::Consensus => Algorithm::Ed25519,
        }
    }

    /// The BLAKE3 derivation context of this purpose's `algorithm` keys.
    /// Fixed forever: see the module docs.
    const fn context(self, algorithm: Algorithm) -> &'static str {
        match (self, algorithm) {
            (Self::WalSigning, Algorithm::Ed25519) => "kurator 2026-11 replica WAL signing key",
            (Self::Transport, Algorithm::Ed25519) => "kurator 2026-11 replica transport key",
            (Self::Consensus, Algorithm::Ed25519) => "kurator 2026-11 replica consensus key",
        }
    }
}

impl NodeSecret {
    /// The secret `seed`. It must come from a cryptographically secure
    /// source and stay secret.
    #[must_use]
    pub const fn from_seed(seed: [u8; 32]) -> Self {
        Self(seed)
    }

    /// The secret bytes, to store them.
    #[must_use]
    pub const fn to_seed(&self) -> [u8; 32] {
        self.0
    }

    /// The seed of the key for `purpose`.
    fn derive(&self, purpose: KeyPurpose, algorithm: Algorithm) -> [u8; 32] {
        blake3::derive_key(purpose.context(algorithm), &self.0)
    }

    /// The signing key for `purpose`, of [`KeyPurpose::algorithm`].
    #[must_use]
    pub fn signing_key(&self, purpose: KeyPurpose) -> SigningKey {
        let algorithm = purpose.algorithm();
        SigningKey::from_seed(algorithm, self.derive(purpose, algorithm))
    }

    /// The public key for `purpose`.
    #[must_use]
    pub fn public_key(&self, purpose: KeyPurpose) -> VerifyingKey {
        self.signing_key(purpose).verifying_key()
    }
}

impl fmt::Debug for NodeSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeSecret(..)")
    }
}

#[cfg(test)]
mod tests {
    use super::{KeyPurpose, NodeSecret};
    use crate::{Algorithm, SigningKey};

    #[test]
    fn purposes_get_unrelated_keys_and_secrets_stay_hidden() {
        let secret = NodeSecret::from_seed([7; 32]);
        let wal = secret.public_key(KeyPurpose::WalSigning);
        let transport = secret.public_key(KeyPurpose::Transport);
        let consensus = secret.public_key(KeyPurpose::Consensus);
        assert_ne!(wal, transport, "one key per purpose");
        assert_ne!(consensus, transport, "consensus is not transport");
        assert_ne!(consensus, wal, "consensus is not WAL");
        assert_ne!(
            transport,
            SigningKey::from_seed(Algorithm::Ed25519, [7; 32]).verifying_key(),
            "derived, not the seed itself"
        );
        assert_eq!(
            NodeSecret::from_seed(secret.to_seed()).public_key(KeyPurpose::Transport),
            transport,
            "the same seed gives the same keys"
        );
        assert_ne!(
            NodeSecret::from_seed([8; 32]).public_key(KeyPurpose::Transport),
            transport,
            "another seed gives other keys"
        );
        assert_eq!(format!("{secret:?}"), "NodeSecret(..)", "debug hides it");
    }

    #[test]
    fn keys_use_their_purposes_algorithm() {
        let secret = NodeSecret::from_seed([7; 32]);
        for purpose in [
            KeyPurpose::WalSigning,
            KeyPurpose::Transport,
            KeyPurpose::Consensus,
        ] {
            assert_eq!(
                secret.signing_key(purpose).algorithm(),
                purpose.algorithm(),
                "{purpose:?}"
            );
        }
    }

    #[test]
    fn derivation_contexts_are_fixed() {
        // A changed context silently changes every node's keys.
        let secret = NodeSecret::from_seed([0; 32]);
        assert_eq!(
            secret.public_key(KeyPurpose::Transport).to_string(),
            TRANSPORT_KEY_OF_ZERO_SEED,
            "transport"
        );
        assert_eq!(
            secret.public_key(KeyPurpose::WalSigning).to_string(),
            WAL_KEY_OF_ZERO_SEED,
            "WAL"
        );
        assert_eq!(
            secret.public_key(KeyPurpose::Consensus).to_string(),
            CONSENSUS_KEY_OF_ZERO_SEED,
            "consensus"
        );
    }

    const TRANSPORT_KEY_OF_ZERO_SEED: &str =
        "ed25519:1143ed0efdb8425791bfc3fd92e758ed6f627e48c5800b080376b1a8f78b8940";
    const WAL_KEY_OF_ZERO_SEED: &str =
        "ed25519:9a708b2b2d9f535bdeebf1f3f67fb62ee51a706aa49b67849c22347158dc87a9";
    const CONSENSUS_KEY_OF_ZERO_SEED: &str =
        "ed25519:777483e09d11455320bc17c3e691e7c3045f4a571a5c5498c46d3ff246a7a5d2";
}
