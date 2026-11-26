//! A replica's identity: one secret, and a key for each thing the replica
//! signs, derived from it.
//!
//! ICP gives a node separate keys for separate purposes, so that a key
//! leaked or misused in one protocol proves nothing in another. Kurator
//! does the same, but derives every key from one 32-byte [`NodeSecret`]
//! with BLAKE3's key derivation, so an operator keeps one file. Each
//! [`KeyPurpose`] has its own derivation context, and contexts never
//! change: changing one changes the key.

use std::fmt;

use kurator_wal::{SigningKey, VerifyingKey};

/// The one secret a replica holds. Every key it uses is derived from it.
///
/// `Debug` shows nothing of it.
#[derive(Clone, PartialEq, Eq)]
pub struct NodeSecret([u8; 32]);

/// What a derived key is for. Keys for different purposes are unrelated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KeyPurpose {
    /// Signs this replica's own Raft log (`kurator_wal::Integrity::Signed`).
    WalSigning,
    /// Authenticates this replica to other replicas in the transport's
    /// handshake. Its public half is the key in the replica's
    /// [`Peer`](crate::Peer).
    Transport,
}

impl KeyPurpose {
    /// The BLAKE3 derivation context. Fixed forever: see the module docs.
    const fn context(self) -> &'static str {
        match self {
            Self::WalSigning => "kurator 2026-11 replica WAL signing key",
            Self::Transport => "kurator 2026-11 replica transport key",
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

    /// The secret of the key for `purpose`, for a protocol that needs the
    /// raw seed, such as a TLS stack loading an Ed25519 key.
    #[must_use]
    pub fn derive(&self, purpose: KeyPurpose) -> [u8; 32] {
        blake3::derive_key(purpose.context(), &self.0)
    }

    /// The signing key for `purpose`.
    #[must_use]
    pub fn signing_key(&self, purpose: KeyPurpose) -> SigningKey {
        SigningKey::from_seed(self.derive(purpose))
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

    #[test]
    fn purposes_get_unrelated_keys_and_secrets_stay_hidden() {
        let secret = NodeSecret::from_seed([7; 32]);
        let wal = secret.public_key(KeyPurpose::WalSigning);
        let transport = secret.public_key(KeyPurpose::Transport);
        assert_ne!(wal, transport, "one key per purpose");
        assert_ne!(
            secret.derive(KeyPurpose::Transport),
            [7; 32],
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
    fn derivation_contexts_are_fixed() {
        // A changed context silently changes every replica's keys.
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
    }

    const TRANSPORT_KEY_OF_ZERO_SEED: &str =
        "1143ed0efdb8425791bfc3fd92e758ed6f627e48c5800b080376b1a8f78b8940";
    const WAL_KEY_OF_ZERO_SEED: &str =
        "9a708b2b2d9f535bdeebf1f3f67fb62ee51a706aa49b67849c22347158dc87a9";
}
