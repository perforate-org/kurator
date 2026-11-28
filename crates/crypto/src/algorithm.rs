//! The signature algorithms OleaphDB knows, and their fixed parameters.

use std::fmt;

/// A signature algorithm.
///
/// Its [`id`](Self::id) names it in every binary encoding of a key or a
/// signature, and its [`name`](Self::name) in the text form of a key. Both
/// are fixed forever: changing one makes stored keys and logs unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Algorithm {
    /// Ed25519 (RFC 8032), checked strictly: no malleable signatures, no
    /// weak keys.
    Ed25519,
}

impl Algorithm {
    /// Every algorithm this build knows.
    pub const ALL: [Self; 1] = [Self::Ed25519];

    /// The longest signature of any algorithm this build knows. A test
    /// checks it against [`ALL`](Self::ALL).
    pub const MAX_SIGNATURE_LEN: usize = ed25519_dalek::SIGNATURE_LENGTH;

    /// The byte that names this algorithm in binary encodings.
    #[must_use]
    pub const fn id(self) -> u8 {
        match self {
            Self::Ed25519 => 1,
        }
    }

    /// The algorithm `id` names, if this build knows it.
    #[must_use]
    pub const fn from_id(id: u8) -> Option<Self> {
        match id {
            1 => Some(Self::Ed25519),
            _ => None,
        }
    }

    /// The name that starts the text form of a key.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ed25519 => "ed25519",
        }
    }

    /// The length of a public key.
    #[must_use]
    pub const fn public_len(self) -> usize {
        match self {
            Self::Ed25519 => ed25519_dalek::PUBLIC_KEY_LENGTH,
        }
    }

    /// The length of a signature.
    #[must_use]
    pub const fn signature_len(self) -> usize {
        match self {
            Self::Ed25519 => ed25519_dalek::SIGNATURE_LENGTH,
        }
    }

    /// The TLS 1.3 `SignatureScheme` code point (RFC 8446, section 4.2.3)
    /// for handshakes signed with this algorithm.
    #[must_use]
    pub const fn tls_scheme(self) -> u16 {
        match self {
            Self::Ed25519 => 0x0807,
        }
    }

    /// The algorithm whose [`name`](Self::name) is `name`.
    pub(crate) fn named(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|algorithm| algorithm.name() == name)
    }
}

impl fmt::Display for Algorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::Algorithm;

    #[test]
    fn identifiers_are_fixed_and_round_trip() {
        // Changing either one makes stored keys and logs unreadable.
        assert_eq!(Algorithm::Ed25519.id(), 1, "id");
        assert_eq!(Algorithm::Ed25519.name(), "ed25519", "name");
        for algorithm in Algorithm::ALL {
            assert_eq!(Algorithm::from_id(algorithm.id()), Some(algorithm), "id");
            assert_eq!(
                Algorithm::named(&algorithm.to_string()),
                Some(algorithm),
                "name"
            );
        }
        assert_eq!(Algorithm::from_id(0), None, "unknown id");
        assert_eq!(Algorithm::named("rsa"), None, "unknown name");
    }

    #[test]
    fn parameters_are_those_of_the_standards() {
        assert_eq!(Algorithm::Ed25519.public_len(), 32, "public key");
        assert_eq!(Algorithm::Ed25519.signature_len(), 64, "signature");
        assert_eq!(Algorithm::Ed25519.tls_scheme(), 0x0807, "TLS");
        assert_eq!(
            Algorithm::ALL
                .map(Algorithm::signature_len)
                .into_iter()
                .max(),
            Some(Algorithm::MAX_SIGNATURE_LEN),
            "longest"
        );
    }
}

#[cfg(kani)]
mod proofs {
    use super::Algorithm;

    /// Every id either names nothing or names the algorithm that has it.
    #[kani::proof]
    fn ids_are_exact() {
        let id: u8 = kani::any();
        if let Some(algorithm) = Algorithm::from_id(id) {
            assert!(algorithm.id() == id, "round trip");
        }
    }
}
