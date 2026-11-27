//! Signing keys, verifying keys and signatures, each tagged with its
//! algorithm.

use std::fmt;
use std::str::FromStr;

use crate::Algorithm;
use crate::der;

/// A secret key that signs.
///
/// `Debug` shows only the public half. The secret is zeroed when the key is
/// dropped.
#[derive(Clone, PartialEq, Eq)]
pub struct SigningKey(Secret);

#[derive(Clone, PartialEq, Eq)]
enum Secret {
    Ed25519(Box<ed25519_dalek::SigningKey>),
}

impl SigningKey {
    /// The `algorithm` key whose secret is `seed`. The seed must come from
    /// a cryptographically secure source and stay secret.
    #[must_use]
    pub fn from_seed(algorithm: Algorithm, seed: [u8; 32]) -> Self {
        match algorithm {
            Algorithm::Ed25519 => Self(Secret::Ed25519(Box::new(
                ed25519_dalek::SigningKey::from_bytes(&seed),
            ))),
        }
    }

    /// The algorithm the key signs with.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        match self.0 {
            Secret::Ed25519(_) => Algorithm::Ed25519,
        }
    }

    /// The public half, which checks this key's signatures.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        match &self.0 {
            Secret::Ed25519(key) => VerifyingKey(Public::Ed25519(Box::new(key.verifying_key()))),
        }
    }

    /// Signs `message`.
    #[must_use]
    pub fn sign(&self, message: &[u8]) -> Signature {
        match &self.0 {
            Secret::Ed25519(key) => {
                use ed25519_dalek::Signer as _;
                Signature {
                    algorithm: Algorithm::Ed25519,
                    bytes: Box::new(key.sign(message).to_bytes()),
                }
            }
        }
    }

    /// The secret as a PKCS #8 private key, as TLS stacks load one. The
    /// bytes are as secret as the key.
    #[must_use]
    pub fn to_pkcs8(&self) -> Vec<u8> {
        match &self.0 {
            Secret::Ed25519(key) => der::pkcs8(Algorithm::Ed25519, &key.to_bytes()),
        }
    }
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SigningKey")
            .field(&self.verifying_key())
            .finish()
    }
}

/// A public key that checks signatures.
///
/// Its text form is the algorithm's name, a colon and the key in lowercase
/// hex: `ed25519:1143…8940`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct VerifyingKey(Public);

#[derive(Clone, PartialEq, Eq, Hash)]
enum Public {
    Ed25519(Box<ed25519_dalek::VerifyingKey>),
}

impl VerifyingKey {
    /// The `algorithm` key encoded as `bytes`, or `None` when they are not
    /// a valid key of that algorithm.
    #[must_use]
    pub fn from_parts(algorithm: Algorithm, bytes: &[u8]) -> Option<Self> {
        match algorithm {
            Algorithm::Ed25519 => ed25519_dalek::VerifyingKey::from_bytes(bytes.try_into().ok()?)
                .ok()
                .map(|key| Self(Public::Ed25519(Box::new(key)))),
        }
    }

    /// The algorithm whose signatures the key checks.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        match self.0 {
            Public::Ed25519(_) => Algorithm::Ed25519,
        }
    }

    /// The encoded key, [`Algorithm::public_len`] bytes long.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Public::Ed25519(key) => key.as_bytes(),
        }
    }

    /// Whether `signature` is this key's signature over `message`. A
    /// signature of another algorithm never is.
    #[must_use]
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        match &self.0 {
            Public::Ed25519(key) => <&[u8; ed25519_dalek::SIGNATURE_LENGTH]>::try_from(
                signature.as_bytes(),
            )
            .is_ok_and(|bytes| {
                signature.algorithm == Algorithm::Ed25519
                    && key
                        .verify_strict(message, &ed25519_dalek::Signature::from_bytes(bytes))
                        .is_ok()
            }),
        }
    }

    /// The key as a DER `SubjectPublicKeyInfo`: what a raw public key
    /// "certificate" holds in TLS (RFC 7250).
    #[must_use]
    pub fn to_spki(&self) -> Vec<u8> {
        der::spki(self.algorithm(), self.as_bytes())
    }

    /// The key in `spki`, or `None` when it is anything but what
    /// [`to_spki`](Self::to_spki) writes for a valid key.
    #[must_use]
    pub fn from_spki(spki: &[u8]) -> Option<Self> {
        let (algorithm, bytes) = der::spki_key(spki)?;
        Self::from_parts(algorithm, bytes)
    }
}

impl fmt::Debug for VerifyingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VerifyingKey({self})")
    }
}

impl fmt::Display for VerifyingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:", self.algorithm())?;
        self.as_bytes()
            .iter()
            .try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl FromStr for VerifyingKey {
    type Err = InvalidKey;

    /// Reads the text form that `Display` writes.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (name, hex) = text.split_once(':').ok_or(InvalidKey::Algorithm)?;
        let algorithm = Algorithm::named(name).ok_or(InvalidKey::Algorithm)?;
        let bytes = from_hex(hex).ok_or(InvalidKey::Hex)?;
        Self::from_parts(algorithm, &bytes).ok_or(InvalidKey::Key(algorithm))
    }
}

/// Lowercase or uppercase hex, two digits per byte.
fn from_hex(hex: &str) -> Option<Vec<u8>> {
    let mut digits = hex.chars();
    let mut bytes = Vec::with_capacity(hex.len().checked_div(2)?);
    while let Some(high) = digits.next() {
        let high = high.to_digit(16)?;
        let low = digits.next()?.to_digit(16)?;
        bytes.push(u8::try_from(high.checked_mul(16)?.checked_add(low)?).ok()?);
    }
    Some(bytes)
}

/// Why text is not a public key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidKey {
    /// It does not start with the name of an algorithm this build knows,
    /// followed by a colon.
    #[error("not `<algorithm>:<hex>` with a known algorithm")]
    Algorithm,
    /// The key is not an even number of hex digits.
    #[error("the key is not hex")]
    Hex,
    /// The bytes are not a valid key of the algorithm.
    #[error("not a valid {0} public key")]
    Key(Algorithm),
}

/// A signature, tagged with the algorithm that made it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    algorithm: Algorithm,
    /// Exactly [`Algorithm::signature_len`] bytes.
    bytes: Box<[u8]>,
}

impl Signature {
    /// The `algorithm` signature encoded as `bytes`, or `None` when they
    /// are not as long as that algorithm's signatures.
    #[must_use]
    pub fn from_parts(algorithm: Algorithm, bytes: &[u8]) -> Option<Self> {
        (bytes.len() == algorithm.signature_len()).then(|| Self {
            algorithm,
            bytes: bytes.into(),
        })
    }

    /// The algorithm that made the signature.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// The encoded signature, [`Algorithm::signature_len`] bytes long.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{InvalidKey, Signature, SigningKey, VerifyingKey};
    use crate::Algorithm;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_seed(Algorithm::Ed25519, [seed; 32])
    }

    #[test]
    fn signatures_check_out_only_for_their_key_and_message() {
        let signature = key(1).sign(b"message");
        assert_eq!(signature.algorithm(), Algorithm::Ed25519, "tagged");
        assert_eq!(signature.as_bytes().len(), 64, "length");
        let public = key(1).verifying_key();
        assert!(public.verify(b"message", &signature), "own");
        assert!(!public.verify(b"other", &signature), "other message");
        assert!(
            !key(2).verifying_key().verify(b"message", &signature),
            "other key"
        );
        let mut bytes = signature.as_bytes().to_vec();
        if let Some(byte) = bytes.first_mut() {
            *byte ^= 1;
        }
        let damaged = Signature::from_parts(Algorithm::Ed25519, &bytes).unwrap();
        assert!(!public.verify(b"message", &damaged), "damaged");
        assert_eq!(
            Signature::from_parts(Algorithm::Ed25519, &[0; 63]),
            None,
            "short"
        );
    }

    #[test]
    fn keys_know_their_algorithm_and_hide_their_secret() {
        let secret = key(1);
        assert_eq!(secret.algorithm(), Algorithm::Ed25519, "secret");
        let public = secret.verifying_key();
        assert_eq!(public.algorithm(), Algorithm::Ed25519, "public");
        assert_eq!(public.as_bytes().len(), 32, "length");
        assert_eq!(
            format!("{secret:?}"),
            format!("SigningKey(VerifyingKey({public}))"),
            "debug shows the public half"
        );
        assert_eq!(secret.to_pkcs8().len(), 48, "PKCS #8");
    }

    #[test]
    fn text_forms_name_the_algorithm() {
        let public = key(0).verifying_key();
        let text = public.to_string();
        assert!(text.starts_with("ed25519:"), "{text}");
        assert_eq!(text.len(), 8 + 64, "hex");
        assert_eq!(text.parse::<VerifyingKey>(), Ok(public.clone()), "parses");
        assert_eq!(
            text.to_uppercase().replace("ED25519", "ed25519").parse(),
            Ok(public),
            "either case of hex"
        );
        let hex = text.trim_start_matches("ed25519:");
        assert_eq!(
            hex.parse::<VerifyingKey>(),
            Err(InvalidKey::Algorithm),
            "untagged"
        );
        assert_eq!(
            format!("rsa:{hex}").parse::<VerifyingKey>(),
            Err(InvalidKey::Algorithm),
            "unknown"
        );
        assert_eq!(
            format!("ed25519:{hex}0").parse::<VerifyingKey>(),
            Err(InvalidKey::Hex),
            "odd"
        );
        assert_eq!(
            "ed25519:zz".parse::<VerifyingKey>(),
            Err(InvalidKey::Hex),
            "not hex"
        );
        assert_eq!(
            format!("ed25519:{hex}00").parse::<VerifyingKey>(),
            Err(InvalidKey::Key(Algorithm::Ed25519)),
            "too long"
        );
    }

    proptest! {
        #[test]
        fn public_keys_round_trip(seed in any::<[u8; 32]>()) {
            let public = SigningKey::from_seed(Algorithm::Ed25519, seed).verifying_key();
            prop_assert_eq!(
                VerifyingKey::from_parts(public.algorithm(), public.as_bytes()),
                Some(public.clone())
            );
            prop_assert_eq!(VerifyingKey::from_spki(&public.to_spki()), Some(public.clone()));
            prop_assert_eq!(public.to_string().parse::<VerifyingKey>(), Ok(public));
        }
    }
}
