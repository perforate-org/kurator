//! The two DER structures TLS needs for a key, written and read byte for
//! byte: each algorithm's keys have one encoding of a fixed length, so a
//! parser is a prefix check.
//!
//! ```text
//! Ed25519 SubjectPublicKeyInfo (RFC 8410)  30 2a 30 05 06 03 2b 65 70 03 21 00 | key [32]
//! Ed25519 OneAsymmetricKey v1 (RFC 8410)   30 2e 02 01 00 30 05 06 03 2b 65 70 04 22 04 20 | seed [32]
//! ```

use crate::Algorithm;

/// The bytes of an Ed25519 `SubjectPublicKeyInfo` before the key.
const ED25519_SPKI: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// The bytes of an Ed25519 PKCS #8 private key before the seed.
const ED25519_PKCS8: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

const fn spki_prefix(algorithm: Algorithm) -> &'static [u8] {
    match algorithm {
        Algorithm::Ed25519 => &ED25519_SPKI,
    }
}

const fn pkcs8_prefix(algorithm: Algorithm) -> &'static [u8] {
    match algorithm {
        Algorithm::Ed25519 => &ED25519_PKCS8,
    }
}

/// The `SubjectPublicKeyInfo` of the `algorithm` public key `key`.
pub(crate) fn spki(algorithm: Algorithm, key: &[u8]) -> Vec<u8> {
    [spki_prefix(algorithm), key].concat()
}

/// The algorithm and the key bytes in `spki`, or `None` when `spki` is
/// anything but what [`spki`] writes for a key of the right length.
pub(crate) fn spki_key(spki: &[u8]) -> Option<(Algorithm, &[u8])> {
    Algorithm::ALL.into_iter().find_map(|algorithm| {
        let key = spki.strip_prefix(spki_prefix(algorithm))?;
        (key.len() == algorithm.public_len()).then_some((algorithm, key))
    })
}

/// The PKCS #8 encoding of the `algorithm` secret `seed`.
pub(crate) fn pkcs8(algorithm: Algorithm, seed: &[u8]) -> Vec<u8> {
    [pkcs8_prefix(algorithm), seed].concat()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{pkcs8, spki, spki_key};
    use crate::Algorithm;

    #[test]
    fn the_encodings_have_their_fixed_lengths() {
        assert_eq!(spki(Algorithm::Ed25519, &[7; 32]).len(), 44, "SPKI");
        let private = pkcs8(Algorithm::Ed25519, &[7; 32]);
        assert_eq!(private.len(), 48, "PKCS #8");
        assert_eq!(private.get(16..), Some([7; 32].as_slice()), "seed last");
    }

    proptest! {
        #[test]
        fn keys_roundtrip(key in any::<[u8; 32]>()) {
            let encoded = spki(Algorithm::Ed25519, &key);
            prop_assert_eq!(spki_key(&encoded), Some((Algorithm::Ed25519, key.as_slice())));
        }

        #[test]
        fn anything_else_is_rejected(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            if let Some((algorithm, key)) = spki_key(&bytes) {
                prop_assert_eq!(spki(algorithm, key), bytes);
            }
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::{spki, spki_key};
    use crate::Algorithm;

    /// Parsing never panics, and accepts exactly what [`spki`] writes.
    #[kani::proof]
    #[kani::unwind(50)]
    fn spki_parsing_is_total_and_exact() {
        let bytes: [u8; 46] = kani::any();
        let len: usize = kani::any_where(|len| *len <= bytes.len());
        if let Some(input) = bytes.get(..len)
            && let Some((algorithm, key)) = spki_key(input)
        {
            assert!(key.len() == algorithm.public_len(), "full length");
            assert!(
                spki(algorithm, key) == input,
                "only canonical encodings parse"
            );
        }
    }

    /// Every key survives the encoding.
    #[kani::proof]
    #[kani::unwind(50)]
    fn spki_roundtrips() {
        let key: [u8; 32] = kani::any();
        let encoded = spki(Algorithm::Ed25519, &key);
        assert!(
            spki_key(&encoded) == Some((Algorithm::Ed25519, key.as_slice())),
            "roundtrip"
        );
    }
}
