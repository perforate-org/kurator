//! The two DER structures TLS needs for an Ed25519 key, written and read
//! byte for byte: an Ed25519 key has one encoding, so a parser is a
//! prefix check.
//!
//! ```text
//! SubjectPublicKeyInfo (RFC 8410)   30 2a 30 05 06 03 2b 65 70 03 21 00 | key [32]
//! OneAsymmetricKey v1 (RFC 8410)    30 2e 02 01 00 30 05 06 03 2b 65 70 04 22 04 20 | seed [32]
//! ```

/// The bytes of an Ed25519 `SubjectPublicKeyInfo` before the key.
const SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// The bytes of an Ed25519 PKCS #8 private key before the seed.
const PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// The `SubjectPublicKeyInfo` of the Ed25519 public key `key`: what a raw
/// public key "certificate" holds (RFC 7250).
pub(crate) fn spki(key: [u8; 32]) -> Vec<u8> {
    [SPKI_PREFIX.as_slice(), key.as_slice()].concat()
}

/// The Ed25519 public key in `spki`, or `None` when `spki` is anything but
/// what [`spki`] writes.
pub(crate) fn spki_key(spki: &[u8]) -> Option<[u8; 32]> {
    spki.strip_prefix(SPKI_PREFIX.as_slice())?.try_into().ok()
}

/// The PKCS #8 encoding of the Ed25519 secret `seed`, as TLS stacks load
/// private keys.
pub(crate) fn pkcs8(seed: [u8; 32]) -> Vec<u8> {
    [PKCS8_PREFIX.as_slice(), seed.as_slice()].concat()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{pkcs8, spki, spki_key};

    #[test]
    fn the_encodings_have_their_fixed_lengths() {
        assert_eq!(spki([7; 32]).len(), 44, "SPKI");
        assert_eq!(pkcs8([7; 32]).len(), 48, "PKCS #8");
        assert_eq!(&pkcs8([7; 32])[16..], &[7; 32], "seed last");
    }

    proptest! {
        #[test]
        fn keys_roundtrip(key in any::<[u8; 32]>()) {
            prop_assert_eq!(spki_key(&spki(key)), Some(key));
        }

        #[test]
        fn anything_else_is_rejected(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            if let Some(key) = spki_key(&bytes) {
                prop_assert_eq!(spki(key), bytes);
            }
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::{spki, spki_key};

    /// Parsing never panics, and accepts exactly what [`spki`] writes.
    #[kani::proof]
    #[kani::unwind(50)]
    fn spki_parsing_is_total_and_exact() {
        let bytes: [u8; 46] = kani::any();
        let len: usize = kani::any_where(|len| *len <= bytes.len());
        if let Some(input) = bytes.get(..len)
            && let Some(key) = spki_key(input)
        {
            assert!(spki(key) == input, "only canonical encodings parse");
        }
    }

    /// Every key survives the encoding.
    #[kani::proof]
    #[kani::unwind(50)]
    fn spki_roundtrips() {
        let key: [u8; 32] = kani::any();
        assert!(spki_key(&spki(key)) == Some(key), "roundtrip");
    }
}
