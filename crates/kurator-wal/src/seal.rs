//! Signed records: a hash chain over the log, signed record by record.
//!
//! A signed record's frame payload is the caller's payload followed by a
//! seal:
//!
//! ```text
//! payload | prev digest [32] | signature [64]
//! ```
//!
//! `prev` is the [`Digest`] of the record before, so deleting, reordering
//! or replacing a record breaks the chain. The record's own digest is
//! BLAKE3 over `lsn | prev | payload`, and the signature is Ed25519 over
//! that digest by the key of the node that wrote the log.

use std::fmt;

/// Bytes a seal adds to a record.
pub(crate) const SEAL_LEN: usize = DIGEST_LEN + SIGNATURE_LEN;

const DIGEST_LEN: usize = 32;
const SIGNATURE_LEN: usize = 64;

/// The digest that links a record to the one before it.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest([u8; DIGEST_LEN]);

impl Digest {
    /// The digest that comes before the first record of a new log.
    pub const GENESIS: Self = Self([0; DIGEST_LEN]);

    /// Wraps raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; DIGEST_LEN]) -> Self {
        Self(bytes)
    }

    /// The raw bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; DIGEST_LEN] {
        self.0
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({self})")
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

/// The key a node signs its log with.
///
/// `Debug` shows only the public half. The secret is zeroed when the key is
/// dropped.
#[derive(Clone, PartialEq, Eq)]
pub struct SigningKey(Box<ed25519_dalek::SigningKey>);

impl SigningKey {
    /// The key whose secret is `seed`. The seed must come from a
    /// cryptographically secure source and stay secret.
    #[must_use]
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self(Box::new(ed25519_dalek::SigningKey::from_bytes(&seed)))
    }

    /// The public half, which checks this key's signatures.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        VerifyingKey(Box::new(self.0.verifying_key()))
    }
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SigningKey")
            .field(&self.verifying_key())
            .finish()
    }
}

/// The public key that checks a node's signatures.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct VerifyingKey(Box<ed25519_dalek::VerifyingKey>);

impl VerifyingKey {
    /// The key encoded as `bytes`, or `None` when they are not a valid
    /// point on the curve.
    #[must_use]
    pub fn from_bytes(bytes: &[u8; 32]) -> Option<Self> {
        ed25519_dalek::VerifyingKey::from_bytes(bytes)
            .ok()
            .map(|key| Self(Box::new(key)))
    }

    /// The encoded key.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0.to_bytes()
    }
}

impl fmt::Debug for VerifyingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VerifyingKey({})", Digest(self.to_bytes()))
    }
}

/// How the records of a log are protected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Integrity {
    /// A CRC-32C per record: detects torn and damaged writes.
    #[default]
    Checksum,
    /// A CRC-32C, the digest of the record before, and a signature by this
    /// key per record: also detects records that were changed, removed or
    /// reordered, and shows which node wrote them.
    Signed(SigningKey),
}

impl Integrity {
    /// The protection this asks for, without the key.
    #[must_use]
    pub const fn protection(&self) -> Protection {
        match self {
            Self::Checksum => Protection::Checksum,
            Self::Signed(_) => Protection::Signature,
        }
    }
}

/// How the records of a log are protected, as its segment headers say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Protection {
    /// See [`Integrity::Checksum`].
    #[error("checksummed")]
    Checksum,
    /// See [`Integrity::Signed`].
    #[error("signed")]
    Signature,
}

/// Why a signed record does not check out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SealDamage {
    /// The record is too short to hold a seal.
    #[error("record has no seal")]
    Missing,
    /// The record, or a segment header, names a different record before
    /// it: records were removed, reordered or replaced.
    #[error("record does not follow the record before it")]
    Chain,
    /// The signature does not match the record.
    #[error("record signature does not match")]
    Signature,
}

/// The chain of a signed log at one point: the digest of the last record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Head(Digest);

impl Head {
    pub(crate) const fn new(digest: Digest) -> Self {
        Self(digest)
    }

    pub(crate) const fn digest(self) -> Digest {
        self.0
    }

    /// Seals `payload` as record `lsn`, which follows this head, and
    /// returns the seal and the head after it.
    pub(crate) fn seal(self, key: &SigningKey, lsn: u64, payload: &[u8]) -> ([u8; SEAL_LEN], Self) {
        let digest = record_digest(lsn, self.0, payload);
        let signature = sign(key, digest);
        let mut seal = [0; SEAL_LEN];
        for (slot, byte) in seal.iter_mut().zip(self.0.0.iter().chain(signature.iter())) {
            *slot = *byte;
        }
        (seal, Self(digest))
    }

    /// Checks the signed record `lsn`, which must follow this head, and
    /// returns its payload and the head after it.
    pub(crate) fn open<'a>(
        self,
        key: &VerifyingKey,
        lsn: u64,
        record: &'a [u8],
    ) -> Result<(&'a [u8], Self), SealDamage> {
        let (payload, prev, signature) = split(record).ok_or(SealDamage::Missing)?;
        if prev != self.0 {
            return Err(SealDamage::Chain);
        }
        let digest = record_digest(lsn, prev, payload);
        if !verify(key, digest, &signature) {
            return Err(SealDamage::Signature);
        }
        Ok((payload, Self(digest)))
    }
}

/// Splits a signed record into payload, previous digest and signature.
fn split(record: &[u8]) -> Option<(&[u8], Digest, [u8; SIGNATURE_LEN])> {
    let (payload, seal) = record.split_at_checked(record.len().checked_sub(SEAL_LEN)?)?;
    let (prev, signature) = seal.split_first_chunk::<DIGEST_LEN>()?;
    Some((payload, Digest(*prev), signature.try_into().ok()?))
}

fn record_digest(lsn: u64, prev: Digest, payload: &[u8]) -> Digest {
    let digest = blake3::Hasher::new_derive_key("kurator-wal 2026-11 record digest")
        .update(&lsn.to_le_bytes())
        .update(&prev.0)
        .update(payload)
        .finalize();
    Digest(*digest.as_bytes())
}

fn sign(key: &SigningKey, digest: Digest) -> [u8; SIGNATURE_LEN] {
    use ed25519_dalek::Signer as _;
    key.0.sign(&digest.0).to_bytes()
}

fn verify(key: &VerifyingKey, digest: Digest, signature: &[u8; SIGNATURE_LEN]) -> bool {
    key.0
        .verify_strict(&digest.0, &ed25519_dalek::Signature::from_bytes(signature))
        .is_ok()
}

#[cfg(kani)]
mod proofs {
    use super::{SEAL_LEN, split};

    /// Splitting a record never panics, and finds a seal exactly when the
    /// record is long enough to hold one, with the payload in front of it.
    /// BLAKE3 and Ed25519 are beyond a bounded model checker; the unit
    /// tests check them.
    #[kani::proof]
    #[kani::unwind(130)]
    fn split_is_total() {
        let record: [u8; 100] = kani::any();
        let len: usize = kani::any_where(|len| *len <= record.len());
        match split(&record[..len]) {
            Some((payload, _, _)) => {
                assert_eq!(payload.len() + SEAL_LEN, len, "payload is the rest")
            }
            None => assert!(len < SEAL_LEN, "only short records have no seal"),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{Digest, Head, Integrity, Protection, SEAL_LEN, SealDamage, SigningKey};

    fn key() -> SigningKey {
        SigningKey::from_seed([7; 32])
    }

    fn sealed(head: Head, lsn: u64, payload: &[u8]) -> (Vec<u8>, Head) {
        let (seal, next) = head.seal(&key(), lsn, payload);
        ([payload, &seal].concat(), next)
    }

    #[test]
    fn a_sealed_record_opens_to_its_payload() {
        let head = Head::new(Digest::GENESIS);
        let (record, next) = sealed(head, 1, b"payload");
        assert_eq!(record.len(), 7 + SEAL_LEN, "seal length");
        assert_eq!(
            head.open(&key().verifying_key(), 1, &record),
            Ok((b"payload".as_slice(), next)),
            "opens"
        );
        assert_ne!(next.digest(), Digest::GENESIS, "the head moves");
    }

    #[test]
    fn the_wrong_predecessor_breaks_the_chain() {
        let (record, _) = sealed(Head::new(Digest::GENESIS), 1, b"payload");
        let other = Head::new(Digest::from_bytes([1; 32]));
        assert_eq!(
            other.open(&key().verifying_key(), 1, &record),
            Err(SealDamage::Chain),
            "chain"
        );
    }

    #[test]
    fn another_position_or_key_fails_the_signature() {
        let head = Head::new(Digest::GENESIS);
        let (record, _) = sealed(head, 1, b"payload");
        assert_eq!(
            head.open(&key().verifying_key(), 2, &record),
            Err(SealDamage::Signature),
            "position"
        );
        let stranger = SigningKey::from_seed([8; 32]).verifying_key();
        assert_eq!(
            head.open(&stranger, 1, &record),
            Err(SealDamage::Signature),
            "key"
        );
        assert_eq!(
            head.open(&key().verifying_key(), 1, &record[..SEAL_LEN - 1]),
            Err(SealDamage::Missing),
            "too short"
        );
    }

    #[test]
    fn integrity_names_its_protection() {
        assert_eq!(Integrity::default(), Integrity::Checksum, "default");
        assert_eq!(
            Integrity::Checksum.protection(),
            Protection::Checksum,
            "checksum"
        );
        assert_eq!(
            Integrity::Signed(key()).protection(),
            Protection::Signature,
            "signed"
        );
    }

    #[test]
    fn keys_and_digests_print_as_hex() {
        let digest = Digest::from_bytes([0xAB; 32]);
        assert_eq!(digest.to_string(), "ab".repeat(32), "display");
        assert_eq!(format!("{digest:?}"), format!("Digest({digest})"), "debug");
        let public = key().verifying_key();
        assert!(
            format!("{:?}", key()).contains(&Digest::from_bytes(public.to_bytes()).to_string()),
            "debug shows the public key"
        );
        assert_eq!(
            super::VerifyingKey::from_bytes(&public.to_bytes()),
            Some(public),
            "roundtrip"
        );
    }

    proptest! {
        #[test]
        fn any_change_to_a_sealed_record_is_detected(payload in proptest::collection::vec(any::<u8>(), 0..32), byte in any::<prop::sample::Index>(), bit in 0_u32..8) {
            let head = Head::new(Digest::GENESIS);
            let (mut record, _) = sealed(head, 3, &payload);
            let byte = byte.index(record.len());
            record[byte] ^= 1_u8.rotate_left(bit);
            prop_assert!(head.open(&key().verifying_key(), 3, &record).is_err(), "flipped bit {bit} of byte {byte}");
        }
    }
}
