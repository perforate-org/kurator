//! Signed records: a hash chain over the log, signed batch by batch.
//!
//! A signed record's frame payload is the caller's payload followed by a
//! seal, which ends in its kind:
//!
//! ```text
//! linked: payload | prev digest [32] | 0
//! signed: payload | prev digest [32] | signature [64] | 1
//! ```
//!
//! `prev` is the [`Digest`] of the record before, so deleting, reordering
//! or replacing a record breaks the chain. The record's own digest is
//! BLAKE3 over `lsn | prev | payload`. The last record of every synced
//! batch is signed: Ed25519 over its digest by the key of the node that
//! wrote the log. Through the chain, that one signature covers every
//! record of the batch.

use std::fmt;

/// The most bytes a seal adds to a record.
pub(crate) const MAX_SEAL_LEN: usize = SIGNED_LEN;

const DIGEST_LEN: usize = 32;
const SIGNATURE_LEN: usize = 64;
const LINKED_LEN: usize = DIGEST_LEN + 1;
const SIGNED_LEN: usize = DIGEST_LEN + SIGNATURE_LEN + 1;
const LINKED: u8 = 0;
const SIGNED: u8 = 1;

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
        write!(f, "VerifyingKey({self})")
    }
}

/// Lowercase hex, as [`Digest`] shows itself.
impl fmt::Display for VerifyingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Digest(self.to_bytes()).fmt(f)
    }
}

/// How the records of a log are protected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Integrity {
    /// A CRC-32C per record: detects torn and damaged writes.
    #[default]
    Checksum,
    /// A CRC-32C and the digest of the record before per record, and a
    /// signature by this key per synced batch: also detects records that
    /// were changed, removed or reordered, and shows which node wrote them.
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
    /// A batch ends without the signed record that closes it, in a place
    /// where a crash cannot have cut it short.
    #[error("batch is not signed")]
    Unsigned,
}

/// The seal of one record, as it is written after the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Seal {
    /// Links the record to the one before it; a later record of the same
    /// batch is signed.
    Linked([u8; LINKED_LEN]),
    /// Links the record and signs it, closing its batch.
    Signed([u8; SIGNED_LEN]),
}

impl Seal {
    pub(crate) const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Linked(bytes) => bytes,
            Self::Signed(bytes) => bytes,
        }
    }
}

/// A record whose seal checked out, and whether it closes its batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Opened<'a> {
    /// Its batch goes on: the record counts once a later one is signed.
    Linked(&'a [u8]),
    /// It is signed, and with it every linked record before it.
    Signed(&'a [u8]),
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

    /// Links `payload` as record `lsn`, which follows this head, and
    /// returns the seal and the head after it.
    pub(crate) fn link(self, lsn: u64, payload: &[u8]) -> (Seal, Self) {
        let mut seal = [LINKED; LINKED_LEN];
        fill(&mut seal, &[&self.0.0]);
        (
            Seal::Linked(seal),
            Self(record_digest(lsn, self.0, payload)),
        )
    }

    /// Links and signs `payload` as record `lsn`, which follows this head,
    /// and returns the seal and the head after it.
    pub(crate) fn sign(self, key: &SigningKey, lsn: u64, payload: &[u8]) -> (Seal, Self) {
        let digest = record_digest(lsn, self.0, payload);
        let mut seal = [SIGNED; SIGNED_LEN];
        fill(&mut seal, &[&self.0.0, &sign(key, digest)]);
        (Seal::Signed(seal), Self(digest))
    }

    /// Checks the sealed record `lsn`, which must follow this head, and
    /// returns its payload and the head after it. A linked record is only
    /// checked against the chain; the signature that closes its batch
    /// vouches for it.
    pub(crate) fn open<'a>(
        self,
        key: &VerifyingKey,
        lsn: u64,
        record: &'a [u8],
    ) -> Result<(Opened<'a>, Self), SealDamage> {
        let split = split(record).ok_or(SealDamage::Missing)?;
        if split.prev != self.0 {
            return Err(SealDamage::Chain);
        }
        let digest = record_digest(lsn, split.prev, split.payload);
        let opened = match split.signature {
            None => Opened::Linked(split.payload),
            Some(signature) if verify(key, digest, &signature) => Opened::Signed(split.payload),
            Some(_) => return Err(SealDamage::Signature),
        };
        Ok((opened, Self(digest)))
    }
}

/// Copies `parts` to the front of `seal`, leaving its kind byte at the end.
fn fill(seal: &mut [u8], parts: &[&[u8]]) {
    for (slot, byte) in seal
        .iter_mut()
        .zip(parts.iter().flat_map(|part| part.iter()))
    {
        *slot = *byte;
    }
}

/// A sealed record taken apart.
#[derive(Debug, PartialEq, Eq)]
struct Split<'a> {
    payload: &'a [u8],
    prev: Digest,
    /// Present on the record that closes a batch.
    signature: Option<[u8; SIGNATURE_LEN]>,
}

/// Splits a sealed record into payload, previous digest and signature.
fn split(record: &[u8]) -> Option<Split<'_>> {
    let (kind, rest) = record.split_last()?;
    let seal_len = match *kind {
        LINKED => LINKED_LEN,
        SIGNED => SIGNED_LEN,
        _ => return None,
    };
    let (payload, seal) =
        rest.split_at_checked(rest.len().checked_sub(seal_len.checked_sub(1)?)?)?;
    let (prev, signature) = seal.split_first_chunk::<DIGEST_LEN>()?;
    let signature = match *kind {
        SIGNED => Some(signature.try_into().ok()?),
        _ => None,
    };
    Some(Split {
        payload,
        prev: Digest(*prev),
        signature,
    })
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
    use super::{LINKED_LEN, SIGNED_LEN, split};

    /// Splitting a record never panics, and finds a seal exactly when the
    /// record ends in a known kind and is long enough to hold that seal,
    /// with the payload in front of it. BLAKE3 and Ed25519 are beyond a
    /// bounded model checker; the unit tests check them.
    #[kani::proof]
    #[kani::unwind(130)]
    fn split_is_total() {
        let record: [u8; 100] = kani::any();
        let len: usize = kani::any_where(|len| *len <= record.len());
        let record = &record[..len];
        let seal_len = match record.last() {
            Some(0) => Some(LINKED_LEN),
            Some(1) => Some(SIGNED_LEN),
            _ => None,
        };
        match (split(record), seal_len) {
            (Some(split), Some(seal_len)) => {
                assert_eq!(split.payload.len() + seal_len, len, "payload is the rest");
                assert_eq!(
                    split.signature.is_some(),
                    seal_len == SIGNED_LEN,
                    "signed kind"
                );
            }
            (None, Some(seal_len)) => assert!(len < seal_len, "only short records have no seal"),
            (None, None) => {}
            (Some(_), None) => panic!("a seal of no known kind"),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{
        Digest, Head, Integrity, LINKED_LEN, Opened, Protection, SIGNED_LEN, SealDamage, SigningKey,
    };

    fn key() -> SigningKey {
        SigningKey::from_seed([7; 32])
    }

    fn signed(head: Head, lsn: u64, payload: &[u8]) -> (Vec<u8>, Head) {
        let (seal, next) = head.sign(&key(), lsn, payload);
        ([payload, seal.as_bytes()].concat(), next)
    }

    fn linked(head: Head, lsn: u64, payload: &[u8]) -> (Vec<u8>, Head) {
        let (seal, next) = head.link(lsn, payload);
        ([payload, seal.as_bytes()].concat(), next)
    }

    #[test]
    fn sealed_records_open_to_their_payloads() {
        let head = Head::new(Digest::GENESIS);
        let (first, middle) = linked(head, 1, b"first");
        let (second, last) = signed(middle, 2, b"second");
        assert_eq!(first.len(), 5 + LINKED_LEN, "linked seal length");
        assert_eq!(second.len(), 6 + SIGNED_LEN, "signed seal length");
        let verifying = key().verifying_key();
        assert_eq!(
            head.open(&verifying, 1, &first),
            Ok((Opened::Linked(b"first".as_slice()), middle)),
            "linked"
        );
        assert_eq!(
            middle.open(&verifying, 2, &second),
            Ok((Opened::Signed(b"second".as_slice()), last)),
            "signed"
        );
        assert_ne!(middle.digest(), Digest::GENESIS, "the head moves");
        assert_ne!(last.digest(), middle.digest(), "and moves again");
    }

    #[test]
    fn linking_and_signing_move_the_head_alike() {
        let head = Head::new(Digest::GENESIS);
        assert_eq!(
            head.link(4, b"payload").1,
            head.sign(&key(), 4, b"payload").1,
            "the digest does not depend on the signature"
        );
    }

    #[test]
    fn the_wrong_predecessor_breaks_the_chain() {
        let (record, _) = linked(Head::new(Digest::GENESIS), 1, b"payload");
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
        let (record, _) = signed(head, 1, b"payload");
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
    }

    #[test]
    fn short_records_and_unknown_kinds_have_no_seal() {
        let head = Head::new(Digest::GENESIS);
        let verifying = key().verifying_key();
        let (record, _) = signed(head, 1, b"");
        let short = &record[1..];
        assert_eq!(
            head.open(&verifying, 1, short),
            Err(SealDamage::Missing),
            "short"
        );
        let mut unknown = record.clone();
        *unknown.last_mut().unwrap() = 2;
        assert_eq!(
            head.open(&verifying, 1, &unknown),
            Err(SealDamage::Missing),
            "kind"
        );
        assert_eq!(
            head.open(&verifying, 1, &[]),
            Err(SealDamage::Missing),
            "empty"
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
        fn any_change_to_a_signed_record_is_detected(payload in proptest::collection::vec(any::<u8>(), 0..32), byte in any::<prop::sample::Index>(), bit in 0_u32..8) {
            let head = Head::new(Digest::GENESIS);
            let (mut record, _) = signed(head, 3, &payload);
            let byte = byte.index(record.len());
            record[byte] ^= 1_u8.rotate_left(bit);
            // Even a kind byte flipped to "linked" fails: the chain then
            // reads the signature as the previous digest.
            prop_assert!(head.open(&key().verifying_key(), 3, &record).is_err(), "flipped bit {bit} of byte {byte}");
        }
    }
}
