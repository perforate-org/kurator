//! Signed records: a hash chain over the log, signed batch by batch.
//!
//! A signed record's frame payload is the caller's payload followed by a
//! seal, which ends in its kind:
//!
//! ```text
//! linked: payload | prev digest [32] | 0
//! signed: payload | prev digest [32] | signature | algorithm u8 | 1
//! ```
//!
//! `prev` is the [`Digest`] of the record before, so deleting, reordering
//! or replacing a record breaks the chain. The record's own digest is
//! BLAKE3 over `lsn | prev | payload`. The last record of every synced
//! batch is signed over its digest by the key of the node that wrote the
//! log. Through the chain, that one signature covers every record of the
//! batch. The signature is as long as its algorithm's signatures, and the
//! algorithm's id follows it, so a log signed with one algorithm stays
//! readable after a node moves to another.

use std::fmt;

use oleaph_crypto::{Algorithm, Signature, SigningKey, VerifyingKey};

/// The most bytes a seal adds to a record.
pub(crate) const MAX_SEAL_LEN: usize = signed_len(Algorithm::MAX_SIGNATURE_LEN);

const DIGEST_LEN: usize = 32;
const LINKED_LEN: usize = DIGEST_LEN + 1;
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
    /// The signature does not match the record, or is by another
    /// algorithm than the log's key.
    #[error("record signature does not match")]
    Signature,
    /// The record is signed with an algorithm this build does not know.
    #[error("record is signed with unknown algorithm {0}")]
    Algorithm(u8),
    /// A batch ends without the signed record that closes it, in a place
    /// where a crash cannot have cut it short.
    #[error("batch is not signed")]
    Unsigned,
}

/// The seal of one record, as it is written after the payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Seal {
    /// Links the record to the one before it; a later record of the same
    /// batch is signed.
    Linked([u8; LINKED_LEN]),
    /// Links the record and signs it, closing its batch.
    Signed(Vec<u8>),
}

impl Seal {
    pub(crate) fn as_bytes(&self) -> &[u8] {
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
        let signature = key.sign(&digest.0);
        let seal = [
            self.0.0.as_slice(),
            signature.as_bytes(),
            &[signature.algorithm().id(), SIGNED],
        ]
        .concat();
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
        let split = split(record)?;
        if split.prev != self.0 {
            return Err(SealDamage::Chain);
        }
        let digest = record_digest(lsn, split.prev, split.payload);
        let opened = match split.signature {
            None => Opened::Linked(split.payload),
            Some(signature) if key.verify(&digest.0, &signature) => Opened::Signed(split.payload),
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
    signature: Option<Signature>,
}

/// The length of a signed seal whose signature is `signature_len` long.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "signatures are a few kilobytes at most, far from overflowing"
)]
const fn signed_len(signature_len: usize) -> usize {
    DIGEST_LEN + signature_len + 2
}

/// Splits a sealed record into payload, previous digest and signature.
fn split(record: &[u8]) -> Result<Split<'_>, SealDamage> {
    let (kind, rest) = record.split_last().ok_or(SealDamage::Missing)?;
    let (seal, algorithm) = match *kind {
        LINKED => (LINKED_LEN.checked_sub(1), None),
        SIGNED => {
            let (id, _) = rest.split_last().ok_or(SealDamage::Missing)?;
            let algorithm = Algorithm::from_id(*id).ok_or(SealDamage::Algorithm(*id))?;
            (
                signed_len(algorithm.signature_len()).checked_sub(1),
                Some(algorithm),
            )
        }
        _ => return Err(SealDamage::Missing),
    };
    let (payload, seal) = seal
        .and_then(|len| rest.len().checked_sub(len))
        .and_then(|at| rest.split_at_checked(at))
        .ok_or(SealDamage::Missing)?;
    let (prev, signature) = seal
        .split_first_chunk::<DIGEST_LEN>()
        .ok_or(SealDamage::Missing)?;
    let signature = match algorithm {
        Some(algorithm) => Some(
            signature
                .split_last()
                .and_then(|(_, bytes)| Signature::from_parts(algorithm, bytes))
                .ok_or(SealDamage::Missing)?,
        ),
        None => None,
    };
    Ok(Split {
        payload,
        prev: Digest(*prev),
        signature,
    })
}

fn record_digest(lsn: u64, prev: Digest, payload: &[u8]) -> Digest {
    let digest = blake3::Hasher::new_derive_key("oleaph-wal 2026-11 record digest")
        .update(&lsn.to_le_bytes())
        .update(&prev.0)
        .update(payload)
        .finalize();
    Digest(*digest.as_bytes())
}

#[cfg(kani)]
mod proofs {
    use oleaph_crypto::Algorithm;

    use super::{LINKED_LEN, SealDamage, signed_len, split};

    /// Splitting a record never panics, and finds a seal exactly when the
    /// record ends in a known kind (and, when signed, a known algorithm)
    /// and is long enough to hold that seal, with the payload in front of
    /// it. BLAKE3 and the signatures are beyond a bounded model checker;
    /// the unit tests check them.
    #[kani::proof]
    #[kani::unwind(130)]
    fn split_is_total() {
        let record: [u8; 100] = kani::any();
        let len: usize = kani::any_where(|len| *len <= record.len());
        let record = &record[..len];
        let algorithm = len
            .checked_sub(2)
            .and_then(|at| record.get(at))
            .and_then(|id| Algorithm::from_id(*id));
        let seal_len = match record.last() {
            Some(0) => Some(LINKED_LEN),
            Some(1) => algorithm.map(|algorithm| signed_len(algorithm.signature_len())),
            _ => None,
        };
        match (split(record), seal_len) {
            (Ok(split), Some(seal_len)) => {
                assert_eq!(split.payload.len() + seal_len, len, "payload is the rest");
                assert_eq!(
                    split.signature.map(|signature| signature.algorithm()),
                    algorithm.filter(|_| record.last() == Some(&1)),
                    "signed kind"
                );
            }
            (Err(SealDamage::Missing), Some(seal_len)) => {
                assert!(len < seal_len, "only short records have no seal");
            }
            (Err(SealDamage::Algorithm(_)), None) => {
                assert!(record.last() == Some(&1), "only signed seals name one");
            }
            (Err(_), None) => {}
            (Ok(_), None) => panic!("a seal of no known kind"),
            (Err(_), Some(_)) => panic!("a whole seal that does not split"),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use oleaph_crypto::{Algorithm, SigningKey};

    use super::{Digest, Head, Integrity, LINKED_LEN, Opened, Protection, SealDamage, signed_len};

    fn key() -> SigningKey {
        SigningKey::from_seed(Algorithm::Ed25519, [7; 32])
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
        assert_eq!(second.len(), 6 + 32 + 64 + 2, "signed seal length");
        assert_eq!(signed_len(64), 98, "signed seal length");
        assert_eq!(second.get(second.len() - 2), Some(&1), "Ed25519 id");
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
        let stranger = SigningKey::from_seed(Algorithm::Ed25519, [8; 32]).verifying_key();
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
        let mut algorithm = record.clone();
        let at = algorithm.len() - 2;
        algorithm[at] = 0;
        assert_eq!(
            head.open(&verifying, 1, &algorithm),
            Err(SealDamage::Algorithm(0)),
            "algorithm"
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
    fn digests_print_as_hex() {
        let digest = Digest::from_bytes([0xAB; 32]);
        assert_eq!(digest.to_string(), "ab".repeat(32), "display");
        assert_eq!(format!("{digest:?}"), format!("Digest({digest})"), "debug");
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
