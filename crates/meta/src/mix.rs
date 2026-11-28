//! Leave for one transaction to span two namespaces of different
//! assurances, which the operator gives each pair explicitly.
//!
//! Such a transaction is only as safe as its weaker side: the crash fault
//! tolerant side trusts its operator, so its part may be lost or forged
//! by them, even as the Byzantine fault tolerant side decides it.

use std::fmt;

use oleaph_codec::{DecodeError, Reader};
use oleaph_shard::NamespaceId;

/// The prefix of every leave's key.
pub(crate) const MIXES: &[u8] = b"mix/";
/// The first key after every leave's.
pub(crate) const MIXES_END: &[u8] = b"mix0";

/// Two namespaces that one transaction may span, the lower id first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mix {
    low: NamespaceId,
    high: NamespaceId,
}

impl Mix {
    /// The pair of `one` and `other`, in either order; `None` when they
    /// are the same namespace.
    #[must_use]
    pub fn new(one: NamespaceId, other: NamespaceId) -> Option<Self> {
        let (low, high) = if one < other {
            (one, other)
        } else {
            (other, one)
        };
        (low != high).then_some(Self { low, high })
    }

    /// Both namespaces, the lower id first.
    #[must_use]
    pub const fn namespaces(self) -> (NamespaceId, NamespaceId) {
        (self.low, self.high)
    }

    /// The key of its record: `mix/<low u32 BE><high u32 BE>`.
    pub(crate) fn key(self) -> Vec<u8> {
        [
            MIXES,
            &self.low.get().to_be_bytes(),
            &self.high.get().to_be_bytes(),
        ]
        .concat()
    }

    /// The pair whose record has the key `key`.
    pub(crate) fn from_key(key: &[u8]) -> Result<Self, DecodeError> {
        let ids = key.strip_prefix(MIXES).ok_or(DecodeError::Invalid)?;
        let (low, high) = ids.split_first_chunk::<4>().ok_or(DecodeError::Invalid)?;
        let high: [u8; 4] = high.try_into().map_err(|_| DecodeError::Invalid)?;
        Self::ordered(
            NamespaceId::new(u32::from_be_bytes(*low)),
            NamespaceId::new(u32::from_be_bytes(high)),
        )
    }

    /// Writes `low u32 | high u32`.
    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.low.get().to_le_bytes());
        out.extend_from_slice(&self.high.get().to_le_bytes());
    }

    /// Reads what [`Mix::encode`] wrote off the front of `reader`.
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let low = NamespaceId::new(reader.read()?);
        Self::ordered(low, NamespaceId::new(reader.read()?))
    }

    /// The pair `low` and `high`, which must be in that order.
    fn ordered(low: NamespaceId, high: NamespaceId) -> Result<Self, DecodeError> {
        if low < high {
            Ok(Self { low, high })
        } else {
            Err(DecodeError::Invalid)
        }
    }
}

/// `ns1+ns4`.
impl fmt::Display for Mix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}+{}", self.low, self.high)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use oleaph_codec::{DecodeError, Reader};
    use oleaph_shard::NamespaceId;
    use proptest::prelude::*;

    use super::Mix;

    pub(crate) fn mixes() -> impl Strategy<Value = Mix> {
        (any::<u32>(), any::<u32>()).prop_filter_map("two namespaces", |(one, other)| {
            Mix::new(NamespaceId::new(one), NamespaceId::new(other))
        })
    }

    #[test]
    fn a_pair_is_two_namespaces_in_order() {
        let (one, four) = (NamespaceId::new(1), NamespaceId::new(4));
        assert_eq!(Mix::new(four, one), Mix::new(one, four), "either order");
        assert_eq!(Mix::new(one, one), None, "not one namespace");
        let mix = Mix::new(four, one).unwrap();
        assert_eq!(mix.namespaces(), (one, four), "lower first");
        assert_eq!(mix.to_string(), "ns1+ns4", "display");
        assert_eq!(Mix::from_key(&mix.key()), Ok(mix), "by its key");
        let mut backwards = super::MIXES.to_vec();
        backwards.extend_from_slice(&4_u32.to_be_bytes());
        backwards.extend_from_slice(&1_u32.to_be_bytes());
        assert_eq!(
            Mix::from_key(&backwards),
            Err(DecodeError::Invalid),
            "order"
        );
        assert_eq!(
            Mix::from_key(&backwards[..9]),
            Err(DecodeError::Invalid),
            "short"
        );
        assert_eq!(Mix::from_key(b"mox/"), Err(DecodeError::Invalid), "prefix");
        let mut same = Vec::new();
        same.extend_from_slice(&[1, 0, 0, 0, 1, 0, 0, 0]);
        assert_eq!(
            Mix::decode(&mut Reader::new(&same)),
            Err(DecodeError::Invalid),
            "one namespace twice"
        );
    }

    proptest! {
        #[test]
        fn pairs_roundtrip(mix in mixes()) {
            let mut out = Vec::new();
            mix.encode(&mut out);
            let mut reader = Reader::new(&out);
            prop_assert_eq!(Mix::decode(&mut reader), Ok(mix));
            prop_assert!(reader.finish().is_ok());
            prop_assert_eq!(Mix::from_key(&mix.key()), Ok(mix));
        }
    }
}

#[cfg(kani)]
mod proofs {
    use oleaph_codec::Reader;

    use super::Mix;

    /// Decoding a pair never panics, and a decoded pair encodes back to
    /// exactly the bytes it was read from.
    #[kani::proof]
    fn pairs_decode_totally_and_roundtrip() {
        let buf: [u8; 8] = kani::any();
        let mut reader = Reader::new(&buf);
        if let Ok(decoded) = Mix::decode(&mut reader) {
            let mut out = Vec::new();
            decoded.encode(&mut out);
            assert_eq!(out.as_slice(), &buf[..], "roundtrip");
        }
    }
}
