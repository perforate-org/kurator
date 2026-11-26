//! Record frames: `payload len u32 | crc32c u32 | lsn u64 | payload`.

use crate::bytes;
use crate::checksum::crc32c;
use crate::error::{Error, InvalidFrame, Result};
use crate::lsn::Lsn;
use crate::seal::MAX_SEAL_LEN;

/// Size of the frame header in bytes.
pub(crate) const HEADER_LEN: usize = 16;

/// Largest payload a record can hold, in bytes.
pub const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// Largest payload a frame can hold: a record's payload and its seal.
const MAX_FRAME_PAYLOAD_LEN: usize = MAX_PAYLOAD_LEN + MAX_SEAL_LEN;

/// What the start of a buffer holds.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decoded<'a> {
    /// Nothing: the buffer is empty.
    End,
    /// A valid frame of `len` bytes.
    Record {
        lsn: Lsn,
        payload: &'a [u8],
        len: usize,
    },
    /// Bytes that are not a valid frame.
    Invalid(InvalidFrame),
}

/// Appends the frame for `payload` and then `seal` at `lsn` to `out`.
pub(crate) fn encode(lsn: Lsn, payload: &[u8], seal: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let too_large = || Error::PayloadTooLarge { len: payload.len() };
    let total = Some(payload.len())
        .filter(|len| *len <= MAX_PAYLOAD_LEN)
        .and_then(|len| len.checked_add(seal.len()))
        .filter(|len| *len <= MAX_FRAME_PAYLOAD_LEN)
        .ok_or_else(too_large)?;
    let len = u32::try_from(total).map_err(|_| too_large())?.to_le_bytes();
    let lsn = lsn.get().to_le_bytes();
    let crc = crc32c(&[&len, &lsn, payload, seal]).to_le_bytes();
    out.reserve(HEADER_LEN.saturating_add(total));
    for part in [&len[..], &crc, &lsn, payload, seal] {
        out.extend_from_slice(part);
    }
    Ok(())
}

/// Reads the frame at the start of `buf`.
pub(crate) fn decode(buf: &[u8]) -> Decoded<'_> {
    if buf.is_empty() {
        return Decoded::End;
    }
    let (Some(len), Some(crc), Some(lsn)) = (
        bytes::array_at::<4>(buf, 0),
        bytes::u32_at(buf, 4),
        bytes::array_at::<8>(buf, 8),
    ) else {
        return Decoded::Invalid(InvalidFrame::TruncatedHeader);
    };
    let Some(payload_len) = usize::try_from(u32::from_le_bytes(len))
        .ok()
        .filter(|len| *len <= MAX_FRAME_PAYLOAD_LEN)
    else {
        return Decoded::Invalid(InvalidFrame::Oversized);
    };
    let Some(payload) = buf
        .get(HEADER_LEN..)
        .and_then(|rest| rest.get(..payload_len))
    else {
        return Decoded::Invalid(InvalidFrame::TruncatedPayload);
    };
    if crc32c(&[&len, &lsn, payload]) != crc {
        return Decoded::Invalid(InvalidFrame::ChecksumMismatch);
    }
    Decoded::Record {
        lsn: Lsn::new(u64::from_le_bytes(lsn)),
        payload,
        len: HEADER_LEN.saturating_add(payload_len),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{Decoded, HEADER_LEN, decode, encode};
    use crate::error::InvalidFrame;
    use crate::lsn::Lsn;

    fn framed(lsn: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode(Lsn::new(lsn), payload, &[], &mut out).unwrap();
        out
    }

    #[test]
    fn the_largest_payload_fits() {
        let payload = vec![0; super::MAX_PAYLOAD_LEN];
        assert_eq!(super::MAX_PAYLOAD_LEN, 64 << 20, "64 MiB");
        let frame = framed(1, &payload);
        assert!(matches!(decode(&frame), Decoded::Record { .. }), "decodes");
    }

    #[test]
    fn a_seal_follows_the_payload_and_may_exceed_the_payload_limit() {
        let mut out = Vec::new();
        encode(Lsn::new(1), b"ab", b"cd", &mut out).unwrap();
        assert!(
            matches!(
                decode(&out),
                Decoded::Record {
                    payload: b"abcd",
                    ..
                }
            ),
            "payload then seal"
        );
        let payload = vec![0; super::MAX_PAYLOAD_LEN];
        let seal = [0; crate::seal::MAX_SEAL_LEN];
        out.clear();
        encode(Lsn::new(1), &payload, &seal, &mut out).unwrap();
        assert!(
            matches!(decode(&out), Decoded::Record { .. }),
            "sealed maximum"
        );
        let too_large = vec![0; super::MAX_PAYLOAD_LEN + 1];
        assert!(
            encode(Lsn::new(1), &too_large, &[], &mut out).is_err(),
            "payload above the limit"
        );
    }

    #[test]
    fn empty_buffer_is_the_end() {
        assert_eq!(decode(&[]), Decoded::End, "empty");
    }

    #[test]
    fn short_header_is_truncated() {
        let frame = framed(1, b"abc");
        assert_eq!(
            decode(&frame[..HEADER_LEN - 1]),
            Decoded::Invalid(InvalidFrame::TruncatedHeader),
            "one byte short"
        );
    }

    #[test]
    fn short_payload_is_truncated() {
        let frame = framed(1, b"abc");
        assert_eq!(
            decode(&frame[..frame.len() - 1]),
            Decoded::Invalid(InvalidFrame::TruncatedPayload),
            "one byte short"
        );
    }

    #[test]
    fn huge_length_is_oversized() {
        let mut frame = framed(1, b"abc");
        frame[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            decode(&frame),
            Decoded::Invalid(InvalidFrame::Oversized),
            "length u32::MAX"
        );
    }

    #[test]
    fn a_zeroed_header_is_not_a_record() {
        assert_eq!(
            decode(&[0; HEADER_LEN]),
            Decoded::Invalid(InvalidFrame::ChecksumMismatch),
            "preallocated zeros"
        );
    }

    proptest! {
        #[test]
        fn roundtrip(lsn: u64, payload in proptest::collection::vec(any::<u8>(), 0..512), tail in proptest::collection::vec(any::<u8>(), 0..32)) {
            let mut buf = framed(lsn, &payload);
            let len = buf.len();
            buf.extend_from_slice(&tail);
            prop_assert_eq!(decode(&buf), Decoded::Record { lsn: Lsn::new(lsn), payload: &payload, len });
        }

        #[test]
        fn any_single_bit_flip_is_detected(payload in proptest::collection::vec(any::<u8>(), 1..64), byte in any::<prop::sample::Index>(), bit in 0_u32..8) {
            let mut buf = framed(9, &payload);
            let byte = byte.index(buf.len());
            buf[byte] ^= 1_u8.rotate_left(bit);
            prop_assert!(matches!(decode(&buf), Decoded::Invalid(_)), "flipped bit {bit} of byte {byte}");
        }

        #[test]
        fn arbitrary_bytes_never_panic(buf in proptest::collection::vec(any::<u8>(), 0..64)) {
            let _: Decoded<'_> = decode(&buf);
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::{Decoded, HEADER_LEN, decode, encode};
    use crate::lsn::Lsn;

    /// Decoding never panics and never reads past the buffer.
    #[kani::proof]
    #[kani::unwind(21)]
    fn decode_is_total() {
        let buf: [u8; 20] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        if let Decoded::Record {
            payload,
            len: frame,
            ..
        } = decode(&buf[..len])
        {
            assert!(frame <= len, "frame fits the buffer");
            assert_eq!(frame, HEADER_LEN + payload.len(), "frame length");
        }
    }

    /// Whatever is encoded decodes to itself.
    #[kani::proof]
    #[kani::unwind(21)]
    fn encode_then_decode_roundtrips() {
        let payload: [u8; 2] = kani::any();
        let len: usize = kani::any_where(|len| *len <= payload.len());
        let lsn = Lsn::new(kani::any());
        let mut out = Vec::new();
        assert!(
            encode(lsn, &payload[..len], &[], &mut out).is_ok(),
            "small payload"
        );
        assert_eq!(
            decode(&out),
            Decoded::Record {
                lsn,
                payload: &payload[..len],
                len: out.len()
            },
            "roundtrip"
        );
    }
}
