//! CRC-32C (Castagnoli) over the parts of a frame.

/// Checksum of `parts`, as if they were one contiguous buffer.
#[cfg(not(kani))]
pub(crate) fn crc32c(parts: &[&[u8]]) -> u32 {
    parts
        .iter()
        .fold(0, |crc, part| crc32c::crc32c_append(crc, part))
}

/// Checksum of `parts` with the bitwise reference algorithm; a unit test
/// checks that it agrees with the `crc32c` crate.
#[cfg(test)]
pub(crate) fn reference(parts: &[&[u8]]) -> u32 {
    const POLYNOMIAL: u32 = 0x82F6_3B78;
    let mut crc = u32::MAX;
    for byte in parts.iter().flat_map(|part| part.iter()) {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLYNOMIAL & mask);
        }
    }
    !crc
}

/// Stand-in checksum for proofs.
///
/// Kani cannot model the CPU feature detection and SIMD of the `crc32c`
/// crate, and a symbolic CRC makes the solver time out. Proofs are about
/// framing (bounds, lengths, round trips), which hold for any deterministic
/// checksum, so they run against this cheap one. CRC-32C itself is checked
/// by the unit tests.
#[cfg(kani)]
pub(crate) use proof_checksum as crc32c;

#[cfg(kani)]
fn proof_checksum(parts: &[&[u8]]) -> u32 {
    parts
        .iter()
        .flat_map(|part| part.iter())
        .fold(0, |sum, byte| sum.rotate_left(5) ^ u32::from(*byte))
}

#[cfg(test)]
mod tests {
    use super::{crc32c, reference};

    #[test]
    fn reference_matches_the_crc32c_crate() {
        let data: Vec<u8> = (0..=255).collect();
        let (left, right) = data.split_at(100);
        assert_eq!(
            reference(&<[&[u8]; 2]>::from((left, right))),
            crc32c(&[&data]),
            "split input"
        );
        assert_eq!(reference(&[b"123456789"]), 0xE306_9283, "check value");
        assert_eq!(reference(&[]), 0, "empty input");
    }
}
