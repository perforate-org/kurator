//! Bounds-checked little-endian reads.

/// Reads `N` bytes at `at`, or `None` when `buf` is too short.
pub(crate) fn array_at<const N: usize>(buf: &[u8], at: usize) -> Option<[u8; N]> {
    buf.get(at..at.checked_add(N)?)?.try_into().ok()
}

/// Reads a little-endian `u16` at `at`.
pub(crate) fn u16_at(buf: &[u8], at: usize) -> Option<u16> {
    array_at(buf, at).map(u16::from_le_bytes)
}

/// Reads a little-endian `u32` at `at`.
pub(crate) fn u32_at(buf: &[u8], at: usize) -> Option<u32> {
    array_at(buf, at).map(u32::from_le_bytes)
}

/// Reads a little-endian `u64` at `at`.
pub(crate) fn u64_at(buf: &[u8], at: usize) -> Option<u64> {
    array_at(buf, at).map(u64::from_le_bytes)
}
