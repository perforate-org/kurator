//! A shard's state as chunks, and the root that names it.
//!
//! The state is encoded as [`ShardState::encode`] writes it, which every
//! replica holding the same state writes byte for byte alike, and cut
//! into chunks of [`CHUNK`] bytes. Each chunk is named by its hash, and
//! the state by the root over its length and its chunks' hashes, so that
//! a replica catching up can take each chunk from anyone and check it.

use std::io;

use kurator_shard::ShardState;

use crate::checkpoint::StateRoot;

/// How large a chunk is, the last one aside.
const CHUNK: usize = 1 << 20;

/// The key derivation context of chunk hashes. Fixed forever.
const CHUNK_CONTEXT: &str = "kurator 2026-11 bft state chunk";

/// The key derivation context of state roots. Fixed forever.
const ROOT_CONTEXT: &str = "kurator 2026-11 bft state root";

/// The hash of each chunk of a shard's encoded state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    length: u64,
    chunks: Vec<[u8; 32]>,
}

impl Manifest {
    /// The manifest of `state`.
    ///
    /// # Errors
    ///
    /// When a key or value is too long for its length prefix, which a
    /// state that took it in never holds.
    pub fn of(state: &ShardState) -> io::Result<Self> {
        let mut encoded = Vec::new();
        state.encode(&mut encoded)?;
        Ok(Self::over(&encoded))
    }

    /// The manifest of the encoded state `bytes`.
    pub(super) fn over(bytes: &[u8]) -> Self {
        let chunks = bytes
            .chunks(CHUNK)
            .map(|chunk| {
                *blake3::Hasher::new_derive_key(CHUNK_CONTEXT)
                    .update(chunk)
                    .finalize()
                    .as_bytes()
            })
            .collect();
        Self {
            length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            chunks,
        }
    }

    /// The hash of each chunk, in order.
    #[must_use]
    pub fn chunks(&self) -> &[[u8; 32]] {
        &self.chunks
    }

    /// The root that names the state.
    #[must_use]
    pub fn root(&self) -> StateRoot {
        let mut hasher = blake3::Hasher::new_derive_key(ROOT_CONTEXT);
        let _length = hasher.update(&self.length.to_le_bytes());
        for chunk in &self.chunks {
            let _chunk = hasher.update(chunk);
        }
        StateRoot::new(*hasher.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use kurator_shard::ShardState;
    use kurator_storage::{Version, WriteBatch};

    use super::{CHUNK, Manifest};

    #[test]
    fn the_root_names_the_state() {
        let empty = Manifest::of(&ShardState::default()).unwrap();
        assert_eq!(
            empty,
            Manifest::of(&ShardState::default()).unwrap(),
            "fixed"
        );
        assert_eq!(empty.chunks().len(), 1, "one short chunk");
        let mut state = ShardState::default();
        let _applied = state.apply(
            Version::new(1),
            kurator_storage::Command::Kv(WriteBatch::new().put("a", "1")),
        );
        let written = Manifest::of(&state).unwrap();
        assert_ne!(written.root(), empty.root(), "another state");
    }

    #[test]
    fn large_states_take_many_chunks() {
        let bytes = vec![7_u8; CHUNK.saturating_mul(2).saturating_add(1)];
        let manifest = Manifest::over(&bytes);
        assert_eq!(manifest.chunks().len(), 3, "two whole and one byte");
        assert_eq!(manifest.chunks()[0], manifest.chunks()[1], "same bytes");
        assert_ne!(manifest.chunks()[1], manifest.chunks()[2], "one byte");
        let shorter = Manifest::over(&bytes[..CHUNK.saturating_mul(2)]);
        assert_ne!(shorter.root(), manifest.root(), "the length counts");
        let reordered = Manifest {
            length: manifest.length,
            chunks: manifest.chunks.iter().rev().copied().collect(),
        };
        assert_ne!(reordered.root(), manifest.root(), "the order counts");
    }
}
