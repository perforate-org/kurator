//! When a shard grows too large: the footprint above which it splits.

use std::fmt;
use std::num::NonZeroU64;

use kurator_storage::{Footprint, KvState};

/// The largest [`Footprint`] a shard keeps before it splits in two.
///
/// A shard's snapshot is about as large as its footprint, and a new
/// replica, or one that fell behind, receives the whole snapshot, so the
/// ceiling bounds what a replica moves at once. Smaller shards also spread
/// load over more Raft groups; each group costs its heartbeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ceiling(NonZeroU64);

impl Ceiling {
    /// 64 MiB: well under the largest snapshot a replica sends, and in
    /// the range `TiKV` splits its regions at.
    pub const DEFAULT: Self = match NonZeroU64::new(64 << 20) {
        Some(bytes) => Self(bytes),
        None => Self(NonZeroU64::MIN),
    };

    /// A ceiling of `bytes`.
    #[must_use]
    pub const fn new(bytes: NonZeroU64) -> Self {
        Self(bytes)
    }

    /// The bytes.
    #[must_use]
    pub const fn bytes(self) -> NonZeroU64 {
        self.0
    }

    /// Where a shard whose state is `kv` should split: its
    /// [middle](KvState::middle) when its footprint is above the ceiling,
    /// otherwise `None`. Also `None` when the shard is a single key, which
    /// no split can cut.
    #[must_use]
    pub fn split_point(self, kv: &KvState) -> Option<Vec<u8>> {
        let above = kv.footprint() > Footprint::new(self.0.get());
        above.then(|| kv.middle().map(<[u8]>::to_vec)).flatten()
    }
}

impl Default for Ceiling {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The bytes, as `67108864 B`.
impl fmt::Display for Ceiling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} B", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use kurator_storage::{Command, KvState, Version, WriteBatch};

    use super::Ceiling;

    fn ceiling(bytes: u64) -> Ceiling {
        Ceiling::new(NonZeroU64::new(bytes).unwrap())
    }

    fn state(keys: &[&str]) -> KvState {
        let mut state = KvState::default();
        for (step, key) in (1..).zip(keys) {
            let batch = WriteBatch::new().put(*key, "value");
            let _outcome = state.apply(Version::new(step), Command::Kv(batch));
        }
        state
    }

    #[test]
    fn a_shard_splits_only_above_the_ceiling() {
        let four = state(&["a", "b", "c", "d"]);
        let footprint = four.footprint().bytes();
        assert_eq!(ceiling(footprint).split_point(&four), None, "at it");
        assert_eq!(
            ceiling(footprint - 1).split_point(&four),
            Some(b"c".to_vec()),
            "just above it"
        );
        let one = state(&["a"]);
        assert_eq!(ceiling(1).split_point(&one), None, "one key");
        assert_eq!(Ceiling::default(), Ceiling::DEFAULT, "default");
        assert_eq!(Ceiling::DEFAULT.bytes().get(), 64 << 20, "64 MiB");
        assert_eq!(ceiling(12).to_string(), "12 B", "display");
    }
}
