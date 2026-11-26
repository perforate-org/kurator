//! Shard ids and epochs.

use std::fmt;

/// Names a shard for as long as it exists. A split keeps the id for its
/// left half and gives the right half a new one, never used before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(u64);

impl ShardId {
    /// The shard a new cluster starts with: it holds every key.
    pub const FIRST: Self = Self(1);

    /// Wraps a raw id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s{}", self.0)
    }
}

/// How many times a shard's span changed. Each split moves both halves to
/// the next epoch, so a request routed by an older picture of the shard
/// can tell it is out of date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(u64);

impl Epoch {
    /// The epoch of a shard whose span never changed.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw epoch.
    #[must_use]
    pub const fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    /// The raw epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The epoch after this one, if there is one.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{Epoch, ShardId};

    #[test]
    fn ids_and_epochs_show_and_count() {
        assert_eq!(ShardId::new(7).to_string(), "s7", "id");
        assert_eq!(ShardId::FIRST.get(), 1, "first");
        assert_eq!(Epoch::new(3).to_string(), "e3", "epoch");
        assert_eq!(Epoch::ZERO.next(), Some(Epoch::new(1)), "next");
        assert_eq!(Epoch::new(u64::MAX).next(), None, "last");
        assert_eq!(Epoch::new(9).get(), 9, "raw");
    }
}
