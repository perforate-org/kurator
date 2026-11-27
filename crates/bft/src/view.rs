//! Views: the numbered turns of the protocol, each with its own leader.

use std::fmt;

/// A turn of the protocol. Each view has one leader, who may propose one
/// block in it; every replica votes at most once in a view, and moves to
/// the next view once it sees a certificate for this one.
///
/// View 0 is the genesis block's: nobody proposes in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct View(u64);

impl View {
    /// The genesis block's view.
    pub const GENESIS: Self = Self(0);

    /// Wraps a raw view number.
    #[must_use]
    pub const fn new(view: u64) -> Self {
        Self(view)
    }

    /// The raw view number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The view after this one. The last view has none: a shard would
    /// need to run for longer than the universe has existed to reach it.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }

    /// Whether `self` comes right after `before`.
    #[must_use]
    pub const fn follows(self, before: Self) -> bool {
        matches!(before.next(), Some(next) if next.0 == self.0)
    }
}

impl fmt::Display for View {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::View;

    #[test]
    fn views_count_up_to_the_last() {
        assert_eq!(View::GENESIS.next(), Some(View::new(1)), "first");
        assert_eq!(View::new(u64::MAX).next(), None, "last");
        assert!(View::new(4).follows(View::new(3)), "next");
        assert!(!View::new(5).follows(View::new(3)), "skips one");
        assert!(!View::new(3).follows(View::new(3)), "same");
        assert!(!View::new(0).follows(View::new(u64::MAX)), "no wrap");
        assert_eq!(View::new(7).to_string(), "v7", "display");
        assert_eq!(View::new(7).get(), 7, "raw");
    }
}

#[cfg(kani)]
mod proofs {
    use super::View;

    /// A view follows exactly the one before it, and the last view has
    /// no successor to wrap around to.
    #[kani::proof]
    fn follows_is_one_step() {
        let before = View::new(kani::any());
        let after = View::new(kani::any());
        let one_step = before.get().checked_add(1) == Some(after.get());
        assert_eq!(after.follows(before), one_step, "one step");
        assert_eq!(
            before.next().map(View::get),
            before.get().checked_add(1),
            "next"
        );
    }
}
