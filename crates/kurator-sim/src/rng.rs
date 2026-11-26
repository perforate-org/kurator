//! A small seeded random number generator.
//!
//! `SplitMix64`: fast, statistically good enough for fault schedules, and
//! defined here so that a seed means the same run on every build, whatever
//! the versions of other crates.

/// A deterministic stream of random numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// The stream for `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next number.
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number below `bound`, or 0 when `bound` is 0.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64().checked_rem(bound).unwrap_or(0)
    }

    /// A number in `low..=high`; `low` when the range is empty.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        let span = high.saturating_sub(low).saturating_add(1);
        low.saturating_add(self.below(span))
    }

    /// True `per_mille` times in a thousand.
    pub fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }

    /// One of `items`, or `None` when there are none.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        let len = u64::try_from(items.len()).ok()?;
        let index = usize::try_from(self.below(len)).ok()?;
        items.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::Rng;

    #[test]
    fn a_seed_always_gives_the_same_stream() {
        let mut first = Rng::new(42);
        let mut second = Rng::new(42);
        let a: Vec<u64> = (0..8).map(|_| first.next_u64()).collect();
        let b: Vec<u64> = (0..8).map(|_| second.next_u64()).collect();
        assert_eq!(a, b, "same seed");
        assert_ne!(Rng::new(43).next_u64(), a[0], "another seed");
        // Pinned, so that a seed reproduces the same run on every build.
        assert_eq!(
            Rng::new(0).next_u64(),
            0xE220_A839_7B1D_CDAF,
            "reference value"
        );
    }

    #[test]
    fn helpers_stay_in_range() {
        let mut rng = Rng::new(7);
        for _ in 0..1000 {
            assert!(rng.below(10) < 10, "below");
            let n = rng.between(5, 8);
            assert!((5..=8).contains(&n), "between");
        }
        assert_eq!(rng.below(0), 0, "empty bound");
        assert_eq!(rng.between(9, 3), 9, "empty range");
        assert!(!rng.chance(0), "never");
        assert!(rng.chance(1000), "always");
        assert_eq!(rng.pick::<u8>(&[]), None, "nothing to pick");
        assert_eq!(rng.pick(&[1]), Some(&1), "only choice");
    }
}
