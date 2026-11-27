//! A latency histogram: counts by bucket, kept in atomics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The upper bounds of the buckets, in microseconds: from 50 µs, where a
/// sync on a fast disk ends, to 10 s, past any timeout of a write.
pub(crate) const BOUNDS: [u64; 17] = [
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// How long something took, many times over.
///
/// It keeps a count in each of 17 buckets, from 50 µs to 10 s, and one past the last,
/// the sum, and the count. Recording takes no lock, so any thread records
/// at any time; a reader sees each counter as some recording left it, not
/// all of them at one instant.
#[derive(Debug, Default)]
pub struct Histogram {
    /// One count per bucket, not cumulative; the last is past every bound.
    buckets: [AtomicU64; 18],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    /// Counts one thing that took `took`.
    pub fn record(&self, took: Duration) {
        let micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        let bucket = BOUNDS
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(BOUNDS.len());
        if let Some(counter) = self.buckets.get(bucket) {
            let _before = counter.fetch_add(1, Ordering::Relaxed);
        }
        let _sum = self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        let _count = self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// How many things it counted.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// How long they took together, in microseconds.
    #[must_use]
    pub fn sum_micros(&self) -> u64 {
        self.sum_micros.load(Ordering::Relaxed)
    }

    /// For each bound of `BOUNDS`, how many took at most that long; the
    /// count past the last bound is [`Histogram::count`].
    pub(crate) fn cumulative(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        BOUNDS
            .iter()
            .zip(&self.buckets)
            .scan(0_u64, |below, (bound, counter)| {
                *below = below.saturating_add(counter.load(Ordering::Relaxed));
                Some((*bound, *below))
            })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use proptest::prelude::*;

    use super::{BOUNDS, Histogram};

    #[test]
    fn a_recording_lands_in_the_first_bucket_that_holds_it() {
        let histogram = Histogram::default();
        histogram.record(Duration::from_micros(50));
        histogram.record(Duration::from_micros(51));
        histogram.record(Duration::from_secs(60));
        let cumulative: Vec<(u64, u64)> = histogram.cumulative().take(3).collect();
        assert_eq!(cumulative, [(50, 1), (100, 2), (250, 2)], "by bound");
        assert_eq!(histogram.cumulative().last(), Some((10_000_000, 2)), "last");
        assert_eq!(histogram.count(), 3, "count");
        assert_eq!(histogram.sum_micros(), 60_000_101, "sum");
    }

    proptest! {
        #[test]
        fn cumulative_counts_never_fall_and_end_at_most_at_the_count(
            micros in proptest::collection::vec(0_u64..20_000_000, 0..40),
        ) {
            let histogram = Histogram::default();
            for took in &micros {
                histogram.record(Duration::from_micros(*took));
            }
            let cumulative: Vec<(u64, u64)> = histogram.cumulative().collect();
            prop_assert_eq!(cumulative.len(), BOUNDS.len());
            let rising = cumulative.windows(2).all(|pair| match pair {
                [(_, low), (_, high)] => low <= high,
                _ => true,
            });
            prop_assert!(rising, "cumulative");
            for (bound, below) in cumulative {
                let expected = micros.iter().filter(|took| **took <= bound).count();
                prop_assert_eq!(below, u64::try_from(expected).unwrap());
            }
            prop_assert_eq!(histogram.count(), u64::try_from(micros.len()).unwrap());
        }
    }
}
