//! The runtime a client runs on over QUIC: the wall clock and Tokio's
//! timers.

use std::future::Future;
use std::time::{Duration, SystemTime};

use oleaph_client::Time;
use oleaph_storage::Deadline;

/// The wall clock, in milliseconds since the Unix epoch, and Tokio's
/// timers.
///
/// Coordinators on different machines share this clock as well as their
/// clocks agree; one that is off only resolves a transaction too early or
/// too late.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct Wall;

impl Time for Wall {
    fn now(&self) -> Deadline {
        let since = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        Deadline::new(u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
    }

    async fn pause(&self, length: Duration) {
        tokio::time::sleep(length).await;
    }

    async fn within<F>(&self, limit: Duration, future: F) -> Option<F::Output>
    where
        F: Future + Send,
        F::Output: Send,
    {
        tokio::time::timeout(limit, future).await.ok()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use oleaph_client::Time as _;

    use super::Wall;

    #[tokio::test]
    async fn the_wall_clock_runs_and_times_out() {
        let before = Wall.now();
        Wall.pause(Duration::from_millis(5)).await;
        assert!(Wall.now() > before, "time passed");
        assert!(before.millis() > 1_700_000_000_000, "since the epoch");
        let late = Wall
            .within(Duration::from_millis(1), std::future::pending::<()>())
            .await;
        assert_eq!(late, None, "timed out");
        let soon = Wall.within(Duration::from_secs(1), async { 3 }).await;
        assert_eq!(soon, Some(3), "in time");
    }
}
