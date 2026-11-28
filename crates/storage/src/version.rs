//! Versions of the state, and how many of them stay readable.

use std::fmt;
use std::num::NonZeroU64;

use oleaph_wal::Lsn;

/// A version of the state: the position of the command that made it.
///
/// Every command that changes the state gets a version above the one
/// before it. A single-node [`Db`](crate::Db) uses the command's log
/// sequence number, a replica the index of its Raft entry, so every
/// replica gives the same command the same version. [`Version::ZERO`] is
/// the empty state before any command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Version(u64);

impl Version {
    /// The empty state before any command.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw version.
    #[must_use]
    pub const fn new(version: u64) -> Self {
        Self(version)
    }

    /// The raw version.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<Lsn> for Version {
    fn from(lsn: Lsn) -> Self {
        Self(lsn.get())
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// How many versions behind the newest one a read may still look.
///
/// With a retention of `r` and the newest version `v`, every version from
/// `v - r` on reads as it was; older ones are gone, and a transaction that
/// read one of them aborts. The retention is part of the state and of its
/// snapshots, so every replica forgets the same versions at the same time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Retention(NonZeroU64);

impl Retention {
    /// The retention a new state starts with: long enough for a transaction
    /// to run while thousands of others commit.
    pub const DEFAULT: Self = match NonZeroU64::new(4096) {
        Some(versions) => Self(versions),
        None => Self(NonZeroU64::MIN),
    };

    /// Keeps `versions` versions behind the newest one readable.
    #[must_use]
    pub const fn new(versions: NonZeroU64) -> Self {
        Self(versions)
    }

    /// The number of versions kept.
    #[must_use]
    pub const fn get(self) -> NonZeroU64 {
        self.0
    }

    /// The oldest version that still reads as it was when `newest` is the
    /// newest one.
    #[must_use]
    pub const fn horizon(self, newest: Version) -> Version {
        Version(newest.0.saturating_sub(self.0.get()))
    }

    /// Which window of [`Retention::get`] versions `version` falls in.
    /// Every time the newest version enters a new window, every key
    /// forgets the versions behind the horizon.
    pub(crate) const fn window(self, version: Version) -> u64 {
        match version.0.checked_div(self.0.get()) {
            Some(window) => window,
            None => 0,
        }
    }
}

impl Default for Retention {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::{Retention, Version};
    use oleaph_wal::Lsn;

    #[test]
    fn the_horizon_trails_the_newest_version() {
        let retention = Retention::new(NonZeroU64::new(3).unwrap());
        assert_eq!(retention.horizon(Version::new(10)), Version::new(7), "back");
        assert_eq!(retention.horizon(Version::new(2)), Version::ZERO, "floor");
        assert_eq!(retention.window(Version::new(5)), 1, "window");
        assert_eq!(retention.window(Version::new(6)), 2, "next window");
        assert_eq!(Retention::default().get().get(), 4096, "default");
    }

    #[test]
    fn versions_show_and_convert() {
        assert_eq!(Version::new(4).to_string(), "v4", "display");
        assert_eq!(Version::from(Lsn::new(9)), Version::new(9), "from lsn");
        assert_eq!(Version::new(9).get(), 9, "raw");
    }
}

#[cfg(kani)]
mod proofs {
    use super::{Retention, Version};

    /// The horizon never passes the newest version, and stays within the
    /// retention of it.
    #[kani::proof]
    fn horizon_stays_behind_the_newest_version() {
        let retention = Retention(kani::any());
        let newest = Version(kani::any());
        let horizon = retention.horizon(newest);
        assert!(horizon <= newest, "behind");
        assert!(
            newest.0 - horizon.0 <= retention.0.get(),
            "within the retention"
        );
    }
}
