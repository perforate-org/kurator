//! Log sequence numbers.

use std::fmt;

/// Position of a record in the log.
///
/// The first record is `1`, and each record is one higher than the one
/// before it. [`Lsn::ZERO`] means "before the first record".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(u64);

impl Lsn {
    /// The position before the first record.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The position after this one.
    ///
    /// Saturates at `u64::MAX`, which a log never reaches: at a million
    /// records a second it would take half a million years.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A family of files named by a log position: `<lsn as 20 digits><suffix>`,
/// so that names sort in log order.
///
/// ```
/// use oleaph_wal::{Lsn, LsnFiles};
///
/// const SNAPSHOTS: LsnFiles = LsnFiles::new(".snap");
/// assert_eq!(SNAPSHOTS.name(Lsn::new(7)), "00000000000000000007.snap");
/// assert_eq!(SNAPSHOTS.parse("00000000000000000007.snap"), Some(Lsn::new(7)));
/// assert_eq!(SNAPSHOTS.parse("7.snap"), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsnFiles {
    suffix: &'static str,
}

impl LsnFiles {
    const DIGITS: usize = 20;

    /// Files ending in `suffix`.
    #[must_use]
    pub const fn new(suffix: &'static str) -> Self {
        Self { suffix }
    }

    /// The file name for `lsn`.
    #[must_use]
    pub fn name(self, lsn: Lsn) -> String {
        format!("{:0width$}{}", lsn.0, self.suffix, width = Self::DIGITS)
    }

    /// The position a file name holds, or `None` for other names.
    #[must_use]
    pub fn parse(self, name: &str) -> Option<Lsn> {
        let digits = name.strip_suffix(self.suffix)?;
        let well_formed =
            digits.len() == Self::DIGITS && digits.bytes().all(|byte| byte.is_ascii_digit());
        well_formed.then(|| digits.parse().ok().map(Lsn)).flatten()
    }
}

/// The consecutive, non-empty run of positions given to one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsnRange {
    first: Lsn,
    last: Lsn,
}

impl LsnRange {
    /// The positions from `first` up to, but not including, `end`; `None`
    /// when that is empty.
    pub(crate) fn until(first: Lsn, end: Lsn) -> Option<Self> {
        let last = Lsn(end.0.checked_sub(1)?);
        (first <= last).then_some(Self { first, last })
    }

    /// The first position.
    #[must_use]
    pub const fn first(self) -> Lsn {
        self.first
    }

    /// The last position.
    #[must_use]
    pub const fn last(self) -> Lsn {
        self.last
    }

    /// Every position, in order.
    pub fn iter(self) -> impl Iterator<Item = Lsn> {
        (self.first.0..=self.last.0).map(Lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::{Lsn, LsnRange};

    #[test]
    fn until_is_empty_when_end_is_not_after_first() {
        assert_eq!(LsnRange::until(Lsn(3), Lsn(3)), None, "equal");
        assert_eq!(LsnRange::until(Lsn(3), Lsn(2)), None, "reversed");
        assert_eq!(LsnRange::until(Lsn(0), Lsn(0)), None, "zero");
    }

    #[test]
    fn iter_yields_every_position() {
        let range = LsnRange::until(Lsn(3), Lsn(6)).unwrap();
        assert_eq!(range.first(), Lsn(3), "first");
        assert_eq!(range.last(), Lsn(5), "last");
        assert_eq!(
            range.iter().collect::<Vec<_>>(),
            [Lsn(3), Lsn(4), Lsn(5)],
            "positions"
        );
    }

    #[test]
    fn file_names_reject_other_shapes() {
        let files = super::LsnFiles::new(".wal");
        for name in [
            "42.wal",
            "0000000000000000004x.wal",
            "00000000000000000042.snap",
            "+0000000000000000042.wal",
            "99999999999999999999.wal",
        ] {
            assert_eq!(files.parse(name), None, "{name}");
        }
        assert_eq!(
            files.parse(&files.name(Lsn(u64::MAX))),
            Some(Lsn(u64::MAX)),
            "max"
        );
    }

    #[test]
    fn next_saturates() {
        assert_eq!(Lsn(u64::MAX).next(), Lsn(u64::MAX), "saturates");
        assert_eq!(Lsn::ZERO.next(), Lsn(1), "first record");
        assert_eq!(Lsn(7).to_string(), "7", "display");
    }
}

#[cfg(kani)]
mod proofs {
    use super::{Lsn, LsnRange};

    /// A range exists exactly when it is non-empty, and spans `first..end`.
    #[kani::proof]
    fn until_matches_its_definition() {
        let (first, end) = (Lsn(kani::any()), Lsn(kani::any()));
        match LsnRange::until(first, end) {
            Some(range) => {
                assert!(first < end, "non-empty");
                assert_eq!(range.first(), first, "first");
                assert_eq!(range.last().next(), end, "last is end - 1");
            }
            None => assert!(first >= end, "empty"),
        }
    }
}
