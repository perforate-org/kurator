//! The in-memory state: every key with the versions of its value that are
//! still readable.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::ops::Bound;

use kurator_codec::{DecodeError, Reader, put_bytes};
use kurator_wal::Lsn;

use crate::batch::KvOp;
use crate::command::Command;
use crate::outcome::{Abort, Outcome, Unreadable};
use crate::txn::Transaction;
use crate::version::{Retention, Version};

/// The key-value state, versioned: each key keeps the values it had at
/// every version its [`Retention`] still reads.
///
/// A state changes only by [`KvState::apply`], one [`Command`] at a time,
/// each at a version above the last, so replicas that apply the same
/// commands at the same versions hold the same state. A
/// [`Transaction`] command commits or aborts by what the state holds when
/// it applies, so they also make the same decisions.
///
/// Reads look at one version through a [`View`]: [`KvState::latest`] for
/// the newest, [`KvState::view`] for an older one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvState {
    keys: BTreeMap<Vec<u8>, History>,
    version: Version,
    retention: Retention,
}

impl Default for KvState {
    fn default() -> Self {
        Self::new(Retention::DEFAULT)
    }
}

impl KvState {
    /// An empty state that keeps `retention` versions readable.
    #[must_use]
    pub const fn new(retention: Retention) -> Self {
        Self {
            keys: BTreeMap::new(),
            version: Version::ZERO,
            retention,
        }
    }

    /// Applies `command` at `version`, which must be above
    /// [`KvState::version`]. A write batch always commits; a transaction
    /// commits unless a command after its read version wrote what it read.
    pub fn apply(&mut self, version: Version, command: Command) -> Outcome {
        debug_assert!(version > self.version, "versions only grow");
        let writes = match command {
            Command::Kv(batch) => batch,
            Command::Txn(transaction) => match self.check(&transaction) {
                Ok(()) => transaction.into_writes(),
                Err(abort) => return Outcome::Aborted(abort),
            },
        };
        let entering = self.retention.window(version) != self.retention.window(self.version);
        self.version = version;
        let horizon = self.horizon();
        for op in writes.into_ops() {
            let (key, value) = match op {
                KvOp::Put { key, value } => (key, Some(value)),
                KvOp::Delete { key } => (key, None),
            };
            let history = self.keys.entry(key).or_default();
            history.write(version, value);
            history.forget(horizon);
        }
        if entering {
            self.keys.retain(|_, history| {
                history.forget(horizon);
                !history.is_gone(horizon)
            });
        }
        Outcome::Committed(version)
    }

    /// Whether `transaction` may commit now: its read version is readable
    /// and nothing it read was written after it.
    fn check(&self, transaction: &Transaction) -> Result<(), Abort> {
        let read_at = transaction.read_at();
        self.readable(read_at)?;
        let written_since = |key: &[u8], history: &History| {
            let version = history.newest();
            (version > read_at).then(|| Abort::Conflict {
                key: key.to_vec(),
                version,
            })
        };
        let reads = transaction.reads();
        let keys = reads
            .keys()
            .iter()
            .filter_map(|key| Some((key.as_slice(), self.keys.get(key)?)));
        let ranges = reads
            .ranges()
            .iter()
            .flat_map(|range| self.histories(range.start(), range.end()));
        keys.chain(ranges)
            .find_map(|(key, history)| written_since(key, history))
            .map_or(Ok(()), Err)
    }

    /// The newest version.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }

    /// How many versions stay readable.
    #[must_use]
    pub const fn retention(&self) -> Retention {
        self.retention
    }

    /// The oldest version that still reads as it was.
    #[must_use]
    pub const fn horizon(&self) -> Version {
        self.retention.horizon(self.version)
    }

    fn readable(&self, at: Version) -> Result<(), Unreadable> {
        if at > self.version {
            return Err(Unreadable::Ahead {
                at,
                newest: self.version,
            });
        }
        let horizon = self.horizon();
        if at < horizon {
            return Err(Unreadable::Forgotten { at, horizon });
        }
        Ok(())
    }

    /// The state as it was at version `at`.
    ///
    /// # Errors
    ///
    /// When `at` is behind [`KvState::horizon`] or ahead of
    /// [`KvState::version`].
    pub fn view(&self, at: Version) -> Result<View<'_>, Unreadable> {
        self.readable(at)?;
        Ok(View { state: self, at })
    }

    /// The state at its newest version.
    #[must_use]
    pub const fn latest(&self) -> View<'_> {
        View {
            state: self,
            at: self.version,
        }
    }

    /// The newest value of `key`; see [`View::get`].
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.latest().get(key)
    }

    /// The newest entries between `start` and `end`; see [`View::range`].
    pub fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        self.latest().range(start, end)
    }

    /// Every entry at the newest version, in key order.
    #[must_use = "iterators do nothing unless consumed"]
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.latest().range(Bound::Unbounded, Bound::Unbounded)
    }

    /// How many keys have a value at the newest version.
    #[must_use]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Whether no key has a value at the newest version.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    /// The keys between `start` and `end` with their histories. An empty or
    /// reversed range yields nothing.
    fn histories<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> impl Iterator<Item = (&'a [u8], &'a History)> + 'a {
        is_ordered(start, end)
            .then(|| self.keys.range::<[u8], _>((start, end)))
            .into_iter()
            .flatten()
            .map(|(key, history)| (key.as_slice(), history))
    }

    /// Writes the state as
    ///
    /// ```text
    /// retention u64 | version u64 | keys u64
    /// (key | versions u32 | (version u64 | tag u8 | value?)*)*
    /// ```
    ///
    /// keys in order and each key's versions oldest first; tag 1 carries a
    /// length prefixed value, tag 0 marks a removal.
    ///
    /// # Errors
    ///
    /// When `out` fails or a key or value is too long for its prefix.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        out.write_all(&self.retention.get().get().to_le_bytes())?;
        out.write_all(&self.version.get().to_le_bytes())?;
        let keys = u64::try_from(self.keys.len()).map_err(io::Error::other)?;
        out.write_all(&keys.to_le_bytes())?;
        for (key, history) in &self.keys {
            put_bytes(out, key)?;
            history.encode(out)?;
        }
        Ok(())
    }

    /// Reads a state written by [`KvState::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] when the input ends early, and
    /// [`DecodeError::Invalid`] when the retention is zero, keys are out of
    /// order, or a key's versions are empty, out of order or newer than
    /// the state.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let retention = NonZeroU64::new(reader.read()?).ok_or(DecodeError::Invalid)?;
        let version = Version::new(reader.read()?);
        let count = reader.read::<u64>()?;
        let mut keys = BTreeMap::new();
        for _ in 0..count {
            let key = reader.bytes()?.to_vec();
            let history = History::decode(reader, version)?;
            if keys.last_key_value().is_some_and(|(last, _)| *last >= key) {
                return Err(DecodeError::Invalid);
            }
            drop(keys.insert(key, history));
        }
        Ok(Self {
            keys,
            version,
            retention: Retention::new(retention),
        })
    }
}

/// The state as it was at one version.
#[derive(Debug, Clone, Copy)]
pub struct View<'a> {
    state: &'a KvState,
    at: Version,
}

impl<'a> View<'a> {
    /// The version it shows.
    #[must_use]
    pub const fn at(self) -> Version {
        self.at
    }

    /// The value of `key` at this version.
    #[must_use]
    pub fn get(self, key: &[u8]) -> Option<&'a [u8]> {
        self.state.keys.get(key)?.at(self.at)
    }

    /// The entries with keys between `start` and `end` at this version, in
    /// key order. An empty or reversed range yields nothing.
    pub fn range(
        self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        let at = self.at;
        self.state
            .histories(start, end)
            .filter_map(move |(key, history)| Some((key, history.at(at)?)))
    }
}

/// The versions of one key's value, oldest first; `None` where the key was
/// removed. Never empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct History(Vec<(Version, Option<Vec<u8>>)>);

impl History {
    /// The value at version `at`: the one written last at or before it.
    fn at(&self, at: Version) -> Option<&[u8]> {
        let written = self.0.partition_point(|(version, _)| *version <= at);
        let (_, value) = self.0.get(written.checked_sub(1)?)?;
        value.as_deref()
    }

    /// The version that wrote the key last.
    fn newest(&self) -> Version {
        self.0.last().map_or(Version::ZERO, |(version, _)| *version)
    }

    /// Records `value` at `version`, the newest. A batch that writes a
    /// key twice keeps the last write.
    fn write(&mut self, version: Version, value: Option<Vec<u8>>) {
        match self.0.last_mut() {
            Some((last, written)) if *last == version => *written = value,
            Some(_) | None => self.0.push((version, value)),
        }
    }

    /// Drops the versions no read at `horizon` or later can see: all but
    /// the newest at or before it.
    fn forget(&mut self, horizon: Version) {
        let seen = self.0.partition_point(|(version, _)| *version <= horizon);
        drop(self.0.drain(..seen.saturating_sub(1)));
    }

    /// Whether every readable version sees the key removed, so the key can
    /// go. A transaction that read it at a readable version cannot
    /// conflict with the removal either: the removal is not after it.
    fn is_gone(&self, horizon: Version) -> bool {
        match self.0.as_slice() {
            [(version, None)] => *version <= horizon,
            _ => false,
        }
    }

    fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        let count = u32::try_from(self.0.len()).map_err(io::Error::other)?;
        out.write_all(&count.to_le_bytes())?;
        for (version, value) in &self.0 {
            out.write_all(&version.get().to_le_bytes())?;
            match value {
                Some(value) => {
                    out.write_all(&[1])?;
                    put_bytes(out, value)?;
                }
                None => out.write_all(&[0])?,
            }
        }
        Ok(())
    }

    fn decode(reader: &mut Reader<'_>, newest: Version) -> Result<Self, DecodeError> {
        let count = reader.read::<u32>()?;
        let mut versions: Vec<(Version, Option<Vec<u8>>)> = Vec::new();
        for _ in 0..count {
            let version = Version::new(reader.read()?);
            let value = match reader.read::<u8>()? {
                0 => None,
                1 => Some(reader.bytes()?.to_vec()),
                other => return Err(DecodeError::Tag(other)),
            };
            let ordered = versions.last().is_none_or(|(last, _)| *last < version);
            if !ordered || version > newest {
                return Err(DecodeError::Invalid);
            }
            versions.push((version, value));
        }
        if versions.is_empty() {
            return Err(DecodeError::Invalid);
        }
        Ok(Self(versions))
    }
}

/// A [`KvState`] and the last log record applied to it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LoggedState {
    state: KvState,
    applied: Lsn,
}

impl LoggedState {
    pub(crate) const fn new(state: KvState, applied: Lsn) -> Self {
        Self { state, applied }
    }

    /// Applies the command logged at `lsn`, which must come after
    /// [`LoggedState::applied`], at the version `lsn` names.
    pub(crate) fn apply(&mut self, lsn: Lsn, command: Command) -> Outcome {
        debug_assert!(lsn > self.applied, "records apply in log order");
        self.applied = lsn;
        self.state.apply(Version::from(lsn), command)
    }

    pub(crate) const fn state(&self) -> &KvState {
        &self.state
    }

    /// The last record applied, or [`Lsn::ZERO`].
    pub(crate) const fn applied(&self) -> Lsn {
        self.applied
    }
}

/// Whether `BTreeMap::range` accepts the bounds: it panics when start is
/// after end, or when both exclude the same key.
fn is_ordered(start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    match (start, end) {
        (Bound::Excluded(start), Bound::Excluded(end)) => start < end,
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => start <= end,
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
    }
}

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod proofs {
    use super::History;
    use crate::version::Version;

    /// Forgetting what the horizon cannot see changes no read at or after
    /// the horizon, nor the version that wrote the key last; and a key is
    /// gone only when such a read finds it removed. Two versions are
    /// enough to drop one; values are empty, so only their presence
    /// matters.
    #[kani::proof]
    #[kani::unwind(3)]
    fn forgetting_keeps_every_readable_version() {
        let (older, newer): (u8, u8) = kani::any();
        kani::assume(older < newer);
        let (older_removed, newer_removed): (bool, bool) = kani::any();
        let value = |removed: bool| if removed { None } else { Some(Vec::new()) };
        let mut history = History(vec![
            (Version::new(u64::from(older)), value(older_removed)),
            (Version::new(u64::from(newer)), value(newer_removed)),
        ]);
        let horizon = Version::new(u64::from(kani::any::<u8>()));
        let at = Version::new(u64::from(kani::any::<u8>()));
        kani::assume(at >= horizon);
        let read = history.at(at).is_some();
        let newest = history.newest();
        history.forget(horizon);
        assert_eq!(history.at(at).is_some(), read, "same read");
        assert_eq!(history.newest(), newest, "same last write");
        assert!(!history.0.is_empty(), "never empty");
        if history.is_gone(horizon) {
            assert!(!read, "gone only when removed");
        }
    }
}
