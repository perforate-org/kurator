//! The in-memory state: every key with the versions of its value that are
//! still readable.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::ops::Bound;

use oleaph_codec::{DecodeError, Reader, put_bytes};
use oleaph_wal::Lsn;

use crate::across::TxnId;
use crate::batch::{KvOp, WriteBatch};
use crate::command::Command;
use crate::outcome::{Abort, Outcome, Unreadable};
use crate::span::Span;
use crate::two_phase::{Collect, Decide, Decided, Decision, Prepare};
use crate::txn::{ReadSet, Transaction};
use crate::version::{Retention, Version};

pub use self::footprint::Footprint;
use self::locks::Locks;

mod footprint;
mod locks;

/// The key-value state, versioned: each key keeps the values it had at
/// every version its [`Retention`] still reads.
///
/// A state changes only by [`KvState::apply`], one [`Command`] at a time,
/// each at a version above the last, so replicas that apply the same
/// commands at the same versions hold the same state. A
/// [`Transaction`] command commits or aborts by what the state holds when
/// it applies, so they also make the same decisions. So do the two phases
/// of a transaction across shards, [`Prepare`] and [`Decide`]: the state
/// holds the parts it prepared, locked, until they are decided.
///
/// Reads look at one version through a [`View`]: [`KvState::latest`] for
/// the newest, [`KvState::view`] for an older one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvState {
    keys: BTreeMap<Vec<u8>, History>,
    version: Version,
    retention: Retention,
    locks: Locks,
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
            locks: Locks::EMPTY,
        }
    }

    /// Applies `command` at `version`, which must be above
    /// [`KvState::version`]. A write batch commits unless a prepared
    /// transaction holds a key it writes; a transaction commits unless,
    /// besides, a command after its read version wrote what it read. A
    /// prepare checks its part as a transaction and locks it; a decision
    /// applies the part it decides, or drops it. A step of [`Collect`]
    /// moves the fence or forgets decisions behind it, and writes nothing.
    pub fn apply(&mut self, version: Version, command: Command) -> Outcome {
        debug_assert!(version > self.version, "versions only grow");
        let writes = match command {
            Command::Kv(batch) => match self.locks.conflict(&ReadSet::default(), &batch) {
                None => batch,
                Some(abort) => return Outcome::Aborted(abort),
            },
            Command::Txn(transaction) => match self.check(&transaction) {
                Ok(()) => transaction.into_writes(),
                Err(abort) => return Outcome::Aborted(abort),
            },
            Command::Prepare(prepare) => return self.prepare(version, prepare),
            Command::Decide(decide) => match self.decide(version, &decide) {
                Ok(writes) => writes,
                Err(outcome) => return outcome,
            },
            Command::Collect(step) => return self.collect(version, step),
        };
        self.write(version, writes)
    }

    /// Prepares `prepare` at `version`, unless its transaction was decided
    /// already: then this is how. A transaction behind the fence is not
    /// prepared.
    fn prepare(&mut self, version: Version, prepare: Prepare) -> Outcome {
        if let Some(outcome) = self.locks.told(prepare.id()) {
            return outcome;
        }
        if self.locks.is_fenced(prepare.deadline()) {
            return Outcome::Aborted(Abort::Fenced(prepare.id()));
        }
        if !self.locks.is_prepared(prepare.id()) {
            if let Err(abort) = self.check(prepare.transaction()) {
                return Outcome::Aborted(abort);
            }
            self.locks.lock(prepare);
        }
        Outcome::Prepared(version)
    }

    /// Decides `decide` at `version`: the writes to apply when it commits
    /// a part this state prepared, otherwise how the transaction came out.
    /// A transaction behind the fence that this state does not know of is
    /// not decided: it may have been, and been forgotten.
    fn decide(&mut self, version: Version, decide: &Decide) -> Result<WriteBatch, Outcome> {
        let id = decide.id();
        if let Some(outcome) = self.locks.told(id) {
            return Err(outcome);
        }
        let held = self.locks.is_prepared(id);
        if !held && self.locks.is_fenced(decide.deadline()) {
            return Err(Outcome::Aborted(Abort::Fenced(id)));
        }
        let commits = decide.decision() == Decision::Commit && held;
        let decided = if commits {
            Decided::Committed(version)
        } else {
            Decided::Aborted
        };
        match self
            .locks
            .decide(id, decide.primary(), decide.deadline(), decided)
        {
            Some(prepared) if commits => Ok(prepared.into_transaction().into_writes()),
            Some(_) | None => Err(Outcome::Aborted(Abort::RolledBack(id))),
        }
    }

    /// Takes `step` of forgetting decisions, at `version`.
    fn collect(&mut self, version: Version, step: Collect) -> Outcome {
        match step {
            Collect::Fence(horizon) => {
                if let Err(abort) = self.locks.fence(horizon) {
                    return Outcome::Aborted(abort);
                }
            }
            Collect::Forget(horizon) => self.locks.forget(horizon),
        }
        Outcome::Committed(version)
    }

    /// Applies `writes` at `version`.
    fn write(&mut self, version: Version, writes: WriteBatch) -> Outcome {
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

    /// Moves every key at or after `at`, with its history, into a new state
    /// at the same version and retention; this state keeps the keys before
    /// `at`. Each half reads at every readable version as the whole did
    /// for its keys.
    #[must_use]
    pub fn split_off(&mut self, at: &[u8]) -> Self {
        Self {
            keys: self.keys.split_off(at),
            version: self.version,
            retention: self.retention,
            locks: self.locks.split_off(at),
        }
    }

    /// Whether a transaction across shards is prepared here and not decided
    /// yet. A shard does not split while one is: its keys could straddle
    /// the halves.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.locks.is_locked()
    }

    /// The transactions across shards prepared here and not decided yet:
    /// their primary keys say where to ask how they were decided, and
    /// their deadlines when that may be done.
    pub fn pending(&self) -> impl Iterator<Item = &Prepare> {
        self.locks.pending()
    }

    /// The transactions across shards decided here whose decisions are
    /// still kept, in id order.
    pub fn decisions(&self) -> impl Iterator<Item = (TxnId, Decided)> + '_ {
        self.locks.decisions()
    }

    /// Whether every key, and every part of a transaction prepared here,
    /// is in `span`.
    #[must_use]
    pub fn fits(&self, span: &Span) -> bool {
        self.keys.keys().all(|key| span.contains(key)) && self.locks.fits(span)
    }

    /// How the transaction `id` was decided here, if it was.
    #[must_use]
    pub fn decided(&self, id: TxnId) -> Option<Decided> {
        self.locks.decided(id)
    }

    /// Whether `transaction` may commit now: its read version is readable,
    /// nothing it read was written after it, and no prepared transaction
    /// holds what it touches.
    fn check(&self, transaction: &Transaction) -> Result<(), Abort> {
        self.validate(transaction)?;
        self.locks
            .conflict(transaction.reads(), transaction.writes())
            .map_or(Ok(()), Err)
    }

    /// Whether `transaction`'s read version is readable and nothing it read
    /// was written after it.
    fn validate(&self, transaction: &Transaction) -> Result<(), Abort> {
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

    /// Every key the state keeps versions of, removed ones included, in
    /// key order.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.keys.keys().map(Vec::as_slice)
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

    /// The bytes the keys take, every kept version included. It walks
    /// every key.
    #[must_use]
    pub fn footprint(&self) -> Footprint {
        self.keys
            .iter()
            .map(|(key, history)| Footprint::of(key, history))
            .fold(Footprint::ZERO, Footprint::saturating_add)
    }

    /// The key to split the state at so that its footprint is cut in two
    /// halves as nearly even as whole keys allow.
    ///
    /// It is the first key past the first whose keys before it take at
    /// least half, or the last key when the last key alone takes more
    /// than half. `None` when the state keeps fewer than two keys, so that
    /// no split leaves both sides with a key. It walks every key twice.
    #[must_use]
    pub fn middle(&self) -> Option<&[u8]> {
        // Half rounded up, so an even cut is taken at once.
        let half = self.footprint().bytes().div_ceil(2);
        let mut keys = self.keys.iter();
        let (first, kept) = keys.next()?;
        let mut before = Footprint::of(first, kept).bytes();
        let mut last = None;
        for (key, history) in keys {
            if before >= half {
                return Some(key);
            }
            last = Some(key.as_slice());
            before = before.saturating_add(Footprint::of(key, history).bytes());
        }
        last
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
    /// locks
    /// ```
    ///
    /// keys in order and each key's versions oldest first; tag 1 carries a
    /// length prefixed value, tag 0 marks a removal. The locks are the
    /// prepared transactions and the decisions.
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
        self.locks.encode(out)
    }

    /// Reads a state written by [`KvState::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] when the input ends early, and
    /// [`DecodeError::Invalid`] when the retention is zero, keys are out of
    /// order, or a key's versions are empty, out of order or newer than
    /// the state, or the locks repeat a transaction.
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
            locks: Locks::decode(reader, version)?,
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
