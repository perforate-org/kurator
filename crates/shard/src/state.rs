//! The state of one shard: its keys, and what it is.

use std::io::{self, Write};

use kurator_codec::{DecodeError, Reader};
use kurator_storage::{Command, KvState, Outcome, Version};

use crate::shard::{Halves, Shard, Split};

/// What applying an entry to a shard did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Applied {
    /// A command applied, or aborted, as [`KvState::apply`] decided.
    Data(Outcome),
    /// The shard split: it is now the left half, and the right half is a
    /// new shard.
    Split(Halves),
    /// The entry does not fit the shard as it is now: a command touches a
    /// key outside its span, or a split names another epoch or a key not
    /// strictly inside it. Nothing changed; this is the shard as it is, for
    /// the sender to route again.
    Misrouted(Shard),
}

/// One shard's state: the [`Shard`] it is and the [`KvState`] of its
/// keys.
///
/// Like a key-value state, it changes only by applying entries in order,
/// and every replica that applies the same entries holds the same state.
/// The span is checked as each entry applies, not as it is proposed, so a
/// command that was routed before a split and applies after it is
/// refused alike everywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardState {
    shard: Shard,
    kv: KvState,
}

impl Default for ShardState {
    fn default() -> Self {
        Self::new(Shard::first(), KvState::default())
    }
}

impl ShardState {
    /// The shard `shard` holding `kv`.
    #[must_use]
    pub const fn new(shard: Shard, kv: KvState) -> Self {
        Self { shard, kv }
    }

    /// What the shard is.
    #[must_use]
    pub const fn shard(&self) -> &Shard {
        &self.shard
    }

    /// Its keys.
    #[must_use]
    pub const fn kv(&self) -> &KvState {
        &self.kv
    }

    /// Applies `command` at `version`, which must be above the state's
    /// version, if everything it touches is in the shard's span.
    pub fn apply(&mut self, version: Version, command: Command) -> Applied {
        if !command.fits(self.shard.span()) {
            return Applied::Misrouted(self.shard.clone());
        }
        Applied::Data(self.kv.apply(version, command))
    }

    /// Splits the shard as `split` asks: this state keeps the left half
    /// and the right half comes back, at the same version.
    ///
    /// # Errors
    ///
    /// When the split does not apply to this shard as it is now; see
    /// [`Shard::split`]. Nothing changes.
    pub fn split(&mut self, split: &Split) -> Result<(Halves, Self), Shard> {
        let Some(halves) = self.shard.split(split) else {
            return Err(self.shard.clone());
        };
        let right = self.kv.split_off(split.at());
        self.shard = halves.left().clone();
        let right = Self::new(halves.right().clone(), right);
        Ok((halves, right))
    }

    /// Writes the state as `shard | kv`, as [`Shard::encode`] and
    /// [`KvState::encode`] write them.
    ///
    /// # Errors
    ///
    /// When `out` fails or a key or value is too long for its prefix.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        self.shard.encode(out)?;
        self.kv.encode(out)
    }

    /// Reads a state written by [`ShardState::encode`] off the front of
    /// `reader`.
    ///
    /// # Errors
    ///
    /// As [`Shard::decode`] and [`KvState::decode`], and
    /// [`DecodeError::Invalid`] when a key lies outside the span.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let shard = Shard::decode(reader)?;
        let kv = KvState::decode(reader)?;
        let span = shard.span();
        let outside = kv.keys().any(|key| !span.contains(key));
        if outside {
            return Err(DecodeError::Invalid);
        }
        Ok(Self::new(shard, kv))
    }
}

#[cfg(test)]
mod tests {
    use kurator_codec::{DecodeError, Reader};
    use kurator_storage::{Command, End, KvState, Outcome, Span, Version, WriteBatch};

    use super::{Applied, ShardState};
    use crate::id::{Epoch, ShardId};
    use crate::shard::{Shard, Split};

    fn put(key: &str) -> Command {
        Command::Kv(WriteBatch::new().put(key, "v"))
    }

    fn roundtrip(state: &ShardState) -> Result<ShardState, DecodeError> {
        let mut out = Vec::new();
        state.encode(&mut out).map_err(|_| DecodeError::Truncated)?;
        let mut reader = Reader::new(&out);
        let decoded = ShardState::decode(&mut reader)?;
        reader.finish()?;
        Ok(decoded)
    }

    #[test]
    fn a_split_hands_the_right_half_over() {
        let mut state = ShardState::default();
        for (version, key) in (1..).zip(["apple", "melon", "zucchini"]) {
            let applied = state.apply(Version::new(version), put(key));
            assert_eq!(
                applied,
                Applied::Data(Outcome::Committed(Version::new(version))),
                "{key}"
            );
        }
        let split = Split::new("m", ShardId::new(2), Epoch::ZERO);
        let (halves, right) = state.split(&split).unwrap();
        assert_eq!(state.shard(), halves.left(), "left");
        assert_eq!(right.shard(), halves.right(), "right");
        assert_eq!(state.kv().iter().count(), 1, "left keys");
        assert_eq!(right.kv().iter().count(), 2, "right keys");
        assert_eq!(right.kv().version(), Version::new(3), "same version");
        assert_eq!(
            state.split(&split),
            Err(halves.left().clone()),
            "a stale split"
        );
        assert_eq!(
            state.apply(Version::new(4), put("melon")),
            Applied::Misrouted(halves.left().clone()),
            "a key it gave away"
        );
        assert_eq!(state.kv().version(), Version::new(3), "nothing applied");
        assert_eq!(roundtrip(&state), Ok(state.clone()), "left roundtrips");
        assert_eq!(roundtrip(&right), Ok(right), "right roundtrips");
    }

    #[test]
    fn a_state_with_a_key_outside_its_span_does_not_decode() {
        let mut kv = KvState::default();
        let _outcome = kv.apply(Version::new(1), put("z"));
        let span = Span::new("", End::Before(b"m".to_vec())).unwrap();
        let state = ShardState::new(Shard::new(ShardId::FIRST, span, Epoch::ZERO), kv);
        assert_eq!(roundtrip(&state), Err(DecodeError::Invalid), "outside");
    }
}
