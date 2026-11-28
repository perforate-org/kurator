//! Where a key is: the namespace it is a key of, and the spans, the
//! half-open ranges of keys, that shards hold.

use std::fmt;
use std::io::{self, Write};
use std::ops::Bound;

use oleaph_codec::{DecodeError, Reader, put_bytes};

/// Names a namespace: a key space of its own, split into shards of its
/// own, whose consensus is chosen when it is made. The same key in two
/// namespaces is two keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceId(u32);

impl NamespaceId {
    /// The namespace a cluster is founded with: every key a client names
    /// without a namespace.
    pub const DEFAULT: Self = Self(0);

    /// Wraps a raw id.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ns{}", self.0)
    }
}

const END_UNBOUNDED: u8 = 0;
const END_BEFORE: u8 = 1;

/// Where a [`Span`] ends.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum End {
    /// Just before this key, which is not in the span.
    Before(Vec<u8>),
    /// After every key.
    Unbounded,
}

impl End {
    /// Whether `key` comes before the end.
    #[must_use]
    pub fn is_after(&self, key: &[u8]) -> bool {
        match self {
            Self::Before(end) => key < end.as_slice(),
            Self::Unbounded => true,
        }
    }
}

/// A non-empty half-open range of keys, `[start, end)`: every key from
/// `start` on, up to but not including `end`.
///
/// ```
/// use oleaph_storage::{End, Span};
///
/// let span = Span::new("b", End::Before(b"m".to_vec())).expect("not empty");
/// assert!(span.contains(b"b") && span.contains(b"lemon"));
/// assert!(!span.contains(b"a") && !span.contains(b"m"));
/// let (left, right) = span.split_at(b"f").expect("inside");
/// assert_eq!(left.end(), &End::Before(b"f".to_vec()));
/// assert_eq!(right.start(), b"f");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Span {
    start: Vec<u8>,
    end: End,
}

impl Span {
    /// Every key.
    pub const FULL: Self = Self {
        start: Vec::new(),
        end: End::Unbounded,
    };

    /// The keys from `start` up to `end`, or `None` when there are none.
    #[must_use]
    pub fn new(start: impl Into<Vec<u8>>, end: End) -> Option<Self> {
        let start = start.into();
        end.is_after(&start).then_some(Self { start, end })
    }

    /// The first key.
    #[must_use]
    pub fn start(&self) -> &[u8] {
        &self.start
    }

    /// Where it ends.
    #[must_use]
    pub const fn end(&self) -> &End {
        &self.end
    }

    /// Whether `key` is in the span.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.is_after(key)
    }

    /// Whether every key between `start` and `end` is in the span.
    #[must_use]
    pub fn holds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
        let from = self.start.as_slice();
        let starts_inside = match start {
            Bound::Unbounded => from.is_empty(),
            Bound::Included(key) => key >= from,
            Bound::Excluded(key) => key >= from || is_successor(from, key),
        };
        let ends_inside = match (end, &self.end) {
            (_, End::Unbounded) => true,
            (Bound::Unbounded, End::Before(_)) => false,
            (Bound::Included(key), End::Before(limit)) => key < limit.as_slice(),
            (Bound::Excluded(key), End::Before(limit)) => key <= limit.as_slice(),
        };
        starts_inside && ends_inside
    }

    /// The keys between `start` and `end` that are in the span, as bounds
    /// that [`Span::holds`].
    #[must_use]
    pub fn clip<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
    ) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        let from = self.start.as_slice();
        let start = match start {
            Bound::Included(key) | Bound::Excluded(key) if key >= from => start,
            Bound::Unbounded | Bound::Included(_) | Bound::Excluded(_) => Bound::Included(from),
        };
        let end = match (end, &self.end) {
            (_, End::Unbounded) => end,
            (Bound::Included(key), End::Before(limit)) if key < limit.as_slice() => end,
            (Bound::Excluded(key), End::Before(limit)) if key <= limit.as_slice() => end,
            (Bound::Unbounded | Bound::Included(_) | Bound::Excluded(_), End::Before(limit)) => {
                Bound::Excluded(limit.as_slice())
            }
        };
        (start, end)
    }

    /// The two spans either side of `at`, which must be in the span and
    /// after its start, so that neither is empty.
    #[must_use]
    pub fn split_at(&self, at: &[u8]) -> Option<(Self, Self)> {
        (self.contains(at) && at > self.start.as_slice()).then(|| {
            let left = Self {
                start: self.start.clone(),
                end: End::Before(at.to_vec()),
            };
            let right = Self {
                start: at.to_vec(),
                end: self.end.clone(),
            };
            (left, right)
        })
    }

    /// Writes the span as `start | 0` when it is unbounded, or
    /// `start | 1 end`; keys length prefixed.
    ///
    /// # Errors
    ///
    /// When `out` fails or a key is too long for its prefix.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        put_bytes(out, &self.start)?;
        match &self.end {
            End::Unbounded => out.write_all(&[END_UNBOUNDED]),
            End::Before(end) => {
                out.write_all(&[END_BEFORE])?;
                put_bytes(out, end)
            }
        }
    }

    /// Reads a span written by [`Span::encode`] off the front of `reader`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] when the input ends early,
    /// [`DecodeError::Tag`] for an unknown end, and
    /// [`DecodeError::Invalid`] when the span is empty.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let start = reader.bytes()?.to_vec();
        let end = match reader.read::<u8>()? {
            END_UNBOUNDED => End::Unbounded,
            END_BEFORE => End::Before(reader.bytes()?.to_vec()),
            other => return Err(DecodeError::Tag(other)),
        };
        Self::new(start, end).ok_or(DecodeError::Invalid)
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, ", self.start.escape_ascii())?;
        match &self.end {
            End::Before(end) => write!(f, "{})", end.escape_ascii()),
            End::Unbounded => f.write_str("..)"),
        }
    }
}

/// Whether `key` is the first key after `before`: `before` and a zero.
fn is_successor(key: &[u8], before: &[u8]) -> bool {
    key.split_last() == Some((&0, before))
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::{self, Excluded, Included, Unbounded};

    use proptest::prelude::*;

    use super::{End, NamespaceId, Span};
    use crate::txn::KeyRange;
    use oleaph_codec::{DecodeError, Reader};

    fn span(start: &[u8], end: Option<&[u8]>) -> Span {
        let end = end.map_or(End::Unbounded, |end| End::Before(end.to_vec()));
        Span::new(start, end).unwrap()
    }

    fn key() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(0_u8..3, 0..3)
    }

    fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
        prop_oneof![
            Just(Unbounded),
            key().prop_map(Included),
            key().prop_map(Excluded),
        ]
    }

    fn spans() -> impl Strategy<Value = Span> {
        (key(), proptest::option::of(key())).prop_filter_map("empty", |(start, end)| {
            Span::new(start, end.map_or(End::Unbounded, End::Before))
        })
    }

    fn borrowed(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
        bound.as_ref().map(Vec::as_slice)
    }

    /// Every key `holds` and `clip` are judged on: all short keys over a
    /// small alphabet, so each one sits next to its neighbours.
    fn every_key() -> Vec<Vec<u8>> {
        let mut keys = vec![Vec::new()];
        let mut longest = vec![Vec::new()];
        for _ in 0..3 {
            longest = longest
                .iter()
                .flat_map(|prefix| (0..3).map(move |byte| [prefix.as_slice(), &[byte]].concat()))
                .collect();
            keys.extend(longest.iter().cloned());
        }
        keys
    }

    #[test]
    fn namespace_ids_show_and_wrap() {
        assert_eq!(NamespaceId::new(4).to_string(), "ns4", "display");
        assert_eq!(NamespaceId::DEFAULT.get(), 0, "the default namespace");
        assert_eq!(NamespaceId::new(5).get(), 5, "raw");
    }

    #[test]
    fn empty_spans_do_not_exist() {
        assert_eq!(Span::new("b", End::Before(b"b".to_vec())), None, "empty");
        assert_eq!(Span::new("c", End::Before(b"b".to_vec())), None, "reversed");
        assert!(Span::new("", End::Unbounded).is_some(), "full");
        assert_eq!(Span::FULL, span(b"", None), "the full span");
    }

    #[test]
    fn splits_need_a_key_strictly_inside() {
        let span = span(b"b", Some(b"m"));
        assert_eq!(span.split_at(b"b"), None, "at the start");
        assert_eq!(span.split_at(b"m"), None, "at the end");
        assert_eq!(span.split_at(b"a"), None, "before");
        let (left, right) = span.split_at(b"c").unwrap();
        assert_eq!(left.to_string(), "[b, c)", "left");
        assert_eq!(right.to_string(), "[c, m)", "right");
        assert_eq!(Span::FULL.to_string(), "[, ..)", "unbounded");
    }

    #[test]
    fn an_excluded_start_holds_from_its_successor() {
        let span = span(b"a\0", None);
        assert!(span.holds(Excluded(b"a"), Unbounded), "successor");
        assert!(!span.holds(Excluded(b"\0"), Unbounded), "before");
        assert!(!span.holds(Unbounded, Unbounded), "unbounded");
        assert!(Span::FULL.holds(Unbounded, Unbounded), "full");
    }

    #[test]
    fn a_range_ending_at_the_end_is_held_only_when_it_excludes_it() {
        let span = span(b"b", Some(b"m"));
        assert!(span.holds(Included(b"b"), Excluded(b"m")), "up to the end");
        assert!(
            !span.holds(Included(b"b"), Included(b"m")),
            "the end itself"
        );
        assert!(span.holds(Included(b"b"), Included(b"l")), "before the end");
        assert!(!span.holds(Included(b"b"), Unbounded), "past the end");
    }

    #[test]
    fn bad_encodings_are_rejected() {
        let mut out = Vec::new();
        span(b"a", Some(b"b")).encode(&mut out).unwrap();
        let mut bad = out.clone();
        bad[5] = 7;
        assert_eq!(
            Span::decode(&mut Reader::new(&bad)),
            Err(DecodeError::Tag(7)),
            "tag"
        );
        let mut empty = Vec::new();
        Span {
            start: b"b".to_vec(),
            end: End::Before(b"a".to_vec()),
        }
        .encode(&mut empty)
        .unwrap();
        assert_eq!(
            Span::decode(&mut Reader::new(&empty)),
            Err(DecodeError::Invalid),
            "empty"
        );
    }

    proptest! {
        #[test]
        fn clipping_keeps_the_keys_inside(span in spans(), start in bound(), end in bound()) {
            let range = KeyRange::new(borrowed(&start), borrowed(&end));
            let (from, to) = span.clip(borrowed(&start), borrowed(&end));
            prop_assert!(span.holds(from, to), "clipped ranges are held");
            let clipped = KeyRange::new(from, to);
            for key in every_key() {
                prop_assert_eq!(
                    clipped.contains(&key),
                    range.contains(&key) && span.contains(&key),
                    "key {:?}", key
                );
            }
        }

        #[test]
        fn a_range_is_held_when_no_key_escapes(
            span in spans(),
            start in bound(),
            end in bound(),
        ) {
            // The first key of the range, the neighbours of its bounds and
            // the span's own bounds are enough to find a key that escapes.
            let first = match &start {
                Unbounded => Vec::new(),
                Included(key) => key.clone(),
                Excluded(key) => [key.as_slice(), &[0]].concat(),
            };
            let range = KeyRange::new(borrowed(&start), borrowed(&end));
            let mut keys = every_key();
            keys.push(first.clone());
            keys.push(span.start().to_vec());
            if let End::Before(limit) = span.end() {
                keys.push(limit.clone());
            }
            let held = span.holds(borrowed(&start), borrowed(&end));
            let escapes = keys.iter().any(|key| range.contains(key) && !span.contains(key));
            if range.contains(&first) {
                prop_assert_eq!(held, !escapes, "exact for a range with keys");
            } else {
                prop_assert!(!escapes, "an empty range has no keys");
            }
        }

        #[test]
        fn splits_partition_the_span(span in spans(), at in key(), probe in key()) {
            if let Some((left, right)) = span.split_at(&at) {
                prop_assert_eq!(
                    span.contains(&probe),
                    left.contains(&probe) || right.contains(&probe),
                    "covered"
                );
                prop_assert!(!(left.contains(&probe) && right.contains(&probe)), "disjoint");
                prop_assert!(right.contains(&at) && !left.contains(&at), "at goes right");
            }
        }

        #[test]
        fn spans_roundtrip(span in spans()) {
            let mut out = Vec::new();
            span.encode(&mut out).unwrap();
            let mut reader = Reader::new(&out);
            prop_assert_eq!(Span::decode(&mut reader), Ok(span));
            prop_assert!(reader.finish().is_ok(), "nothing left");
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::Span;
    use oleaph_codec::Reader;

    /// Decoding a span never panics, and a decoded span encodes back to
    /// exactly the bytes it was read from.
    #[kani::proof]
    #[kani::unwind(12)]
    fn spans_decode_totally_and_roundtrip() {
        let buf: [u8; 11] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let mut reader = Reader::new(&buf[..len]);
        if let Ok(decoded) = Span::decode(&mut reader) {
            let read = len - reader.rest().len();
            let mut out = Vec::new();
            assert!(decoded.encode(&mut out).is_ok(), "small keys");
            assert_eq!(out.as_slice(), &buf[..read], "roundtrip");
        }
    }
}
