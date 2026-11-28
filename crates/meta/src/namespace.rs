//! Namespaces as the meta shard records them: each one's id, name and
//! consensus.

use std::fmt;
use std::io::{self, Write};
use std::str::FromStr;

use oleaph_codec::{DecodeError, Reader};
use oleaph_shard::NamespaceId;

/// The longest name a namespace may have, in bytes.
const MAX_NAME: usize = 63;

/// The name of [`NamespaceId::DEFAULT`].
const DEFAULT_NAME: &str = "default";

/// The name a namespace goes by: 1 to 63 lowercase ASCII letters, digits
/// and hyphens, starting with a letter and not ending with a hyphen, as a
/// DNS label is. No two namespaces share a name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceName(String);

/// A name that is not a [`NamespaceName`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
#[error(
    "a namespace name is 1 to 63 lowercase letters, digits and hyphens, \
     starting with a letter and not ending with a hyphen"
)]
pub struct BadName;

impl NamespaceName {
    /// The name of [`NamespaceId::DEFAULT`]: `default`.
    #[must_use]
    pub fn default_name() -> Self {
        Self(DEFAULT_NAME.to_owned())
    }

    /// The name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name `bytes` spell, read off the wire.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::check(bytes).map_err(|BadName| DecodeError::Invalid)?;
        let name = String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::Invalid)?;
        Ok(Self(name))
    }

    /// Checks `bytes` against the rule [`NamespaceName`] states.
    fn check(bytes: &[u8]) -> Result<(), BadName> {
        let (Some(first), Some(last)) = (bytes.first(), bytes.last()) else {
            return Err(BadName);
        };
        let allowed =
            |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-';
        let fits = bytes.len() <= MAX_NAME
            && first.is_ascii_lowercase()
            && *last != b'-'
            && bytes.iter().all(allowed);
        if fits { Ok(()) } else { Err(BadName) }
    }
}

impl FromStr for NamespaceName {
    type Err = BadName;

    fn from_str(name: &str) -> Result<Self, BadName> {
        Self::check(name.as_bytes())?;
        Ok(Self(name.to_owned()))
    }
}

impl fmt::Display for NamespaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How a namespace's shards agree on what they hold, chosen when it is
/// made and kept for as long as it exists.
///
/// The names are those of the faults a namespace survives, not of the
/// algorithms that survive them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Assurance {
    /// Crash fault tolerant, for speed: every shard is a Raft group whose
    /// replicas one operator runs, and which survives fewer than half of
    /// them stopping, but trusts them all.
    Cft,
}

/// The tag of [`Assurance::Cft`].
const CFT: u8 = 1;

impl Assurance {
    /// Writes the assurance as one byte, its tag.
    pub(crate) fn encode(self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        let tag = match self {
            Self::Cft => CFT,
        };
        out.write_all(&[tag])
    }

    /// Reads an assurance written by [`Assurance::encode`] off the front
    /// of `reader`.
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match reader.read::<u8>()? {
            CFT => Ok(Self::Cft),
            other => Err(DecodeError::Tag(other)),
        }
    }
}

/// An assurance that is not `cft`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
#[error("the assurance is `cft`")]
pub struct BadAssurance;

impl FromStr for Assurance {
    type Err = BadAssurance;

    fn from_str(name: &str) -> Result<Self, BadAssurance> {
        match name {
            "cft" => Ok(Self::Cft),
            _ => Err(BadAssurance),
        }
    }
}

impl fmt::Display for Assurance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cft => "cft",
        })
    }
}

/// A namespace as the registry records it under its name: its id and its
/// assurance. The name is the registry's key for it, not part of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Namespace {
    id: NamespaceId,
    assurance: Assurance,
}

impl Namespace {
    /// The namespace `id`, kept as `assurance` says.
    #[must_use]
    pub const fn new(id: NamespaceId, assurance: Assurance) -> Self {
        Self { id, assurance }
    }

    /// The namespace a cluster is founded with, called `default`:
    /// [`NamespaceId::DEFAULT`], crash fault tolerant like the meta shard.
    #[must_use]
    pub const fn founding() -> Self {
        Self::new(NamespaceId::DEFAULT, Assurance::Cft)
    }

    /// Its id.
    #[must_use]
    pub const fn id(&self) -> NamespaceId {
        self.id
    }

    /// Its assurance.
    #[must_use]
    pub const fn assurance(&self) -> Assurance {
        self.assurance
    }

    /// Writes the namespace as `id u32 | assurance u8`.
    ///
    /// # Errors
    ///
    /// When `out` fails.
    pub fn encode(&self, out: &mut (impl Write + ?Sized)) -> io::Result<()> {
        out.write_all(&self.id.get().to_le_bytes())?;
        self.assurance.encode(out)
    }

    /// Decodes what [`Namespace::encode`] wrote, all of `bytes`.
    ///
    /// # Errors
    ///
    /// When `bytes` is not one whole namespace.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let id = NamespaceId::new(reader.read()?);
        let assurance = Assurance::decode(&mut reader)?;
        reader.finish()?;
        Ok(Self::new(id, assurance))
    }
}

/// `ns2 cft`: the id and the assurance.
impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.id, self.assurance)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use oleaph_codec::DecodeError;
    use oleaph_shard::NamespaceId;
    use proptest::prelude::*;

    use super::{Assurance, BadAssurance, BadName, Namespace, NamespaceName};

    pub(crate) fn names() -> impl Strategy<Value = NamespaceName> {
        "[a-z]([a-z0-9-]{0,61}[a-z0-9])?".prop_map(|name| name.parse().unwrap())
    }

    pub(crate) fn namespaces() -> impl Strategy<Value = Namespace> {
        any::<u32>().prop_map(|id| Namespace::new(NamespaceId::new(id), Assurance::Cft))
    }

    #[test]
    fn names_follow_the_rule() {
        for good in ["a", "payments", "eu-west-1", "a1", &"a".repeat(63)] {
            assert!(good.parse::<NamespaceName>().is_ok(), "{good}");
        }
        for bad in [
            "",
            "1a",
            "-a",
            "a-",
            "Payments",
            "a_b",
            "a.b",
            "ä",
            &"a".repeat(64),
        ] {
            assert_eq!(bad.parse::<NamespaceName>(), Err(BadName), "{bad:?}");
        }
        let name: NamespaceName = "payments".parse().unwrap();
        assert_eq!(name.to_string(), "payments", "display");
        assert_eq!(name.as_str(), "payments", "as text");
        assert_eq!(NamespaceName::default_name().as_str(), "default", "default");
    }

    #[test]
    fn assurances_read_and_show() {
        assert_eq!("cft".parse(), Ok(Assurance::Cft), "cft");
        assert_eq!("raft".parse::<Assurance>(), Err(BadAssurance), "raft");
        assert_eq!(Assurance::Cft.to_string(), "cft", "display");
    }

    #[test]
    fn the_founding_namespace_is_the_default() {
        let founding = Namespace::founding();
        assert_eq!(founding.id(), NamespaceId::DEFAULT, "id");
        assert_eq!(founding.assurance(), Assurance::Cft, "assurance");
        assert_eq!(founding.to_string(), "ns0 cft", "display");
    }

    #[test]
    fn damaged_namespaces_are_rejected() {
        let mut out = Vec::new();
        Namespace::founding().encode(&mut out).unwrap();
        let mut tagged = out.clone();
        tagged[4] = 9;
        assert_eq!(
            Namespace::decode(&tagged),
            Err(DecodeError::Tag(9)),
            "unknown assurance"
        );
        assert_eq!(
            NamespaceName::from_bytes(b"a-"),
            Err(DecodeError::Invalid),
            "a bad name"
        );
        assert_eq!(
            NamespaceName::from_bytes(&[0xFF]),
            Err(DecodeError::Invalid),
            "not a name"
        );
        assert_eq!(
            NamespaceName::from_bytes(b"ok"),
            Ok("ok".parse().unwrap()),
            "a name"
        );
        let longer = [out.as_slice(), &[0]].concat();
        assert_eq!(
            Namespace::decode(&longer),
            Err(DecodeError::Trailing),
            "trailing"
        );
    }

    proptest! {
        #[test]
        fn namespaces_roundtrip(namespace in namespaces()) {
            let mut out = Vec::new();
            namespace.encode(&mut out).unwrap();
            prop_assert_eq!(Namespace::decode(&out), Ok(namespace));
            for cut in 0..out.len() {
                let (head, _) = out.split_at(cut);
                prop_assert!(Namespace::decode(head).is_err(), "cut at {}", cut);
            }
        }

        #[test]
        fn names_parse_as_checked(name in "[a-z0-9_-]{0,65}") {
            let parsed = name.parse::<NamespaceName>();
            let fits = !name.is_empty()
                && name.len() <= 63
                && name.starts_with(|c: char| c.is_ascii_lowercase())
                && !name.ends_with('-')
                && !name.contains('_');
            prop_assert_eq!(parsed.is_ok(), fits);
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::{Namespace, NamespaceName};

    /// Decoding a namespace never panics, and a decoded namespace encodes
    /// back to exactly the bytes it was read from.
    #[kani::proof]
    fn namespaces_decode_totally_and_roundtrip() {
        let buf: [u8; 6] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let bytes = &buf[..len];
        if let Ok(decoded) = Namespace::decode(bytes) {
            let mut out = Vec::new();
            assert!(decoded.encode(&mut out).is_ok(), "into memory");
            assert_eq!(out.as_slice(), bytes, "roundtrip");
        }
    }

    /// Reading a name off the wire never panics, and a name read spells
    /// the bytes it was read from.
    #[kani::proof]
    #[kani::unwind(6)]
    fn names_read_totally() {
        let buf: [u8; 4] = kani::any();
        let len: usize = kani::any_where(|len| *len <= buf.len());
        let bytes = &buf[..len];
        if let Ok(name) = NamespaceName::from_bytes(bytes) {
            assert_eq!(name.as_str().as_bytes(), bytes, "the same bytes");
        }
    }
}
