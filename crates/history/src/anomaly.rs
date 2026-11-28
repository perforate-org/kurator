//! What a check finds: anomalies, or a summary of a history that has none.

use std::fmt;

use crate::record::{Element, Key, TxnIndex};

/// How one committed transaction depends on another, by Adya's names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Dependency {
    /// `ww`: the second installed the version of a key right after the
    /// first's.
    Write,
    /// `wr`: the second read the version of a key the first installed.
    Read,
    /// `rw`: the first read a version of a key, and the second installed
    /// the next one.
    Anti,
}

impl Dependency {
    /// Every kind of dependency.
    pub const ALL: [Self; 3] = [Self::Write, Self::Read, Self::Anti];

    const fn name(self) -> &'static str {
        match self {
            Self::Write => "ww",
            Self::Read => "wr",
            Self::Anti => "rw",
        }
    }
}

/// Which phenomenon a dependency cycle is, by Adya's names: the cycles a
/// weaker isolation level allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum CycleKind {
    /// G0, write cycle: `ww` dependencies only.
    G0,
    /// G1c, circular information flow: `ww` and `wr`, at least one `wr`.
    G1c,
    /// G-single, read skew: exactly one `rw`.
    GSingle,
    /// G2, anti-dependency cycle: more than one `rw` (write skew among
    /// them).
    G2,
}

impl CycleKind {
    const fn name(self) -> &'static str {
        match self {
            Self::G0 => "G0",
            Self::G1c => "G1c",
            Self::GSingle => "G-single",
            Self::G2 => "G2",
        }
    }
}

/// One edge of a [`Cycle`]: `from` precedes `to` by `dependency` on `key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Step {
    from: TxnIndex,
    dependency: Dependency,
    key: Key,
}

impl Step {
    pub(crate) const fn new(from: TxnIndex, dependency: Dependency, key: Key) -> Self {
        Self {
            from,
            dependency,
            key,
        }
    }

    pub(crate) const fn from(self) -> TxnIndex {
        self.from
    }
}

/// A cycle of dependencies among committed transactions: no serial order
/// of them has each before the next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cycle {
    kind: CycleKind,
    /// Each step's `from`; the last step leads back to the first `from`.
    steps: Vec<Step>,
}

impl Cycle {
    pub(crate) const fn new(kind: CycleKind, steps: Vec<Step>) -> Self {
        Self { kind, steps }
    }

    /// Which phenomenon it is.
    #[must_use]
    pub const fn kind(&self) -> CycleKind {
        self.kind
    }

    /// The transactions on it, in order.
    pub fn txns(&self) -> impl Iterator<Item = TxnIndex> + '_ {
        self.steps.iter().map(|step| step.from)
    }
}

impl fmt::Display for Cycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} cycle:", self.kind.name())?;
        for step in &self.steps {
            write!(
                f,
                " {} -{} {}->",
                step.from,
                step.dependency.name(),
                step.key
            )?;
        }
        self.steps
            .first()
            .map_or(Ok(()), |first| write!(f, " {}", first.from))
    }
}

/// Something in a history that no serial execution of its committed
/// transactions explains.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Anomaly {
    /// Two appends of the same element: the history itself is malformed.
    Duplicated {
        /// The element appended twice.
        element: Element,
    },
    /// A read of a key that does not show what the transaction read or
    /// appended there before.
    Internal {
        /// The transaction.
        txn: TxnIndex,
        /// The key it read.
        key: Key,
    },
    /// A read of an element that no other transaction appended to that
    /// key.
    Garbage {
        /// The transaction that read it.
        reader: TxnIndex,
        /// The key it read.
        key: Key,
        /// The element nobody appended there.
        element: Element,
    },
    /// G1a: a read of what an aborted transaction appended.
    AbortedRead {
        /// The transaction that read it.
        reader: TxnIndex,
        /// The aborted transaction.
        writer: TxnIndex,
        /// The key.
        key: Key,
        /// The element the aborted transaction appended.
        element: Element,
    },
    /// G1b: a read of a version that another transaction went on to
    /// overwrite itself, so it never installed it.
    IntermediateRead {
        /// The transaction that read it.
        reader: TxnIndex,
        /// The transaction whose intermediate version it read.
        writer: TxnIndex,
        /// The key.
        key: Key,
        /// The last element of the version read.
        element: Element,
    },
    /// A version of a key that holds one element twice.
    Repeated {
        /// The key.
        key: Key,
        /// The element it holds twice.
        element: Element,
    },
    /// Two versions of a key read, neither a prefix of the other: no single
    /// order of appends made both.
    Incompatible {
        /// The key.
        key: Key,
        /// One version.
        one: Vec<Element>,
        /// The other.
        other: Vec<Element>,
    },
    /// A dependency cycle among committed transactions.
    Cycle(Cycle),
}

impl fmt::Display for Anomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicated { element } => write!(f, "{element} appended twice"),
            Self::Internal { txn, key } => {
                write!(f, "{txn} read {key} unlike what it read and wrote")
            }
            Self::Garbage {
                reader,
                key,
                element,
            } => write!(f, "{reader} read {element} in {key}, which nobody wrote"),
            Self::AbortedRead {
                reader,
                writer,
                key,
                element,
            } => write!(
                f,
                "G1a: {reader} read {element} in {key} of aborted {writer}"
            ),
            Self::IntermediateRead {
                reader,
                writer,
                key,
                element,
            } => write!(
                f,
                "G1b: {reader} read {key} up to {element}, an intermediate version of {writer}"
            ),
            Self::Repeated { key, element } => write!(f, "{key} holds {element} twice"),
            Self::Incompatible { key, one, other } => {
                write!(f, "{key} read as {} and as {}", List(one), List(other))
            }
            Self::Cycle(cycle) => cycle.fmt(f),
        }
    }
}

/// A version of a key, as its elements in brackets.
struct List<'a>(&'a [Element]);

impl fmt::Display for List<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (index, element) in self.0.iter().enumerate() {
            let separator = if index == 0 { "" } else { ", " };
            write!(f, "{separator}{element}")?;
        }
        write!(f, "]")
    }
}

/// Every anomaly a check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violations {
    anomalies: Vec<Anomaly>,
}

impl Violations {
    pub(crate) const fn new(anomalies: Vec<Anomaly>) -> Self {
        Self { anomalies }
    }

    /// The anomalies, each once.
    #[must_use]
    pub fn anomalies(&self) -> &[Anomaly] {
        &self.anomalies
    }
}

impl fmt::Display for Violations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} anomalies:", self.anomalies.len())?;
        self.anomalies
            .iter()
            .try_for_each(|anomaly| write!(f, "\n  {anomaly}"))
    }
}

impl std::error::Error for Violations {}

/// What a check that found no anomaly looked at: a pass over a history
/// with no dependencies says little.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checked {
    committed: usize,
    dependencies: [usize; 3],
}

impl Checked {
    pub(crate) const fn new(committed: usize, dependencies: [usize; 3]) -> Self {
        Self {
            committed,
            dependencies,
        }
    }

    /// The transactions that committed, those of unknown outcome whose
    /// writes were read among them.
    #[must_use]
    pub const fn committed(&self) -> usize {
        self.committed
    }

    /// How many pairs of transactions depend on each other by `dependency`.
    #[must_use]
    pub fn dependencies(&self, dependency: Dependency) -> usize {
        Dependency::ALL
            .iter()
            .zip(self.dependencies)
            .find_map(|(kind, count)| (*kind == dependency).then_some(count))
            .unwrap_or(0)
    }
}

impl fmt::Display for Checked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} committed", self.committed)?;
        Dependency::ALL
            .iter()
            .zip(self.dependencies)
            .try_for_each(|(kind, count)| write!(f, ", {count} {}", kind.name()))
    }
}
