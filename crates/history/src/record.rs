//! What a test records: transactions, each a sequence of reads and appends,
//! and how each ended.

use std::fmt;

/// A key, holding a list of [`Element`]s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(u64);

impl Key {
    /// The key numbered `number`.
    #[must_use]
    pub const fn new(number: u64) -> Self {
        Self(number)
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "k{}", self.0)
    }
}

/// An element of a key's list. A history appends each element once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Element(u64);

impl Element {
    /// The element `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Its value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Element {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A transaction's place in its [`History`], in the order it was recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnIndex(usize);

impl TxnIndex {
    pub(crate) const fn new(index: usize) -> Self {
        Self(index)
    }
}

impl fmt::Display for TxnIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}", self.0)
    }
}

/// How a transaction ended, as its client saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fate {
    /// The database said it committed.
    Committed,
    /// The database said it wrote nothing, or it never reached the
    /// database.
    Aborted,
    /// The client cannot tell: it may have committed.
    Unknown,
}

/// One step of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Op {
    /// It read `key`'s whole list.
    Read { key: Key, list: Vec<Element> },
    /// It appended `element` to `key`'s list.
    Append { key: Key, element: Element },
}

/// A transaction as its client saw it: its reads, with what they returned,
/// and its appends, in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Txn {
    ops: Vec<Op>,
}

impl Txn {
    /// A transaction that did nothing yet.
    #[must_use]
    pub const fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// It read `list` at `key`.
    pub fn read(&mut self, key: Key, list: Vec<Element>) {
        self.ops.push(Op::Read { key, list });
    }

    /// It appended `element` to the list at `key`.
    pub fn append(&mut self, key: Key, element: Element) {
        self.ops.push(Op::Append { key, element });
    }

    pub(crate) fn ops(&self) -> &[Op] {
        &self.ops
    }
}

/// A transaction and how it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Recorded {
    txn: Txn,
    fate: Fate,
}

impl Recorded {
    pub(crate) fn ops(&self) -> &[Op] {
        self.txn.ops()
    }

    pub(crate) const fn fate(&self) -> Fate {
        self.fate
    }
}

/// Every transaction a test ran, in any order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    txns: Vec<Recorded>,
}

impl History {
    /// No transactions.
    #[must_use]
    pub const fn new() -> Self {
        Self { txns: Vec::new() }
    }

    /// Adds `txn`, which ended as `fate`.
    pub fn record(&mut self, txn: Txn, fate: Fate) -> TxnIndex {
        let index = TxnIndex::new(self.txns.len());
        self.txns.push(Recorded { txn, fate });
        index
    }

    pub(crate) fn txns(&self) -> impl Iterator<Item = (TxnIndex, &Recorded)> {
        self.txns
            .iter()
            .enumerate()
            .map(|(index, recorded)| (TxnIndex::new(index), recorded))
    }
}
