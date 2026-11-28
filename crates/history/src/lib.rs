//! Checks a recorded history of transactions for isolation anomalies, in
//! the manner of Elle's list-append checker (Kingsbury and Alvaro,
//! "Elle: Inferring Isolation Anomalies from Experimental Observations",
//! VLDB 2021).
//!
//! The database is a black box. Each key holds a list, and a transaction
//! either reads a key's whole list or appends an element to it. Every
//! element is appended once in the whole history, so an element read tells
//! which transaction wrote it, and since a list only grows, the lists read
//! of one key tell the order in which its versions were installed. From
//! those the checker builds the dependency graph of the committed
//! transactions (Adya, "Weak Consistency", 1999):
//!
//! - *write* (`ww`): one transaction installed the version of a key right
//!   after another's;
//! - *read* (`wr`): one read the version another installed;
//! - *anti* (`rw`): one read a version, and another installed the next.
//!
//! A history is serializable only if that graph has no cycle, and only if
//! no transaction read what an aborted transaction wrote, an intermediate
//! version of another, or something nobody wrote. [`check`]
//! reports each such anomaly it finds, or a [`Checked`] summary of what it
//! looked at, to tell a pass from a history too thin to fail.
//!
//! What it does not do: it does not check real-time order (strict
//! serializability), predicates (ranges), or that reads of transactions
//! that did not commit were consistent. A transaction whose outcome is
//! unknown counts as committed exactly when a committed transaction read
//! something it wrote. A pass means the checker found no anomaly in this
//! history, not that the database has none.
//!
//! ```
//! use oleaph_history::{Element, Fate, History, Key, Txn, check};
//!
//! let (key, one, two) = (Key::new(0), Element::new(1), Element::new(2));
//! let mut history = History::new();
//! let mut first = Txn::new();
//! first.read(key, Vec::new());
//! first.append(key, one);
//! history.record(first, Fate::Committed);
//! let mut second = Txn::new();
//! second.read(key, vec![one]);
//! second.append(key, two);
//! history.record(second, Fate::Committed);
//! assert!(check(&history).is_ok());
//! ```

mod anomaly;
mod graph;
mod observe;
mod record;

pub use anomaly::{Anomaly, Checked, Cycle, CycleKind, Dependency, Violations};
pub use observe::check;
pub use record::{Element, Fate, History, Key, Txn, TxnIndex};
