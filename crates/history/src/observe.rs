//! From a history to what its committed transactions observed: who wrote
//! each element, which versions of each key were read, in which order the
//! versions were installed, and the anomalies on the way.

use std::collections::{BTreeMap, BTreeSet};

use crate::anomaly::{Anomaly, Checked, Dependency, Violations};
use crate::graph::Graph;
use crate::record::{Element, Fate, History, Key, Op, TxnIndex};

/// What one transaction saw and did, its reads with its own appends taken
/// out.
#[derive(Debug, Default)]
struct View {
    /// For each key it read, the version it read before its own appends.
    reads: BTreeMap<Key, Vec<Element>>,
    /// For each key, the elements it appended, in order.
    appends: BTreeMap<Key, Vec<Element>>,
    /// For each key it read and appended to, the version it installed.
    installs: BTreeMap<Key, Vec<Element>>,
}

/// A read that does not show what its transaction read or wrote before.
#[derive(Debug)]
struct Inconsistent;

/// What a transaction knows of one key as it runs.
#[derive(Debug)]
enum Seen {
    /// It appended these and read nothing yet.
    Blind(Vec<Element>),
    /// It knows the whole list.
    Known(Vec<Element>),
}

impl View {
    /// What `ops` saw, and the anomalies of `txn` among them.
    fn of(txn: TxnIndex, ops: &[Op], anomalies: &mut Vec<Anomaly>) -> Self {
        let mut view = Self::default();
        let mut seen: BTreeMap<Key, Seen> = BTreeMap::new();
        for op in ops {
            match op {
                Op::Read { key, list } => {
                    let inconsistent = view.read(&mut seen, *key, list).err();
                    let key = *key;
                    anomalies
                        .extend(inconsistent.map(|Inconsistent| Anomaly::Internal { txn, key }));
                }
                Op::Append { key, element } => view.append(&mut seen, *key, *element),
            }
        }
        for (key, known) in seen {
            if let Seen::Known(list) = known
                && view.appends.contains_key(&key)
            {
                let _first: Option<Vec<Element>> = view.installs.insert(key, list);
            }
        }
        view
    }

    /// Takes a read of `list` at `key`, unless it does not show what the
    /// transaction read and appended there before.
    fn read(
        &mut self,
        seen: &mut BTreeMap<Key, Seen>,
        key: Key,
        list: &[Element],
    ) -> Result<(), Inconsistent> {
        let external = match seen.get(&key) {
            Some(Seen::Known(known)) if known.as_slice() == list => return Ok(()),
            Some(Seen::Known(_)) => return Err(Inconsistent),
            Some(Seen::Blind(own)) => list.strip_suffix(own.as_slice()).ok_or(Inconsistent)?,
            None => list,
        };
        let _first: Option<Vec<Element>> = self.reads.insert(key, external.to_vec());
        let _blind: Option<Seen> = seen.insert(key, Seen::Known(list.to_vec()));
        Ok(())
    }

    /// Takes an append of `element` to `key`.
    fn append(&mut self, seen: &mut BTreeMap<Key, Seen>, key: Key, element: Element) {
        self.appends.entry(key).or_default().push(element);
        match seen.entry(key).or_insert_with(|| Seen::Blind(Vec::new())) {
            Seen::Blind(list) | Seen::Known(list) => list.push(element),
        }
    }

    /// Each key it read, with the version it read before its own appends.
    fn reads(&self) -> impl Iterator<Item = (Key, &[Element])> + '_ {
        self.reads.iter().map(|(key, list)| (*key, list.as_slice()))
    }

    /// Each key it read and appended to, with the version it installed.
    fn installs(&self) -> impl Iterator<Item = (Key, &[Element])> + '_ {
        self.installs
            .iter()
            .map(|(key, list)| (*key, list.as_slice()))
    }

    /// Every element it appended, with the key.
    fn appended(&self) -> impl Iterator<Item = (Element, Key)> + '_ {
        self.appends
            .iter()
            .flat_map(|(key, elements)| elements.iter().map(|element| (*element, *key)))
    }

    /// The last element it appended to `key`.
    fn last_append(&self, key: Key) -> Option<Element> {
        self.appends.get(&key)?.last().copied()
    }
}

/// Checks `history`: what it looked at when it found no anomaly.
///
/// # Errors
///
/// [`Violations`], every anomaly it found, when the committed transactions
/// of the history are not serializable.
pub fn check(history: &History) -> Result<Checked, Violations> {
    Analysis::of(history).finish()
}

/// A history taken apart.
#[derive(Debug)]
struct Analysis {
    fates: BTreeMap<TxnIndex, Fate>,
    views: BTreeMap<TxnIndex, View>,
    /// Who appended each element, to which key.
    writers: BTreeMap<Element, (TxnIndex, Key)>,
    /// The committed transactions, and those of unknown outcome whose
    /// writes they read.
    committed: BTreeSet<TxnIndex>,
    anomalies: Vec<Anomaly>,
}

impl Analysis {
    /// Takes `history` apart.
    fn of(history: &History) -> Self {
        let mut anomalies = Vec::new();
        let fates = history
            .txns()
            .map(|(index, recorded)| (index, recorded.fate()))
            .collect();
        let views: BTreeMap<TxnIndex, View> = history
            .txns()
            .map(|(index, recorded)| (index, View::of(index, recorded.ops(), &mut anomalies)))
            .collect();
        let appended = views.iter().flat_map(|(index, view)| {
            view.appended()
                .map(move |(element, key)| (element, (*index, key)))
        });
        let mut writers = BTreeMap::new();
        for (element, writer) in appended {
            if writers.insert(element, writer).is_some() {
                anomalies.push(Anomaly::Duplicated { element });
            }
        }
        let mut analysis = Self {
            fates,
            views,
            writers,
            committed: BTreeSet::new(),
            anomalies,
        };
        analysis.committed = analysis.committed();
        analysis
    }

    /// The committed transactions, with every transaction of unknown
    /// outcome that one of them read from.
    fn committed(&self) -> BTreeSet<TxnIndex> {
        let mut committed: BTreeSet<TxnIndex> = self
            .fates
            .iter()
            .filter(|(_, fate)| **fate == Fate::Committed)
            .map(|(index, _)| *index)
            .collect();
        let mut pending: Vec<TxnIndex> = committed.iter().copied().collect();
        while let Some(reader) = pending.pop() {
            let found: BTreeSet<TxnIndex> = self
                .read_from(reader)
                .filter(|writer| self.fates.get(writer) == Some(&Fate::Unknown))
                .filter(|writer| !committed.contains(writer))
                .collect();
            pending.extend(&found);
            committed.extend(found);
        }
        committed
    }

    /// Every transaction that appended an element `reader` read.
    fn read_from(&self, reader: TxnIndex) -> impl Iterator<Item = TxnIndex> + '_ {
        self.views
            .get(&reader)
            .into_iter()
            .flat_map(View::reads)
            .flat_map(|(_, list)| list)
            .filter_map(|element| self.writers.get(element).map(|(writer, _)| *writer))
    }

    /// The reads of the committed transactions: reader, key, version.
    fn reads(&self) -> impl Iterator<Item = (TxnIndex, Key, &[Element])> + '_ {
        self.committed.iter().flat_map(move |reader| {
            self.views
                .get(reader)
                .into_iter()
                .flat_map(View::reads)
                .map(|(key, list)| (*reader, key, list))
        })
    }

    /// Checks what each committed transaction read: that some other
    /// committed transaction appended each element, and installed the
    /// version read.
    fn check_reads(&mut self) {
        let mut found = Vec::new();
        for (reader, key, list) in self.reads() {
            found.extend(
                list.iter()
                    .filter_map(|element| self.origin(reader, key, *element)),
            );
            found.extend(self.intermediate(reader, key, list));
        }
        self.anomalies.extend(found);
    }

    /// What is wrong with where `element`, read by `reader` at `key`, came
    /// from, if anything.
    fn origin(&self, reader: TxnIndex, key: Key, element: Element) -> Option<Anomaly> {
        let garbage = Anomaly::Garbage {
            reader,
            key,
            element,
        };
        let Some((writer, written)) = self.writers.get(&element).copied() else {
            return Some(garbage);
        };
        if written != key {
            return Some(garbage);
        }
        if writer == reader {
            return Some(Anomaly::Internal { txn: reader, key });
        }
        (self.fates.get(&writer) == Some(&Fate::Aborted)).then_some(Anomaly::AbortedRead {
            reader,
            writer,
            key,
            element,
        })
    }

    /// The intermediate read, if `list` read by `reader` at `key` ends
    /// before the last element its writer appended there.
    fn intermediate(&self, reader: TxnIndex, key: Key, list: &[Element]) -> Option<Anomaly> {
        let element = *list.last()?;
        let (writer, _) = *self.writers.get(&element)?;
        let installed = self.views.get(&writer)?.last_append(key)?;
        (writer != reader && installed != element).then_some(Anomaly::IntermediateRead {
            reader,
            writer,
            key,
            element,
        })
    }

    /// Each key's versions in the order they were installed: the longest
    /// version read or installed, when every other is a prefix of it.
    fn orders(&mut self) -> BTreeMap<Key, Vec<Element>> {
        let mut versions: BTreeMap<Key, Vec<&[Element]>> = BTreeMap::new();
        for (_, key, list) in self.reads() {
            versions.entry(key).or_default().push(list);
        }
        for index in &self.committed {
            let installs = self.views.get(index).into_iter().flat_map(View::installs);
            for (key, list) in installs {
                versions.entry(key).or_default().push(list);
            }
        }
        let mut orders = BTreeMap::new();
        let mut found = Vec::new();
        for (key, mut lists) in versions {
            lists.sort_by_key(|list| list.len());
            match order(key, &lists) {
                Ok(order) => {
                    let _first: Option<Vec<Element>> = orders.insert(key, order.to_vec());
                }
                Err(anomaly) => found.push(anomaly),
            }
        }
        self.anomalies.extend(found);
        orders
    }

    /// The dependency graph of the committed transactions, by `orders`.
    fn graph(&self, orders: &BTreeMap<Key, Vec<Element>>) -> Graph {
        let mut graph = Graph::default();
        let writer = |element: &Element| self.writers.get(element).map(|(writer, _)| *writer);
        for (key, order) in orders {
            let installed: Vec<TxnIndex> = order.iter().filter_map(writer).collect();
            for (from, to) in installed.iter().zip(installed.iter().skip(1)) {
                graph.add(*from, *to, Dependency::Write, *key);
            }
        }
        for (reader, key, list) in self.reads() {
            if let Some(from) = list.last().and_then(writer) {
                graph.add(from, reader, Dependency::Read, key);
            }
            let next = orders.get(&key).and_then(|order| order.get(list.len()));
            if let Some(to) = next.and_then(writer) {
                graph.add(reader, to, Dependency::Anti, key);
            }
        }
        graph
    }

    /// Finishes the check: a summary, or every anomaly found.
    fn finish(mut self) -> Result<Checked, Violations> {
        self.check_reads();
        let orders = self.orders();
        let graph = self.graph(&orders);
        self.anomalies.extend(graph.cycle().map(Anomaly::Cycle));
        if !self.anomalies.is_empty() {
            return Err(Violations::new(distinct(self.anomalies)));
        }
        let dependencies = Dependency::ALL.map(|dependency| graph.count(dependency));
        Ok(Checked::new(self.committed.len(), dependencies))
    }
}

/// The order of `key`'s versions `lists`, shortest first: the longest, when
/// each is a prefix of the next and it holds no element twice.
fn order<'a>(key: Key, lists: &[&'a [Element]]) -> Result<&'a [Element], Anomaly> {
    let mut longest: &[Element] = &[];
    for list in lists {
        if !list.starts_with(longest) {
            return Err(Anomaly::Incompatible {
                key,
                one: longest.to_vec(),
                other: list.to_vec(),
            });
        }
        longest = list;
    }
    let mut seen = BTreeSet::new();
    longest
        .iter()
        .find(|element| !seen.insert(**element))
        .map_or(Ok(longest), |element| {
            Err(Anomaly::Repeated {
                key,
                element: *element,
            })
        })
}

/// `anomalies`, each once, in the order first found.
fn distinct(anomalies: Vec<Anomaly>) -> Vec<Anomaly> {
    let mut once = Vec::new();
    for anomaly in anomalies {
        if !once.contains(&anomaly) {
            once.push(anomaly);
        }
    }
    once
}
