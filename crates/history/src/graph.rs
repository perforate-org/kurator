//! The dependency graph of committed transactions, and its cycles.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::anomaly::{Cycle, CycleKind, Dependency, Step};
use crate::record::{Key, TxnIndex};

/// Which dependencies hold from one transaction to another, each with a
/// key it holds by.
type Labels = BTreeMap<Dependency, Key>;

/// Edges between committed transactions, labelled by their dependencies.
#[derive(Debug, Default)]
pub(crate) struct Graph {
    edges: BTreeMap<TxnIndex, BTreeMap<TxnIndex, Labels>>,
}

/// The cycles to look for, weakest isolation first: the edge that closes
/// one, and the dependencies the rest of it may take.
const SEARCHES: [(CycleKind, Dependency, &[Dependency]); 4] = [
    (CycleKind::G0, Dependency::Write, &[Dependency::Write]),
    (
        CycleKind::G1c,
        Dependency::Read,
        &[Dependency::Write, Dependency::Read],
    ),
    (
        CycleKind::GSingle,
        Dependency::Anti,
        &[Dependency::Write, Dependency::Read],
    ),
    (CycleKind::G2, Dependency::Anti, &Dependency::ALL),
];

impl Graph {
    /// `from` precedes `to` by `dependency` on `key`. A transaction does
    /// not depend on itself.
    pub(crate) fn add(&mut self, from: TxnIndex, to: TxnIndex, dependency: Dependency, key: Key) {
        if from != to {
            let labels = self.edges.entry(from).or_default().entry(to).or_default();
            let _first: &mut Key = labels.entry(dependency).or_insert(key);
        }
    }

    /// How many pairs of transactions depend on each other by
    /// `dependency`.
    pub(crate) fn count(&self, dependency: Dependency) -> usize {
        self.edges
            .values()
            .flat_map(BTreeMap::values)
            .filter(|labels| labels.contains_key(&dependency))
            .count()
    }

    /// A cycle of the strongest phenomenon the graph holds, if any: a G0
    /// cycle before a G1c one, and so on.
    pub(crate) fn cycle(&self) -> Option<Cycle> {
        SEARCHES.iter().find_map(|(kind, closing, rest)| {
            self.edges_by(*closing).find_map(|(from, to, key)| {
                let path = self.path(to, from, rest)?;
                let mut steps = vec![Step::new(from, *closing, key)];
                steps.extend(path);
                Some(Cycle::new(*kind, steps))
            })
        })
    }

    /// Every edge with `dependency`: from, to, and the key.
    fn edges_by(
        &self,
        dependency: Dependency,
    ) -> impl Iterator<Item = (TxnIndex, TxnIndex, Key)> + '_ {
        self.edges.iter().flat_map(move |(from, targets)| {
            targets.iter().filter_map(move |(to, labels)| {
                labels.get(&dependency).map(|key| (*from, *to, *key))
            })
        })
    }

    /// A shortest path from `start` to `end` by edges with one of
    /// `allowed`, as its steps.
    fn path(&self, start: TxnIndex, end: TxnIndex, allowed: &[Dependency]) -> Option<Vec<Step>> {
        let mut came: BTreeMap<TxnIndex, Step> = BTreeMap::new();
        let mut visited = BTreeSet::from([start]);
        let mut queue = VecDeque::from([start]);
        while let Some(node) = queue.pop_front() {
            if node == end {
                return Some(steps(&came, start, end));
            }
            let unseen: Vec<(TxnIndex, Step)> = self
                .next(node, allowed)
                .filter(|(to, _)| !visited.contains(to))
                .collect();
            for (to, step) in unseen {
                let _new: bool = visited.insert(to);
                let _first: Option<Step> = came.insert(to, step);
                queue.push_back(to);
            }
        }
        None
    }

    /// The transactions `node` precedes by one of `allowed`, each with the
    /// step to it.
    fn next<'a>(
        &'a self,
        node: TxnIndex,
        allowed: &'a [Dependency],
    ) -> impl Iterator<Item = (TxnIndex, Step)> + 'a {
        self.edges
            .get(&node)
            .into_iter()
            .flatten()
            .filter_map(move |(to, labels)| {
                let (dependency, key) = allowed
                    .iter()
                    .find_map(|dependency| Some((*dependency, *labels.get(dependency)?)))?;
                Some((*to, Step::new(node, dependency, key)))
            })
    }
}

/// The steps from `start` to `end` that `came` recorded, each node's step
/// into it.
fn steps(came: &BTreeMap<TxnIndex, Step>, start: TxnIndex, end: TxnIndex) -> Vec<Step> {
    let mut steps = Vec::new();
    let mut node = end;
    while node != start {
        let Some(step) = came.get(&node) else {
            break;
        };
        steps.push(*step);
        node = step.from();
    }
    steps.reverse();
    steps
}
