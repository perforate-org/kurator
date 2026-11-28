//! Backups: the state of every shard a node holds, written out of the
//! node, and planted again as the first state of a new cluster.
//!
//! ```text
//! <backup>/<id>    oleaph_codec::Format "OLBK", version 1, around
//!                  applied u64: the index of the last entry the replica
//!                  applied, then oleaph_shard::ShardState::encode
//! ```
//!
//! A backup is written to a temporary directory beside its target and
//! renamed into place once every file is synced, so a backup that exists
//! is whole.
//!
//! - **What it holds.** Each shard's state as the node's replica applied
//!   it: its keys with the versions they keep, the transactions across
//!   shards it holds prepared, and the decisions it remembers. No log and
//!   no membership: the cluster it is restored into has its own.
//! - **Where the new cluster starts.** A planted shard's log starts after
//!   every entry of the old shard's, so the placement its new leader
//!   reports to the meta shard is newer than any the old cluster's meta
//!   shard recorded, and replaces it.
//! - **Transactions across shards.** Each shard is backed up as of its own
//!   last entry, so a transaction that committed between two shards'
//!   moments may be committed in one and only prepared in the other.
//!   [`Backup::plant`] settles that: a transaction one shard holds
//!   prepared is decided as any shard of the backup remembers deciding it.
//!   A shard forgets a decision only well after the transaction's
//!   deadline (see `Collect`), so this holds for every backup taken in
//!   less time than that.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write as _};
use std::path::Path;

use oleaph_codec::{DecodeError, Format};
use oleaph_shard::{ShardId, ShardState};
use oleaph_storage::{Command, Decide, Decided, Decision, TxnId, Version};
use oleaph_wal::sync_dir;

use crate::error::{Error, Result, SnapshotDamage};
use crate::layout::Layout;
use crate::snapshot::SnapshotBytes;
use crate::state_machine::SEED_LEADER;
use crate::types::{LogId, Membership, NodeId, Peer, SnapshotMeta, StoredMembership};

const FORMAT: Format = Format::new(*b"OLBK", 1);
const TEMP_PREFIX: &str = ".backup-";

/// The states of the shards one node held, each as of the last entry its
/// replica applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Backup {
    shards: BTreeMap<ShardId, Held>,
}

/// One shard of a backup: its state, and the index of the last entry its
/// replica applied, which the state's version may be below.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Held {
    state: ShardState,
    applied: u64,
}

impl Backup {
    /// A backup of `shards`, each a state with the index of the last
    /// entry its replica applied.
    #[must_use]
    pub fn new(shards: impl IntoIterator<Item = (ShardState, u64)>) -> Self {
        Self {
            shards: shards
                .into_iter()
                .map(|(state, applied)| (state.shard().id(), Held { state, applied }))
                .collect(),
        }
    }

    /// Every shard's state, by id.
    pub fn shards(&self) -> impl Iterator<Item = &ShardState> {
        self.shards.values().map(|held| &held.state)
    }

    /// Writes the backup to the directory `to`, which must not exist yet.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when `to` exists, or a file cannot be written.
    pub fn write(&self, to: &Path) -> Result<()> {
        if to.exists() {
            return Err(
                io::Error::new(io::ErrorKind::AlreadyExists, to.display().to_string()).into(),
            );
        }
        // Absolute, so that even a bare name has a parent to write beside.
        let to = std::path::absolute(to)?;
        let parent = to.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "a backup needs a parent directory",
            )
        })?;
        let temp = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempdir_in(parent)?;
        for (id, held) in &self.shards {
            let framed = FORMAT.write(Vec::new(), |out| {
                out.write_all(&held.applied.to_le_bytes())?;
                held.state.encode(out)
            })?;
            let mut file = fs::File::create(temp.path().join(id.get().to_string()))?;
            file.write_all(&framed)?;
            file.sync_all()?;
        }
        sync_dir(temp.path())?;
        fs::rename(temp.keep(), &to)?;
        sync_dir(parent)?;
        Ok(())
    }

    /// Reads the backup in `from`. Entries that name no shard are left
    /// alone.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when `from` cannot be read, and [`Error::Snapshot`]
    /// when a shard's file is damaged or holds another shard.
    pub fn read(from: &Path) -> Result<Self> {
        let mut shards = BTreeMap::new();
        for entry in fs::read_dir(from)? {
            let path = entry?.path();
            let Some(id) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse().ok())
                .map(ShardId::new)
            else {
                continue;
            };
            let damaged = |damage| Error::Snapshot {
                path: path.clone(),
                damage,
            };
            let held = decode(&fs::read(&path)?)
                .map_err(|error| damaged(SnapshotDamage::Format(error)))?;
            if held.state.shard().id() != id {
                return Err(damaged(SnapshotDamage::MetaMismatch));
            }
            drop(shards.insert(id, held));
        }
        Ok(Self { shards })
    }

    /// Plants the backup in the empty node directory `root` as the first
    /// state of every shard, with `node`, reached as `peer`, as each
    /// shard's only voter. Opening a store in `root` then opens a replica
    /// of every shard, which elects itself; other nodes join the shards as
    /// learners. Transactions across shards are settled first, as the
    /// module says.
    ///
    /// # Errors
    ///
    /// [`Error::Initialized`] when `root` holds a shard or a seed already,
    /// and [`Error::Io`] when a seed cannot be written.
    pub fn plant(mut self, root: &Path, node: NodeId, peer: Peer) -> Result<()> {
        let layout = Layout::open(root)?;
        if !layout.is_empty()? {
            return Err(Error::Initialized);
        }
        self.settle();
        let voters = BTreeSet::from([node]);
        let membership = Membership::new(vec![voters], BTreeMap::from([(node, peer)]))
            .map_err(|error| Error::Raft(Box::new(error)))?;
        for (id, held) in &self.shards {
            // After every entry of the old log, and after every version.
            let last = held.applied.max(held.state.kv().version().get());
            let seeded = LogId::new(SEED_LEADER, last.saturating_add(1));
            let meta = SnapshotMeta {
                last_log_id: Some(seeded),
                last_membership: StoredMembership::new(Some(seeded), membership.clone()),
            };
            layout.sow(*id, &SnapshotBytes::encode(&meta, &held.state)?)?;
        }
        Ok(())
    }

    /// Decides every transaction a shard holds prepared that some shard
    /// remembers deciding, as that shard decided it, each decision at the
    /// version after the shard's last.
    fn settle(&mut self) {
        let mut known = BTreeMap::<TxnId, Decision>::new();
        for (id, decided) in self
            .shards
            .values()
            .flat_map(|held| held.state.kv().decisions())
        {
            // Only a committed transaction commits anywhere, so a commit
            // anywhere decides it.
            match decided {
                Decided::Committed(_) => drop(known.insert(id, Decision::Commit)),
                Decided::Aborted | _ => {
                    let _seen: &mut Decision = known.entry(id).or_insert(Decision::Abort);
                }
            }
        }
        for Held { state, .. } in self.shards.values_mut() {
            let settled: Vec<Decide> = state
                .kv()
                .pending()
                .filter_map(|prepared| {
                    let decision = known.get(&prepared.id())?;
                    let primary = prepared.primary().to_vec();
                    Some(Decide::new(
                        prepared.id(),
                        primary,
                        prepared.deadline(),
                        *decision,
                    ))
                })
                .collect();
            for decide in settled {
                let next = Version::new(state.kv().version().get().saturating_add(1));
                let _applied = state.apply(next, Command::Decide(decide));
            }
        }
    }
}

fn decode(framed: &[u8]) -> Result<Held, DecodeError> {
    let mut reader = FORMAT.read(framed)?;
    let applied = reader.read()?;
    let state = ShardState::decode(&mut reader)?;
    reader.finish()?;
    Ok(Held { state, applied })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use oleaph_shard::{Epoch, ShardId, ShardState, Split};
    use oleaph_storage::{
        Command, Deadline, Decide, Decided, Decision, Draft, TxnId, Version, WriteBatch,
    };

    use super::Backup;
    use crate::error::{Error, SnapshotDamage};
    use crate::layout::Layout;
    use crate::types::NodeId;
    use crate::types::testing::peer;

    /// The first shard split at `m`: `a` on the left, `z` on the right.
    fn halves() -> (ShardState, ShardState) {
        let mut left = ShardState::default();
        let batch = WriteBatch::new().put("a", "1").put("z", "2");
        let _applied = left.apply(Version::new(1), Command::Kv(batch));
        let split = Split::new("m", ShardId::new(2), Epoch::ZERO);
        let (_, right) = left.split(&split).unwrap();
        (left, right)
    }

    /// Prepares the transaction `id`, whose primary is `a`, writing `key`.
    fn prepare(state: &mut ShardState, id: u128, key: &str) {
        let version = state.kv().version();
        let mut draft = Draft::new(version);
        draft.put(key, "t");
        let prepare = draft.into_prepare(TxnId::new(id), b"a".to_vec(), Deadline::new(100));
        let next = Version::new(version.get().checked_add(1).unwrap());
        let _applied = state.apply(next, Command::Prepare(prepare));
    }

    fn decide(state: &mut ShardState, id: u128, decision: Decision) {
        let decide = Decide::new(TxnId::new(id), b"a".to_vec(), Deadline::new(100), decision);
        let next = Version::new(state.kv().version().get().checked_add(1).unwrap());
        let _applied = state.apply(next, Command::Decide(decide));
    }

    #[test]
    fn a_backup_reads_back_as_it_was_written() {
        let dir = tempfile::tempdir().unwrap();
        let (left, right) = halves();
        let backup = Backup::new([(left.clone(), 1), (right.clone(), 1)]);
        let to = dir.path().join("backup");
        backup.write(&to).unwrap();
        assert!(matches!(backup.write(&to), Err(Error::Io(_))), "not twice");
        fs::write(to.join("notes"), "left alone").unwrap();
        let read = Backup::read(&to).unwrap();
        assert_eq!(read, backup, "roundtrip");
        let shards: Vec<&ShardState> = read.shards().collect();
        assert_eq!(shards, [&left, &right], "by id");
        let leftover = fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(leftover, 1, "no temporary directory is left");
    }

    #[test]
    fn a_damaged_or_misfiled_shard_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (left, _) = halves();
        let to = dir.path().join("backup");
        Backup::new([(left, 1)]).write(&to).unwrap();
        let file = to.join("1");
        let bytes = fs::read(&file).unwrap();
        fs::rename(&file, to.join("7")).unwrap();
        assert!(
            matches!(
                Backup::read(&to),
                Err(Error::Snapshot {
                    damage: SnapshotDamage::MetaMismatch,
                    ..
                })
            ),
            "misfiled"
        );
        let (head, _) = bytes.split_at(bytes.len() - 1);
        fs::write(to.join("7"), head).unwrap();
        assert!(
            matches!(
                Backup::read(&to),
                Err(Error::Snapshot {
                    damage: SnapshotDamage::Format(_),
                    ..
                })
            ),
            "damaged"
        );
    }

    #[test]
    fn planting_settles_transactions_a_shard_decided() {
        let (mut left, mut right) = halves();
        // 1 committed on the left, the primary, but only prepared on the
        // right; 2 aborted on the right only; 3 prepared on both.
        prepare(&mut left, 1, "b");
        prepare(&mut right, 1, "y");
        decide(&mut left, 1, Decision::Commit);
        prepare(&mut left, 2, "c");
        prepare(&mut right, 2, "x");
        decide(&mut right, 2, Decision::Abort);
        prepare(&mut left, 3, "d");
        prepare(&mut right, 3, "w");
        let mut backup = Backup::new([(left, 9), (right, 9)]);
        backup.settle();
        let [settled_left, settled_right] = backup.shards().collect::<Vec<_>>()[..] else {
            panic!("two shards");
        };
        assert_eq!(
            settled_right.kv().get(b"y"),
            Some(b"t".as_slice()),
            "committed"
        );
        assert!(
            matches!(
                settled_right.kv().decided(TxnId::new(1)),
                Some(Decided::Committed(_))
            ),
            "and remembered"
        );
        assert_eq!(settled_left.kv().get(b"c"), None, "aborted");
        assert_eq!(
            settled_left.kv().decided(TxnId::new(2)),
            Some(Decided::Aborted),
            "too"
        );
        let pending: Vec<TxnId> = settled_left
            .kv()
            .pending()
            .map(oleaph_storage::Prepare::id)
            .collect();
        assert_eq!(pending, [TxnId::new(3)], "undecided stays prepared");
        assert_eq!(settled_right.kv().pending().count(), 1, "on both");
    }

    #[test]
    fn a_backup_is_planted_only_in_an_empty_node() {
        let root = tempfile::tempdir().unwrap();
        let (left, right) = halves();
        let shard = right.shard().clone();
        // The replica applied past the version: a membership change, say.
        let backup = Backup::new([(left, 1), (right, 4)]);
        backup
            .clone()
            .plant(root.path(), NodeId::new(5), peer(5))
            .unwrap();
        let layout = Layout::open(root.path()).unwrap();
        let seeds = layout.seeds().unwrap();
        assert_eq!(seeds.len(), 2, "a seed a shard");
        let (meta, state) = seeds[1].1.decode().unwrap();
        assert_eq!(state.shard(), &shard, "the shard");
        assert_eq!(
            meta.last_log_id.map(|id| id.index),
            Some(5),
            "after the old log"
        );
        let (first, _) = seeds[0].1.decode().unwrap();
        assert_eq!(
            first.last_log_id.map(|id| id.index),
            Some(2),
            "after the version"
        );
        let voters: Vec<NodeId> = meta.last_membership.membership().voter_ids().collect();
        assert_eq!(voters, [NodeId::new(5)], "the only voter");
        assert!(
            matches!(
                backup.plant(root.path(), NodeId::new(5), peer(5)),
                Err(Error::Initialized)
            ),
            "not over seeds"
        );
    }
}
