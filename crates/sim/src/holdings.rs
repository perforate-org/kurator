//! What one node holds: its replica of each shard.

use std::collections::BTreeMap;

use kurator_shard::{ShardId, ShardState};

/// The state of every shard one node replicates, as it applied them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Holdings(BTreeMap<ShardId, ShardState>);

impl Holdings {
    pub(crate) fn new(shards: impl IntoIterator<Item = ShardState>) -> Self {
        Self(
            shards
                .into_iter()
                .map(|state| (state.shard().id(), state))
                .collect(),
        )
    }

    /// The newest value of `key`, from the shard whose span holds it.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.0
            .values()
            .find(|state| state.shard().span().contains(key))?
            .kv()
            .get(key)
    }

    /// Every entry at the newest version of its shard, shard by shard.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.0.values().flat_map(|state| state.kv().iter())
    }

    /// The state of `shard`, if the node holds it.
    #[must_use]
    pub fn shard(&self, shard: ShardId) -> Option<&ShardState> {
        self.0.get(&shard)
    }

    /// Every shard the node holds, by id.
    pub fn shards(&self) -> impl Iterator<Item = &ShardState> {
        self.0.values()
    }
}

#[cfg(test)]
mod tests {
    use kurator_shard::{Epoch, ShardId, ShardState, Split};
    use kurator_storage::{Command, Version, WriteBatch};

    use super::Holdings;

    #[test]
    fn keys_are_read_from_the_shard_that_holds_them() {
        let mut left = ShardState::default();
        let write = WriteBatch::new().put("apple", "1").put("melon", "2");
        let _applied = left.apply(Version::new(1), Command::Kv(write));
        let (_, right) = left
            .split(&Split::new("m", ShardId::new(2), Epoch::ZERO))
            .unwrap();
        let holdings = Holdings::new([left, right]);
        assert_eq!(holdings.get(b"apple"), Some(b"1".as_slice()), "left");
        assert_eq!(holdings.get(b"melon"), Some(b"2".as_slice()), "right");
        assert_eq!(holdings.iter().count(), 2, "every entry");
        assert_eq!(holdings.shards().count(), 2, "both shards");
        assert!(holdings.shard(ShardId::new(2)).is_some(), "by id");
        assert_eq!(Holdings::default().get(b"apple"), None, "nothing held");
    }
}
