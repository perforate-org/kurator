//! Shards for Kurator: the keys split into spans, each held by its own
//! Raft group, in the manner of `TiKV`'s regions.
//!
//! A [`Shard`] is an id, a [`Span`](kurator_storage::Span) of keys
//! `[start, end)`, and an [`Epoch`] that counts how often the span
//! changed. A [`ShardMap`] tiles every key with shards, so a client finds
//! the shard of any key. A [`ShardState`] is what one shard's replicas
//! hold: the shard and the [`KvState`](kurator_storage::KvState) of its
//! keys.
//!
//! A shard grows until it [splits](Split): the split goes into the shard's
//! own log like any command, and applying it cuts the state at the split
//! key. The shard keeps its id and the keys before the key; the keys from
//! it on become a new shard, at the same version, which a new Raft group
//! takes over with the same members. Both halves move to the next epoch.
//!
//! Every replica checks, as it applies a command, that everything the
//! command touches is in its shard's span. A command routed before a split
//! that applies after it is refused with [`Applied::Misrouted`] on every
//! replica alike, and the refusal tells the sender what the shard is now.
//!
//! ```
//! use kurator_shard::{Applied, Epoch, ShardId, ShardState, Split};
//! use kurator_storage::{Command, Version, WriteBatch};
//!
//! let mut state = ShardState::default();
//! let _ = state.apply(Version::new(1), Command::Kv(WriteBatch::new().put("zebra", "z")));
//! let split = Split::new("m", ShardId::new(2), Epoch::ZERO);
//! let (halves, right) = state.split(&split).expect("the shard is at epoch 0");
//! assert_eq!(right.kv().get(b"zebra"), Some(b"z".as_slice()));
//! let late = state.apply(Version::new(2), Command::Kv(WriteBatch::new().put("zebra", "y")));
//! assert_eq!(late, Applied::Misrouted(halves.left().clone()));
//! ```

mod id;
mod map;
mod shard;
mod state;

pub use id::{Epoch, ShardId};
pub use map::{ShardMap, Stale};
pub use shard::{Halves, Shard, Split};
pub use state::{Applied, ShardState};
