//! What a checkpoint costs a replica: encoding its whole state and hashing
//! it into a manifest, by key count. Every replica does it once an
//! interval of blocks, holding the state.
#![expect(
    clippy::unwrap_used,
    reason = "benchmark setup: a failure should panic"
)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use oleaph_bft::Manifest;
use oleaph_shard::ShardState;
use oleaph_storage::{Command, Version, WriteBatch};
use {
    blake3 as _, futures_channel as _, futures_util as _, oleaph_codec as _, oleaph_consensus as _,
    oleaph_crypto as _, oleaph_wal as _, parking_lot as _, proptest as _, tempfile as _,
    thiserror as _,
};

/// A state of `keys` keys of 16 bytes, each with a value of 64 bytes.
fn state(keys: u64) -> ShardState {
    let mut state = ShardState::default();
    for chunk in 0..keys.div_ceil(1024) {
        let mut batch = WriteBatch::new();
        for index in 0..1024 {
            let key = chunk.saturating_mul(1024).saturating_add(index);
            batch = batch.put([key.to_be_bytes(), [0; 8]].concat(), [0; 64]);
        }
        let _applied = state.apply(Version::new(chunk.saturating_add(1)), Command::Kv(batch));
    }
    state
}

fn manifests(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint");
    for keys in [10_000_u64, 100_000, 1_000_000] {
        let held = state(keys);
        let mut encoded = Vec::new();
        held.encode(&mut encoded).unwrap();
        let bytes = u64::try_from(encoded.len()).unwrap();
        let _: &mut _ = group.throughput(Throughput::Bytes(bytes));
        let _: &mut _ =
            group.bench_with_input(BenchmarkId::new("manifest", keys), &held, |bench, held| {
                bench.iter(|| black_box(Manifest::of(held).unwrap().root()));
            });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = manifests
}
criterion_main!(benches);
