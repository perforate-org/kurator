//! What checking a shard's size costs: the walk over every key that
//! [`KvState::footprint`] and [`KvState::middle`] make, by key count. The
//! leader of a shard makes it on every check, holding the state.
#![expect(
    clippy::unwrap_used,
    reason = "benchmark setup: a failure should panic"
)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use oleaph_crypto as _;
use oleaph_storage::{Command, KvState, Version, WriteBatch};
use proptest as _;
use tempfile as _;

/// A state of `keys` keys of 16 bytes, each with a value of 64 bytes.
fn state(keys: u64) -> KvState {
    let mut state = KvState::default();
    for chunk in 0..keys.div_ceil(1024) {
        let mut batch = WriteBatch::new();
        for index in 0..1024 {
            let key = chunk.saturating_mul(1024).saturating_add(index);
            batch = batch.put([key.to_be_bytes(), [0; 8]].concat(), [0; 64]);
        }
        let _outcome = state.apply(Version::new(chunk.saturating_add(1)), Command::Kv(batch));
    }
    state
}

fn walks(c: &mut Criterion) {
    let mut group = c.benchmark_group("size_check");
    for keys in [10_000_u64, 100_000, 1_000_000] {
        let held = state(keys);
        let _: &mut _ = group.throughput(Throughput::Elements(keys));
        let _: &mut _ =
            group.bench_with_input(BenchmarkId::new("footprint", keys), &held, |bench, held| {
                bench.iter(|| black_box(held.footprint()));
            });
        let _: &mut _ =
            group.bench_with_input(BenchmarkId::new("middle", keys), &held, |bench, held| {
                bench.iter(|| black_box(held.middle().unwrap().len()));
            });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = walks
}
criterion_main!(benches);
