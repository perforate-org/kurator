//! Write throughput: how much group commit buys as writers are added, and
//! what one log append costs by batch size.
#![expect(
    clippy::unwrap_used,
    reason = "benchmark setup: a failure should panic"
)]

use std::hint::black_box;
use std::thread;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use oleaph_crypto::{Algorithm, SigningKey};
use oleaph_storage::{Db, Options, Version};
use oleaph_wal::{Integrity, Start, Wal};
use proptest as _;
use tempfile as _;

const WRITES_PER_THREAD: u64 = 64;

fn concurrent_puts(c: &mut Criterion) {
    let mut group = c.benchmark_group("db_put");
    for writers in [1_u64, 4, 16, 64] {
        let _: &mut _ = group.throughput(Throughput::Elements(
            writers.saturating_mul(WRITES_PER_THREAD),
        ));
        let _: &mut _ = group.bench_with_input(
            BenchmarkId::new("writers", writers),
            &writers,
            |bench, &writers| {
                let dir = tempfile::tempdir().unwrap();
                let db = Db::open(dir.path(), Options::default()).unwrap();
                bench.iter_custom(|rounds| (0..rounds).map(|_| time_writers(&db, writers)).sum());
            },
        );
    }
    group.finish();
}

fn time_writers(db: &Db, writers: u64) -> Duration {
    let put_all = |writer: u64| {
        for i in 0..WRITES_PER_THREAD {
            let _: Version = db
                .put(
                    black_box([writer.to_le_bytes(), i.to_le_bytes()].concat()),
                    [0; 64],
                )
                .unwrap();
        }
    };
    let start = Instant::now();
    thread::scope(|scope| {
        for writer in 0..writers {
            drop(scope.spawn(move || put_all(writer)));
        }
    });
    start.elapsed()
}

fn wal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append_batch");
    let integrities = [
        ("checksum", Integrity::Checksum),
        (
            "signed",
            Integrity::Signed(SigningKey::from_seed(Algorithm::Ed25519, [1; 32])),
        ),
    ];
    for (name, integrity) in integrities {
        for batch in [1_u64, 16, 256] {
            let _: &mut _ = group.throughput(Throughput::Elements(batch));
            let _: &mut _ =
                group.bench_with_input(BenchmarkId::new(name, batch), &batch, |bench, &batch| {
                    let dir = tempfile::tempdir().unwrap();
                    let mut wal = empty_wal(dir.path(), integrity.clone());
                    let payloads = vec![[0_u8; 64]; usize::try_from(batch).unwrap()];
                    bench.iter(|| wal.append_batch(black_box(&payloads)).unwrap());
                });
        }
    }
    group.finish();
}

fn empty_wal(dir: &std::path::Path, integrity: Integrity) -> Wal {
    let mut options = oleaph_wal::Options::default();
    options.integrity = integrity;
    let ignore = |_, _: &[u8]| Ok::<(), std::convert::Infallible>(());
    Wal::open(dir, options, Start::Oldest, ignore).unwrap().0
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(3));
    targets = concurrent_puts, wal_append
}
criterion_main!(benches);
