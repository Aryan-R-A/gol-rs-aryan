use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use gol_rs::gol::distributor::run_benchmark_world; // adjust path to where you put it
use rayon::ThreadPoolBuilder; // add this

fn bench_gol(c: &mut Criterion) {
    let mut group = c.benchmark_group("Gol Benchmark");

    group
        .sampling_mode(criterion::SamplingMode::Flat)
        .sample_size(10);

    let width = 512usize;
    let height = 512usize;
    let turns = 1000u32;

    // Only benchmark a few sensible thread counts
    let thread_counts = [1usize, 2, 4, 8, 16];

    for &threads in &thread_counts {
        group.bench_with_input(
            BenchmarkId::new("Threads", threads),
            &threads,
            |bencher, &t| {
                // Build a dedicated Rayon thread pool of size t
                let pool = ThreadPoolBuilder::new()
                    .num_threads(t)
                    .build()
                    .unwrap();

                bencher.iter(|| {
                    // Run the benchmarked world INSIDE this pool
                    pool.install(|| {
                        run_benchmark_world(turns, width, height);
                    });
                });
            },
        );
    }

    group.finish();
}


criterion_group!(benches, bench_gol);
criterion_main!(benches);
