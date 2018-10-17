#[macro_use]
extern crate criterion;
extern crate rand;
extern crate fnv;
extern crate bytell_hash_map;

use criterion::{Criterion, ParameterizedBenchmark, Throughput, black_box, PlotConfiguration, AxisScale};

use rand::rngs::SmallRng;
use rand::{SeedableRng, Rng};

use fnv::FnvBuildHasher;

type HashMap = std::collections::HashMap<u32, u32, FnvBuildHasher>;
type BytellHashMap = bytell_hash_map::HashMap<u32, u32, FnvBuildHasher>;

trait Map {
    fn with_capacity(capacity: usize) -> Self;
    fn insert(&mut self, k: u32, v: u32);
    fn get(&self, n: &u32) -> Option<&u32>;
    fn remove(&mut self, n: &u32) -> bool;
}

impl Map for HashMap {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, FnvBuildHasher::default())
    }
    fn insert(&mut self, k: u32, v: u32) {
        self.insert(k, v);
    }
    fn get(&self, n: &u32) -> Option<&u32> {
        self.get(n)
    }
    fn remove(&mut self, n: &u32) -> bool {
        self.remove(n).is_some()
    }
}

impl Map for BytellHashMap {
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity, FnvBuildHasher::default())
    }
    fn insert(&mut self, k: u32, v: u32) {
        self.insert(k, v);
    }
    fn get(&self, n: &u32) -> Option<&u32> {
        self.get(n)
    }
    fn remove(&mut self, n: &u32) -> bool {
        self.remove(n).is_some()
    }
}

fn get_hit<H: Map>(b: &mut criterion::Bencher, max: u32) {
    let mut map = H::with_capacity(max as usize);
    let mut rng = SmallRng::from_seed([0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 3, 5]);
    let mut numbers = (0..max).collect::<Vec<_>>();
    rng.shuffle(&mut numbers);
    for n in &numbers {
        map.insert(*n, *n);
    }
    rng.shuffle(&mut numbers);
    b.iter(|| {
        for n in &numbers {
            black_box(map.get(n));
        }
    })
}

fn get_miss<H: Map>(b: &mut criterion::Bencher, max: u32) {
    let mut map = H::with_capacity(max as usize);
    let mut rng = SmallRng::from_seed([0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 3, 5]);
    let mut numbers = (0..max).collect::<Vec<_>>();
    rng.shuffle(&mut numbers);
    for n in &numbers {
        map.insert(2 * n, 2 * n);
    }
    rng.shuffle(&mut numbers);
    b.iter(|| {
        for n in &numbers {
            black_box(map.get(&(2 * n + 1)));
        }
    })
}

fn insert<H: Map>(b: &mut criterion::Bencher, max: u32) {
    let mut map = H::with_capacity(16);
    let mut rng = SmallRng::from_seed([0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 3, 5]);
    let mut numbers = (0..max).collect::<Vec<_>>();
    rng.shuffle(&mut numbers);
    b.iter(|| {
        for n in &numbers {
            map.insert(*n, *n);
        }
    })
}

fn remove<H: Map>(b: &mut criterion::Bencher, max: u32) {
    let mut map = H::with_capacity(max as usize);
    let mut rng = SmallRng::from_seed([0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 3, 5]);
    let mut numbers = (0..max).collect::<Vec<_>>();
    rng.shuffle(&mut numbers);
    for n in &numbers {
        map.insert(*n, *n);
    }
    rng.shuffle(&mut numbers);
    b.iter(|| {
        for n in &numbers {
            black_box(map.remove(n));
        }
    })
}

fn comparisons(c: &mut Criterion) {
    let max = 800_000;
    let data_points = 40;
    let checks = (1..).map(|n| n * (max / data_points)).take(data_points as usize).collect::<Vec<_>>();
    c.bench(
        "compare/get/hit",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| get_hit::<BytellHashMap>(b, *size), checks.clone())
            .with_function("hash-map", |b, size| get_hit::<HashMap>(b, *size))
            .throughput(|n| Throughput::Elements(*n)),
    );
    c.bench(
        "compare/get/miss",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| get_miss::<BytellHashMap>(b, *size), checks.clone())
            .with_function("hash-map", |b, size| get_miss::<HashMap>(b, *size))
            .throughput(|n| Throughput::Elements(*n)),
    );
    c.bench(
        "compare/insert",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| insert::<BytellHashMap>(b, *size), checks.clone())
            .with_function("hash-map", |b, size| insert::<HashMap>(b, *size))
            .throughput(|n| Throughput::Elements(*n)),
    );
    c.bench(
        "compare/remove",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| remove::<BytellHashMap>(b, *size), checks)
            .with_function("hash-map", |b, size| remove::<HashMap>(b, *size))
            .throughput(|n| Throughput::Elements(*n)),
    );
}

fn benchmarks(c: &mut Criterion) {
    let max = 800_000;
    let data_points = 80;
    let checks = (1..).map(|n| n * (max / data_points)).take(data_points as usize).collect::<Vec<_>>();
    c.bench(
        "bench/get/hit",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| get_hit::<BytellHashMap>(b, *size), checks.clone())
            .throughput(|n| Throughput::Elements(*n)),
    );
    c.bench(
        "bench/get/miss",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| get_miss::<BytellHashMap>(b, *size), checks.clone())
            .throughput(|n| Throughput::Elements(*n)),
    );
    c.bench(
        "bench/insert",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| insert::<BytellHashMap>(b, *size), checks.clone())
            .throughput(|n| Throughput::Elements(*n)),
    );
    c.bench(
        "bench/remove",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| remove::<BytellHashMap>(b, *size), checks)
            .throughput(|n| Throughput::Elements(*n)),
    );
}

criterion_group!(benches, comparisons, benchmarks);
criterion_main!(benches);