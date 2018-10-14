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
}

fn get<H: Map>(b: &mut criterion::Bencher, max: u32) {
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

fn criterion_benchmark(c: &mut Criterion) {
    let checks = (1..).map(|n| n * 10000).take(40).collect::<Vec<_>>();
    c.bench(
        "get",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| get::<BytellHashMap>(b, *size), checks.clone())
            .with_function("hash-map", |b, size| get::<HashMap>(b, *size))
            .throughput(|n| Throughput::Elements(*n)),
    );
    c.bench(
        "insert",
        ParameterizedBenchmark::new("bytell-hash-map", |b, size| insert::<BytellHashMap>(b, *size), checks)
            .with_function("hash-map", |b, size| insert::<HashMap>(b, *size))
            .throughput(|n| Throughput::Elements(*n)),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);