use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use store::store::Store;

fn benchmark(c: &mut Criterion) {
    let mut store = Store::create("bench.store").unwrap();

    let mut rng = rand::rng();
    // ok so first we need some kind of data, and some kind of workload
    // for now i'll just do randomly generated keys and vals, within say 1kb
    // then maybe let's do like, big bulk input, then stable like 50/50 split
    // on read and write, then just hella reads, these should probably be
    // different benchmarks, i'm sure there's a way to do that easily, just
    // getting my feet wet for now
    c.bench_function("bulk insert", |b| {
        b.iter_batched_ref(
            || {
                let key_size = rng.random_range(128..256);
                let mut key: Vec<u8> = vec![0; key_size];
                rng.fill(&mut key[..]);
                let val_size = rng.random_range(512..1024);
                let mut val: Vec<u8> = vec![0; val_size];
                rng.fill(&mut val[..]);
                (key, val)
            },
            |(key, val)| {
                store.btree.set(key, val);
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
