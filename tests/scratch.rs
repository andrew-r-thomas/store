use std::fs;

use rand::Rng;
use store::store::Store;

#[test]
fn scratch() {
    let mut store = Store::create("test.store").unwrap();
    let mut rng = rand::rng();
    for _ in 0..10 {
        let key_size = rng.random_range(128..256);
        let mut key: Vec<u8> = vec![0; key_size];
        rng.fill(&mut key[..]);
        let val_size = rng.random_range(512..1024);
        let mut val: Vec<u8> = vec![0; val_size];
        rng.fill(&mut val[..]);

        store.btree.set(&key, &val);
    }

    fs::remove_file("test.store").unwrap();
}
