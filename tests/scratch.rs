use std::{
    collections::{BTreeMap, HashMap},
    fs,
};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use store::store::Store;

#[test]
fn scratch() {
    let mut store = Store::create("test.store").unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(456);
    let mut table = BTreeMap::new();
    for i in 0..100000 {
        let val_len = rng.random_range(512..1024);
        let mut val = vec![0; val_len];
        rng.fill(&mut val[..]);
        let key_len = rng.random_range(128..256);
        let mut key = vec![0; key_len];
        rng.fill(&mut key[..]);
        store.btree.set(&key, &val);
        table.insert(key, val);
        println!("set {i}");
    }

    let mut buf = Vec::new();
    for (key, val) in table.iter() {
        println!("checking");
        buf.clear();
        store.btree.get(&key, &mut buf).unwrap();
        assert_eq!(val, &buf);
    }

    fs::remove_file("test.store").unwrap();
}
