use store::store::Store;

use std::{
    collections::HashMap,
    fs,
    sync::{Arc, RwLock},
    thread,
};

use rand::Rng;

#[test]
fn scratch() {
    let store = Arc::new(Store::create("test.store").unwrap());

    let work_lock = Arc::new(RwLock::new(()));
    for _ in 0..4 {
        let store = store.clone();
        let work_lock = work_lock.clone();
        thread::spawn(move || {
            let _guard = work_lock.read().unwrap();
            let mut rng = rand::rng();
            let mut map = HashMap::new();
            for _ in 0..100 {
                let key_len = rng.random_range(128..256);
                let mut key = vec![0; key_len];
                rng.fill(&mut key[..]);
                let val_len = rng.random_range(512..1024);
                let mut val = vec![0; val_len];
                rng.fill(&mut val[..]);
                store.set(&key, &val);
                map.insert(key, val);
            }
            for (key, val) in map {
                assert_eq!(val, store.get(&key).unwrap());
            }
        });
    }

    drop(work_lock.write().unwrap());
    fs::remove_file("test.store").unwrap();
}
