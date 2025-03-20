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

    const ENTRIES: usize = 6400;
    for t in 0..4 {
        let store = store.clone();
        let work_lock = work_lock.clone();
        thread::Builder::new()
            .name(format!("{t}"))
            .spawn(move || {
                let _guard = work_lock.read().unwrap();
                let mut rng = rand::rng();
                let mut map = HashMap::new();
                // for i in (t * ENTRIES)..(t * ENTRIES) + ENTRIES {
                for _ in 0..ENTRIES {
                    let key_len = rng.random_range(128..256);
                    let mut key = vec![0; key_len];
                    rng.fill(&mut key[..]);
                    let val_len = rng.random_range(512..1024);
                    let mut val = vec![0; val_len];
                    rng.fill(&mut val[..]);
                    // let key = (i as u32).to_be_bytes();
                    // let val = key.clone();
                    store.set(&key, &val);
                    map.insert(key, val);
                }
                // for i in (t * ENTRIES)..(t * ENTRIES) + ENTRIES {
                //     assert_eq!(
                //         i as u32,
                //         u32::from_be_bytes(
                //             store
                //                 .get(&(i as u32).to_be_bytes())
                //                 .unwrap()
                //                 .try_into()
                //                 .unwrap()
                //         )
                //     );
                // }
                for (key, val) in map {
                    assert_eq!(val, store.get(&key).unwrap())
                }
            })
            .unwrap();
    }

    drop(work_lock.write().unwrap());
    fs::remove_file("test.store").unwrap();
}
