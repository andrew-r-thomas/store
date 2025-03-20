use rand_chacha::ChaCha8Rng;
use store::store::Store;

use std::{
    collections::HashMap,
    fs,
    sync::{Arc, RwLock},
    thread,
};

use rand::{Rng, SeedableRng};

#[test]
fn scratch() {
    let store = Arc::new(Store::create("test.store").unwrap());

    // let work_lock = Arc::new(RwLock::new(()));
    // for t in 0..4 {
    //     let store = store.clone();
    //     let work_lock = work_lock.clone();
    //     thread::Builder::new()
    //         .name(format!("{t}"))
    //         .spawn(move || {
    //             let _guard = work_lock.read().unwrap();
    // let mut rng = rand::rng();
    // let mut rng = ChaCha8Rng::seed_from_u64(123);
    for i in 0..440 {
        // let key_len = rng.random_range(128..256);
        // let mut key = vec![0; key_len];
        // rng.fill(&mut key[..]);
        // let val_len = rng.random_range(512..1024);
        // let mut val = vec![0; val_len];
        // rng.fill(&mut val[..]);
        let key = (i as u32).to_be_bytes();
        let val = (i as u32).to_be_bytes();
        store.set(&key, &val);
        println!("set {i}");
    }
    for i in 0..440 {
        println!("getting {i}");
        assert_eq!(
            i,
            u32::from_be_bytes(
                store
                    .get(&(i as u32).to_be_bytes())
                    .unwrap()
                    .try_into()
                    .unwrap()
            )
        );
    }
    //         })
    //         .unwrap();
    // }
    //
    // drop(work_lock.write().unwrap());
    fs::remove_file("test.store").unwrap();
}
