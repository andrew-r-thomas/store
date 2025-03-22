use store::{pager::OwnedCell, store::Store};

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

    const ENTRIES: usize = 12800;
    for t in 0..4 {
        let store = store.clone();
        let work_lock = work_lock.clone();
        thread::Builder::new()
            .name(format!("{t}"))
            .spawn(move || {
                let _guard = work_lock.read().unwrap();
                let mut rng = rand::rng();
                // let mut map = HashMap::new();
                for i in (t * ENTRIES)..(t * ENTRIES) + ENTRIES {
                    let key = (i as u64).to_be_bytes();
                    let val_len = rng.random_range(512..1024);
                    let mut val = vec![0; val_len];
                    rng.fill(&mut val[..]);
                    // let key = (i as u32).to_be_bytes();
                    // let val = key.clone();
                    store.set(&key, &val);
                    // map.insert(i, val);
                }
            })
            .unwrap();
    }
    drop(work_lock.write().unwrap());
    for (cell, i) in store.iter().zip(0..) {
        match cell {
            OwnedCell::InnerCell {
                key: _,
                left_ptr: _,
            } => {
                panic!("inner cell in leaf page!!")
            }
            OwnedCell::LeafCell { key, val: _ } => {
                let i_key = u64::from_be_bytes(key.try_into().unwrap());
                if i_key != i {
                    println!("missmatch! index is {i} and key is {i_key}");
                }
            }
        }
    }

    fs::remove_file("test.store").unwrap();
}
