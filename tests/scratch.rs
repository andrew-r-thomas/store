use std::{collections::HashMap, fs};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use store::store::Store;

#[test]
fn scratch() {
    let mut store = Store::create("test.store").unwrap();
    let mut rng = rand::rng();
    let mut table = HashMap::new();
    for i in 0..1000 {
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

    let mut current_page = store.btree.get_first_page();
    let mut i = 0;
    while { current_page.borrow().right_sib() } != 0 {
        for cell in current_page.borrow().iter_cells() {
            match cell {
                store::pager::Cell::LeafCell { key, val } => {
                    println!("cell {i}");
                    assert_eq!(val, table.get(key).unwrap());
                }
                store::pager::Cell::InnerCell {
                    key: _,
                    left_ptr: _,
                } => {
                    println!("woah there! we got an inner cell in a leaf page!");
                }
            }
            i += 1;
        }
        let right = { current_page.borrow().right_sib() };
        current_page = store.btree.get_page(right);
    }
    for cell in current_page.borrow().iter_cells() {
        match cell {
            store::pager::Cell::LeafCell { key, val } => {
                println!("cell {i}");
                assert_eq!(val, table.get(key).unwrap());
            }
            store::pager::Cell::InnerCell {
                key: _,
                left_ptr: _,
            } => {
                println!("woah there! we got an inner cell in a leaf page!");
            }
        }
        i += 1;
    }

    let mut buf = Vec::new();
    for (key, val) in table {
        println!("checking");
        buf.clear();
        store.btree.get(&key, &mut buf).unwrap();
        assert_eq!(val, buf);
    }

    fs::remove_file("test.store").unwrap();
}
