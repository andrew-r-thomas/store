use std::fs;

use store::{pager::Cell, store::Store};

#[test]
fn scratch() {
    let mut store = Store::create("test.store").unwrap();
    for i in 0..100000 {
        let key = (i as u32).to_be_bytes();
        let val = (i as u32).to_be_bytes();
        store.btree.set(&key, &val);
        println!("set {i}");
    }
    {
        let mut page = store.btree.get_first_page();
        while page.borrow().right_sib() != 0 {
            println!("we in here");
            for (cell, i) in page.borrow().iter_cells().zip(0..) {
                match cell {
                    Cell::LeafCell { key, val } => {
                        println!(
                            "cell {i} has key {} and val {}",
                            u32::from_be_bytes(key.try_into().unwrap()),
                            u32::from_be_bytes(val.try_into().unwrap()),
                        );
                    }
                    Cell::InnerCell {
                        key: _,
                        left_ptr: _,
                    } => {
                        println!("ahhh! inner cell in leaf page!!!");
                    }
                }
            }
            let next = { page.borrow().right_sib() };
            page = store.btree.get_page(next);
        }
        for (cell, i) in page.borrow().iter_cells().zip(0..) {
            match cell {
                Cell::LeafCell { key, val } => {
                    println!(
                        "cell {i} has key {} and val {}",
                        u32::from_be_bytes(key.try_into().unwrap()),
                        u32::from_be_bytes(val.try_into().unwrap()),
                    );
                }
                Cell::InnerCell {
                    key: _,
                    left_ptr: _,
                } => {
                    println!("ahhh! inner cell in leaf page!!!");
                }
            }
        }
    }

    let mut buf = Vec::new();
    for i in 0..1000 {
        buf.clear();
        let key = (i as u32).to_be_bytes();
        store.btree.get(&key, &mut buf).unwrap();
        assert_eq!(i as u32, u32::from_be_bytes((&buf[..]).try_into().unwrap()));
    }

    fs::remove_file("test.store").unwrap();
}
