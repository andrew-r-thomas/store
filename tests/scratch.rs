use std::{
    sync::{Arc, atomic::Ordering},
    thread,
};

use rand::Rng;
use store::mapping_table::{FrameInner, Table};

const BLOCK_SIZE: usize = 1024;
const NUM_PAGES: usize = BLOCK_SIZE * 16;
const PAGE_SIZE: usize = 1024;

#[ignore]
#[test]
fn scratch() {
    let table = Arc::new(Table::<1024>::new(16 * 1024));
    let mut threads = Vec::new();
    for t in 0..4 {
        let table = table.clone();

        threads.push(
            thread::Builder::new()
                .name(format!("{t}"))
                .spawn(move || {
                    let mut rng = rand::rng();
                    for i in 0..10000 {
                        let page_id = rng.random_range(0..NUM_PAGES) as u64;
                        let frame = &table[page_id];
                        let page = frame.load();
                        match page {
                            FrameInner::Mem(buf) => {}
                            FrameInner::Free(next) => {}
                            _ => panic!("uhhh that's not supposed to happen"),
                        }
                    }
                })
                .unwrap(),
        );
    }

    for t in threads {
        t.join().unwrap();
    }
}
