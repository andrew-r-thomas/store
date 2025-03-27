use std::{sync::Arc, thread};

use store::page::{Page, PageNode};

#[test]
fn scratch() {
    let page = Arc::new(Page::new(Arc::new(store::page::PageNode::Base(
        store::page::BaseNode { data: vec![0; 4] },
    ))));

    let mut threads = Vec::new();
    for t in 1..=8 {
        let page = page.clone();
        threads.push(
            thread::Builder::new()
                .name(format!("{t}"))
                .spawn(move || {
                    for i in 1..=100 {
                        loop {
                            let curr = page.read();
                            let delta = Arc::new(PageNode::Delta(store::page::DeltaNode {
                                data: vec![i; t],
                                next: Some(curr.clone()),
                            }));
                            if let Ok(_) = page.update(curr, delta) {
                                break;
                            }
                        }
                    }
                })
                .unwrap(),
        );
    }

    for t in threads {
        t.join().unwrap();
    }

    for node in page.iter() {
        match &*node {
            PageNode::Delta(d) => {
                println!("delta node: {:?}", d.data);
            }
            PageNode::Base(b) => {
                println!("base node: {:?}", b.data);
            }
            _ => panic!("invalid page node type!"),
        }
    }
}
