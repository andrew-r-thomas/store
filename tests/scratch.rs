use std::{
    fs::{self, File},
    sync::Arc,
    thread,
};

use store::log::Log;

#[test]
fn scratch() {
    let file = File::create("scratch.store").unwrap();
    let log = Arc::new(Log::new(1024, 4, file));

    let mut threads = Vec::with_capacity(4);
    for t in 0..4 {
        let log = log.clone();
        threads.push(
            thread::Builder::new()
                .name(format!("{t}"))
                .spawn(move || {
                    let mut buf = Vec::with_capacity(16);
                    for i in 0..10000 {
                        buf.clear();
                        for b in (t as u64).to_be_bytes() {
                            buf.push(b);
                        }
                        for b in (i as u64).to_be_bytes() {
                            buf.push(b);
                        }

                        loop {
                            if let Ok((idx, offset)) = log.reserve(16) {
                                log.write_to_buffer(&buf, idx, offset);
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
    drop(log);

    let bytes = fs::read("scratch.store").unwrap();
    for entry in bytes.chunks_exact(16) {
        let thread = u64::from_be_bytes(entry[0..8].try_into().unwrap());
        let i = u64::from_be_bytes(entry[8..16].try_into().unwrap());
        println!("thread {thread}: {i}");
    }
    fs::remove_file("scratch.store").unwrap();
}
