use std::sync::atomic::{AtomicPtr, Ordering};

type PageId = u64;

pub struct PageTable {
    buf: Vec<AtomicPtr<Page>>,
}
impl PageTable {
    pub fn read(&self, page_id: PageId) -> *mut Page {
        // TODO: validate this unsafe
        let out = self.buf[page_id as usize].load(Ordering::SeqCst);
        if let Page::Disk(_) = unsafe { &*out } {
            todo!("read from disk")
        }
        out
    }

    /// in the case that this is an "update replace" we are assuming that the
    /// [`new`] page is already a base page with everything folded in,
    /// the data parameter is just there for the lss, i think
    ///
    /// in the case of a delta update, the new pointer should be a delta
    /// that already points to the old page and is ready to go
    ///
    /// TODO: figure out what happens to [`data`]
    pub fn update(
        &self,
        page_id: u64,
        current: *mut Page,
        new: *mut Page,
        _data: Vec<u8>,
    ) -> Result<*mut Page, *mut Page> {
        self.buf[page_id as usize].compare_exchange(
            current,
            new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }

    pub fn flush() {
        unimplemented!()
    }
    pub fn make_stable() {
        unimplemented!()
    }
    pub fn high_stable() {
        unimplemented!()
    }
    pub fn allocate() {
        unimplemented!()
    }
    pub fn free() {
        unimplemented!()
    }

    // TODO: might want these to be guards or something
    pub fn begin_tx() {
        unimplemented!()
    }
    pub fn commit_tx() {
        unimplemented!()
    }
    pub fn abort_tx() {
        unimplemented!()
    }
}

pub enum Page {
    Base(BasePage),
    Disk(DiskPage),
    Delta(PageDelta),
    Free(FreePage),
}
pub struct BasePage {
    buf: Vec<u8>,
}

pub struct DiskPage {}
pub struct PageDelta {
    next: *mut Page,
    buf: Vec<u8>,
    offset: usize,
}
pub struct FreePage {
    next: Option<PageId>,
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use rand::Rng;

    use super::*;

    #[test]
    fn scratch() {
        let base1 = Box::new(Page::Base(BasePage {
            buf: vec![12; 1024],
        }));
        let base2 = Box::new(Page::Base(BasePage {
            buf: vec![24; 2048],
        }));
        let page_table = Arc::new(PageTable {
            buf: vec![
                AtomicPtr::new(Box::into_raw(Box::new(Page::Free(FreePage { next: None })))),
                AtomicPtr::new(Box::into_raw(base1)),
                AtomicPtr::new(Box::into_raw(base2)),
            ],
        });

        let mut handles = Vec::new();
        for t in 0..4 {
            let p_table = page_table.clone();
            handles.push(
                thread::Builder::new()
                    .name(format!("{t}"))
                    .spawn(move || {
                        let mut rng = rand::rng();
                        let read_page = |page_id: PageId| {
                            let page = p_table.read(page_id);
                            let mut page_read = unsafe { &*page };
                            while let Page::Delta(d) = page_read {
                                println!("got a delta for page {page_id}");
                                page_read = unsafe { &*d.next };
                            }
                            if let Page::Base(_) = page_read {
                                println!("got to base page for {page_id}");
                            } else {
                                panic!("expected a base page!")
                            }
                        };
                        let write_page = |page_id: PageId| {
                            let page = p_table.read(page_id);
                            let delta = Box::into_raw(Box::new(Page::Delta(PageDelta {
                                next: page,
                                buf: Vec::new(),
                                offset: 2,
                            })));
                            match p_table.update(page_id, page, delta, Vec::new()) {
                                Ok(_) => println!("write CAS succeeded for page {page_id}"),
                                Err(_) => println!("write CAS failed for page {page_id}"),
                            }
                        };

                        for _ in 0..10000 {
                            let read_write = rng.random_bool(0.5);
                            let one_two = rng.random_bool(0.5);
                            match (one_two, read_write) {
                                (true, true) => read_page(1),
                                (true, false) => write_page(1),
                                (false, true) => read_page(2),
                                (false, false) => write_page(2),
                            }
                        }
                    })
                    .unwrap(),
            );
        }
        for h in handles {
            h.join().unwrap()
        }
    }
}
