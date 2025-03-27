use std::sync::{
    Arc,
    atomic::{AtomicPtr, Ordering},
};

type PageId = u64;

pub struct PageTable {
    buf: Vec<AtomicPtr<Arc<Page>>>,
}
impl PageTable {
    pub fn read(&self, page_id: PageId) -> Arc<Page> {
        // TODO: validate this unsafe
        let out = unsafe { &*self.buf[page_id as usize].load(Ordering::SeqCst) }.clone();
        if let Page::Disk(_) = *out {
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
        mut current: Arc<Page>,
        mut new: Arc<Page>,
        _data: Vec<u8>,
    ) -> Result<Arc<Page>, Arc<Page>> {
        match self.buf[page_id as usize].compare_exchange(
            &mut current,
            &mut new,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(p) => Ok(unsafe { &*p }.clone()),
            Err(p) => Ok(unsafe { &*p }.clone()),
        }
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
    next: Arc<Page>,
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

    fn read_page(page_id: PageId, page_table: &Arc<PageTable>) {
        let mut page = page_table.read(page_id);
        // FIX: no deltas showing up
        while let Page::Delta(d) = &*page {
            println!("got a delta for page {page_id}");
            page = d.next.clone();
        }
        if let Page::Base(_) = &*page {
            println!("got to base page for {page_id}");
        } else {
            panic!("expected a base page!")
        }
    }
    fn write_page(page_id: PageId, page_table: &Arc<PageTable>) {
        let page = page_table.read(page_id);
        let delta = Arc::new(Page::Delta(PageDelta {
            next: page.clone(),
            buf: Vec::new(),
            offset: 2,
        }));
        match page_table.update(page_id, page, delta, Vec::new()) {
            Ok(_) => println!("write CAS succeeded for page {page_id}"),
            Err(_) => println!("write CAS failed for page {page_id}"),
        }
    }

    #[ignore]
    #[test]
    fn scratch() {
        let mut base1 = Arc::new(Page::Base(BasePage {
            buf: vec![12; 1024],
        }));
        let mut base2 = Arc::new(Page::Base(BasePage {
            buf: vec![24; 2048],
        }));
        let page_table = Arc::new(PageTable {
            buf: vec![
                AtomicPtr::new(&mut Arc::new(Page::Free(FreePage { next: None }))),
                AtomicPtr::new(&mut base1),
                AtomicPtr::new(&mut base2),
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

                        for _ in 0..10000 {
                            let read_write = rng.random_bool(0.75);
                            let one_two = rng.random_bool(0.5);
                            match (one_two, read_write) {
                                (true, true) => read_page(1, &p_table),
                                (true, false) => write_page(1, &p_table),
                                (false, true) => read_page(2, &p_table),
                                (false, false) => write_page(2, &p_table),
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
