use std::sync::{
    Arc,
    atomic::{AtomicPtr, Ordering},
};

use crate::{
    log::Log,
    page::{Base, Delta, Page},
};

type PageId = u64;

pub struct PageTable<B, D>
where
    B: Base,
    D: Delta,
{
    buf: Vec<Page>,
    log: Log,
}
impl PageTable {
    pub fn read(&self, page_id: PageId) -> Arc<Page> {
        let out = unsafe { &*self.buf[page_id as usize].load(Ordering::SeqCst) }.clone();
        if let Page::Disk(d) = &*out {
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
