use crate::page::{DeltaNode, Page, PageNode, PageNodeInner};

use std::sync::Arc;

pub type PageId = u64;

pub struct PageTable {
    buf: Vec<Page>,
    // log: Log,
}
impl PageTable {
    pub fn read(&self, page_id: PageId) -> Arc<PageNode> {
        let out = self.buf[page_id as usize].read();
        if let PageNodeInner::Disk(_d) = &out.data {
            todo!("read from disk")
        }
        out
    }

    pub fn update_delta(
        &self,
        page_id: PageId,
        current: Arc<PageNode>,
        data: Vec<u8>,
    ) -> Result<Arc<PageNode>, Arc<PageNode>> {
        let delta_node = Arc::new(PageNode {
            data: PageNodeInner::Delta(DeltaNode { data }),
            next: Some(current.clone()),
        });

        self.buf[page_id as usize].update(current, delta_node)
    }
    pub fn update_replace(
        &self,
        page_id: PageId,
        current: Arc<PageNode>,
        new: Arc<PageNode>,
    ) -> Result<Arc<PageNode>, Arc<PageNode>> {
        self.buf[page_id as usize].update(current, new)
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
