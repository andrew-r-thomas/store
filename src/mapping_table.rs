use std::{
    alloc::Layout,
    ops::Index,
    ptr::{NonNull, null_mut},
    sync::{
        Arc, Mutex,
        atomic::{AtomicPtr, Ordering},
    },
};

use crate::{page::PageBuffer, page_table::PageId};

// trying to do something like Fidor's RCU deque with this,
// video here: https://www.youtube.com/watch?v=rxQ5K9lo034
pub struct Table {
    ptr: AtomicPtr<RefBlock>,
    block_size: usize,
    grow_lock: Mutex<()>,
}

impl Table {
    pub fn get(&self, page_id: PageId) -> Frame {}
    pub fn grow(&self) -> Result<(), ()> {
        match self.grow_lock.try_lock() {
            Ok(_guard) => {
                // we'll probably just double the size
                todo!()
            }
            Err(_) => Err(()),
        }
    }
}

pub type RefBlock = Arc<[FrameBlock]>;

pub struct FrameBlock {
    ptr: NonNull<[Frame]>,
}
pub struct Frame {
    ptr: AtomicPtr<PageBuffer>,
}
