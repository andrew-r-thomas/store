use std::{
    alloc::{Layout, alloc},
    ops::Index,
    ptr::{self, NonNull, null_mut},
    sync::{
        Arc, Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

use crate::{page::PageBuffer, page_table::PageId};

// trying to do something like Fidor's RCU deque with this,
// video here: https://www.youtube.com/watch?v=rxQ5K9lo034
pub struct Table {
    ptr: AtomicPtr<Vec<FrameBlock>>,
    grow_lock: Mutex<()>,
    block_size: u64,
}

macro_rules! powof2 {
    ($x:ident) => {
        assert!(
            ($x & ($x - 1)) == 0,
            "{} must be a power of 2!",
            stringify!($x)
        );
    };
}

impl Table {
    pub fn new(capacity: usize, block_size: usize) -> Self {
        powof2!(capacity);
        powof2!(block_size);

        let num_blocks = capacity / block_size;
        let layout = Layout::array::<FrameBlock>(num_blocks).unwrap();
        let ptr = unsafe { alloc(layout) };
    }
    pub fn grow(&self) {
        match self.grow_lock.try_lock() {
            Ok(_guard) => {
                let blocks = unsafe { self.ptr.load(Ordering::Acquire).as_ref() }.unwrap();
                let mut new_blocks = Vec::with_capacity(blocks.len() * 2);
                // ugh
                new_blocks[0..blocks.len()].copy_from_slice(blocks);
            }
            Err(_) => return,
        }
    }
}

impl Index<PageId> for Table {
    type Output = Frame;

    #[inline]
    fn index(&self, index: PageId) -> &Self::Output {
        let block_idx = index >> self.block_size.trailing_zeros();
        let item_idx = index & (self.block_size - 1);
        &unsafe { self.ptr.load(Ordering::Acquire).as_ref() }.unwrap()[block_idx as usize]
            [item_idx as usize]
    }
}

pub type FrameBlock = Vec<Frame>;
pub struct Frame {
    ptr: AtomicPtr<PageBuffer>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch() {
        let a = 8;
        let b = 3;
        powof2!(a);
        powof2!(b);
    }
}
