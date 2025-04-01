use crate::{buffer::PageBuffer, page_table::PageId};

use std::{
    alloc::{Layout, alloc},
    ops::Index,
    ptr,
    sync::{
        Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

// trying to do something like Fidor's RCU deque with this,
// video here: https://www.youtube.com/watch?v=rxQ5K9lo034
pub struct Table {
    ptr: AtomicPtr<Vec<Frame>>,
    len: AtomicUsize,
    grow_lock: Mutex<()>,
    block_size: usize,
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
        let layout = Layout::array::<Vec<Frame>>(num_blocks).unwrap();
        let blocks = unsafe { alloc(layout) } as *mut Vec<Frame>;
        for i in 0..num_blocks {
            let mut block = Vec::with_capacity(block_size);
            for j in 0..block_size {
                let next = {
                    let n = ((i * block_size) + j + 1) as u64;
                    if n >= capacity as u64 { 0 } else { n }
                };
                let frame_inner = Box::new(FrameInner::Free(next));
                block.push(Frame {
                    ptr: AtomicPtr::new(Box::into_raw(frame_inner)),
                });
            }
            let ptr = Box::new(block);
            unsafe {
                ptr::copy(Box::into_raw(ptr), blocks.add(i), 1);
            }
        }

        Self {
            ptr: AtomicPtr::new(blocks),
            len: AtomicUsize::new(capacity),
            grow_lock: Mutex::new(()),
            block_size,
        }
    }
    /// the caller is responsible for fixing the free list
    pub fn grow(&self) -> Result<(), ()> {
        match self.grow_lock.try_lock() {
            Ok(_guard) => {
                let blocks = self.ptr();
                let len = self.len();
                let num_blocks = len / self.block_size;

                // allocate a pointer block to fit one new pointer
                // PERF: we can probably get a benefit from doubling the pointer
                // block size when it's full, and then just storing the new frame
                // block in the next pointer if we have space, but let's just
                // keep it simple for now
                let layout = Layout::array::<Vec<Frame>>(num_blocks + 1).unwrap();
                let new_blocks = unsafe { alloc(layout) } as *mut Vec<Frame>;
                unsafe { std::ptr::copy(blocks, new_blocks, len) };

                // make the new frame block
                let mut new_block = Vec::with_capacity(self.block_size);
                for i in 0..self.block_size {
                    let next = ((num_blocks * self.block_size) + i + 1) as u64;
                    let frame_inner = Box::new(FrameInner::Free(next));
                    new_block.push(Frame {
                        ptr: AtomicPtr::new(Box::into_raw(frame_inner)),
                    });
                }
                let ptr = Box::new(new_block);
                unsafe {
                    ptr::copy(Box::into_raw(ptr), new_blocks.add(num_blocks), 1);
                }

                self.ptr.store(new_blocks, Ordering::Release);
                self.len.store(len + self.block_size, Ordering::Release);

                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
    #[inline]
    pub fn ptr(&self) -> *mut Vec<Frame> {
        self.ptr.load(Ordering::Acquire)
    }
}

impl Index<PageId> for Table {
    type Output = Frame;

    #[inline]
    fn index(&self, index: PageId) -> &Self::Output {
        let block_idx = index >> self.block_size.trailing_zeros();
        let item_idx = index & (self.block_size as u64 - 1);
        unsafe {
            &std::slice::from_raw_parts(self.ptr(), self.len())[block_idx as usize]
                [item_idx as usize]
        }
    }
}

pub struct Frame {
    ptr: AtomicPtr<FrameInner>,
}
pub enum FrameInner {
    Mem(PageBuffer),
    Disk(PageId),
    Free(PageId),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch() {
        let table = Table::new(64, 8);
        for i in 0..64 {
            let frame = &table[i];
            let inner = unsafe { frame.ptr.load(Ordering::Acquire).as_ref().unwrap() };
            match inner {
                FrameInner::Free(next) => println!("frame {i} has next {next}"),
                _ => panic!("we should only have free frames at this point"),
            }
        }
        table.grow().unwrap();
        for i in 0..table.len() {
            let frame = &table[i as u64];
            let inner = unsafe { frame.ptr.load(Ordering::Acquire).as_ref().unwrap() };
            match inner {
                FrameInner::Free(next) => println!("frame {i} has next {next}"),
                _ => panic!("we should only have free frames at this point"),
            }
        }
    }
}
