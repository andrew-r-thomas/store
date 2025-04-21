/*

    TODO:
    - add a way to check (at compile time ideally) that the block size is a power of 2

*/

use std::{
    alloc::{self, Layout},
    ops::Index,
    ptr::{self, NonNull},
    sync::{
        Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

pub struct RCUTable<T, const BLOCK_SIZE: usize> {
    ptr: AtomicPtr<Block<T, BLOCK_SIZE>>,
    pub num_blocks: AtomicUsize,
    grow_lock: Mutex<Vec<*mut Block<T, BLOCK_SIZE>>>,
}

impl<T, const BLOCK_SIZE: usize> RCUTable<T, BLOCK_SIZE> {
    /// the size of [`iter`] needs to be a multiple of [`BLOCK_SIZE`]
    pub fn grow<I: ExactSizeIterator<Item = T>>(&self, mut iter: I) -> Result<(), ()> {
        match self.grow_lock.try_lock() {
            Ok(mut mutex_guard) => {
                // allocate a new pointer block
                let num_new_blocks = iter.len() / BLOCK_SIZE;
                let num_blocks = self.num_blocks();
                let layout =
                    Layout::array::<Block<T, BLOCK_SIZE>>(num_blocks + num_new_blocks).unwrap();
                let new_blocks =
                    unsafe { alloc::alloc_zeroed(layout) } as *mut Block<T, BLOCK_SIZE>;
                if new_blocks.is_null() {
                    alloc::handle_alloc_error(layout)
                }

                // copy the old pointer block
                let blocks = self.ptr();
                unsafe { ptr::copy_nonoverlapping(blocks, new_blocks, num_blocks) };

                // write in the new blocks from the iterator
                let mut cursor = 0;
                while iter.len() > 0 {
                    let block: Block<T, BLOCK_SIZE> = Block::from_iter(&mut iter);
                    unsafe { ptr::write(new_blocks.add(num_blocks + cursor), block) };
                    cursor += 1;
                }

                // add the old pointer block to the garbage list, update to the new pointer, and
                // bump the size
                mutex_guard.push(blocks);
                self.ptr.store(new_blocks, Ordering::Release);
                self.num_blocks
                    .store(num_blocks + num_new_blocks, Ordering::Release);
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    pub fn get(&self, idx: usize) -> &T {
        let block_idx = idx >> BLOCK_SIZE.trailing_zeros();
        let item_idx = idx & (BLOCK_SIZE - 1);
        let block = unsafe { &*(self.ptr().add(block_idx)) };
        unsafe { &*(block.0.add(item_idx)) }
    }

    #[inline]
    pub fn ptr(&self) -> *mut Block<T, BLOCK_SIZE> {
        self.ptr.load(Ordering::Acquire)
    }
    #[inline]
    pub fn num_blocks(&self) -> usize {
        self.num_blocks.load(Ordering::Acquire)
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.num_blocks() * BLOCK_SIZE
    }
}
impl<I: ExactSizeIterator<Item = T>, T, const BLOCK_SIZE: usize> From<I>
    for RCUTable<T, BLOCK_SIZE>
{
    fn from(mut i: I) -> Self {
        let size = i.len();
        let num_blocks = size / BLOCK_SIZE;
        let ptr_block_layout = Layout::array::<Block<T, BLOCK_SIZE>>(num_blocks).unwrap();
        let ptr_block =
            unsafe { alloc::alloc_zeroed(ptr_block_layout) } as *mut Block<T, BLOCK_SIZE>;
        if ptr_block.is_null() {
            alloc::handle_alloc_error(ptr_block_layout)
        }

        let mut cursor = 0;
        while i.len() > 0 {
            let block: Block<T, BLOCK_SIZE> = Block::from_iter(&mut i);
            unsafe { ptr::write(ptr_block.add(cursor), block) };
            cursor += 1;
        }

        Self {
            ptr: AtomicPtr::new(ptr_block),
            num_blocks: AtomicUsize::new(num_blocks),
            grow_lock: Mutex::new(Vec::new()),
        }
    }
}

pub struct Block<T, const SIZE: usize>(*mut T);
impl<T, const SIZE: usize> Block<T, SIZE> {
    pub fn from_iter<I: ExactSizeIterator<Item = T>>(iter: &mut I) -> Self {
        let layout = Layout::array::<T>(SIZE).unwrap();
        let ptr = unsafe { alloc::alloc_zeroed(layout) } as *mut T;
        if ptr.is_null() {
            alloc::handle_alloc_error(layout)
        }
        for i in 0..SIZE {
            unsafe { ptr::write(ptr.add(i), iter.next().unwrap()) };
        }
        Self(ptr)
    }
}
