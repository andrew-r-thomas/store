use std::{
    alloc::{self, Layout},
    ptr,
    sync::{
        Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

use crate::{PageId, page::PageBuffer};

pub struct PageDirectory<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> {
    ptr: AtomicPtr<Block<BLOCK_SIZE, PAGE_SIZE>>,
    num_blocks: AtomicUsize,
    grow_lock: Mutex<Vec<*mut Block<BLOCK_SIZE, PAGE_SIZE>>>,
}

impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> PageDirectory<BLOCK_SIZE, PAGE_SIZE> {
    pub fn new(capacity: usize) -> Self {
        let num_blocks = capacity / BLOCK_SIZE;

        let pointer_block_layout =
            Layout::array::<Block<BLOCK_SIZE, PAGE_SIZE>>(num_blocks).unwrap();
        let pointer_block = unsafe { alloc::alloc_zeroed(pointer_block_layout) }
            as *mut Block<BLOCK_SIZE, PAGE_SIZE>;
        if pointer_block.is_null() {
            alloc::handle_alloc_error(pointer_block_layout)
        }

        for i in 0..num_blocks {
            let block_layout = Layout::array::<Frame<PAGE_SIZE>>(BLOCK_SIZE).unwrap();
            let block = unsafe { alloc::alloc_zeroed(block_layout) } as *mut Frame<PAGE_SIZE>;
            if block.is_null() {
                alloc::handle_alloc_error(block_layout)
            }

            for j in 0..BLOCK_SIZE {
                let frame_inner = FrameInner::Free(((i * BLOCK_SIZE) + j + 1) as u64);
                let frame = Frame {
                    ptr: AtomicPtr::new(Box::into_raw(Box::new(frame_inner))),
                };
                unsafe { ptr::write(block.add(j), frame) };
            }

            unsafe { ptr::write(pointer_block.add(i), Block(block)) };
        }

        Self {
            ptr: AtomicPtr::new(pointer_block),
            num_blocks: AtomicUsize::new(num_blocks),
            grow_lock: Mutex::new(Vec::new()),
        }
    }

    pub fn get(&self, page_id: PageId) -> &PageBuffer<PAGE_SIZE> {
        let frame = self.get_frame(page_id as usize);
        match unsafe { &*frame.ptr.load(Ordering::Acquire) } {
            FrameInner::Mem(page_buffer) => page_buffer,
            // FrameInner::Disk(_) => todo!(),
            FrameInner::Free(_) => panic!(),
        }
    }

    pub fn set(&self, page_id: PageId, buf: PageBuffer<PAGE_SIZE>) {
        let frame = self.get_frame(page_id as usize);
        let ptr = Box::into_raw(Box::new(FrameInner::Mem(buf)));
        frame.ptr.store(ptr, Ordering::Release);
    }

    pub fn push(&self, buf: PageBuffer<PAGE_SIZE>) -> PageId {
        let id = self.pop_free();
        self.set(id, buf);
        id
    }

    pub fn pop_free(&self) -> PageId {
        loop {
            // load the 0th slot, which points to the head of the free list
            let head = self.get_frame(0);
            let head_inner = unsafe { &mut *head.ptr.load(Ordering::Acquire) };
            let head_id = head_inner.unwrap_as_free();

            if head_id >= self.len() as u64 {
                // there are no free pages and we need to grow the table
                self.grow();
                continue;
            }

            let head_frame = self.get_frame(head_id as usize);
            let next_id = unsafe { &*head_frame.ptr.load(Ordering::Acquire) }.unwrap_as_free();
            if let Ok(_) = head.ptr.compare_exchange_weak(
                head_inner,
                Box::into_raw(Box::new(FrameInner::Free(next_id))),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return head_id;
            }
        }
    }

    fn grow(&self) {
        if let Ok(mut grow_guard) = self.grow_lock.try_lock() {
            // allocate a new pointer block
            let num_blocks = self.num_blocks();
            let layout = Layout::array::<Block<BLOCK_SIZE, PAGE_SIZE>>(num_blocks * 2).unwrap();
            let new_blocks =
                unsafe { alloc::alloc_zeroed(layout) } as *mut Block<BLOCK_SIZE, PAGE_SIZE>;
            if new_blocks.is_null() {
                alloc::handle_alloc_error(layout)
            }

            // copy the old pointer block
            let blocks = self.ptr();
            unsafe { ptr::copy_nonoverlapping(blocks, new_blocks, num_blocks) };

            // allocate new blocks and expand free list
            let len = self.len();
            for i in 0..num_blocks {
                let block_layout = Layout::array::<Frame<PAGE_SIZE>>(BLOCK_SIZE).unwrap();
                let block = unsafe { alloc::alloc_zeroed(block_layout) } as *mut Frame<PAGE_SIZE>;
                if block.is_null() {
                    alloc::handle_alloc_error(block_layout)
                }

                for j in 0..BLOCK_SIZE {
                    let frame_inner = FrameInner::Free((len + (i * BLOCK_SIZE) + j + 1) as u64);
                    let frame = Frame {
                        ptr: AtomicPtr::new(Box::into_raw(Box::new(frame_inner))),
                    };
                    unsafe { ptr::write(block.add(j), frame) };
                }

                unsafe { ptr::write(new_blocks.add(num_blocks + i), Block(block)) };
            }

            // add the old pointer block to the garbage list, update to the new pointer, and
            // bump the size
            grow_guard.push(blocks);
            self.ptr.store(new_blocks, Ordering::Release);
            self.num_blocks.store(num_blocks * 2, Ordering::Release);
        }
    }

    #[inline]
    fn ptr(&self) -> *mut Block<BLOCK_SIZE, PAGE_SIZE> {
        self.ptr.load(Ordering::Acquire)
    }
    #[inline]
    fn num_blocks(&self) -> usize {
        self.num_blocks.load(Ordering::Acquire)
    }
    #[inline]
    fn len(&self) -> usize {
        self.num_blocks() * BLOCK_SIZE
    }
    #[inline]
    fn get_frame(&self, idx: usize) -> &Frame<PAGE_SIZE> {
        let block_idx = idx >> BLOCK_SIZE.trailing_zeros();
        let item_idx = idx & (BLOCK_SIZE - 1);

        let block = unsafe { &*(self.ptr().add(block_idx)) };
        unsafe { &*(block.0.add(item_idx)) }
    }
}

struct Block<const SIZE: usize, const PAGE_SIZE: usize>(*mut Frame<PAGE_SIZE>);

struct Frame<const PAGE_SIZE: usize> {
    ptr: AtomicPtr<FrameInner<PAGE_SIZE>>,
}
enum FrameInner<const PAGE_SIZE: usize> {
    Mem(PageBuffer<PAGE_SIZE>),
    // Disk(u64),
    Free(PageId),
}
impl<const PAGE_SIZE: usize> FrameInner<PAGE_SIZE> {
    // pub fn unwrap_as_mem(&self) -> &PageBuffer<PAGE_SIZE> {
    //     if let Self::Mem(buf) = self {
    //         return buf;
    //     }
    //     panic!()
    // }
    // pub fn unwrap_as_disk(&self) -> u64 {
    //     if let Self::Disk(offset) = self {
    //         return *offset;
    //     }
    //     panic!()
    // }
    pub fn unwrap_as_free(&self) -> PageId {
        if let Self::Free(next) = self {
            return *next;
        }
        panic!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch() {
        let page_dir = PageDirectory::<8, 1024>::new(64);
        for i in 1..128 {
            assert_eq!(page_dir.pop_free(), i);
        }
    }
}
