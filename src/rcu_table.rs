/*

    TODO:
    - add a way to check (at compile time ideally) that the block size is a power of 2

*/

use std::{
    alloc::{self, Layout},
    ptr,
    sync::{
        Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

use crate::{PageId, buffer::PageBuffer};

pub struct MappingTable<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> {
    ptr: AtomicPtr<Block<BLOCK_SIZE, PAGE_SIZE>>,
    num_blocks: AtomicUsize,
    grow_lock: Mutex<Vec<*mut Block<BLOCK_SIZE, PAGE_SIZE>>>,
}

impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> MappingTable<BLOCK_SIZE, PAGE_SIZE> {
    pub fn new(capacity: usize) -> Self {
        todo!()
    }

    pub fn get(&self, page_id: PageId) -> &PageBuffer<PAGE_SIZE> {
        let frame = self.get_frame(page_id as usize);
        match unsafe { &*frame.ptr.load(Ordering::Acquire) } {
            FrameInner::Mem(page_buffer) => page_buffer,
            FrameInner::Disk(_) => todo!(),
            FrameInner::Free(_) => panic!(),
        }
    }

    /// this function performs an atomic store, not a compare and swap, so you should have
    /// exclusive write access to the logical page when you call it (i.e. the page is sealed, or
    /// you just created it, etc)
    pub fn set(&self, page_id: PageId, buf: PageBuffer<PAGE_SIZE>) {
        let frame = self.get_frame(page_id as usize);
        let ptr = Box::into_raw(Box::new(FrameInner::Mem(buf)));
        frame.ptr.store(ptr, Ordering::Release);
    }

    pub fn pop_free(&self) -> PageId {
        loop {
            // load the 0th slot, which points to the head of the free list
            let head = self.get_frame(0);
            let head_inner = unsafe { &mut *head.ptr.load(Ordering::Acquire) };
            let head_id = head_inner.unwrap_as_free();

            if head_id == 0 {
                // there are no free pages and we need to grow the table
                self.grow();
                continue;
            }

            let next_id = unsafe { &*self.get_frame(head_id as usize).ptr.load(Ordering::Acquire) }
                .unwrap_as_free();
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

    /// this function performs an atomic store on the frame at [`page_id`], so you should have
    /// exclusive write access to the logical page
    pub fn push_free(&self, page_id: PageId) {
        loop {
            let head = self.get_frame(0);
            let head_inner = unsafe { &mut *head.ptr.load(Ordering::Acquire) };
            let head_id = head_inner.unwrap_as_free();

            let next = Box::into_raw(Box::new(FrameInner::Free(head_id)));
            let frame = self.get_frame(page_id as usize);
            frame.ptr.store(next, Ordering::Release);

            if let Ok(_) =
                head.ptr
                    .compare_exchange_weak(head_inner, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                return;
            }
        }
    }

    // TODO: figure out where exactly we need CAS loops for free list manipulation if we're taking
    // a lock in this function (like we might not need the loops in certain spots if we already
    // have a lock, ykwim)
    fn grow(&self) {
        if let Ok(grow_guard) = self.grow_lock.try_lock() {
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
            todo!();

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
impl<I: ExactSizeIterator<Item = Frame<PAGE_SIZE>>, const BLOCK_SIZE: usize, const PAGE_SIZE: usize>
    From<I> for MappingTable<BLOCK_SIZE, PAGE_SIZE>
{
    fn from(mut i: I) -> Self {
        let size = i.len();
        let num_blocks = size / BLOCK_SIZE;
        let ptr_block_layout = Layout::array::<Block<BLOCK_SIZE, PAGE_SIZE>>(num_blocks).unwrap();
        let ptr_block =
            unsafe { alloc::alloc_zeroed(ptr_block_layout) } as *mut Block<BLOCK_SIZE, PAGE_SIZE>;
        if ptr_block.is_null() {
            alloc::handle_alloc_error(ptr_block_layout)
        }

        let mut cursor = 0;
        while i.len() > 0 {
            let block: Block<BLOCK_SIZE, PAGE_SIZE> = Block::from_iter(&mut i);
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

struct Block<const SIZE: usize, const PAGE_SIZE: usize>(*mut Frame<PAGE_SIZE>);
impl<const SIZE: usize, const PAGE_SIZE: usize> Block<SIZE, PAGE_SIZE> {
    pub fn from_iter<I: ExactSizeIterator<Item = Frame<PAGE_SIZE>>>(iter: &mut I) -> Self {
        let layout = Layout::array::<Frame<PAGE_SIZE>>(SIZE).unwrap();
        let ptr = unsafe { alloc::alloc_zeroed(layout) } as *mut Frame<PAGE_SIZE>;
        if ptr.is_null() {
            alloc::handle_alloc_error(layout)
        }
        for i in 0..SIZE {
            unsafe { ptr::write(ptr.add(i), iter.next().unwrap()) };
        }
        Self(ptr)
    }
}

struct Frame<const PAGE_SIZE: usize> {
    ptr: AtomicPtr<FrameInner<PAGE_SIZE>>,
}
enum FrameInner<const PAGE_SIZE: usize> {
    Mem(PageBuffer<PAGE_SIZE>),
    Disk(u64),
    Free(PageId),
}
impl<const PAGE_SIZE: usize> FrameInner<PAGE_SIZE> {
    pub fn unwrap_as_mem(&self) -> &PageBuffer<PAGE_SIZE> {
        if let Self::Mem(buf) = self {
            return buf;
        }
        panic!()
    }
    pub fn unwrap_as_disk(&self) -> u64 {
        if let Self::Disk(offset) = self {
            return *offset;
        }
        panic!()
    }
    pub fn unwrap_as_free(&self) -> PageId {
        if let Self::Free(next) = self {
            return *next;
        }
        panic!()
    }
}
