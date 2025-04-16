use crate::{buffer::PageBuffer, page_table::PageId};

use std::{
    alloc::{Layout, alloc, handle_alloc_error},
    ops::Index,
    ptr::{self, NonNull},
    sync::{
        Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

pub struct Table<const B: usize> {
    ptr: AtomicPtr<FrameBlock<B>>,
    blocks: AtomicUsize,

    /// we lock the grow to make sure only one thread does it,
    /// grows are rare so a lock isn't a huge deal.
    /// inside the mutex there's just a vec of old pointer blocks,
    /// because we can't delete them immediately.
    /// for now, this vec will just accumulate old blocks and we won't deal
    /// with freeing them
    grow_lock: Mutex<Vec<*mut FrameBlock<B>>>,
}

unsafe impl<const B: usize> Send for Table<B> {}
unsafe impl<const B: usize> Sync for Table<B> {}

// TODO: add some way to assert (ideally at compile time) that B is a power of 2
// something like this: const_assert_eq!(B & (B - 1), 0);
//
// NOTE: for now, we're just gonna try *not* fixing the free list pointers,
// this seems like it will still work, we would just see > len as the end of
// the free list -> we should try to break this to verify
impl<const B: usize> Table<B> {
    pub fn new(capacity: usize) -> Self {
        // allocate the pointer block
        let num_blocks = capacity / B;
        let layout = Layout::array::<FrameBlock<B>>(num_blocks).unwrap();
        let blocks = unsafe { alloc(layout) } as *mut FrameBlock<B>;
        if blocks.is_null() {
            handle_alloc_error(layout);
        }

        for i in 0..num_blocks {
            // make a new frame block and add it to the pointer block
            let block = FrameBlock::new(i * B);
            unsafe {
                ptr::write(blocks.add(i), block);
            }
        }

        Self {
            ptr: AtomicPtr::new(blocks),
            blocks: AtomicUsize::new(num_blocks),
            grow_lock: Mutex::new(Vec::new()),
        }
    }

    /// for now we can assume everything is in memory, but this function will
    /// also handle reading in pages from disk if necessary
    pub fn read(&self, page_id: PageId) -> &PageBuffer {
        assert!((page_id as usize) < self.len());
        let frame = &self[page_id];
        match frame.load() {
            FrameInner::Mem(buf) => buf,
            _ => panic!("page not in memory!"),
        }
    }

    /// since we only update frames when there is a single writer, we can just
    /// do a store on the pointer, and don't have to deal with doing a CAS
    pub fn update(&self, page_id: PageId, new: PageBuffer) {
        assert!((page_id as usize) < self.len());
        let frame = &self[page_id];
        frame.store(&FrameInner::Mem(new));
    }

    pub fn grow(&self) -> Result<(), ()> {
        match self.grow_lock.try_lock() {
            Ok(mut garbage) => {
                let blocks = self.ptr();
                let num_blocks = self.num_blocks();

                // allocate a pointer block to fit one new pointer
                // PERF: we can probably get a benefit from doubling the pointer
                // block size when it's full, and then just storing the new frame
                // block in the next pointer if we have space, but let's just
                // keep it simple for now -> important note, this will be the
                // first step when we run into "the garbase list is getting too big"
                let layout = Layout::array::<FrameBlock<B>>(num_blocks + 1).unwrap();
                let new_blocks = unsafe { alloc(layout) } as *mut FrameBlock<B>;
                if new_blocks.is_null() {
                    handle_alloc_error(layout);
                }
                unsafe { std::ptr::copy_nonoverlapping(blocks, new_blocks, num_blocks) };

                // make the new frame block
                let new_block = FrameBlock::new(num_blocks * B);
                unsafe {
                    ptr::write(new_blocks.add(num_blocks), new_block);
                }

                self.ptr.store(new_blocks, Ordering::Release);
                self.blocks.store(num_blocks + 1, Ordering::Release);

                // for now we'll just keep things super simple and accumulate
                // garbage until we drop the table
                garbage.push(blocks);

                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    /// this function traverses the free list to find a free page id,
    /// if there isn't one, it will grow the mapping table.
    ///
    /// returns the new page id, along with it's buffer
    pub fn pop(&self) -> PageId {
        todo!()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.num_blocks() * B
    }
    #[inline]
    pub fn ptr(&self) -> *mut FrameBlock<B> {
        self.ptr.load(Ordering::Acquire)
    }
    #[inline]
    pub fn num_blocks(&self) -> usize {
        self.blocks.load(Ordering::Acquire)
    }
}

impl<const B: usize> Index<PageId> for Table<B> {
    type Output = Frame;

    #[inline]
    fn index(&self, index: PageId) -> &Self::Output {
        let block_idx = index >> B.trailing_zeros();
        let item_idx = index & (B as u64 - 1);
        unsafe {
            (&*self.ptr().add(block_idx as usize))
                .0
                .add(item_idx as usize)
                .as_ref()
        }
    }
}

/// a heap allocated fixed size array of frames
pub struct FrameBlock<const B: usize>(NonNull<Frame>);
impl<const B: usize> FrameBlock<B> {
    pub fn new(start: usize) -> Self {
        let layout = Layout::array::<Frame>(B).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut Frame;
        let nn = match NonNull::new(ptr) {
            Some(nn) => nn,
            None => handle_alloc_error(layout),
        };
        for i in 0..B {
            let inner = Box::new(FrameInner::Free((start + i + 1) as PageId));
            let frame = Frame {
                ptr: AtomicPtr::new(Box::into_raw(inner)),
            };
            unsafe {
                ptr::write(nn.as_ptr().add(i), frame);
            };
        }

        Self(nn)
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

impl Frame {
    pub fn load(&self) -> &FrameInner {
        unsafe { &*self.ptr.load(Ordering::Acquire) }
    }
    pub fn store(&self, new: &FrameInner) {
        self.ptr
            .store(ptr::from_ref(new).cast_mut(), Ordering::Release);
    }
    pub fn compare_exchange(
        &self,
        current: &FrameInner,
        new: &FrameInner,
    ) -> Result<*mut FrameInner, *mut FrameInner> {
        self.ptr.compare_exchange(
            ptr::from_ref(current).cast_mut(),
            ptr::from_ref(new).cast_mut(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use rand::Rng;

    use super::*;

    #[test]
    fn scratch() {
        let table = Arc::new(Table::<8>::new(16));

        let mut threads = Vec::with_capacity(4);
        for t in 0..4 {
            let table = table.clone();
            threads.push(
                thread::Builder::new()
                    .name(format!("{t}"))
                    .spawn(move || {
                        let mut rng = rand::rng();
                        for _ in 0..64 {
                            if rng.random_bool(0.1) {
                                if let Ok(_) = table.grow() {
                                    println!("table grew!");
                                }
                            }
                        }
                    })
                    .unwrap(),
            );
        }
        for t in threads {
            t.join().unwrap();
        }
        let len = table.len();
        println!("len: {len}");
        for i in 0..len {
            let frame = &table[i as u64];
            let inner = unsafe { frame.ptr.load(Ordering::Acquire).as_ref().unwrap() };
            match inner {
                FrameInner::Free(next) => println!("frame {i} has next {next}"),
                _ => panic!("we should only have free frames at this point"),
            }
        }
    }
}
