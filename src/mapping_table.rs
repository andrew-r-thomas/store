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

// trying to do something like Fidor's RCU deque with this,
// video here: https://www.youtube.com/watch?v=rxQ5K9lo034
pub struct Table<const B: usize> {
    ptr: AtomicPtr<FrameBlock<B>>,
    blocks: AtomicUsize,
    grow_lock: Mutex<()>,
}

// TODO: add some way to assert (ideally at compile time) that B is a power of 2
// something like this: const_assert_eq!(B & (B - 1), 0);
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

        let out = Self {
            ptr: AtomicPtr::new(blocks),
            blocks: AtomicUsize::new(num_blocks),
            grow_lock: Mutex::new(()),
        };

        let last = &out[(capacity - 1) as PageId];
        let end = Box::new(FrameInner::Free(0));
        last.ptr.store(Box::into_raw(end), Ordering::Release);

        out
    }
    pub fn grow(&self) -> Result<(), ()> {
        match self.grow_lock.try_lock() {
            Ok(_guard) => {
                let blocks = self.ptr();
                let num_blocks = self.num_blocks();

                // allocate a pointer block to fit one new pointer
                // PERF: we can probably get a benefit from doubling the pointer
                // block size when it's full, and then just storing the new frame
                // block in the next pointer if we have space, but let's just
                // keep it simple for now
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

                // FIX: problem child
                // fix free list
                // let zero = &self[0];
                // let len = self.len();
                // loop {
                //     let z = zero.ptr.load(Ordering::Acquire);
                //     let i = match unsafe { z.as_ref().unwrap() } {
                //         FrameInner::Free(i) => i,
                //         _ => panic!("non free frame in 0!"),
                //     };
                //     let tail = Box::new(FrameInner::Free(*i));
                //     (&self[(len + B - 1) as u64])
                //         .ptr
                //         .store(Box::into_raw(tail), Ordering::Release);
                //
                //     let head = Box::new(FrameInner::Free(len as u64));
                //     if let Ok(_) = zero.ptr.compare_exchange_weak(
                //         z,
                //         Box::into_raw(head),
                //         Ordering::SeqCst,
                //         Ordering::SeqCst,
                //     ) {
                //         break;
                //     }
                // }

                Ok(())
            }
            Err(_) => Err(()),
        }
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
        let block = unsafe { &*self.ptr().add(block_idx as usize) };
        unsafe { block.0.add(item_idx as usize).as_ref() }
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

        // let table = Table::<32>::new(128);
        // for i in 0..table.len() {
        //     let frame = &table[i as PageId];
        //     let inner = unsafe { frame.ptr.load(Ordering::Acquire).as_ref().unwrap() };
        //     match inner {
        //         FrameInner::Free(next) => println!("frame {i} has next {next}"),
        //         _ => panic!("woah why do we have not a free frame"),
        //     }
        // }
        // table.grow().unwrap();
        // for i in 0..table.len() {
        //     let frame = &table[i as PageId];
        //     let inner = unsafe { frame.ptr.load(Ordering::Acquire).as_ref().unwrap() };
        //     match inner {
        //         FrameInner::Free(next) => println!("frame {i} has next {next}"),
        //         _ => panic!("woah why do we have not a free frame"),
        //     }
        // }
    }
}
