use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    slice,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
};

pub struct PageBuffer<const SIZE: usize> {
    ptr: *mut u8,
    pub read_offset: AtomicUsize,
    pub write_offset: AtomicIsize,
}

impl<const SIZE: usize> PageBuffer<SIZE> {
    pub fn new<'n>() -> (Self, &'n mut [u8]) {
        let layout = Layout::array::<u8>(SIZE).unwrap();
        assert!(SIZE <= isize::MAX as usize);

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout)
        }

        (
            Self {
                ptr,
                read_offset: AtomicUsize::new(SIZE),
                write_offset: AtomicIsize::new(SIZE as isize),
            },
            unsafe { slice::from_raw_parts_mut(ptr, SIZE) },
        )
    }
    pub fn read(&self) -> &[u8] {
        let read_offset = self.read_offset.load(Ordering::Acquire);
        let ptr = unsafe { self.ptr.add(read_offset) };
        unsafe { slice::from_raw_parts(ptr, SIZE - read_offset) }
    }
    pub fn reserve(&self, len: usize) -> ReserveResult<SIZE> {
        let old_write_offset = self.write_offset.fetch_sub(len as isize, Ordering::SeqCst);
        let new_write_offset = old_write_offset - len as isize;
        if new_write_offset <= 0 {
            if old_write_offset > 0 {
                // we caused the buffer to be sealed
                return ReserveResult::Sealer(SealGuard {
                    p: self,
                    bottom: old_write_offset as usize,
                });
            }
            return ReserveResult::Sealed;
        }
        let write = unsafe {
            slice::from_raw_parts_mut(
                self.ptr.add(new_write_offset as usize),
                (old_write_offset - new_write_offset) as usize,
            )
        };
        ReserveResult::Ok(WriteGuard {
            buf: write,
            p: self,
            bottom: old_write_offset as usize,
        })
    }
}

pub enum ReserveResult<'r, const S: usize> {
    Ok(WriteGuard<'r, S>),
    Sealer(SealGuard<'r, S>),
    Sealed,
}

pub struct SealGuard<'s, const S: usize> {
    p: &'s PageBuffer<S>,
    bottom: usize,
}
impl<const S: usize> SealGuard<'_, S> {
    // TODO: so here there might be a benefit to interleaving waiting
    // for writes and returning them, i.e. say we're waiting for 3 writers,
    // when one of them completes, we could in theory process that write
    // and add it to the new compacted buffer, while the other writes are
    // still in flight, since we're already looping here, that *should* get
    // us some more concurrent work happening. one caveat though is that if
    // the writes we're waiting for are for the same item (like say one write
    // updates a key, and the next one deletes it) and in that case we'd
    // probably be doing *more* work, so we should profile this and see if it
    // makes sense to do
    #[inline]
    pub fn wait_for_writers(&self) -> &[u8] {
        loop {
            let r = self.p.read_offset.load(Ordering::Acquire);
            if r == self.bottom {
                return unsafe { slice::from_raw_parts(self.p.ptr.add(r), S - r) };
            }
        }
    }
}

pub struct WriteGuard<'w, const S: usize> {
    pub buf: &'w mut [u8],
    p: &'w PageBuffer<S>,
    bottom: usize,
}
impl<const S: usize> Drop for WriteGuard<'_, S> {
    fn drop(&mut self) {
        while let Err(_) = self.p.read_offset.compare_exchange_weak(
            self.bottom,
            self.bottom - self.buf.len(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {}
    }
}

// pub struct Pool<const PAGE_SIZE: usize> {
//     buf: NonNull<PageBuffer<PAGE_SIZE>>,
//     pop: AtomicUsize,
//     cap: usize,
// }

// impl<const PAGE_SIZE: usize> Pool<PAGE_SIZE> {
// pub fn new(capacity: usize, buffer_capacity: usize) -> Self {
//     let layout = Layout::array::<PageBuffer<PAGE_SIZE>>(capacity).unwrap();
//     let ptr = unsafe { alloc(layout) } as *mut PageBuffer<PAGE_SIZE>;
//     let buf = match NonNull::new(ptr) {
//         Some(nn) => nn,
//         None => handle_alloc_error(layout),
//     };
//     for i in 0..capacity {
//         let page = PageBuffer::new();
//         unsafe { ptr::write(buf.add(i).as_ptr(), page) };
//     }
//     Self {
//         buf,
//         cap: capacity,
//         pop: AtomicUsize::new(0),
//     }
// }
// pub fn pop(&self) -> *mut PageBuffer<PAGE_SIZE> {
//     let i = self.pop.fetch_add(1, Ordering::SeqCst);
//     if i >= self.cap {
//         panic!("ran out of memory in buffer pool")
//     }
//     unsafe { self.buf.add(i).as_ptr() }
// }
// }
