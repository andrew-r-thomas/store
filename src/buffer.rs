use std::{
    alloc::{Layout, alloc, alloc_zeroed, handle_alloc_error},
    ptr::{self, NonNull},
    sync::atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering},
};

pub struct PageBuffer {
    buf: NonNull<[u8]>,
    pub read_offset: AtomicUsize,
    pub write_offset: AtomicIsize,
}

impl PageBuffer {
    pub fn new(capacity: usize) -> Self {
        let layout = Layout::array::<u8>(capacity).unwrap();
        assert!(capacity <= isize::MAX as usize);

        let ptr = match NonNull::new(unsafe { alloc_zeroed(layout) }) {
            Some(nn) => nn,
            None => handle_alloc_error(layout),
        };
        let buf = NonNull::slice_from_raw_parts(ptr, capacity);

        Self {
            buf,
            read_offset: AtomicUsize::new(capacity),
            write_offset: AtomicIsize::new(capacity as isize),
        }
    }
    pub fn read(&self) -> &[u8] {
        let read_offset = self.read_offset.load(Ordering::Acquire);
        let b = &unsafe { self.buf.as_ref() }[read_offset..];
        b
    }
    pub fn reserve(&self, len: usize) -> ReserveResult {
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
        let write = &mut unsafe { self.buf.as_ptr().as_mut().unwrap() }
            [new_write_offset as usize..old_write_offset as usize];
        ReserveResult::Ok(WriteGuard {
            write,
            p: self,
            bottom: old_write_offset as usize,
        })
    }

    /// caller should be the only one with access to this page buffer.
    ///
    /// this is typically called as a utility during page compaction
    /// as there's no need to pay the cost of atomic access when the
    /// new base page is being created and not visible to the rest
    /// of the system.
    pub unsafe fn raw_buffer(&self) -> &mut [u8] {
        unsafe { self.buf.as_ptr().as_mut().unwrap() }
    }
}

pub enum ReserveResult<'r> {
    Ok(WriteGuard<'r>),
    Sealer(SealGuard<'r>),
    Sealed,
}

pub struct SealGuard<'s> {
    p: &'s PageBuffer,
    bottom: usize,
}
impl SealGuard<'_> {
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
                return unsafe { &self.p.buf.as_ref()[r..] };
            }
        }
    }
}

pub struct ReadGuard<'r> {
    p: &'r PageBuffer,
    pub read: &'r [u8],
}

pub struct WriteGuard<'w> {
    pub write: &'w mut [u8],
    p: &'w PageBuffer,
    bottom: usize,
}
impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        while let Err(_) = self.p.read_offset.compare_exchange_weak(
            self.bottom,
            self.bottom - self.write.len(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {}
    }
}

pub struct Pool {
    buf: NonNull<PageBuffer>,
    pop: AtomicUsize,
    cap: usize,
}

impl Pool {
    pub fn new(capacity: usize, buffer_capacity: usize) -> Self {
        let layout = Layout::array::<PageBuffer>(capacity).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut PageBuffer;
        let buf = match NonNull::new(ptr) {
            Some(nn) => nn,
            None => handle_alloc_error(layout),
        };
        for i in 0..capacity {
            let page = PageBuffer::new(buffer_capacity);
            unsafe { ptr::write(buf.add(i).as_ptr(), page) };
        }
        Self {
            buf,
            cap: capacity,
            pop: AtomicUsize::new(0),
        }
    }
    pub fn pop(&self) -> *mut PageBuffer {
        let i = self.pop.fetch_add(1, Ordering::SeqCst);
        if i >= self.cap {
            panic!("ran out of memory in buffer pool")
        }
        unsafe { self.buf.add(i).as_ptr() }
    }
}
