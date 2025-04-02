use std::{
    alloc::{Layout, alloc, alloc_zeroed, handle_alloc_error},
    ptr::{self, NonNull},
    sync::atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering},
};

pub struct PageBuffer {
    buf: NonNull<[u8]>,
    read_offset: AtomicUsize,
    write_offset: AtomicIsize,
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
    pub fn reserve(&self, len: usize) -> Result<WriteGuard, ()> {
        let old_write_offset = self.write_offset.fetch_sub(len as isize, Ordering::SeqCst);
        let new_write_offset = old_write_offset - len as isize;
        if new_write_offset < 0 {
            if old_write_offset >= 0 {
                // we caused the buffer to be sealed
                // first we wait for the other writers to finish
                while self.read_offset.load(Ordering::Acquire) != old_write_offset as usize {}
                todo!("now we can do some compaction")
            }
            return Err(());
        }
        let write = &mut unsafe { self.buf.as_ptr().as_mut().unwrap() }
            [new_write_offset as usize..old_write_offset as usize];
        Ok(WriteGuard {
            write,
            p: self,
            bottom: old_write_offset as usize,
        })
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
