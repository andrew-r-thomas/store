use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    ptr::NonNull,
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
    buf: Vec<AtomicPtr<PoolFrame>>,
}

impl Pool {
    pub fn new(capacity: usize, buffer_capacity: usize) -> Self {
        let mut buf = Vec::with_capacity(capacity);
        for i in 1..capacity {
            let ptr = Box::new(PoolFrame::Buffer {
                buf: PageBuffer::new(buffer_capacity),
                next: i - 1,
            });
            buf.push(AtomicPtr::new(Box::into_raw(ptr)));
        }
        Self { buf }
    }
}

enum PoolFrame {
    Buffer { buf: PageBuffer, next: usize },
    Empty(usize),
    // probably will have something like 'hazard' or 'epoch' for buffers that
    // are in the frame but shouldn't be reclaimed yet
}
