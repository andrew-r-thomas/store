use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    slice,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
};

pub struct PageBuffer<const SIZE: usize> {
    ptr: *mut u8,
    read_offset: AtomicUsize,
    write_offset: AtomicIsize,
}

impl<const SIZE: usize> PageBuffer<SIZE> {
    pub fn new<'n>() -> Self {
        let layout = Layout::array::<u8>(SIZE).unwrap();
        assert!(SIZE <= isize::MAX as usize);

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout)
        }

        Self {
            ptr,
            read_offset: AtomicUsize::new(SIZE),
            write_offset: AtomicIsize::new(SIZE as isize),
        }
    }
    pub fn read(&self) -> &[u8] {
        let read_offset = self.read_offset.load(Ordering::Acquire);
        let ptr = unsafe { self.ptr.add(read_offset) };
        unsafe { slice::from_raw_parts(ptr, SIZE - read_offset) }
    }
    pub fn write_delta<'w, D: Delta>(&self, delta: &D) -> WriteRes<'w> {
        let len = delta.len() + 18;
        let old_write_offset = self.write_offset.fetch_sub(len as isize, Ordering::SeqCst);
        let new_write_offset = old_write_offset - len as isize;

        if new_write_offset <= 0 {
            if old_write_offset > 0 {
                // we caused the buffer to be sealed
                while self.read_offset.load(Ordering::Acquire) != old_write_offset as usize {}
                return WriteRes::Sealer(unsafe {
                    slice::from_raw_parts(
                        self.ptr.add(old_write_offset as usize),
                        SIZE - old_write_offset as usize,
                    )
                });
            }
            // the buffer is already sealed
            return WriteRes::Sealed;
        }

        let buf = unsafe {
            slice::from_raw_parts_mut(self.ptr.add(new_write_offset as usize + 9), delta.len())
        };
        delta.write_to_buf(buf);

        while let Err(_) = self.read_offset.compare_exchange_weak(
            old_write_offset as usize,
            new_write_offset as usize,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {}
        WriteRes::Ok
    }

    pub fn set_top(&mut self, top: usize) {
        self.write_offset.store(top as isize, Ordering::Release);
        self.read_offset.store(top, Ordering::Release);
    }

    pub fn raw_buffer<'r>(&'r mut self) -> &'r mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, SIZE) }
    }
}

pub enum WriteRes<'w> {
    Ok,
    Sealed,
    Sealer(&'w [u8]),
}

pub trait Delta {
    fn len(&self) -> usize;
    fn write_to_buf(&self, buf: &mut [u8]);
}
