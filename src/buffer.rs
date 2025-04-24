use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    marker::PhantomData,
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
    pub fn read<'p, P: PageLayout<'p>>(&self) -> Page<'p, P> {
        let read_offset = self.read_offset.load(Ordering::Acquire);
        let ptr = unsafe { self.ptr.add(read_offset) };
        Page::from(unsafe { slice::from_raw_parts(ptr, SIZE - read_offset) })
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

pub trait PageLayout<'p> {
    type Base: From<&'p [u8]>;
    type Delta: From<(&'p [u8], u8)>;
}
pub struct Page<'p, P: PageLayout<'p>> {
    buf: &'p [u8],
    top: usize,
    bottom: usize,
    _p: PhantomData<P>,
}
impl<'p, P: PageLayout<'p>> Page<'p, P> {
    pub fn reset_iter(&mut self) {
        self.top = 0;
        self.bottom = self.buf.len();
    }
}
impl<'p, P: PageLayout<'p>> From<&'p [u8]> for Page<'p, P> {
    fn from(buf: &'p [u8]) -> Self {
        Self {
            buf,
            top: 0,
            bottom: buf.len(),
            _p: PhantomData,
        }
    }
}
impl<'p, P: PageLayout<'p>> Iterator for Page<'p, P> {
    type Item = PageChunk<'p, P>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        match self.buf[self.top] {
            0 => {
                let out = Some(PageChunk::Base(P::Base::from(
                    &self.buf[self.top + 1..self.buf.len() - 8],
                )));
                self.top = self.buf.len();
                out
            }
            delta_code => {
                let len =
                    u64::from_be_bytes(self.buf[self.top + 1..self.top + 9].try_into().unwrap())
                        as usize;
                let out = Some(PageChunk::Delta(P::Delta::from((
                    &self.buf[self.top + 9..self.top + 9 + len],
                    delta_code,
                ))));
                self.top += 18 + len;
                out
            }
        }
    }
}
impl<'p, P: PageLayout<'p>> DoubleEndedIterator for Page<'p, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        if self.bottom == self.buf.len() {
            let len =
                u64::from_be_bytes(self.buf[self.buf.len() - 8..].try_into().unwrap()) as usize;
            let out = Some(PageChunk::Base(P::Base::from(
                &self.buf[self.buf.len() - (8 + len)..self.buf.len() - 8],
            )));
            self.bottom -= 9 + len;
            return out;
        }
        let delta_code = self.buf[self.bottom - 1];
        let len = u64::from_be_bytes(
            self.buf[self.bottom - 9..self.bottom - 1]
                .try_into()
                .unwrap(),
        ) as usize;
        let out = Some(PageChunk::Delta(P::Delta::from((
            &self.buf[self.bottom - (9 + len)..self.bottom - 9],
            delta_code,
        ))));
        self.bottom -= len + 18;
        out
    }
}
pub enum PageChunk<'c, P: PageLayout<'c>> {
    Delta(P::Delta),
    Base(P::Base),
}
