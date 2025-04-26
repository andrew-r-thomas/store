use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    marker::PhantomData,
    slice,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
};

pub struct PageBuffer<const SIZE: usize> {
    ptr: *mut u8,
    read_offset: AtomicUsize,
    write_offset: AtomicIsize,
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
    pub fn write_delta<'w, P: PageLayout<'w>>(&self, delta: &P::Delta) -> WriteRes<'w, P> {
        let len = delta.len() + 18;
        let old_write_offset = self.write_offset.fetch_sub(len as isize, Ordering::SeqCst);
        let new_write_offset = old_write_offset - len as isize;

        if new_write_offset <= 0 {
            if old_write_offset > 0 {
                // we caused the buffer to be sealed
                while self.read_offset.load(Ordering::Acquire) != old_write_offset as usize {}
                return WriteRes::Sealer(Page::from(unsafe {
                    slice::from_raw_parts(
                        self.ptr.add(old_write_offset as usize),
                        SIZE - old_write_offset as usize,
                    )
                }));
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
}

pub enum WriteRes<'r, P: PageLayout<'r>> {
    Ok,
    Sealed,
    Sealer(Page<'r, P>),
}

// ================================================================================================
// page access types
// ================================================================================================

// page layout traits =============================================================================

pub trait PageLayout<'p> {
    type Base: Base<'p>;
    type BaseMut: BaseMut<'p, Self>;
    type Delta: Delta<'p>;
    type Footer<const FOOTER_SIZE: usize>: From<&'p [u8]>;
}
pub trait Delta<'d>: From<(&'d [u8], u8)> {
    fn len(&self) -> usize;
    fn write_to_buf(&self, buf: &mut [u8]);
}
pub trait Base<'b>: From<&'b [u8]> {}
pub trait BaseMut<'b, P: PageLayout<'b> + ?Sized>: From<(&'b mut [u8], P::Base)> {
    fn apply_delta(&mut self, delta: P::Delta) -> Result<(), ()>;
    fn unwrap(self) -> (&'b mut [u8], usize);
    fn split_into(&mut self, to: &mut Self);
}

pub struct Page<'p, P: PageLayout<'p> + ?Sized> {
    buf: &'p [u8],
    _p: PhantomData<P>,
}
impl<'p, P: PageLayout<'p>> From<&'p [u8]> for Page<'p, P> {
    fn from(buf: &'p [u8]) -> Self {
        Self {
            buf,
            _p: PhantomData,
        }
    }
}
impl<'p, P: PageLayout<'p> + ?Sized> Page<'p, P> {
    pub fn iter_chunks(&self) -> PageChunkIter<'p, P> {
        PageChunkIter::from(self.buf)
    }
    pub fn compact_into(&self, to: &'p mut [u8]) -> P::BaseMut {
        let mut iter = self.iter_chunks().rev();
        let mut base_mut = {
            match iter.next().unwrap() {
                PageChunk::Base(base) => P::BaseMut::from((to, base)),
                _ => panic!(),
            }
        };
        for chunk in iter {
            match chunk {
                PageChunk::Delta(delta) => base_mut.apply_delta(delta).unwrap(),
                _ => panic!(),
            }
        }
        base_mut
    }
}
pub struct PageChunkIter<'p, P: PageLayout<'p> + ?Sized> {
    buf: &'p [u8],
    top: usize,
    bottom: usize,
    _p: PhantomData<P>,
}
impl<'p, P: PageLayout<'p> + ?Sized> From<&'p [u8]> for PageChunkIter<'p, P> {
    fn from(buf: &'p [u8]) -> Self {
        Self {
            buf,
            top: 0,
            bottom: buf.len(),
            _p: PhantomData,
        }
    }
}
impl<'p, P: PageLayout<'p>> PageChunkIter<'p, P> {
    pub fn reset_iter(&mut self) {
        self.top = 0;
        self.bottom = self.buf.len();
    }
}
impl<'p, P: PageLayout<'p> + ?Sized> Iterator for PageChunkIter<'p, P> {
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
impl<'p, P: PageLayout<'p> + ?Sized> DoubleEndedIterator for PageChunkIter<'p, P> {
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
pub enum PageChunk<'c, P: PageLayout<'c> + ?Sized> {
    Delta(P::Delta),
    Base(P::Base),
}
