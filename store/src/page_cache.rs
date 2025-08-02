use std::{
    alloc, collections,
    ops::{Index, IndexMut},
    ptr, slice,
};

use format::{Format, page};

use crate::io;

pub struct PageCache {
    pub buf_pool: Vec<PageBuffer>,
    pub free_list: Vec<usize>,
    pub id_map: collections::BTreeMap<format::PageId, usize>,
    pub hits: Vec<[u64; 2]>,
    pub hit: u64,
    pub free_cap_target: usize,
}
impl PageCache {
    pub fn new(page_size: usize, buf_pool_size: usize, free_cap_target: usize) -> Self {
        Self {
            buf_pool: Vec::from_iter((0..buf_pool_size).map(|_| PageBuffer::new(page_size))),
            free_list: Vec::from_iter(0..buf_pool_size),
            id_map: collections::BTreeMap::new(),
            hits: vec![[u64::MAX, 0]; buf_pool_size],
            hit: 0,
            free_cap_target,
        }
    }

    /// returns the index in the buffer pool for `pid`, if it exists.
    /// also records a "hit" for eviction priority tracking purposes.
    pub fn get(&mut self, pid: format::PageId) -> Option<usize> {
        match self.id_map.get(&pid) {
            Some(&idx) => {
                self.hits[idx][1] = self.hits[idx][0];
                self.hits[idx][0] = self.hit;
                self.hit += 1;
                Some(idx)
            }
            None => None,
        }
    }

    /// insert `pid` at `idx`.
    /// actual modification of the [`PageBuffer`] at `idx` is done separately.
    /// also records a "hit" for eviction priority tracking purposes.
    pub fn insert(&mut self, pid: format::PageId, idx: usize) {
        if let Some(_) = self.id_map.insert(pid, idx) {
            panic!()
        }
        self.hits[idx][0] = self.hit;
        self.hits[idx][1] = 0;
        self.hit += 1;
    }

    /// remove `pid` from cache.
    pub fn remove(&mut self, pid: format::PageId) {
        self.id_map.remove(&pid).unwrap();
    }

    /// pops a free slot in the buffer pool from the free list, panics if free list is empty
    pub fn pop_free(&mut self) -> usize {
        self.free_list.pop().unwrap()
    }

    /// run eviction if necessary
    pub fn tick<IO: io::IOFace>(&mut self, page_store: &mut io::PageStore, io: &mut IO) {
        let mut evict_cands = itertools::Itertools::sorted_by(
            self.id_map
                .iter()
                .map(|(pid, idx)| (self.hits[*idx][0] - self.hits[*idx][1], (*pid, *idx))),
            |a, b| a.0.cmp(&b.0),
        )
        .rev()
        .map(|(_, out)| out);
        while self.free_list.len() < self.free_cap_target {
            let (pid, idx) = evict_cands.next().unwrap();
            // for now we'll just evict the best candidates, but we may have some constraints
            // later from things like txns that prevent us from evicting certain pages
            let page = &mut self.buf_pool[idx];
            let total = page.cap - page.top;
            let chunk = page.flush();
            let base = chunk.len() == total;

            page_store.write_chunk(pid, chunk, io, base);

            // cleanup
            page.clear();
            self.free_list.push(idx);
            self.remove(pid);
        }
    }
}
impl Index<usize> for PageCache {
    type Output = PageBuffer;
    fn index(&self, index: usize) -> &Self::Output {
        self.buf_pool.index(index)
    }
}
impl IndexMut<usize> for PageCache {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.buf_pool.index_mut(index)
    }
}

pub struct PageBuffer {
    ptr: ptr::NonNull<u8>,
    pub top: usize,
    pub cap: usize,
    pub flush: usize,
}
impl PageBuffer {
    pub fn new(cap: usize) -> Self {
        let layout = alloc::Layout::array::<u8>(cap).unwrap();
        let ptr = match ptr::NonNull::new(unsafe { alloc::alloc_zeroed(layout) }) {
            Some(p) => p,
            None => alloc::handle_alloc_error(layout),
        };
        Self {
            ptr,
            cap,
            top: cap,
            flush: cap,
        }
    }

    pub fn read(&self) -> page::Page<'_> {
        page::Page::from(unsafe {
            slice::from_raw_parts(self.ptr.add(self.top).as_ptr(), self.cap - self.top)
        })
    }
    pub fn write(&mut self, op: &page::PageOp) -> bool {
        let len = op.len();

        if len > self.top {
            return false;
        }

        op.write_to_buf(unsafe {
            slice::from_raw_parts_mut(self.ptr.add(self.top - len).as_ptr(), len)
        });
        self.top -= len;

        true
    }

    #[inline]
    pub fn raw_buffer_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }
    pub fn clear(&mut self) {
        self.raw_buffer_mut().fill(0);
        self.top = self.cap;
        self.flush = self.cap;
    }
    pub fn flush(&mut self) -> &[u8] {
        let out =
            &(unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.cap) })[self.top..self.flush];
        self.flush = self.top;
        out
    }
}
impl Drop for PageBuffer {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(
                self.ptr.as_ptr(),
                alloc::Layout::array::<u8>(self.cap).unwrap(),
            )
        }
    }
}
