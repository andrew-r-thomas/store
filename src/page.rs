use core::alloc;
use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    io::Read,
    isize,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::page_table::PageId;

/// this is the type that represents a logical page in the system,
/// it's essentially an atomic append only linked list
///
/// each node in the chain is responsible for it's memory and
/// the memory bellow it, this is simple for all node types other than deltas
pub struct Page {
    /// this has a sneaky hidden arc inside of it
    pub ptr: AtomicPtr<PageNode>,
}

impl Page {
    pub fn new(node: Arc<PageNode>) -> Self {
        Self {
            ptr: AtomicPtr::new(Arc::into_raw(node).cast_mut()),
        }
    }
    pub fn read(&self) -> Arc<PageNode> {
        let ptr = self.ptr.load(Ordering::SeqCst);
        unsafe {
            Arc::increment_strong_count(ptr);
            Arc::from_raw(ptr)
        }
    }
    pub fn size(&self) -> usize {
        self.iter().map(|n| n.size()).sum()
    }

    pub fn iter(&self) -> PageIter {
        PageIter {
            p: Some(self.read()),
        }
    }
    pub fn update(
        &self,
        current: Arc<PageNode>,
        new: Arc<PageNode>,
    ) -> Result<Arc<PageNode>, Arc<PageNode>> {
        let current_ptr = Arc::into_raw(current).cast_mut();
        let new_ptr = Arc::into_raw(new).cast_mut();
        match self
            .ptr
            .compare_exchange(current_ptr, new_ptr, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(n) => Ok(unsafe { Arc::from_raw(n) }),
            Err(o) => Err(unsafe {
                Arc::increment_strong_count(o);
                Arc::from_raw(o)
            }),
        }
    }
}

pub struct PageIter {
    p: Option<Arc<PageNode>>,
}
impl Iterator for PageIter {
    type Item = Arc<PageNode>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.p.is_none() {
            return None;
        }
        let out = self.p.as_ref().unwrap().clone();
        self.p = out.next.clone();
        Some(out)
    }
}

pub struct PageNode {
    pub data: PageNodeInner,
    pub next: Option<Arc<PageNode>>,
}

#[derive(Debug)]
pub enum PageNodeInner {
    Base(BaseNode),
    Delta(DeltaNode),
    Free(FreeNode),
    Disk(DiskNode),
}
impl PageNode {
    /// returns the size of this node in bytes
    pub fn size(&self) -> usize {
        match &self.data {
            PageNodeInner::Delta(d) => d.data.len(),
            PageNodeInner::Base(b) => b.data.len(),
            _ => 0,
        }
    }
}

#[derive(Debug)]
pub struct FreeNode {}
#[derive(Debug)]
pub struct DiskNode {}
#[derive(Debug)]
pub struct BaseNode {
    pub data: Vec<u8>,
}
#[derive(Debug)]
pub struct DeltaNode {
    pub data: Vec<u8>,
}

pub enum Frame {
    Free(PageId),
    Disk(u64), // or something
    Mem(PageBuffer),
}

const MAX_CAP: usize = 1024 * 1024 * 16; // can't spawn a buffer that's larger than 16mb

pub struct PageBuffer {
    buf: NonNull<[u8]>,
    state: AtomicU64,
    header_size: usize,
}

impl PageBuffer {
    pub fn new(capacity: usize, header_size: usize) -> Self {
        let layout = Layout::array::<u8>(capacity + header_size).unwrap();
        assert!(capacity + header_size <= MAX_CAP);

        let ptr = match NonNull::new(unsafe { alloc_zeroed(layout) }) {
            Some(nn) => nn,
            None => handle_alloc_error(layout),
        };
        let buf = NonNull::slice_from_raw_parts(ptr, capacity + header_size);

        Self {
            buf,
            // gotta think about this ref count being 0
            state: AtomicU64::new(encode_state(
                0,
                (capacity + header_size) as u32,
                (capacity + header_size) as u32,
            )),
            header_size,
        }
    }
    pub fn read(&self) -> Result<ReadGuard, ()> {
        let state = self.state.load(Ordering::SeqCst);
        let (rc, read_ptr, write_ptr) = decode_state(state);
        if write_ptr == 0 {
            // the buffer is sealed
            return Err(());
        }
        match self.state.compare_exchange(
            state,
            encode_state(rc + 1, read_ptr, write_ptr),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                let read = &unsafe { self.buf.as_ref() }[read_ptr as usize..];
                Ok(ReadGuard { read, p: self })
            }
            Err(_) => Err(()),
        }
    }
    pub fn reserve(&self, len: usize) -> Result<WriteGuard, ()> {
        let state = self.state.load(Ordering::SeqCst);
        let (rc, read_ptr, write_ptr) = decode_state(state);
        // FIX: need to account for seal header in the sub
        match write_ptr.checked_sub(len as u32) {
            Some(new_write_ptr) => match self.state.compare_exchange(
                state,
                encode_state(rc + 1, read_ptr, new_write_ptr),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    let write = &mut unsafe { self.buf.as_ptr().as_mut().unwrap() }
                        [new_write_ptr as usize..write_ptr as usize];
                    Ok(WriteGuard {
                        p: self,
                        write,
                        bottom: write_ptr,
                    })
                }
                Err(_) => Err(()),
            },
            None => todo!("seal the buffer"),
        }
    }
}

pub struct ReadGuard<'r> {
    p: &'r PageBuffer,
    pub read: &'r [u8],
}
impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.p.state.load(Ordering::SeqCst);
        loop {
            let (rc, write_ptr, read_ptr) = decode_state(state);
            match self.p.state.compare_exchange_weak(
                state,
                encode_state(rc - 1, read_ptr, write_ptr),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if ((rc - 1) == 0) && (write_ptr == 0) {
                        todo!("do a compaction or whatever")
                    }
                    break;
                }
                Err(current_state) => state = current_state,
            }
        }
    }
}

pub struct WriteGuard<'w> {
    bottom: u32,
    p: &'w PageBuffer,
    pub write: &'w mut [u8],
}
impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.p.state.load(Ordering::SeqCst);
        loop {
            let (rc, _, write_ptr) = decode_state(state);
            match self.p.state.compare_exchange_weak(
                encode_state(rc, self.bottom, write_ptr),
                encode_state(rc - 1, self.bottom - self.write.len() as u32, write_ptr),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if ((rc - 1) == 0) && (write_ptr == 0) {
                        todo!("need to do some compaction")
                    }
                    break;
                }
                Err(current_state) => state = current_state,
            }
        }
    }
}

#[inline]
fn decode_state(state: u64) -> (u16, u32, u32) {
    (
        (state >> 48) as u16,
        ((state >> 24) & 0xFFFFFF) as u32,
        (state & 0xFFFFFF) as u32,
    )
}
#[inline]
fn encode_state(rc: u16, read_ptr: u32, write_ptr: u32) -> u64 {
    ((rc as u64) << 48) | (((read_ptr & 0xFFFFFF) as u64) << 24) | ((write_ptr & 0xFFFFFF) as u64)
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn scratch() {
        let page_buff = PageBuffer::new(1023, 1);
        let mut rng = rand::rng();
        for i in 1..12 {
            if rng.random_bool(0.5) {
                let write = page_buff.reserve(i).unwrap();
                write.write.fill(i as u8);
            } else {
                let read = page_buff.read().unwrap();
                println!("current state as of {i}:\n{:?}", read.read);
            }
        }

        let read = page_buff.read().unwrap();
        println!("final state:\n{:?}", read.read);
    }
}
