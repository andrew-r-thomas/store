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

pub struct PageBuffer {
    buf: NonNull<[u8]>,
    read_ptr: AtomicUsize,
    write_ptr: AtomicUsize,
    cap: usize,
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
            read_ptr: AtomicUsize::new(capacity),
            write_ptr: AtomicUsize::new(0),
            cap: capacity,
        }
    }
    pub fn read(&self) -> Result<ReadGuard, ()> {
        if self.write_ptr.load(Ordering::SeqCst) >= self.cap {
            // buffer is sealed
            return Err(());
        }
        let read_ptr = self.read_ptr.load(Ordering::SeqCst);
        let read = &unsafe { self.buf.as_ref() }[read_ptr as usize..];
        Ok(ReadGuard { read, p: self })
    }
    pub fn reserve(&self, len: usize) -> Result<WriteGuard, ()> {
        let old_write_ptr = self.write_ptr.fetch_add(len, Ordering::SeqCst);
        if old_write_ptr + len > self.cap {
            // buffer is sealed
            return Err(());
        }
        let write_offset = self.cap - (old_write_ptr + len);
        let write =
            &mut unsafe { self.buf.as_ptr().as_mut().unwrap() }[write_offset..write_offset + len];
        Ok(WriteGuard {
            write,
            p: self,
            bottom: write_offset + len,
        })
    }
}

pub struct ReadGuard<'r> {
    p: &'r PageBuffer,
    pub read: &'r [u8],
}

pub struct WriteGuard<'w> {
    bottom: usize,
    p: &'w PageBuffer,
    pub write: &'w mut [u8],
}
impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        while let Err(_) = self.p.read_ptr.compare_exchange_weak(
            self.bottom,
            self.bottom - self.write.len(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {}
    }
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
