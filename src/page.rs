use core::alloc;
use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    io::Read,
    isize,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
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
