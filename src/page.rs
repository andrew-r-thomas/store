use std::sync::{
    Arc,
    atomic::{AtomicPtr, Ordering},
};

/// this is the type that represents a logical page in the system,
/// it's essentially an atomic append only linked list
///
/// each node in the chain is responsible for it's memory and
/// the memory bellow it, this is simple for all node types other than deltas
pub struct Page<B: Base, D: Delta<B>> {
    ptr: AtomicPtr<PageNode<B, D>>,
}

impl<B: Base, D: Delta<B>> Page<B, D> {
    pub fn size(&self) -> usize {
        self.iter().map(|n| n.size()).sum()
    }

    pub fn iter(&self) -> PageIter<B, D> {
        PageIter {
            p: Some(unsafe { &*self.ptr.load(Ordering::SeqCst) }.clone()),
        }
    }
}

pub struct PageIter<B: Base, D: Delta<B> + ?Sized> {
    p: Option<PageNode<B, D>>,
}
impl<B, D> Iterator for PageIter<B, D>
where
    B: Base,
    D: Delta<B> + ?Sized,
{
    type Item = PageNode<B, D>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.p.is_none() {
            return None;
        }
        let out = self.p.as_ref().unwrap().clone();
        if let PageNode::Delta(d) = &out {
            self.p = d.next().cloned();
        } else {
        }
        Some(out)
    }
}

pub enum PageNode<B: Base, D: Delta<B> + ?Sized> {
    Base(Arc<B>),
    Delta(Arc<D>),
    Free(FreeNode),
    Disk(DiskNode),
}
impl<B: Base, D: Delta<B>> PageNode<B, D> {
    /// returns the size of this node in bytes
    pub fn size(&self) -> usize {
        match self {
            PageNode::Delta(d) => d.size(),
            PageNode::Base(b) => b.size(),
            _ => 0,
        }
    }
}
// why derive no work
impl<B, D> Clone for PageNode<B, D>
where
    B: Base,
    D: Delta<B> + ?Sized,
{
    fn clone(&self) -> Self {
        match self {
            PageNode::Base(b) => PageNode::Base(b.clone()),
            PageNode::Delta(d) => PageNode::Delta(d.clone()),
            PageNode::Free(f) => PageNode::Free(f.clone()),
            PageNode::Disk(d) => PageNode::Disk(d.clone()),
        }
    }
}

#[derive(Clone)]
pub struct FreeNode {}
#[derive(Clone)]
pub struct DiskNode {}

pub trait Delta<B: Base> {
    /// returns the number of bytes in just this node
    fn size(&self) -> usize;

    /// get the next page in the chain if it exists
    fn next(&self) -> Option<&PageNode<B, Self>>;
}
pub trait Base {
    /// returns the number of bytes in just this node
    fn size(&self) -> usize;
}
