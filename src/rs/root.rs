use crate::{
    format::{Commit, Serialize, Write},
    zipper::Zipper,
};

use std::{
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

/// ## NOTE
/// - we use an empty key (&[]) to store the rightmost child
pub struct Root {
    children: SkipMap<Bytes, Child>,
    oldest_active_ts: Arc<AtomicU64>,
    total_size: AtomicUsize,
    zipper: Zipper,
    config: Config,
}
impl Root {
    pub fn get(&self, target: &[u8], ts: u64) -> GetRes {
        let entry = match self.children.lower_bound(Bound::Included(target)) {
            Some(entry) => entry,
            None => self.children.get(&Bytes::new()).unwrap(),
        };
        let mut out = GetRes::Pid(entry.value().pid);
        let mut ptr = entry.value().chain.load(Ordering::Acquire);
        drop(entry);

        while !ptr.is_null() {
            let head = unsafe { ptr.as_ref() }.unwrap();
            if head.commit.ts <= ts {
                match &head.commit.write {
                    Write::Put(put) => {
                        if put.key == target {
                            out = GetRes::Val(Some((&put.val[..]).into()));
                            break;
                        }
                    }
                    Write::Del(del) => {
                        if del.key == target {
                            out = GetRes::Val(None);
                            break;
                        }
                    }
                }
            }
            ptr = head.next.load(Ordering::Relaxed);
        }

        if !ptr.is_null() {
            let head = unsafe { ptr.as_ref() }.unwrap();
            if head.commit.ts < self.oldest_active_ts.load(Ordering::Acquire) {
                let old_tail = head.next.swap(std::ptr::null_mut(), Ordering::AcqRel);
                Self::free_chain(old_tail);
            }
        }

        out
    }

    /// writes `commit` to respective child chain using a weak cas loop
    pub fn write(&self, commit: Commit) {
        let entry = match self
            .children
            .lower_bound(Bound::Included(&commit.write.key()))
        {
            Some(entry) => entry,
            None => self.children.get(&Bytes::new()).unwrap(),
        };
        let mut head_ptr = entry.value().chain.load(Ordering::Acquire);

        let mut boxed = Box::into_raw(Box::new(CommitChain {
            commit,
            next: AtomicPtr::new(head_ptr),
        }));
        while let Err(new_head) = entry.value().chain.compare_exchange_weak(
            head_ptr,
            boxed,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            let mut b = unsafe { Box::from_raw(boxed) };
            b.next = AtomicPtr::new(new_head);
            boxed = Box::into_raw(b);
            head_ptr = new_head
        }
    }

    pub fn install(&self, ts: u64, writes: Vec<Write>) {
        let mut batch_size = 0;
        for write in writes {
            let commit = Commit { ts, write };
            batch_size += commit.size();
            self.write(commit);
        }
        if self.total_size.fetch_add(batch_size, Ordering::AcqRel) + batch_size
            > self.config.flush_threshold
        {
            todo!()
            // zipper.trigger_zip();
        }
    }

    fn free_chain(mut ptr: *mut CommitChain) {
        while !ptr.is_null() {
            let b = unsafe { Box::from_raw(ptr) };
            ptr = b.next.load(Ordering::Relaxed);
        }
    }
}

pub enum GetRes {
    Val(Option<Vec<u8>>),
    Pid(u64),
}

pub struct Child {
    pid: u64,
    chain: AtomicPtr<CommitChain>,
}
impl Drop for Child {
    fn drop(&mut self) {
        let chain = self.chain.load(Ordering::Relaxed);
        Root::free_chain(chain);
    }
}
pub struct CommitChain {
    commit: Commit,
    next: AtomicPtr<CommitChain>,
}

pub struct Config {
    flush_threshold: usize,
}
