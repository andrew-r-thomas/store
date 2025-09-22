use std::sync::{
    Arc,
    atomic::{AtomicPtr, AtomicU64, Ordering},
};

use crate::format::{Commit, Format, Write};

pub struct Root {
    keys: Vec<Vec<u8>>,
    pids: Vec<u64>,
    chains: Vec<AtomicPtr<CommitChain>>,

    oldest_active_ts: Arc<AtomicU64>,
}
impl Root {
    pub fn get(&self, target: &[u8], ts: u64) -> GetRes {
        let idx = self.search(target);
        let pid = self.pids[idx];

        let chain = &self.chains[idx];
        let mut out = GetRes::Pid(pid);
        let mut ptr = chain.load(Ordering::Acquire);
        while !ptr.is_null() {
            let head = unsafe { ptr.as_ref() }.unwrap();
            let commit = Commit::from_bytes(&head.commit);
            if commit.ts <= ts {
                match &commit.write {
                    Write::Put(put) => {
                        if put.key == target {
                            out = GetRes::Val(Some(put.val.into()));
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
            let commit = Commit::from_bytes(&head.commit);
            if commit.ts < self.oldest_active_ts.load(Ordering::Acquire) {
                let old_tail = head.next.swap(std::ptr::null_mut(), Ordering::AcqRel);
                Self::free_chain(old_tail);
            }
        }

        out
    }

    /// writes `commit` to respective child chain using a weak cas loop
    pub fn write(&self, commit: Commit) {
        let idx = self.search(commit.write.key());
        let head = &self.chains[idx];
        let mut head_ptr = head.load(Ordering::Acquire);
        let mut boxed = Box::into_raw(Box::new(CommitChain {
            commit: commit.to_vec(),
            next: AtomicPtr::new(head_ptr),
        }));
        while let Err(new_head) =
            head.compare_exchange_weak(head_ptr, boxed, Ordering::AcqRel, Ordering::Relaxed)
        {
            let mut b = unsafe { Box::from_raw(boxed) };
            b.next = AtomicPtr::new(new_head);
            boxed = Box::into_raw(b);
            head_ptr = new_head
        }
    }

    /// makes one attempt to write with a strong cas
    pub fn try_write(&self, commit: Commit) -> Result<(), ()> {
        let idx = self.search(commit.write.key());
        let head = &self.chains[idx];
        let head_ptr = head.load(Ordering::Acquire);
        let boxed = Box::into_raw(Box::new(CommitChain {
            commit: commit.to_vec(),
            next: AtomicPtr::new(head_ptr),
        }));
        match head.compare_exchange(head_ptr, boxed, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => Ok(()),
            Err(_) => {
                let _ = unsafe { Box::from_raw(boxed) };
                Err(())
            }
        }
    }

    fn search(&self, target: &[u8]) -> usize {
        let mut left = 0;
        let mut right = self.keys.len() - 1;
        while left <= right {
            let middle = left + ((right - left) / 2);
            let key = self.keys[middle].as_slice();
            if target < key {
                right = middle - 1
            } else if target == key {
                return middle;
            } else {
                left = middle + 1
            }
        }

        return left;
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

pub struct CommitChain {
    commit: Vec<u8>,
    next: AtomicPtr<CommitChain>,
}
