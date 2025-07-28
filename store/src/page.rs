use std::{alloc, slice};

use crate::{
    PageId,
    format::{self, Op},
};

pub struct PageBuffer {
    ptr: *mut u8,
    pub top: usize,
    pub cap: usize,
    pub flush: usize,
}
impl PageBuffer {
    pub fn new(cap: usize) -> Self {
        let layout = std::alloc::Layout::array::<u8>(cap).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout)
        }
        Self {
            ptr,
            cap,
            top: cap,
            flush: cap,
        }
    }

    pub fn read(&self) -> format::Page {
        format::Page::from(unsafe {
            slice::from_raw_parts(self.ptr.add(self.top), self.cap - self.top)
        })
    }
    pub fn write(&mut self, op: &format::PageOp) -> bool {
        let len = op.len();

        if len > self.top {
            return false;
        }

        op.write_to_buf(unsafe { slice::from_raw_parts_mut(self.ptr.add(self.top - len), len) });
        self.top -= len;

        true
    }

    #[inline]
    pub fn raw_buffer_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.cap) }
    }
    pub fn clear(&mut self) {
        self.raw_buffer_mut().fill(0);
        self.top = self.cap;
        self.flush = self.cap;
    }
    pub fn flush(&mut self) -> &[u8] {
        let out = &(unsafe { slice::from_raw_parts(self.ptr, self.cap) })[self.top..self.flush];
        self.flush = self.top;
        out
    }
}
impl Drop for PageBuffer {
    fn drop(&mut self) {
        unsafe { alloc::dealloc(self.ptr, alloc::Layout::array::<u8>(self.cap).unwrap()) }
    }
}

// TODO: this is gonna need another pass after messing with format
// PERF: im just gonna be sloppy and allocate/free all over the place here, for some reason this
// piece keeps tripping me up, so im just gonna make it easy for now. in general, this data is
// pretty ephemeral, and the space can definitely be reused, since it's basically just scratch
// space for doing compactions/splits/merges (eventually), it should be pretty straightforward to
// make this better once i get a better sense of how it'll be used, and it definietly doesn't need
// to be treated as a "page"
pub struct PageMut {
    pub entries: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    pub left_pid: PageId,
    pub right_pid: PageId,
    pub total_size: usize,
    pub page_size: usize,
}
impl PageMut {
    pub fn new(page_size: usize) -> Self {
        Self {
            entries: std::collections::BTreeMap::new(),
            left_pid: PageId(0),
            right_pid: PageId(0),
            total_size: format::Page::ENTRIES_LEN_SIZE,
            page_size,
        }
    }
    pub fn compact(&mut self, page: format::Page) {
        // build up the base page
        self.left_pid = page.left_pid;
        self.right_pid = page.right_pid;
        match page.is_inner() {
            true => {
                for split in page.entries.iter_inner() {
                    self.entries.insert(
                        Vec::from(split.middle_key.0),
                        Vec::from(&split.left_pid.0.to_be_bytes()),
                    );
                    self.total_size += format::LEN_SIZE + split.middle_key.len() + format::PID_SIZE;
                }
            }
            false => {
                for set in page.entries.iter_leaf() {
                    self.entries
                        .insert(Vec::from(set.key.0), Vec::from(set.val.0));
                    self.total_size += (2 * format::LEN_SIZE) + set.key.len() + set.val.len();
                }
            }
        }

        // apply the deltas
        let mut ops: Vec<format::PageOp> = page.ops.collect();
        while let Some(op) = ops.pop() {
            self.apply_op(op);
        }
    }
    pub fn apply_op(&mut self, op: format::PageOp) -> bool {
        match op {
            format::PageOp::Write(write) => match write {
                format::WriteOp::Set(set) => {
                    assert_ne!(self.left_pid.0, u64::MAX);
                    match self
                        .entries
                        .insert(Vec::from(set.key.0), Vec::from(set.val.0))
                    {
                        Some(old_val) => {
                            if old_val.len() < set.val.len()
                                && set.val.len() - old_val.len() > self.page_size - self.total_size
                            {
                                // not enough space within the logical page size
                                self.entries.insert(Vec::from(set.key.0), old_val);
                                return false;
                            }
                            self.total_size -= old_val.len();
                            self.total_size += set.val.len();
                        }
                        None => {
                            if (format::LEN_SIZE * 2) + set.key.len() + set.val.len()
                                > self.page_size - self.total_size
                            {
                                self.entries.remove(set.key.0);
                                return false;
                            }
                            self.total_size +=
                                (format::LEN_SIZE * 2) + set.key.len() + set.val.len()
                        }
                    }
                }
                format::WriteOp::Del(del) => {
                    assert_ne!(self.left_pid.0, u64::MAX);
                    let old = self.entries.remove(del.key.0).unwrap();
                    self.total_size -= (format::LEN_SIZE * 2) + del.key.len() + old.len();
                }
            },
            format::PageOp::SMO(smop) => match smop {
                format::SMOp::Split(split) => {
                    assert_eq!(self.left_pid.0, u64::MAX);
                    // splits are always inserts, not updates
                    if format::LEN_SIZE + split.middle_key.len() + format::PID_SIZE
                        > self.page_size - self.total_size
                    {
                        return false;
                    }
                    self.entries.insert(
                        Vec::from(split.middle_key.0),
                        Vec::from(&split.left_pid.0.to_be_bytes()),
                    );
                    self.total_size += format::LEN_SIZE + split.middle_key.len() + format::PID_SIZE;
                }
            },
        }

        true
    }
    pub fn split_inner(&mut self, middle_key_buf: &mut Vec<u8>) -> Self {
        assert_eq!(self.left_pid.0, u64::MAX);

        let mut other = Self::new(self.page_size);
        other.left_pid = self.left_pid;

        // this is so dumb
        let middle_entry_i = self.entries.len() / 2;
        let mut i = 0;
        while i < middle_entry_i {
            let entry = self.entries.pop_first().unwrap();
            self.total_size -= format::LEN_SIZE + entry.0.len() + format::PID_SIZE;
            other.total_size += format::LEN_SIZE + entry.0.len() + format::PID_SIZE;
            other.entries.insert(entry.0, entry.1);
            i += 1;
        }
        let middle_entry = self.entries.pop_first().unwrap();
        self.total_size -= format::LEN_SIZE + middle_entry.0.len() + format::PID_SIZE;
        middle_key_buf.extend(middle_entry.0);
        other.right_pid = crate::PageId(u64::from_be_bytes(middle_entry.1.try_into().unwrap()));

        other
    }
    pub fn split_leaf(&mut self, middle_key_buf: &mut Vec<u8>) -> Self {
        assert_ne!(self.left_pid.0, u64::MAX);

        let mut other = Self::new(self.page_size);
        other.left_pid = self.left_pid;

        let middle_entry_i = self.entries.len() / 2;
        let mut i = 0;
        while i < middle_entry_i {
            let entry = self.entries.pop_first().unwrap();
            self.total_size -= format::LEN_SIZE + entry.0.len() + format::LEN_SIZE + entry.1.len();
            other.total_size += format::LEN_SIZE + entry.0.len() + format::LEN_SIZE + entry.1.len();
            other.entries.insert(entry.0, entry.1);
            i += 1;
        }
        let middle_entry = self.entries.pop_first().unwrap();
        middle_key_buf.extend(&middle_entry.0);
        self.total_size -=
            format::LEN_SIZE + middle_entry.0.len() + format::LEN_SIZE + middle_entry.1.len();
        other.total_size +=
            format::LEN_SIZE + middle_entry.0.len() + format::LEN_SIZE + middle_entry.1.len();
        other.entries.insert(middle_entry.0, middle_entry.1);

        other
    }
    #[inline]
    pub fn set_right_pid(&mut self, page_id: PageId) {
        self.right_pid = page_id;
    }
    #[inline]
    pub fn set_left_pid(&mut self, page_id: PageId) {
        self.left_pid = page_id;
    }
    pub fn clear(&mut self) {
        self.entries.clear();
        self.left_pid.0 = 0;
        self.right_pid.0 = 0;
        self.total_size = format::Page::ENTRIES_LEN_SIZE;
    }
    pub fn pack(&mut self, page_buf: &mut PageBuffer) {
        todo!()
    }
}
