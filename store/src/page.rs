use std::{alloc, collections, slice};

use format::{self, Format, op, page};

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

    pub fn read(&self) -> page::Page<'_> {
        page::Page::from(unsafe {
            slice::from_raw_parts(self.ptr.add(self.top), self.cap - self.top)
        })
    }
    pub fn write(&mut self, op: &page::PageOp) -> bool {
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
    pub pid: format::PageId,
    pub ops: Vec<u8>,
    pub entries: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    pub left_pid: format::PageId,
    pub right_pid: format::PageId,

    pub total_compacted_size: usize,
    pub page_size: usize,
}
impl PageMut {
    pub fn new(page_size: usize, pid: format::PageId) -> Self {
        Self {
            pid,
            ops: Vec::new(),
            entries: std::collections::BTreeMap::new(),
            left_pid: format::PageId(0),
            right_pid: format::PageId(0),
            total_compacted_size: page::Page::ENTRIES_LEN_SIZE + (format::PageId::SIZE * 2),
            page_size,
        }
    }
    pub fn compact(&mut self, mut page: page::Page, oldest_active_ts: format::Timestamp) {
        assert!(self.ops.len() == 0);
        assert!(self.entries.len() == 0);
        // build up the base page
        self.left_pid = page.left_pid;
        self.right_pid = page.right_pid;
        match page.is_inner() {
            true => {
                for entry in page.entries.iter_inner() {
                    self.entries.insert(
                        Vec::from(entry.middle_key),
                        Vec::from(&entry.left_pid.0.to_be_bytes()),
                    );
                    self.total_compacted_size += entry.len();
                }

                // inner pages don't have timestamps, so we just apply all the ops to the entries
                let mut ops = page.ops.collect::<Vec<_>>();
                while let Some(op) = ops.pop() {
                    self.apply_op(op);
                }
            }
            false => {
                for entry in page.entries.iter_leaf() {
                    self.entries
                        .insert(Vec::from(entry.key), Vec::from(entry.val));
                    self.total_compacted_size += entry.len();
                }

                // add anything newer than oldest active to self.ops
                let mut last = None;
                while let Some(op) = page.ops.next() {
                    match op {
                        page::PageOp::Write(page_write) => {
                            if page_write.ts < oldest_active_ts {
                                last = Some(op);
                                break;
                            }
                            let old_len = self.ops.len();
                            self.ops.resize(old_len + op.len(), 0);
                            op.write_to_buf(&mut self.ops[old_len..]);
                        }
                        _ => panic!(),
                    }
                }

                // apply ops older than oldest active to entries (actual compaction)
                let mut remaining = page.ops.collect::<Vec<_>>();
                while let Some(op) = remaining.pop() {
                    self.apply_op(op);
                }
                if let Some(op) = last {
                    self.apply_op(op);
                }
            }
        }
    }
    pub fn write_op(&mut self, op: page::PageOp) -> Result<(), ()> {
        if op.len() + self.total_compacted_size + self.ops.len() > self.page_size {
            return Err(());
        }

        match op {
            page::PageOp::Write(page_write) => {
                let old_len = self.ops.len();
                self.ops.resize(old_len + page_write.len(), 0);
                self.ops.copy_within(0..old_len, page_write.len());
                page_write.write_to_buf(&mut self.ops[0..page_write.len()]);
            }
            page::PageOp::SMO(_) => self.apply_op(op),
        }

        Ok(())
    }
    fn apply_op(&mut self, op: page::PageOp) {
        match op {
            page::PageOp::Write(write) => match write.write {
                op::WriteOp::Set(set) => {
                    assert_ne!(self.left_pid.0, u64::MAX);
                    match self.entries.insert(Vec::from(set.key), Vec::from(set.val)) {
                        Some(old_val) => {
                            self.total_compacted_size -= old_val.len();
                            self.total_compacted_size += set.val.len();
                        }
                        None => {
                            let entry = page::LeafEntry {
                                key: set.key,
                                val: set.val,
                            };
                            self.total_compacted_size += entry.len();
                        }
                    }
                }
                op::WriteOp::Del(del) => {
                    assert_ne!(self.left_pid.0, u64::MAX);
                    match self.entries.remove(del.key) {
                        Some(old) => {
                            let entry = page::LeafEntry {
                                key: del.key,
                                val: &old,
                            };
                            self.total_compacted_size -= entry.len();
                        }
                        None => panic!(
                            "tried to delete non existent key: {:?} in page {}",
                            del.key, self.pid.0
                        ),
                    }
                }
            },
            page::PageOp::SMO(smop) => match smop {
                page::SMOp::Split(split) => {
                    assert_eq!(self.left_pid.0, u64::MAX);
                    // splits are always inserts, not updates
                    let entry = page::InnerEntry {
                        left_pid: split.left_pid,
                        middle_key: split.middle_key,
                    };
                    self.entries.insert(
                        Vec::from(split.middle_key),
                        Vec::from(&split.left_pid.0.to_be_bytes()),
                    );
                    self.total_compacted_size += entry.len();
                }
            },
        }
    }
    pub fn split_inner(&mut self, middle_key_buf: &mut Vec<u8>, to_pid: format::PageId) -> Self {
        assert_eq!(self.left_pid.0, u64::MAX);
        assert_eq!(self.ops.len(), 0);

        let mut other = Self::new(self.page_size, to_pid);
        other.left_pid = self.left_pid;

        // this is so dumb
        let middle_entry_i = self.entries.len() / 2;
        let mut i = 0;
        while i < middle_entry_i {
            let (k, v) = self.entries.pop_first().unwrap();
            let entry = page::InnerEntry {
                left_pid: format::PageId(u64::from_be_bytes(v[..].try_into().unwrap())),
                middle_key: &k,
            };
            self.total_compacted_size -= entry.len();
            other.total_compacted_size += entry.len();
            other.entries.insert(k, v);
            i += 1;
        }
        let (k, v) = self.entries.pop_first().unwrap();
        let middle_entry = page::InnerEntry {
            left_pid: format::PageId(u64::from_be_bytes(v[..].try_into().unwrap())),
            middle_key: &k,
        };
        self.total_compacted_size -= middle_entry.len();
        middle_key_buf.extend(middle_entry.middle_key);
        other.right_pid = middle_entry.left_pid;

        other
    }
    pub fn split_leaf(&mut self, middle_key_buf: &mut Vec<u8>, to_pid: format::PageId) -> Self {
        assert_ne!(self.left_pid.0, u64::MAX);

        let mut other = Self::new(self.page_size, to_pid);
        other.left_pid = self.left_pid;

        // first we find the middle key
        let mut keys = collections::BTreeSet::new();
        let op_iter = format::FormatIter::<page::PageWrite>::from(&self.ops[..]);
        for op in op_iter {
            keys.insert(op.key().to_vec());
        }
        for (key, _) in &self.entries {
            keys.insert(key.clone());
        }

        let middle_key_i = keys.len() / 2;
        while keys.len() > middle_key_i + 1 {
            keys.pop_first().unwrap();
        }
        let middle_key = keys.pop_first().unwrap();
        middle_key_buf.extend(&middle_key);

        while let Some((k, v)) = self.entries.pop_first() {
            if k > middle_key {
                self.entries.insert(k, v);
                break;
            }
            let entry = page::LeafEntry { key: &k, val: &v };
            self.total_compacted_size -= entry.len();
            other.total_compacted_size += entry.len();
            other.entries.insert(k, v);
        }

        let mut new_self_ops = Vec::new();
        for op in format::FormatIter::<'_, page::PageWrite<'_>>::from(&self.ops[..]) {
            if op.key() <= &middle_key[..] {
                let old_len = other.ops.len();
                other.ops.resize(old_len + op.len(), 0);
                op.write_to_buf(&mut other.ops[old_len..old_len + op.len()]);
            } else {
                let old_len = new_self_ops.len();
                new_self_ops.resize(old_len + op.len(), 0);
                op.write_to_buf(&mut new_self_ops[old_len..old_len + op.len()]);
            }
        }

        self.ops = new_self_ops;

        other
    }
    #[inline]
    pub fn set_right_pid(&mut self, page_id: format::PageId) {
        self.right_pid = page_id;
    }
    #[inline]
    pub fn set_left_pid(&mut self, page_id: format::PageId) {
        self.left_pid = page_id;
    }
    pub fn clear(&mut self) {
        self.entries.clear();
        self.ops.clear();
        self.left_pid.0 = 0;
        self.right_pid.0 = 0;
        self.total_compacted_size = page::Page::ENTRIES_LEN_SIZE + (format::PageId::SIZE * 2);
    }
    pub fn pack(&mut self, page_buf: &mut PageBuffer) {
        assert_eq!(page_buf.cap, self.page_size);

        page_buf.clear();
        let top = page_buf.cap - (self.total_compacted_size + self.ops.len());
        page_buf.top = top;
        let buf = &mut page_buf.raw_buffer_mut()[top..];

        let mut cursor = 0;
        buf[cursor..cursor + self.ops.len()].copy_from_slice(&self.ops);
        cursor += self.ops.len();

        let entries_start = cursor;
        for (k, v) in self.entries.iter() {
            if self.left_pid.0 == u64::MAX {
                let entry = page::InnerEntry {
                    left_pid: format::PageId::from_bytes(&v).unwrap(),
                    middle_key: &k,
                };
                entry.write_to_buf(&mut buf[cursor..cursor + entry.len()]);
                cursor += entry.len();
            } else {
                let entry = page::LeafEntry { key: &k, val: &v };
                entry.write_to_buf(&mut buf[cursor..cursor + entry.len()]);
                cursor += entry.len();
            }
        }

        buf[cursor..cursor + page::Page::ENTRIES_LEN_SIZE]
            .copy_from_slice(&((cursor - entries_start) as u64).to_be_bytes());
        cursor += page::Page::ENTRIES_LEN_SIZE;
        self.left_pid
            .write_to_buf(&mut buf[cursor..cursor + self.left_pid.len()]);
        cursor += self.left_pid.len();
        self.right_pid
            .write_to_buf(&mut buf[cursor..cursor + self.right_pid.len()]);
        cursor += self.right_pid.len();

        assert_eq!(cursor, buf.len());
    }
}
