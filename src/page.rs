use std::{alloc, mem, slice};

use crate::PageId;

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

    pub fn read(&self) -> Page {
        Page::from(unsafe { slice::from_raw_parts(self.ptr.add(self.top), self.cap - self.top) })
    }
    pub fn write_delta(&mut self, delta: &Delta) -> bool {
        let len = delta.len();

        if len > self.top {
            return false;
        }

        delta.write_to_buf(unsafe { slice::from_raw_parts_mut(self.ptr.add(self.top - len), len) });
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
            left_pid: 0,
            right_pid: 0,
            total_size: BasePage::ENTRIES_START + BASE_LEN_SIZE,
            page_size,
        }
    }
    pub fn compact(&mut self, page: Page) {
        // build up the base page
        self.left_pid = page.base.left_pid;
        self.right_pid = page.base.right_pid;
        match page.is_inner() {
            true => {
                for (key, pid) in page.base.iter_entries_inner() {
                    self.entries
                        .insert(Vec::from(key), Vec::from(&pid.to_be_bytes()));
                    self.total_size += LEN_SIZE + key.len() + PID_SIZE;
                }
            }
            false => {
                for (key, val) in page.base.iter_entries_leaf() {
                    self.entries.insert(Vec::from(key), Vec::from(val));
                    self.total_size += (2 * LEN_SIZE) + key.len() + val.len();
                }
            }
        }

        // apply the deltas
        for delta in page.deltas.rev() {
            self.apply_delta(&delta);
        }
    }
    pub fn apply_delta(&mut self, delta: &Delta) -> bool {
        match delta {
            Delta::Set(set_delta) => {
                assert_ne!(self.left_pid, u64::MAX);
                match self
                    .entries
                    .insert(Vec::from(set_delta.key), Vec::from(set_delta.val))
                {
                    Some(old_val) => {
                        if old_val.len() < set_delta.val.len()
                            && set_delta.val.len() - old_val.len()
                                > self.page_size - self.total_size
                        {
                            // not enough space within the logical page size
                            self.entries.insert(Vec::from(set_delta.key), old_val);
                            return false;
                        }
                        self.total_size -= old_val.len();
                        self.total_size += set_delta.val.len();
                    }
                    None => {
                        if (LEN_SIZE * 2) + set_delta.key.len() + set_delta.val.len()
                            > self.page_size - self.total_size
                        {
                            self.entries.remove(set_delta.key);
                            return false;
                        }
                        self.total_size +=
                            (LEN_SIZE * 2) + set_delta.key.len() + set_delta.val.len()
                    }
                }
            }
            Delta::Split(split_delta) => {
                assert_eq!(self.left_pid, u64::MAX);
                // splits are always inserts, not updates
                if LEN_SIZE + split_delta.middle_key.len() + PID_SIZE
                    > self.page_size - self.total_size
                {
                    return false;
                }
                self.entries.insert(
                    Vec::from(split_delta.middle_key),
                    Vec::from(&split_delta.left_pid.to_be_bytes()),
                );
                self.total_size += LEN_SIZE + split_delta.middle_key.len() + PID_SIZE;
            }
            Delta::Del(del_delta) => {
                assert_ne!(self.left_pid, u64::MAX);
                let old = self.entries.remove(del_delta.key).unwrap();
                self.total_size -= (LEN_SIZE * 2) + del_delta.key.len() + old.len();
            }
        }

        true
    }
    pub fn split_inner(&mut self, middle_key_buf: &mut Vec<u8>) -> Self {
        assert_eq!(self.left_pid, u64::MAX);

        let mut other = Self::new(self.page_size);
        other.left_pid = self.left_pid;

        // this is so dumb
        let middle_entry_i = self.entries.len() / 2;
        let mut i = 0;
        while i < middle_entry_i {
            let entry = self.entries.pop_first().unwrap();
            self.total_size -= LEN_SIZE + entry.0.len() + PID_SIZE;
            other.total_size += LEN_SIZE + entry.0.len() + PID_SIZE;
            other.entries.insert(entry.0, entry.1);
            i += 1;
        }
        let middle_entry = self.entries.pop_first().unwrap();
        self.total_size -= LEN_SIZE + middle_entry.0.len() + PID_SIZE;
        middle_key_buf.extend(middle_entry.0);
        other.right_pid = u64::from_be_bytes(middle_entry.1.try_into().unwrap());

        other
    }
    pub fn split_leaf(&mut self, middle_key_buf: &mut Vec<u8>) -> Self {
        assert_ne!(self.left_pid, u64::MAX);

        let mut other = Self::new(self.page_size);
        other.left_pid = self.left_pid;
        // other.right_pid = self.right_pid;

        let middle_entry_i = self.entries.len() / 2;
        let mut i = 0;
        while i < middle_entry_i {
            let entry = self.entries.pop_first().unwrap();
            self.total_size -= LEN_SIZE + entry.0.len() + LEN_SIZE + entry.1.len();
            other.total_size += LEN_SIZE + entry.0.len() + LEN_SIZE + entry.1.len();
            other.entries.insert(entry.0, entry.1);
            i += 1;
        }
        let middle_entry = self.entries.pop_first().unwrap();
        middle_key_buf.extend(&middle_entry.0);
        self.total_size -= LEN_SIZE + middle_entry.0.len() + LEN_SIZE + middle_entry.1.len();
        other.total_size += LEN_SIZE + middle_entry.0.len() + LEN_SIZE + middle_entry.1.len();
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
        self.left_pid = 0;
        self.right_pid = 0;
        self.total_size = BasePage::ENTRIES_START + BASE_LEN_SIZE;
    }
    pub fn pack(&mut self, page_buf: &mut PageBuffer) {
        assert_eq!(page_buf.cap, self.page_size);

        page_buf.clear();
        let top = page_buf.cap - self.total_size;
        page_buf.top = top;
        let buf = &mut page_buf.raw_buffer_mut()[top..];

        buf[BasePage::NUM_ENTRIES_RANGE]
            .copy_from_slice(&(self.entries.len() as u16).to_be_bytes());
        buf[BasePage::LEFT_PID_RANGE].copy_from_slice(&self.left_pid.to_be_bytes());
        buf[BasePage::RIGHT_PID_RANGE].copy_from_slice(&self.right_pid.to_be_bytes());

        let mut cursor = BasePage::ENTRIES_START;
        for entry in self.entries.iter() {
            buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(entry.0.len() as u32).to_be_bytes());
            cursor += LEN_SIZE;
            buf[cursor..cursor + entry.0.len()].copy_from_slice(entry.0);
            cursor += entry.0.len();

            if self.left_pid == u64::MAX {
                buf[cursor..cursor + PID_SIZE].copy_from_slice(entry.1);
                cursor += PID_SIZE;
            } else {
                buf[cursor..cursor + LEN_SIZE]
                    .copy_from_slice(&(entry.1.len() as u32).to_be_bytes());
                cursor += LEN_SIZE;
                buf[cursor..cursor + entry.1.len()].copy_from_slice(entry.1);
                cursor += entry.1.len();
            }
        }

        let buf_len = buf.len();
        buf[buf_len - BASE_LEN_SIZE..]
            .copy_from_slice(&((buf_len - BASE_LEN_SIZE) as u64).to_be_bytes());
    }
}
