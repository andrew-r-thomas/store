use std::{
    alloc::{self, Layout},
    collections::BTreeMap,
    ops::Range,
    slice,
};

use crate::PageId;

pub struct PageBuffer {
    ptr: *mut u8,
    top: usize,
    cap: usize,
}
impl PageBuffer {
    pub fn new(cap: usize) -> Self {
        let layout = Layout::array::<u8>(cap).unwrap();
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout)
        }
        Self { ptr, cap, top: cap }
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
    #[inline]
    pub fn raw_buffer(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.cap) }
    }
}

const CODE_SIZE: usize = 1;
const LEN_SIZE: usize = 4;
const PID_SIZE: usize = 8;
const BASE_LEN_SIZE: usize = 8;

pub struct Page<'p> {
    pub deltas: DeltaIter<'p>,
    pub base: BasePage<'p>,
}
impl Page<'_> {
    #[inline]
    pub fn is_inner(&self) -> bool {
        self.base.is_inner()
    }

    pub fn search_inner(&mut self, target: &[u8]) -> PageId {
        let mut best = None;
        while let Some(delta) = self.deltas.next() {
            match delta {
                Delta::Split(split_delta) => {
                    if target <= split_delta.middle_key {
                        match best {
                            Some((key, _)) => {
                                if split_delta.middle_key < key {
                                    best = Some((split_delta.middle_key, split_delta.left_pid));
                                }
                            }
                            None => best = Some((split_delta.middle_key, split_delta.left_pid)),
                        }
                    }
                }
                _ => panic!(),
            }
        }

        match (self.base.search_inner(target), best) {
            (Some((base_key, base_pid)), Some((best_key, best_pid))) => {
                if base_key < best_key {
                    base_pid
                } else {
                    best_pid
                }
            }
            (Some((_, pid)), None) => pid,
            (None, Some((_, pid))) => pid,
            (None, None) => self.base.right_pid,
        }
    }
    pub fn search_leaf(&mut self, target: &[u8]) -> Option<&[u8]> {
        while let Some(delta) = self.deltas.next() {
            match delta {
                Delta::Set(set_delta) => {
                    if set_delta.key == target {
                        return Some(set_delta.val);
                    }
                }
                _ => panic!(),
            }
        }

        self.base.search_leaf(target)
    }
}
impl<'p> From<&'p [u8]> for Page<'p> {
    fn from(buf: &'p [u8]) -> Self {
        let base_len =
            u64::from_be_bytes(buf[buf.len() - BASE_LEN_SIZE..].try_into().unwrap()) as usize;

        let deltas = DeltaIter::from(&buf[..buf.len() - (BASE_LEN_SIZE + base_len)]);
        let base =
            BasePage::from(&buf[buf.len() - (BASE_LEN_SIZE + base_len)..buf.len() - BASE_LEN_SIZE]);

        Self { deltas, base }
    }
}

pub struct DeltaIter<'i> {
    pub buf: &'i [u8],
    pub top: usize,
    pub bottom: usize,
}
impl<'i> From<&'i [u8]> for DeltaIter<'i> {
    fn from(buf: &'i [u8]) -> Self {
        Self {
            buf,
            top: 0,
            bottom: buf.len(),
        }
    }
}
impl<'i> Iterator for DeltaIter<'i> {
    type Item = Delta<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }

        let delta = Delta::from_top(&self.buf[self.top..]);
        self.top += delta.len();

        Some(delta)
    }
}
impl<'i> DoubleEndedIterator for DeltaIter<'i> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }

        let delta = Delta::from_bottom(&self.buf[..self.bottom]);
        self.bottom -= delta.len();

        Some(delta)
    }
}

// TODO: want to think about the trade offs between having this be an enum vs a trait, they're
// pretty much always going to be stack allocated things, so having a vtable seems really dumb, but
// also having it be an enum might make us have to branch a lot more than we need to (could maybe
// have a top level enum that contains things that implement a trait, idk, play around with it)
pub enum Delta<'d> {
    Set(SetDelta<'d>),
    Split(SplitDelta<'d>),
}
#[derive(Copy, Clone)]
pub struct SetDelta<'d> {
    pub key: &'d [u8],
    pub val: &'d [u8],
}
#[derive(Copy, Clone)]
pub struct SplitDelta<'d> {
    pub middle_key: &'d [u8],
    pub left_pid: PageId,
}
impl<'d> Delta<'d> {
    const SET_CODE: u8 = 1;
    const SPLIT_CODE: u8 = 2;

    pub fn from_top(buf: &'d [u8]) -> Self {
        match *buf.first().unwrap() {
            Self::SET_CODE => {
                let key_len_start = CODE_SIZE;
                let key_len_end = key_len_start + LEN_SIZE;
                let key_len =
                    u32::from_be_bytes(buf[key_len_start..key_len_end].try_into().unwrap())
                        as usize;

                let val_len = u32::from_be_bytes(
                    buf[key_len_end..key_len_end + LEN_SIZE].try_into().unwrap(),
                ) as usize;

                let key_start = CODE_SIZE + (LEN_SIZE * 2);
                let key_end = key_start + key_len;
                let key = &buf[key_start..key_end];

                let val = &buf[key_end..key_end + val_len];

                Self::Set(SetDelta { key, val })
            }
            Self::SPLIT_CODE => {
                let key_len_start = CODE_SIZE;
                let key_len_end = key_len_start + LEN_SIZE;
                let key_len =
                    u32::from_be_bytes(buf[key_len_start..key_len_end].try_into().unwrap())
                        as usize;

                let left_pid = u64::from_be_bytes(
                    buf[key_len_end..key_len_end + PID_SIZE].try_into().unwrap(),
                );

                let key_start = key_len_end + PID_SIZE;
                let key_end = key_start + key_len;
                let middle_key = &buf[key_start..key_end];

                Self::Split(SplitDelta {
                    middle_key,
                    left_pid,
                })
            }
            _ => panic!(),
        }
    }
    pub fn from_bottom(buf: &'d [u8]) -> Self {
        match *buf.last().unwrap() {
            Self::SET_CODE => {
                let val_len_end = buf.len() - CODE_SIZE;
                let val_len_start = val_len_end - LEN_SIZE;
                let val_len =
                    u32::from_be_bytes(buf[val_len_start..val_len_end].try_into().unwrap())
                        as usize;

                let key_len = u32::from_be_bytes(
                    buf[val_len_start - LEN_SIZE..val_len_start]
                        .try_into()
                        .unwrap(),
                ) as usize;

                let val_end = val_len_start - LEN_SIZE;
                let val_start = val_end - val_len;
                let val = &buf[val_start..val_end];

                let key = &buf[val_start - key_len..val_start];

                Self::Set(SetDelta { key, val })
            }
            Self::SPLIT_CODE => {
                let key_len_end = buf.len() - CODE_SIZE;
                let key_len_start = key_len_end - LEN_SIZE;
                let key_len =
                    u32::from_be_bytes(buf[key_len_start..key_len_end].try_into().unwrap())
                        as usize;

                let key_start = key_len_start - key_len;
                let middle_key = &buf[key_start..key_len_start];

                let left_pid =
                    u64::from_be_bytes(buf[key_start - PID_SIZE..key_start].try_into().unwrap());

                Self::Split(SplitDelta {
                    middle_key,
                    left_pid,
                })
            }
            _ => panic!(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Set(set_delta) => {
                CODE_SIZE
                    + (LEN_SIZE * 2)
                    + set_delta.key.len()
                    + set_delta.val.len()
                    + (LEN_SIZE * 2)
                    + CODE_SIZE
            }
            Self::Split(split_delta) => {
                CODE_SIZE
                    + LEN_SIZE
                    + PID_SIZE
                    + split_delta.middle_key.len()
                    + LEN_SIZE
                    + CODE_SIZE
            }
        }
    }

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        let mut cursor = 0;
        match self {
            Self::Set(set_delta) => {
                let key_len = &(set_delta.key.len() as u32).to_be_bytes();
                let val_len = &(set_delta.val.len() as u32).to_be_bytes();

                buf[cursor] = Self::SET_CODE;
                cursor += CODE_SIZE;

                buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
                cursor += LEN_SIZE;

                buf[cursor..cursor + LEN_SIZE].copy_from_slice(val_len);
                cursor += LEN_SIZE;

                buf[cursor..cursor + set_delta.key.len()].copy_from_slice(set_delta.key);
                cursor += set_delta.key.len();

                buf[cursor..cursor + set_delta.val.len()].copy_from_slice(set_delta.val);
                cursor += set_delta.val.len();

                buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
                cursor += LEN_SIZE;

                buf[cursor..cursor + LEN_SIZE].copy_from_slice(val_len);
                cursor += LEN_SIZE;

                buf[cursor] = Self::SET_CODE;
            }
            Self::Split(split_delta) => {
                let key_len = &(split_delta.middle_key.len() as u32).to_be_bytes();

                buf[cursor] = Self::SPLIT_CODE;
                cursor += CODE_SIZE;

                buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
                cursor += LEN_SIZE;

                buf[cursor..cursor + PID_SIZE].copy_from_slice(&split_delta.left_pid.to_be_bytes());
                cursor += PID_SIZE;

                buf[cursor..cursor + split_delta.middle_key.len()]
                    .copy_from_slice(split_delta.middle_key);
                cursor += split_delta.middle_key.len();

                buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
                cursor += LEN_SIZE;

                buf[cursor] = Self::SPLIT_CODE;
            }
        }
    }
}

pub struct BasePage<'b> {
    pub num_entries: usize,
    pub left_pid: PageId,
    pub right_pid: PageId,
    pub entries: &'b [u8],
}
impl BasePage<'_> {
    pub const NUM_ENTRIES_RANGE: Range<usize> = 0..2;
    pub const LEFT_PID_RANGE: Range<usize> = 2..10;
    pub const RIGHT_PID_RANGE: Range<usize> = 10..18;
    pub const ENTRIES_START: usize = 18;

    pub fn search_inner(&self, target: &[u8]) -> Option<(&[u8], PageId)> {
        for entry in self.iter_entries_inner() {
            if target <= entry.0 {
                return Some(entry);
            }
        }
        None
    }
    pub fn search_leaf(&self, target: &[u8]) -> Option<&[u8]> {
        for entry in self.iter_entries_leaf() {
            if target == entry.0 {
                return Some(entry.1);
            }
        }
        None
    }

    pub fn iter_entries_inner(&self) -> impl Iterator<Item = (&[u8], PageId)> {
        let mut cursor = 0;
        (0..self.num_entries).map(move |_| {
            let key_len =
                u32::from_be_bytes(self.entries[cursor..cursor + LEN_SIZE].try_into().unwrap())
                    as usize;
            cursor += LEN_SIZE;

            let key = &self.entries[cursor..cursor + key_len];
            cursor += key_len;

            let pid =
                u64::from_be_bytes(self.entries[cursor..cursor + PID_SIZE].try_into().unwrap());
            cursor += PID_SIZE;

            (key, pid)
        })
    }
    pub fn iter_entries_leaf(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut cursor = 0;
        (0..self.num_entries).map(move |_| {
            let key_len =
                u32::from_be_bytes(self.entries[cursor..cursor + LEN_SIZE].try_into().unwrap())
                    as usize;
            cursor += LEN_SIZE;

            let key = &self.entries[cursor..cursor + key_len];
            cursor += key_len;

            let val_len =
                u32::from_be_bytes(self.entries[cursor..cursor + LEN_SIZE].try_into().unwrap())
                    as usize;
            cursor += LEN_SIZE;
            let val = &self.entries[cursor..cursor + val_len];
            cursor += val_len;

            (key, val)
        })
    }

    #[inline]
    pub fn is_inner(&self) -> bool {
        self.left_pid == u64::MAX
    }
}
impl<'b> From<&'b [u8]> for BasePage<'b> {
    fn from(buf: &'b [u8]) -> Self {
        let num_entries =
            u16::from_be_bytes(buf[Self::NUM_ENTRIES_RANGE].try_into().unwrap()) as usize;
        let left_pid = u64::from_be_bytes(buf[Self::LEFT_PID_RANGE].try_into().unwrap());
        let right_pid = u64::from_be_bytes(buf[Self::RIGHT_PID_RANGE].try_into().unwrap());

        let entries = &buf[Self::ENTRIES_START..];

        Self {
            num_entries,
            left_pid,
            right_pid,
            entries,
        }
    }
}

// PERF: im just gonna be sloppy and allocate/free all over the place here, for some reason this
// piece keeps tripping me up, so im just gonna make it easy for now. in general, this data is
// pretty ephemeral, and the space can definitely be reused, since it's basically just scratch
// space for doing compactions/splits/merges (eventually), it should be pretty straightforward to
// make this better once i get a better sense of how it'll be used, and it definietly doesn't need
// to be treated as a "page"
pub struct PageMut {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
    left_pid: PageId,
    right_pid: PageId,
    total_size: usize,
    page_size: usize,
}
impl PageMut {
    pub fn new(page_size: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
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
