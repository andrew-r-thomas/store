use std::{
    alloc::{self, Layout},
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
    pub fn raw_buffer(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.cap) }
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
                                    Some((split_delta.middle_key, split_delta.left_pid));
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
            (Some(inner_entry), Some((key, pid))) => {
                if inner_entry.key < key {
                    inner_entry.left_pid
                } else {
                    pid
                }
            }
            (Some(inner_entry), None) => inner_entry.left_pid,
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
            u64::from_be_bytes(buf[buf.len() - Self::BASE_LEN_SIZE..].try_into().unwrap()) as usize;

        let deltas = DeltaIter::from(&buf[..buf.len() - (Self::BASE_LEN_SIZE + base_len)]);
        let base = BasePage::from(
            &buf[buf.len() - (Self::BASE_LEN_SIZE + base_len)..buf.len() - Self::BASE_LEN_SIZE],
        );

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
    pub offsets: &'b [u8],
    pub entries: &'b [u8],
}
impl BasePage<'_> {
    const NUM_ENTRIES_RANGE: Range<usize> = 0..2;
    const LEFT_PID_RANGE: Range<usize> = 2..10;
    const RIGHT_PID_RANGE: Range<usize> = 10..18;

    const OFFSETS_START: usize = 18;

    const OFFSET_SIZE: usize = 2;

    // TODO: binary search
    //
    /// returns the leftmost entry where the key is <= target, if it exists
    pub fn search_inner(&self, target: &[u8]) -> Option<InnerEntry> {
        for e in 0..self.num_entries {
            let off_start = e * Self::OFFSET_SIZE;
            let off_end = off_start + Self::OFFSET_SIZE;
            let offset =
                u16::from_be_bytes(self.offsets[off_start..off_end].try_into().unwrap()) as usize;

            let mut cursor = self.entries.len() - offset;

            let key_len =
                u32::from_be_bytes(self.entries[cursor..cursor + LEN_SIZE].try_into().unwrap())
                    as usize;
            cursor += LEN_SIZE;

            let key = &self.entries[cursor..cursor + key_len];
            cursor += key_len;

            if target <= key {
                let left_pid =
                    u64::from_be_bytes(self.entries[cursor..cursor + PID_SIZE].try_into().unwrap());
                return Some(InnerEntry { key, left_pid });
            }
        }

        None
    }
    pub fn search_leaf(&self, target: &[u8]) -> Option<&[u8]> {
        for e in 0..self.num_entries {
            let off_start = e * Self::OFFSET_SIZE;
            let off_end = off_start + Self::OFFSET_SIZE;
            let offset =
                u16::from_be_bytes(self.offsets[off_start..off_end].try_into().unwrap()) as usize;

            let mut cursor = self.entries.len() - offset;

            let key_len =
                u32::from_be_bytes(self.entries[cursor..cursor + LEN_SIZE].try_into().unwrap())
                    as usize;
            cursor += LEN_SIZE;

            let key = &self.entries[cursor..cursor + key_len];
            cursor += key_len;

            if target == key {
                let val_len =
                    u32::from_be_bytes(self.entries[cursor..cursor + LEN_SIZE].try_into().unwrap())
                        as usize;
                cursor += LEN_SIZE;
                let val = &self.entries[cursor..cursor + val_len];
                return Some(val);
            }
        }

        None
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

        let offsets_end = Self::OFFSETS_START + (Self::OFFSET_SIZE * num_entries);
        let offsets = &buf[Self::OFFSETS_START..offsets_end];

        let entries = &buf[offsets_end..];

        Self {
            num_entries,
            left_pid,
            right_pid,
            offsets,
            entries,
        }
    }
}

pub struct InnerEntry<'e> {
    key: &'e [u8],
    left_pid: PageId,
}

pub struct PageMut<'p> {
    pub buf: &'p mut [u8],
    pub bottom: usize,
}
impl PageMut<'_> {
    pub fn split_into(&mut self, _other: &mut Self, _middle_key_buf: &mut Vec<u8>) {
        todo!()
    }
    pub fn compact(&mut self, _page: Page) {
        todo!()
    }
    pub fn apply_delta(&mut self, _delta: &Delta) -> bool {
        todo!()
    }
    pub fn pack(self) {
        todo!()
    }
    pub fn set_right_pid(&mut self, _page_id: PageId) {
        todo!()
    }
}
impl<'p> From<&'p mut [u8]> for PageMut<'p> {
    fn from(buf: &'p mut [u8]) -> Self {
        buf.fill(0);
        Self {
            bottom: buf.len() - BASE_LEN_SIZE,
            buf,
        }
    }
}
