use std::{
    alloc::{self, Layout},
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
    #[inline]
    pub fn read(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.add(self.top), self.cap - self.top) }
    }
    pub fn write_delta<D: Delta>(&mut self, delta: &D) -> bool {
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
const FOOTER_SIZE: usize = 9;

pub trait Delta {
    const CODE: u8;

    fn len(&self) -> usize;
    fn write_to_buf(&self, buf: &mut [u8]);
}

const INNER_CODE: u8 = 1;
// const LEAF_CODE: u8 = 0;
#[inline]
pub fn is_inner(buf: &[u8]) -> bool {
    *buf.last().unwrap() == INNER_CODE
}

pub struct PageMut<'p> {
    pub buf: &'p mut [u8],
    pub bottom: usize,
}
impl PageMut<'_> {
    pub fn pack(self) {
        todo!()
    }
}

pub mod inner {
    use super::{CODE_SIZE, Delta, LEN_SIZE, PID_SIZE, PageMut};

    use crate::PageId;

    use std::ops::Range;

    pub enum InnerDelta<'d> {
        Split(&'d SplitDelta<'d>),
    }
    pub struct SplitDelta<'d> {
        pub middle_key: &'d [u8],
        pub left_page_id: PageId,
    }
    impl Delta for SplitDelta<'_> {
        const CODE: u8 = 2;

        fn len(&self) -> usize {
            CODE_SIZE + LEN_SIZE + PID_SIZE + self.middle_key.len() + LEN_SIZE + CODE_SIZE
        }
        fn write_to_buf(&self, buf: &mut [u8]) {
            let mut cursor = 0;
            let key_len = &(self.middle_key.len() as u32).to_be_bytes();

            buf[cursor] = Self::CODE;
            cursor += CODE_SIZE;

            buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
            cursor += LEN_SIZE;

            buf[cursor..cursor + PID_SIZE].copy_from_slice(&self.left_page_id.to_be_bytes());
            cursor += PID_SIZE;

            buf[cursor..cursor + self.middle_key.len()].copy_from_slice(self.middle_key);
            cursor += self.middle_key.len();

            buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
            cursor += LEN_SIZE;

            buf[cursor] = Self::CODE;
        }
    }

    pub fn find_child(_page: &[u8], _key: &[u8]) -> PageId {
        todo!()
    }
    pub fn compact<'p>(_old: &[u8], _new: &mut [u8]) -> PageMut<'p> {
        todo!()
    }
    pub fn apply_delta(_page: &mut PageMut, _delta: InnerDelta) -> bool {
        todo!()
    }
    pub fn split<'p>(
        _from: &mut PageMut,
        _to: &mut [u8],
        _middle_key_buf: &mut Vec<u8>,
    ) -> PageMut<'p> {
        todo!()
    }
    const RIGHT_PID_RANGE: Range<usize> = 2..10;
    #[inline]
    pub fn set_right_pid(page: &mut [u8], page_id: PageId) {
        page[RIGHT_PID_RANGE].copy_from_slice(&page_id.to_be_bytes());
    }
}

pub mod leaf {
    use crate::page::{FOOTER_SIZE, PID_SIZE};

    use super::{CODE_SIZE, Delta, LEN_SIZE, PageMut};

    use std::ops::Range;

    pub enum LeafDelta<'d> {
        Set(&'d SetDelta<'d>),
    }
    pub struct SetDelta<'d> {
        pub key: &'d [u8],
        pub val: &'d [u8],
    }
    impl Delta for SetDelta<'_> {
        const CODE: u8 = 1;

        fn len(&self) -> usize {
            CODE_SIZE
                + (LEN_SIZE * 2)
                + self.key.len()
                + self.val.len()
                + (LEN_SIZE * 2)
                + CODE_SIZE
        }
        fn write_to_buf(&self, buf: &mut [u8]) {
            let mut cursor = 0;

            let key_len = &(self.key.len() as u32).to_be_bytes();
            let val_len = &(self.val.len() as u32).to_be_bytes();

            buf[cursor] = Self::CODE;
            cursor += CODE_SIZE;

            buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
            cursor += LEN_SIZE;
            buf[cursor..cursor + LEN_SIZE].copy_from_slice(val_len);
            cursor += LEN_SIZE;

            buf[cursor..cursor + self.key.len()].copy_from_slice(self.key);
            cursor += self.key.len();
            buf[cursor..cursor + self.val.len()].copy_from_slice(self.val);
            cursor += self.val.len();

            buf[cursor..cursor + LEN_SIZE].copy_from_slice(key_len);
            cursor += LEN_SIZE;
            buf[cursor..cursor + LEN_SIZE].copy_from_slice(val_len);
            cursor += LEN_SIZE;

            buf[cursor] = Self::CODE;
        }
    }

    pub fn get<'g>(_page: &[u8], _key: &[u8]) -> Option<&'g [u8]> {
        todo!()
    }
    pub fn compact<'p>(_old: &[u8], _new: &mut [u8]) -> PageMut<'p> {
        todo!()
    }
    const NUM_ENTRIES_RANGE: Range<usize> = 0..2;
    const SLOTS_START: usize = 2;
    pub fn apply_delta(page: &mut PageMut, delta: LeafDelta) -> bool {
        match delta {
            LeafDelta::Set(set_delta) => {
                let num_entries =
                    u16::from_be_bytes(page.buf[NUM_ENTRIES_RANGE].try_into().unwrap()) as usize;
                let mut cursor = page.buf.len() - FOOTER_SIZE;
                for e in 0..num_entries {
                    let key_len_start = SLOTS_START + ((LEN_SIZE * 2) * e);
                    let key_len_end = key_len_start + LEN_SIZE;
                    let key_len = u32::from_be_bytes(
                        page.buf[key_len_start..key_len_end].try_into().unwrap(),
                    ) as usize;
                }
            }
        }
        todo!()
    }
    pub fn split<'p>(
        _from: &mut PageMut,
        _to: &mut [u8],
        _middle_key_buf: &mut Vec<u8>,
    ) -> PageMut<'p> {
        todo!()
    }
}

pub struct Page<'p> {
    pub deltas: DeltaIter<'p>,
    pub base: BasePage<'p>,
}
pub struct DeltaIter<'i> {
    pub buf: &'i [u8],
    pub top: usize,
    pub bottom: usize,
}
// TODO: maybe we want slotted pages?
pub struct BasePage<'b> {
    pub entries: &'b [u8],
    pub num_entries: usize,
    pub left_pid: PageId,
    pub right_pid: PageId,
}
impl Page<'_> {
    pub fn get_eq(&self, _target: &[u8]) -> Option<&[u8]> {
        todo!()
    }
    pub fn get_lte(&self, _target: &[u8]) -> Option<&[u8]> {
        todo!()
    }
}
