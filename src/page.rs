use std::{
    alloc::{self, Layout},
    slice,
};

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
pub trait Delta {
    const CODE: u8;

    fn len(&self) -> usize;
    fn write_to_buf(&self, buf: &mut [u8]);
}

const INNER_CODE: u8 = 1;
const LEAF_CODE: u8 = 0;
#[inline]
pub fn is_inner(buf: &[u8]) -> bool {
    *buf.last().unwrap() == INNER_CODE
}

pub struct PageMut<'p> {
    pub buf: &'p mut [u8],
    pub bottom: usize,
    pub top: usize,
}
impl PageMut<'_> {
    pub fn pack(self) {
        todo!()
    }
}

pub mod inner {
    use crate::PageId;

    use super::{CODE_SIZE, Delta, LEN_SIZE, PID_SIZE};

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
}

pub mod leaf {
    use super::{CODE_SIZE, Delta, LEN_SIZE, PageMut};

    pub enum LeafDelta<'d> {
        Set(SetDelta<'d>),
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
    pub fn apply_delta(_page: &mut PageMut, _delta: &LeafDelta) -> bool {
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
