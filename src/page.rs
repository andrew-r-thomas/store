use std::{
    alloc::{self, Layout, alloc_zeroed, handle_alloc_error},
    ops::Range,
    slice,
};

use crate::PageId;

pub struct PageBuffer<const SIZE: usize> {
    ptr: *mut u8,
    top: usize,
}

impl<const SIZE: usize> PageBuffer<SIZE> {
    pub fn new<'n>() -> Self {
        let layout = Layout::array::<u8>(SIZE).unwrap();
        assert!(SIZE <= isize::MAX as usize);

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout)
        }

        Self { ptr, top: SIZE }
    }
    pub fn read(&self) -> Page<'_> {
        Page::from(unsafe { slice::from_raw_parts(self.ptr.add(self.top), SIZE - self.top) })
    }
    pub fn write_delta<D: Delta>(&mut self, delta: &D) -> Result<(), ()> {
        let len = delta.len();
        if len > self.top {
            return Err(());
        }

        let buf = unsafe { slice::from_raw_parts_mut(self.ptr.add(self.top - len), len) };
        delta.write_to_buf(buf);

        self.top -= len;
        Ok(())
    }

    #[inline]
    pub fn raw_buffer<'r>(&'r mut self) -> &'r mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, SIZE) }
    }

    pub fn from_raw_buffer(buf: &mut [u8], top: usize) -> Self {
        assert!(buf.len() == SIZE);
        let ptr = buf.as_mut_ptr();
        Self { ptr, top }
    }
}

pub trait Delta {
    fn len(&self) -> usize;
    fn write_to_buf(&self, buf: &mut [u8]);
}

// ================================================================================================
// page access types
// ================================================================================================

/// constants used by [`Page`], and each XPageMut type respectively ===============================
const LEAF_ID: u8 = 0;
const INNER_ID: u8 = 1;

const BASE_LEN_SIZE: usize = 8;
const ID_SIZE: usize = 1;
const FOOTER_SIZE: usize = BASE_LEN_SIZE + ID_SIZE;

pub enum Page<'p> {
    Leaf(LeafPage<'p>),
    Inner(InnerPage<'p>),
}
impl<'p> Page<'p> {
    // sizes

    pub fn unwrap_as_leaf(self) -> LeafPage<'p> {
        match self {
            Page::Leaf(leaf) => leaf,
            _ => panic!(),
        }
    }
    pub fn unwrap_as_inner(self) -> InnerPage<'p> {
        match self {
            Page::Inner(inner) => inner,
            _ => panic!(),
        }
    }
}
impl<'p> From<&'p [u8]> for Page<'p> {
    fn from(buf: &'p [u8]) -> Self {
        let base_len = u64::from_be_bytes(
            buf[buf.len() - FOOTER_SIZE..buf.len() - ID_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        match *buf.last().unwrap() {
            LEAF_ID => Self::Leaf(LeafPage::from((&buf[..buf.len() - FOOTER_SIZE], base_len))),
            INNER_ID => Self::Inner(InnerPage::from((&buf[..buf.len() - FOOTER_SIZE], base_len))),
            _ => panic!(),
        }
    }
}

// inner page =====================================================================================
//
// NOTE: for now we're assuming the right page id will not be updated (at least for a given
// instance of a page/pagebuffer, but it may get updated after sealing/compaction etc)
pub struct InnerPage<'p> {
    buf: &'p [u8],
    base_len: usize,
}
impl<'p> InnerPage<'p> {
    pub fn find_child(&self, target: &[u8]) -> PageId {
        let mut best = None;
        for chunk in self.iter_deltas() {
            match chunk {
                InnerDelta::Set { key, left_page_id } => {
                    if target <= key {
                        match best {
                            None => best = Some((key, left_page_id)),
                            Some((k, _)) => {
                                if key < k {
                                    best = Some((key, left_page_id));
                                }
                            }
                        }
                    }
                }
            }
        }
        let base_best = self.base().find_child(target);
        match best {
            None => base_best.1,
            Some((k, id)) => {
                if base_best.0 < k {
                    base_best.1
                } else {
                    id
                }
            }
        }
    }
    fn iter_deltas(&self) -> InnerDeltaIter<'p> {
        InnerDeltaIter::from(&self.buf[..self.buf.len() - self.base_len])
    }
    fn base(&self) -> InnerBase<'p> {
        InnerBase::from(&self.buf[self.buf.len() - self.base_len..])
    }
}
impl<'p> From<(&'p [u8], usize)> for InnerPage<'p> {
    fn from((buf, base_len): (&'p [u8], usize)) -> Self {
        Self { buf, base_len }
    }
}

struct InnerDeltaIter<'i> {
    buf: &'i [u8],
    top: usize,
    bottom: usize,
}
impl<'i> From<&'i [u8]> for InnerDeltaIter<'i> {
    fn from(buf: &'i [u8]) -> Self {
        Self {
            buf,
            top: 0,
            bottom: buf.len(),
        }
    }
}
impl<'i> Iterator for InnerDeltaIter<'i> {
    type Item = InnerDelta<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        let delta = InnerDelta::from_top(&self.buf[self.top..]);
        self.top += delta.len();
        Some(delta)
    }
}
impl DoubleEndedIterator for InnerDeltaIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        let delta = InnerDelta::from_bottom(&self.buf[..self.bottom]);
        self.bottom -= delta.len();
        Some(delta)
    }
}

pub enum InnerDelta<'d> {
    Set { key: &'d [u8], left_page_id: PageId },
}
impl<'d> InnerDelta<'d> {
    const SET_ID: u8 = 1;

    const ID_SIZE: usize = 1;
    const KEY_LEN_SIZE: usize = 4;
    const LEFT_PAGE_ID_SIZE: usize = 8;

    fn from_top(buf: &'d [u8]) -> Self {
        let mut cursor = 0;
        match buf[cursor] {
            Self::SET_ID => {
                cursor += Self::ID_SIZE;

                let key_len = u32::from_be_bytes(
                    buf[cursor..cursor + Self::KEY_LEN_SIZE].try_into().unwrap(),
                ) as usize;
                cursor += Self::KEY_LEN_SIZE;

                let left_page_id = u64::from_be_bytes(
                    buf[cursor..cursor + Self::LEFT_PAGE_ID_SIZE]
                        .try_into()
                        .unwrap(),
                );
                cursor += Self::LEFT_PAGE_ID_SIZE;

                let key = &buf[cursor..cursor + key_len];

                Self::Set { key, left_page_id }
            }
            _ => panic!(),
        }
    }
    fn from_bottom(buf: &'d [u8]) -> Self {
        let mut cursor = buf.len();
        match buf[cursor - Self::ID_SIZE] {
            Self::SET_ID => {
                cursor -= Self::ID_SIZE;

                let key_len = u32::from_be_bytes(
                    buf[cursor - Self::KEY_LEN_SIZE..cursor].try_into().unwrap(),
                ) as usize;
                cursor -= Self::KEY_LEN_SIZE;

                let key = &buf[cursor - key_len..cursor];
                cursor -= key_len;

                let left_page_id = u64::from_be_bytes(
                    buf[cursor - Self::LEFT_PAGE_ID_SIZE..cursor]
                        .try_into()
                        .unwrap(),
                );

                Self::Set { key, left_page_id }
            }
            _ => panic!(),
        }
    }
}
impl Delta for InnerDelta<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Set { key, .. } => {
                Self::ID_SIZE
                    + Self::KEY_LEN_SIZE
                    + Self::LEFT_PAGE_ID_SIZE
                    + key.len()
                    + Self::KEY_LEN_SIZE
                    + Self::ID_SIZE
            }
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert!(self.len() == buf.len());
        match self {
            Self::Set { key, left_page_id } => {
                let key_len = &(key.len() as u32).to_be_bytes();
                let mut cursor = 0;

                buf[cursor] = Self::SET_ID;
                cursor += Self::ID_SIZE;

                buf[cursor..cursor + Self::KEY_LEN_SIZE].copy_from_slice(key_len);
                cursor += Self::KEY_LEN_SIZE;

                buf[cursor..cursor + Self::LEFT_PAGE_ID_SIZE]
                    .copy_from_slice(&left_page_id.to_be_bytes());
                cursor += Self::LEFT_PAGE_ID_SIZE;

                buf[cursor..cursor + key.len()].copy_from_slice(key);
                cursor += key.len();

                buf[cursor..cursor + Self::KEY_LEN_SIZE].copy_from_slice(key_len);
                cursor += Self::KEY_LEN_SIZE;

                buf[cursor] = Self::SET_ID;
            }
        }
    }
}

#[derive(Copy, Clone)]
struct InnerBase<'b> {
    buf: &'b [u8],
}
impl<'b> InnerBase<'b> {
    const NUM_ENTRIES: Range<usize> = 0..2;
    const RIGHT_PAGE_ID: Range<usize> = 2..10;
    const SLOTS_START: usize = 10;

    const KEY_LEN_SIZE: usize = 4;
    const LEFT_PAGE_ID_SIZE: usize = 8;
    const SLOT_SIZE: usize = Self::KEY_LEN_SIZE + Self::LEFT_PAGE_ID_SIZE;

    fn find_child(self, target: &'b [u8]) -> (&'b [u8], PageId) {
        let num_entries = self.num_entries() as usize;

        let mut cursor = self.buf.len();
        for e in 0..num_entries {
            let key_len_start = Self::SLOTS_START + (e * Self::SLOT_SIZE);
            let key_len_end = key_len_start + Self::KEY_LEN_SIZE;
            let key_len =
                u32::from_be_bytes(self.buf[key_len_start..key_len_end].try_into().unwrap())
                    as usize;

            let key = &self.buf[cursor - key_len..cursor];
            cursor -= key_len;

            if target <= key {
                let left = u64::from_be_bytes(
                    self.buf[key_len_end..key_len_end + Self::LEFT_PAGE_ID_SIZE]
                        .try_into()
                        .unwrap(),
                );
                return (key, left);
            }
        }

        // NOTE: returning target (or anything for the key) is a bit meaningless here, but at least
        // right now it doesn't seem like it'll mess anything up, but keep and eye out
        (target, self.right_page_id())
    }

    #[inline]
    fn num_entries(&self) -> u16 {
        u16::from_be_bytes(self.buf[Self::NUM_ENTRIES].try_into().unwrap())
    }
    #[inline]
    fn right_page_id(&self) -> PageId {
        u64::from_be_bytes(self.buf[Self::RIGHT_PAGE_ID].try_into().unwrap())
    }

    fn header(&self) -> &[u8] {
        &self.buf[..Self::SLOTS_START + (Self::SLOT_SIZE * self.num_entries() as usize)]
    }
    fn keys(&self) -> &[u8] {
        &self.buf[Self::SLOTS_START + (Self::SLOT_SIZE * self.num_entries() as usize)..]
    }
}
impl<'b> From<&'b [u8]> for InnerBase<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}

pub struct InnerPageMut<'p> {
    buf: &'p mut [u8],
    bottom: usize,
}
impl InnerPageMut<'_> {
    const NUM_ENTRIES: Range<usize> = 0..2;
    const RIGHT_PAGE_ID: Range<usize> = 2..10;
    const SLOTS_START: usize = 10;

    const KEY_LEN_SIZE: usize = 4;
    const LEFT_PAGE_ID_SIZE: usize = 8;
    const SLOT_SIZE: usize = Self::KEY_LEN_SIZE + Self::LEFT_PAGE_ID_SIZE;

    /// NOTE: this fn is temporary until [`crate::pool::Pool`] is finished
    pub fn new(len: usize) -> Self {
        let layout = Layout::array::<u8>(len).unwrap();

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout)
        }

        let buf = unsafe { slice::from_raw_parts_mut(ptr, len) };
        *buf.last_mut().unwrap() = INNER_ID;

        Self {
            buf,
            bottom: len - FOOTER_SIZE,
        }
    }

    /// NOTE: this should only be called on an empty fresh [`InnerPageMut`]
    pub fn compact(&mut self, other: InnerPage) {
        assert_eq!(self.buf.len() - FOOTER_SIZE, self.bottom);
        let base = other.base();

        // copy the header
        let header = base.header();
        self.buf[..header.len()].copy_from_slice(header);

        // copy the keys
        let keys = base.keys();
        self.buf[self.bottom - keys.len()..self.bottom].copy_from_slice(keys);
        self.bottom -= keys.len();

        // apply the deltas
        for delta in other.iter_deltas().rev() {
            self.apply_delta(&delta).unwrap();
        }
    }
    pub fn apply_delta(&mut self, delta: &InnerDelta) -> Result<(), ()> {
        match delta {
            InnerDelta::Set { key, left_page_id } => {
                let num_entries = self.num_entries() as usize;
                let top = Self::SLOTS_START + (num_entries * Self::SLOT_SIZE);
                if self.bottom - top < Self::SLOT_SIZE + key.len() {
                    return Err(());
                }
                let mut cursor = self.buf.len() - FOOTER_SIZE;

                for e in 0..num_entries {
                    let key_len_start = Self::SLOTS_START + (e * Self::SLOT_SIZE);
                    let key_len_end = key_len_start + Self::KEY_LEN_SIZE;
                    let key_len = u32::from_be_bytes(
                        self.buf[key_len_start..key_len_end].try_into().unwrap(),
                    ) as usize;

                    if *key <= &self.buf[cursor - key_len..cursor] {
                        // insert the new slot
                        self.buf
                            .copy_within(key_len_start..top, key_len_start + Self::SLOT_SIZE);
                        self.buf[key_len_start..key_len_end]
                            .copy_from_slice(&(key.len() as u32).to_be_bytes());
                        self.buf[key_len_end..key_len_end + Self::LEFT_PAGE_ID_SIZE]
                            .copy_from_slice(&left_page_id.to_be_bytes());
                        self.set_num_entries((num_entries + 1) as u16);

                        // insert the new key
                        self.buf
                            .copy_within(self.bottom..cursor, self.bottom - key.len());
                        self.buf[cursor - key.len()..cursor].copy_from_slice(key);
                        self.bottom -= key.len();
                        return Ok(());
                    }

                    cursor -= key_len;
                }

                // none of the existing keys were >= to key, so we need to add it at the end
                self.buf[top..top + Self::KEY_LEN_SIZE]
                    .copy_from_slice(&(key.len() as u32).to_be_bytes());
                self.buf[top + Self::KEY_LEN_SIZE..top + Self::SLOT_SIZE]
                    .copy_from_slice(&left_page_id.to_be_bytes());
                self.set_num_entries((num_entries + 1) as u16);

                self.buf[self.bottom - key.len()..self.bottom].copy_from_slice(key);
                self.bottom -= key.len();

                return Ok(());
            }
        }
    }
    pub fn split_into(&mut self, other: &mut Self, out: &mut Vec<u8>) {
        let num_entries = self.num_entries() as usize;
        let middle_entry = num_entries / 2;
        let top = Self::SLOTS_START + (num_entries * Self::SLOT_SIZE);

        let mut cursor = self.buf.len() - FOOTER_SIZE;
        for e in 0..middle_entry {
            let key_len_start = Self::SLOTS_START + (e * Self::SLOT_SIZE);
            let key_len_end = key_len_start + Self::KEY_LEN_SIZE;
            let key_len =
                u32::from_be_bytes(self.buf[key_len_start..key_len_end].try_into().unwrap())
                    as usize;

            let left_page_id = u64::from_be_bytes(
                self.buf[key_len_end..key_len_end + Self::LEFT_PAGE_ID_SIZE]
                    .try_into()
                    .unwrap(),
            );

            let key = &self.buf[cursor - key_len..cursor];

            let delta = InnerDelta::Set { key, left_page_id };
            other.apply_delta(&delta).unwrap();

            cursor -= key_len;
        }

        let key_len_start = Self::SLOTS_START + (middle_entry * Self::SLOT_SIZE);
        let key_len_end = key_len_start + Self::KEY_LEN_SIZE;
        let middle_key_len =
            u32::from_be_bytes(self.buf[key_len_start..key_len_end].try_into().unwrap()) as usize;

        let middle_left_page_id = u64::from_be_bytes(
            self.buf[key_len_end..key_len_end + Self::LEFT_PAGE_ID_SIZE]
                .try_into()
                .unwrap(),
        );
        self.set_right_page_id(middle_left_page_id);

        let middle_key = &self.buf[cursor - middle_key_len..cursor];
        out.extend(middle_key);

        self.buf.copy_within(
            key_len_end + Self::LEFT_PAGE_ID_SIZE..top,
            Self::SLOTS_START,
        );
        self.set_num_entries((num_entries - (middle_entry + 1)) as u16);

        let new_keys_range = self.bottom..cursor - middle_key_len;
        self.buf.copy_within(
            new_keys_range.clone(),
            (self.buf.len() - FOOTER_SIZE) - new_keys_range.len(),
        );
        self.bottom = (self.buf.len() - FOOTER_SIZE) - new_keys_range.len();
    }
    pub fn unpack<const PAGE_SIZE: usize>(self) -> PageBuffer<PAGE_SIZE> {
        assert!(self.buf.len() == PAGE_SIZE);

        let top = Self::SLOTS_START + (self.num_entries() as usize * Self::SLOT_SIZE);
        self.buf.copy_within(..top, self.bottom - top);

        let base_len = (PAGE_SIZE - (self.bottom - top)) - FOOTER_SIZE;
        self.buf[PAGE_SIZE - FOOTER_SIZE..PAGE_SIZE - 1]
            .copy_from_slice(&(base_len as u64).to_be_bytes());

        PageBuffer::from_raw_buffer(self.buf, self.bottom - top)
    }
    pub fn set_right_page_id(&mut self, page_id: PageId) {
        self.buf[Self::RIGHT_PAGE_ID].copy_from_slice(&page_id.to_be_bytes());
    }

    #[inline]
    fn num_entries(&self) -> u16 {
        u16::from_be_bytes(self.buf[Self::NUM_ENTRIES].try_into().unwrap())
    }
    #[inline]
    fn set_num_entries(&mut self, num_entries: u16) {
        self.buf[Self::NUM_ENTRIES].copy_from_slice(&num_entries.to_be_bytes());
    }
}
impl<'p> From<&'p mut [u8]> for InnerPageMut<'p> {
    fn from(buf: &'p mut [u8]) -> Self {
        *buf.last_mut().unwrap() = INNER_ID;
        Self {
            bottom: buf.len() - FOOTER_SIZE,
            buf,
        }
    }
}

// leaf page ======================================================================================
pub struct LeafPage<'p> {
    buf: &'p [u8],
    base_len: usize,
}
impl<'p> LeafPage<'p> {
    pub fn get(self, target: &[u8]) -> Option<&'p [u8]> {
        for delta in self.iter_deltas() {
            match delta {
                // LeafDelta::Del(key) => {
                //     if target == key {
                //         return None;
                //     }
                // }
                LeafDelta::Set { key, val } => {
                    if target == key {
                        return Some(val);
                    }
                }
            }
        }

        self.base().get(target)
    }
    fn iter_deltas(&self) -> LeafDeltaIter<'p> {
        LeafDeltaIter::from(&self.buf[..self.buf.len() - self.base_len])
    }
    fn base(&self) -> LeafBase<'p> {
        LeafBase::from(&self.buf[self.buf.len() - self.base_len..])
    }
}
impl<'p> From<(&'p [u8], usize)> for LeafPage<'p> {
    fn from((buf, base_len): (&'p [u8], usize)) -> Self {
        Self { buf, base_len }
    }
}

struct LeafDeltaIter<'i> {
    buf: &'i [u8],
    top: usize,
    bottom: usize,
}
impl<'i> From<&'i [u8]> for LeafDeltaIter<'i> {
    fn from(buf: &'i [u8]) -> Self {
        Self {
            buf,
            top: 0,
            bottom: buf.len(),
        }
    }
}
impl<'i> Iterator for LeafDeltaIter<'i> {
    type Item = LeafDelta<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        let delta = LeafDelta::from_top(&self.buf[self.top..]);
        self.top += delta.len();
        Some(delta)
    }
}
impl<'i> DoubleEndedIterator for LeafDeltaIter<'i> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        let delta = LeafDelta::from_bottom(&self.buf[..self.bottom]);
        self.bottom -= delta.len();
        Some(delta)
    }
}
pub enum LeafDelta<'d> {
    Set { key: &'d [u8], val: &'d [u8] },
    // Del(&'d [u8]),
}
impl<'d> LeafDelta<'d> {
    const SET_ID: u8 = 1;
    // const DEL_ID: u8 = 2;

    const ID_SIZE: usize = 1;
    const LEN_SIZE: usize = 4;

    fn from_top(buf: &'d [u8]) -> Self {
        let mut cursor = 0;
        match buf[cursor] {
            Self::SET_ID => {
                cursor += Self::ID_SIZE;

                let key_len =
                    u32::from_be_bytes(buf[cursor..cursor + Self::LEN_SIZE].try_into().unwrap())
                        as usize;
                cursor += Self::LEN_SIZE;
                let val_len =
                    u32::from_be_bytes(buf[cursor..cursor + Self::LEN_SIZE].try_into().unwrap())
                        as usize;
                cursor += Self::LEN_SIZE;

                let key = &buf[cursor..cursor + key_len];
                cursor += key_len;
                let val = &buf[cursor..cursor + val_len];

                Self::Set { key, val }
            }
            // Self::DEL_ID => {
            //     cursor += Self::ID_SIZE;
            //
            //     let key_len =
            //         u32::from_be_bytes(buf[cursor..cursor + Self::LEN_SIZE].try_into().unwrap())
            //             as usize;
            //     cursor += Self::LEN_SIZE;
            //
            //     let key = &buf[cursor..cursor + key_len];
            //
            //     Self::Del(key)
            // }
            _ => panic!(),
        }
    }
    fn from_bottom(buf: &'d [u8]) -> Self {
        let mut cursor = buf.len();
        match buf[cursor - Self::ID_SIZE] {
            Self::SET_ID => {
                cursor -= Self::ID_SIZE;

                let val_len =
                    u32::from_be_bytes(buf[cursor - Self::LEN_SIZE..cursor].try_into().unwrap())
                        as usize;
                cursor -= Self::LEN_SIZE;
                let key_len =
                    u32::from_be_bytes(buf[cursor - Self::LEN_SIZE..cursor].try_into().unwrap())
                        as usize;
                cursor -= Self::LEN_SIZE;

                let val = &buf[cursor - val_len..cursor];
                cursor -= val_len;
                let key = &buf[cursor - key_len..cursor];

                Self::Set { key, val }
            }
            // Self::DEL_ID => {
            //     cursor -= Self::ID_SIZE;
            //
            //     let key_len =
            //         u32::from_be_bytes(buf[cursor - Self::LEN_SIZE..cursor].try_into().unwrap())
            //             as usize;
            //     cursor -= Self::LEN_SIZE;
            //
            //     let key = &buf[cursor - key_len..cursor];
            //
            //     Self::Del(key)
            // }
            _ => panic!(),
        }
    }
}
impl Delta for LeafDelta<'_> {
    fn len(&self) -> usize {
        match self {
            // Self::Del(key) => {
            //     Self::ID_SIZE + Self::LEN_SIZE + key.len() + Self::LEN_SIZE + Self::ID_SIZE
            // }
            Self::Set { key, val } => {
                Self::ID_SIZE
                    + (Self::LEN_SIZE * 2)
                    + key.len()
                    + val.len()
                    + (Self::LEN_SIZE * 2)
                    + Self::ID_SIZE
            }
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert!(self.len() == buf.len());
        let mut cursor = 0;
        match self {
            // Self::Del(key) => {
            //     let key_len = &(key.len() as u32).to_be_bytes();
            //
            //     buf[cursor] = Self::DEL_ID;
            //     cursor += Self::ID_SIZE;
            //
            //     buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(key_len);
            //     cursor += Self::LEN_SIZE;
            //
            //     buf[cursor..cursor + key.len()].copy_from_slice(key);
            //     cursor += key.len();
            //
            //     buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(key_len);
            //     cursor += Self::LEN_SIZE;
            //
            //     buf[cursor] = Self::DEL_ID;
            // }
            Self::Set { key, val } => {
                let key_len = &(key.len() as u32).to_be_bytes();
                let val_len = &(val.len() as u32).to_be_bytes();

                buf[cursor] = Self::SET_ID;
                cursor += Self::ID_SIZE;

                buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(key_len);
                cursor += Self::LEN_SIZE;
                buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(val_len);
                cursor += Self::LEN_SIZE;

                buf[cursor..cursor + key.len()].copy_from_slice(key);
                cursor += key.len();
                buf[cursor..cursor + val.len()].copy_from_slice(val);
                cursor += val.len();

                buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(key_len);
                cursor += Self::LEN_SIZE;
                buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(val_len);
                cursor += Self::LEN_SIZE;

                buf[cursor] = Self::SET_ID;
            }
        }
    }
}

#[derive(Copy, Clone)]
struct LeafBase<'b> {
    buf: &'b [u8],
}
impl<'b> LeafBase<'b> {
    const NUM_ENTRIES: Range<usize> = 0..2;
    const SLOTS_START: usize = 2;

    const LEN_SIZE: usize = 4;
    const SLOT_SIZE: usize = Self::LEN_SIZE * 2;

    fn get(self, target: &[u8]) -> Option<&'b [u8]> {
        let num_entries = self.num_entries() as usize;
        let mut cursor = self.buf.len();
        for e in 0..num_entries {
            let key_len_start = Self::SLOTS_START + (e * Self::SLOT_SIZE);
            let key_len_end = key_len_start + Self::LEN_SIZE;
            let key_len =
                u32::from_be_bytes(self.buf[key_len_start..key_len_end].try_into().unwrap())
                    as usize;
            let val_len = u32::from_be_bytes(
                self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;

            let key = &self.buf[cursor - (key_len + val_len)..cursor - val_len];
            if target == key {
                return Some(&self.buf[cursor - val_len..cursor]);
            }

            cursor -= key_len + val_len;
        }
        None
    }
    #[inline]
    fn num_entries(&self) -> u16 {
        u16::from_be_bytes(self.buf[Self::NUM_ENTRIES].try_into().unwrap())
    }

    fn header(&self) -> &[u8] {
        &self.buf[..Self::SLOTS_START + (Self::SLOT_SIZE * self.num_entries() as usize)]
    }
    fn entries(&self) -> &[u8] {
        &self.buf[Self::SLOTS_START + (Self::SLOT_SIZE * self.num_entries() as usize)..]
    }
}
impl<'b> From<&'b [u8]> for LeafBase<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}

pub struct LeafPageMut<'p> {
    buf: &'p mut [u8],
    bottom: usize,
}
impl LeafPageMut<'_> {
    const NUM_ENTRIES: Range<usize> = 0..2;

    const SLOTS_START: usize = 2;
    const SLOT_SIZE: usize = 8;
    const LEN_SIZE: usize = 4;

    /// NOTE: this is mostly temporary until [`crate::pool::Pool`] is finished
    pub fn new(len: usize) -> Self {
        let layout = Layout::array::<u8>(len).unwrap();

        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout)
        }

        let buf = unsafe { slice::from_raw_parts_mut(ptr, len) };
        *buf.last_mut().unwrap() = LEAF_ID;

        Self {
            buf,
            bottom: len - FOOTER_SIZE,
        }
    }
    pub fn compact(&mut self, other: LeafPage) {
        assert_eq!(self.buf.len() - FOOTER_SIZE, self.bottom);
        let base = other.base();

        // copy the header
        let header = base.header();
        self.buf[..header.len()].copy_from_slice(header);

        // copy the actual data
        let entries = base.entries();
        self.buf[self.bottom - entries.len()..self.bottom].copy_from_slice(entries);
        self.bottom -= entries.len();

        // apply the deltas in reverse order
        for delta in other.iter_deltas().rev() {
            self.apply_delta(&delta).unwrap();
        }
    }
    pub fn apply_delta(&mut self, delta: &LeafDelta) -> Result<(), ()> {
        let num_entries = self.num_entries() as usize;
        let top = Self::SLOTS_START + (num_entries * Self::SLOT_SIZE);
        let mut cursor = self.buf.len() - FOOTER_SIZE;

        match delta {
            // LeafDelta::Del(key) => {
            //     let num_entries = self.num_entries() as usize;
            //     let mut cursor = self.buf.len() - Self::FOOTER_SIZE;
            //     for e in 0..num_entries {
            //         let key_len_start = Self::SLOTS_START + (e * Self::SLOT_SIZE);
            //         let key_len_end = key_len_start + Self::LEN_SIZE;
            //         let key_len = u32::from_be_bytes(
            //             self.buf[key_len_start..key_len_end].try_into().unwrap(),
            //         ) as usize;
            //
            //         let val_len = u32::from_be_bytes(
            //             self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
            //                 .try_into()
            //                 .unwrap(),
            //         ) as usize;
            //
            //         let k = &self.buf[cursor - (key_len + val_len)..cursor - val_len];
            //         if *key == k {
            //             self.buf
            //                 .copy_within(key_len_start + Self::SLOT_SIZE..top, key_len_start);
            //             self.set_num_entries((num_entries - 1) as u16);
            //
            //             self.buf.copy_within(
            //                 self.bottom..cursor - (key_len + val_len),
            //                 self.bottom + key_len + val_len,
            //             );
            //             self.bottom += key_len + val_len;
            //
            //             return Ok(());
            //         }
            //
            //         cursor -= key_len + val_len;
            //     }
            //
            //     // we're trying to delete something that isn't in here, this is legal, but just
            //     // want to catch it for now in case something weird happens during dev, will delete
            //     // later
            //     panic!()
            // }
            LeafDelta::Set { key, val } => {
                for e in 0..num_entries {
                    let key_len_start = Self::SLOTS_START + (e * Self::SLOT_SIZE);
                    let key_len_end = key_len_start + Self::LEN_SIZE;
                    let key_len = u32::from_be_bytes(
                        self.buf[key_len_start..key_len_end].try_into().unwrap(),
                    ) as usize;

                    let val_len = u32::from_be_bytes(
                        self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
                            .try_into()
                            .unwrap(),
                    ) as usize;

                    let k = &self.buf[cursor - (key_len + val_len)..cursor - val_len];
                    if *key < k {
                        // insert
                        if self.bottom - top < Self::SLOT_SIZE + key.len() + val.len() {
                            return Err(());
                        }

                        // insert the new slot
                        self.buf
                            .copy_within(key_len_start..top, key_len_start + Self::SLOT_SIZE);
                        self.buf[key_len_start..key_len_end]
                            .copy_from_slice(&(key.len() as u32).to_be_bytes());
                        self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
                            .copy_from_slice(&(val.len() as u32).to_be_bytes());
                        self.set_num_entries((num_entries + 1) as u16);

                        // move the actual data and write the new data
                        self.buf.copy_within(
                            self.bottom..cursor,
                            self.bottom - (key.len() + val.len()),
                        );
                        self.buf[cursor - (key.len() + val.len())..cursor - val.len()]
                            .copy_from_slice(key);
                        self.buf[cursor - val.len()..cursor].copy_from_slice(val);
                        self.bottom -= key.len() + val.len();

                        return Ok(());
                    } else if *key == k {
                        // update
                        // TODO: probably should get rid of this branch, most of the logic is the
                        // same, and it's just a difference in + vs -, so we should just use an
                        // isize

                        if val.len() <= val_len {
                            // guaranteed to have room
                            let gap = val_len - val.len();

                            // overwrite the val len
                            self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
                                .copy_from_slice(&(val.len() as u32).to_be_bytes());

                            // shift down entries and this entry's key
                            self.buf
                                .copy_within(self.bottom..cursor - val_len, self.bottom + gap);
                            self.bottom += gap;

                            // copy the new val
                            self.buf[cursor - val.len()..cursor].copy_from_slice(val);
                        } else {
                            // need to check if we have space
                            let gap = val.len() - val_len;
                            if self.bottom - top < gap {
                                return Err(());
                            }

                            // overwrite the val len
                            self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
                                .copy_from_slice(&(val.len() as u32).to_be_bytes());

                            // shift up the entries above, and this entry's key
                            self.buf
                                .copy_within(self.bottom..cursor - key_len, self.bottom - gap);
                            self.bottom -= gap;

                            // copy the new val
                            self.buf[cursor - val.len()..cursor].copy_from_slice(val);
                        }

                        return Ok(());
                    }
                    cursor -= key_len + val_len;
                }

                // key is greater than anything existing, so goes at the very end
                if self.bottom - top < Self::SLOT_SIZE + key.len() + val.len() {
                    return Err(());
                }

                self.buf[top..top + Self::LEN_SIZE]
                    .copy_from_slice(&(key.len() as u32).to_be_bytes());
                self.buf[top + Self::LEN_SIZE..top + Self::SLOT_SIZE]
                    .copy_from_slice(&(val.len() as u32).to_be_bytes());
                self.set_num_entries((num_entries + 1) as u16);

                self.buf[self.bottom - (key.len() + val.len())..self.bottom - val.len()]
                    .copy_from_slice(key);
                self.buf[self.bottom - val.len()..self.bottom].copy_from_slice(val);
                self.bottom -= key.len() + val.len();

                return Ok(());
            }
        }
    }
    // FIX: deadass don't work
    /// NOTE: we always split left
    pub fn split_into(&mut self, other: &mut Self, out: &mut Vec<u8>) {
        let num_entries = self.num_entries() as usize;
        let middle_entry = num_entries / 2;
        let top = Self::SLOTS_START + (num_entries * Self::SLOT_SIZE);

        // ingest data up to and including middle_entry into other
        let mut cursor = self.buf.len() - FOOTER_SIZE;
        for e in 0..middle_entry {
            let key_len_start = Self::SLOTS_START + (e * Self::SLOT_SIZE);
            let key_len_end = key_len_start + Self::LEN_SIZE;
            let key_len =
                u32::from_be_bytes(self.buf[key_len_start..key_len_end].try_into().unwrap())
                    as usize;

            let val_len = u32::from_be_bytes(
                self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;

            let delta = LeafDelta::Set {
                key: &self.buf[cursor - (key_len + val_len)..cursor - val_len],
                val: &self.buf[cursor - val_len..cursor],
            };
            other.apply_delta(&delta).unwrap();
            cursor -= key_len + val_len;
        }

        let key_len_start = Self::SLOTS_START + (middle_entry * Self::SLOT_SIZE);
        let key_len_end = key_len_start + Self::LEN_SIZE;
        let key_len =
            u32::from_be_bytes(self.buf[key_len_start..key_len_end].try_into().unwrap()) as usize;

        let val_len = u32::from_be_bytes(
            self.buf[key_len_end..key_len_end + Self::LEN_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        let delta = LeafDelta::Set {
            key: &self.buf[cursor - (key_len + val_len)..cursor - val_len],
            val: &self.buf[cursor - val_len..cursor],
        };
        other.apply_delta(&delta).unwrap();
        out.extend(&self.buf[cursor - (key_len + val_len)..cursor - val_len]);

        self.buf
            .copy_within(key_len_end + Self::LEN_SIZE..top, Self::SLOTS_START);
        self.set_num_entries((num_entries - (middle_entry + 1)) as u16);

        let new_entries_range = self.bottom..cursor - (key_len + val_len);
        self.buf.copy_within(
            new_entries_range.clone(),
            (self.buf.len() - FOOTER_SIZE) - new_entries_range.len(),
        );
        self.bottom = (self.buf.len() - FOOTER_SIZE) - new_entries_range.len();
    }
    pub fn unpack<const PAGE_SIZE: usize>(self) -> PageBuffer<PAGE_SIZE> {
        assert!(self.buf.len() == PAGE_SIZE);

        let top = Self::SLOTS_START + (self.num_entries() as usize * Self::SLOT_SIZE);
        self.buf.copy_within(..top, self.bottom - top);

        let base_len = (PAGE_SIZE - (self.bottom - top)) - FOOTER_SIZE;
        self.buf[PAGE_SIZE - FOOTER_SIZE..PAGE_SIZE - 1]
            .copy_from_slice(&(base_len as u64).to_be_bytes());

        PageBuffer::from_raw_buffer(self.buf, self.bottom - top)
    }
    #[inline]
    fn num_entries(&self) -> u16 {
        u16::from_be_bytes(self.buf[Self::NUM_ENTRIES].try_into().unwrap())
    }
    #[inline]
    fn set_num_entries(&mut self, num_entries: u16) {
        self.buf[Self::NUM_ENTRIES].copy_from_slice(&num_entries.to_be_bytes());
    }
}
impl<'p> From<&'p mut [u8]> for LeafPageMut<'p> {
    fn from(buf: &'p mut [u8]) -> Self {
        Self {
            bottom: buf.len() - FOOTER_SIZE,
            buf,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn leaf() {
        const PAGE_SIZE: usize = 1024;
        let mut leaf_mut = LeafPageMut::new(PAGE_SIZE);
        let mut key = Vec::new();
        let mut val = Vec::new();

        for i in 0..10 {
            key.clear();
            val.clear();
            for _ in 0..i {
                key.push(i);
                val.push(i);
            }
            leaf_mut
                .apply_delta(&LeafDelta::Set {
                    key: &key,
                    val: &val,
                })
                .unwrap();
        }

        let mut leaf_buf: PageBuffer<PAGE_SIZE> = leaf_mut.unpack();
        for i in 10..20 {
            key.clear();
            val.clear();
            for _ in 0..i {
                key.push(i);
                val.push(i);
            }

            leaf_buf
                .write_delta(&LeafDelta::Set {
                    key: &key,
                    val: &val,
                })
                .unwrap();
        }

        let leaf_page = leaf_buf.read().unwrap_as_leaf();
        for (delta, i) in leaf_page.iter_deltas().zip((10..20).rev()) {
            key.clear();
            val.clear();
            for _ in 0..i {
                key.push(i);
                val.push(i);
            }

            match delta {
                LeafDelta::Set {
                    key: d_key,
                    val: d_val,
                } => {
                    assert_eq!(&key, d_key);
                    assert_eq!(&val, d_val);
                }
            }
        }

        let leaf_base = leaf_page.base();
        for i in 0..10 {
            key.clear();
            val.clear();
            for _ in 0..i {
                key.push(i);
                val.push(i);
            }
            assert_eq!(leaf_base.get(&key).unwrap(), &val);
        }

        let mut compacted_mut = LeafPageMut::new(PAGE_SIZE);
        compacted_mut.compact(leaf_page);

        let mut to_mut = LeafPageMut::new(PAGE_SIZE);
        compacted_mut.split_into(&mut to_mut, &mut key);

        let left_buf: PageBuffer<PAGE_SIZE> = to_mut.unpack();
        let right_buf: PageBuffer<PAGE_SIZE> = compacted_mut.unpack();
        let left_base = left_buf.read().unwrap_as_leaf().base();
        let right_base = right_buf.read().unwrap_as_leaf().base();

        for i in 0..11 {
            key.clear();
            val.clear();
            for _ in 0..i {
                key.push(i);
                val.push(i);
            }

            assert_eq!(left_base.get(&key).unwrap(), &val);
        }
        for i in 11..20 {
            key.clear();
            val.clear();
            for _ in 0..i {
                key.push(i);
                val.push(i);
            }

            assert_eq!(right_base.get(&key).unwrap(), &val);
        }
    }

    #[test]
    fn inner() {
        const PAGE_SIZE: usize = 1024;
        let mut inner_mut = InnerPageMut::new(PAGE_SIZE);
        let mut key = Vec::new();

        for i in 0..10 {
            key.clear();
            for _ in 0..i {
                key.push(i);
            }
            inner_mut
                .apply_delta(&InnerDelta::Set {
                    key: &key,
                    left_page_id: i as u64,
                })
                .unwrap();
        }

        let mut inner_buf: PageBuffer<PAGE_SIZE> = inner_mut.unpack();
        for i in 10..20 {
            key.clear();
            for _ in 0..i {
                key.push(i);
            }

            inner_buf
                .write_delta(&InnerDelta::Set {
                    key: &key,
                    left_page_id: i as u64,
                })
                .unwrap();
        }

        let inner_page = inner_buf.read().unwrap_as_inner();
        for (delta, i) in inner_page.iter_deltas().zip((10..20).rev()) {
            key.clear();
            for _ in 0..i {
                key.push(i);
            }

            match delta {
                InnerDelta::Set {
                    key: d_key,
                    left_page_id,
                } => {
                    assert_eq!(&key, d_key);
                    assert_eq!(left_page_id, i as u64);
                }
            }
        }

        let inner_base = inner_page.base();
        for i in 0..10 {
            key.clear();
            for _ in 0..i {
                key.push(i);
            }
            assert_eq!(inner_base.find_child(&key).1, i as u64);
        }

        let mut compacted_mut = InnerPageMut::new(PAGE_SIZE);
        compacted_mut.compact(inner_page);
        let mut to_mut = InnerPageMut::new(PAGE_SIZE);
        compacted_mut.split_into(&mut to_mut, &mut key);

        let left_buf: PageBuffer<PAGE_SIZE> = to_mut.unpack();
        let right_buf: PageBuffer<PAGE_SIZE> = compacted_mut.unpack();
        let left_base = left_buf.read().unwrap_as_inner().base();
        let right_base = right_buf.read().unwrap_as_inner().base();

        for i in 0..10 {
            key.clear();
            for _ in 0..i {
                key.push(i);
            }

            assert_eq!(left_base.find_child(&key).1, i as u64);
        }
        for i in 11..20 {
            key.clear();
            for _ in 0..i {
                key.push(i);
            }

            assert_eq!(right_base.find_child(&key).1, i as u64);
        }
    }
}
