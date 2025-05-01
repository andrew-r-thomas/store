use std::{
    alloc::{Layout, alloc_zeroed, handle_alloc_error},
    slice,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
};

use crate::PageId;

pub struct PageBuffer<const SIZE: usize> {
    ptr: *mut u8,
    read_offset: AtomicUsize,
    write_offset: AtomicIsize,
}

impl<const SIZE: usize> PageBuffer<SIZE> {
    pub fn new<'n>() -> Self {
        let layout = Layout::array::<u8>(SIZE).unwrap();
        assert!(SIZE <= isize::MAX as usize);

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout)
        }

        Self {
            ptr,
            read_offset: AtomicUsize::new(SIZE),
            write_offset: AtomicIsize::new(SIZE as isize),
        }
    }
    pub fn read(&self) -> Page<'_> {
        let read_offset = self.read_offset.load(Ordering::Acquire);
        Page::from(unsafe { slice::from_raw_parts(self.ptr.add(read_offset), SIZE - read_offset) })
    }
    pub fn write_delta<D: Delta>(&self, delta: &D) -> WriteRes<'_> {
        let len = delta.len();
        let old_write_offset = self.write_offset.fetch_sub(len as isize, Ordering::SeqCst);
        let new_write_offset = old_write_offset - len as isize;

        if new_write_offset <= 0 {
            if old_write_offset > 0 {
                // we caused the buffer to be sealed
                while self.read_offset.load(Ordering::Acquire) != old_write_offset as usize {}
                return WriteRes::Sealer(Page::from(unsafe {
                    slice::from_raw_parts(
                        self.ptr.add(old_write_offset as usize),
                        SIZE - old_write_offset as usize,
                    )
                }));
            }
            // the buffer is already sealed
            return WriteRes::Sealed;
        }

        let buf =
            unsafe { slice::from_raw_parts_mut(self.ptr.add(new_write_offset as usize), len) };
        delta.write_to_buf(buf);

        while let Err(_) = self.read_offset.compare_exchange_weak(
            old_write_offset as usize,
            new_write_offset as usize,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {}

        WriteRes::Ok
    }

    #[inline]
    pub fn raw_buffer<'r>(&'r mut self) -> &'r mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, SIZE) }
    }

    pub fn from_raw_buffer(buf: &mut [u8], top: usize) -> Self {
        assert!(buf.len() == SIZE);
        let ptr = buf.as_mut_ptr();
        Self {
            ptr,
            read_offset: AtomicUsize::new(top),
            write_offset: AtomicIsize::new(top as isize),
        }
    }
}

pub enum WriteRes<'w> {
    Ok,
    Sealed,
    Sealer(Page<'w>),
}

pub trait Delta {
    fn len(&self) -> usize;
    fn write_to_buf(&self, buf: &mut [u8]);
}

// ================================================================================================
// page access types
// ================================================================================================
//
// TODO: pages seem to make the most sense sorted bottom to top, still easy for reads, and makes
// building the mutable pages much simpler, so i think we need to adjust some of the logic for that

pub enum Page<'p> {
    Leaf(LeafPage<'p>),
    Inner(InnerPage<'p>),
}
impl<'p> Page<'p> {
    const LEAF_ID: u8 = 0;
    const INNER_ID: u8 = 1;
    const FOOTER_SIZE: usize = 9;

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
            buf[buf.len() - Self::FOOTER_SIZE..buf.len() - 1]
                .try_into()
                .unwrap(),
        ) as usize;
        match *buf.last().unwrap() {
            Self::LEAF_ID => Self::Leaf(LeafPage::from(&buf[0..buf.len() - Self::FOOTER_SIZE])),
            Self::INNER_ID => Self::Inner(InnerPage::from((
                &buf[0..buf.len() - Self::FOOTER_SIZE],
                base_len,
            ))),
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
impl<'p> From<(&'p [u8], usize)> for InnerPage<'p> {
    fn from((buf, base_len): (&'p [u8], usize)) -> Self {
        Self { buf, base_len }
    }
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
            None => best = Some(base_best),
            Some((k, _)) => {
                if base_best.0 < k {
                    best = Some(base_best);
                }
            }
        }

        best.unwrap().1
    }
    fn iter_deltas(&self) -> InnerDeltaIter<'p> {
        InnerDeltaIter::from(&self.buf[..self.buf.len() - self.base_len])
    }
    fn base(&self) -> InnerBase<'p> {
        InnerBase::from(&self.buf[self.buf.len() - self.base_len..])
    }
}

struct InnerDeltaIter<'i> {
    buf: &'i [u8],
    top: usize,
    bottom: usize,
}
impl InnerDeltaIter<'_> {
    const SET_ID: u8 = 1;
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
        match self.buf[self.top] {
            Self::SET_ID => {
                let key_len =
                    u32::from_be_bytes(self.buf[self.top + 1..self.top + 5].try_into().unwrap())
                        as usize;
                let out = Some(InnerDelta::Set {
                    key: &self.buf[self.top + 5..self.top + 5 + key_len],
                    left_page_id: u64::from_be_bytes(
                        self.buf[self.top + 5 + key_len..self.top + 5 + key_len + 8]
                            .try_into()
                            .unwrap(),
                    ),
                });
                self.top += 5 + key_len + 8 + 5;
                out
            }
            _ => panic!(),
        }
    }
}
impl DoubleEndedIterator for InnerDeltaIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        match self.buf[self.bottom - 1] {
            Self::SET_ID => {
                let key_len = u32::from_be_bytes(
                    self.buf[self.bottom - 5..self.bottom - 1]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let out = Some(InnerDelta::Set {
                    key: &self.buf[self.bottom - (key_len + 8 + 5)..self.bottom - (8 + 5)],
                    left_page_id: u64::from_be_bytes(
                        self.buf[self.bottom - (8 + 5)..self.bottom - 5]
                            .try_into()
                            .unwrap(),
                    ),
                });
                self.bottom -= 5 + key_len + 8 + 5;
                out
            }
            _ => panic!(),
        }
    }
}

pub enum InnerDelta<'c> {
    Set { key: &'c [u8], left_page_id: PageId },
    // Del(&'c [u8]),
}
impl Delta for InnerDelta<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Set { key, .. } => 5 + 8 + key.len() + 5,
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert!(self.len() == buf.len());
        match self {
            Self::Set { key, left_page_id } => {
                let key_len = &(key.len() as u32).to_be_bytes();
                let mut cursor = 0;

                buf[cursor] = 1;
                cursor += 1;

                buf[cursor..cursor + 4].copy_from_slice(key_len);
                cursor += 4;

                buf[cursor..cursor + 8].copy_from_slice(&left_page_id.to_be_bytes());
                cursor += 8;

                buf[cursor..cursor + key.len()].copy_from_slice(key);
                cursor += key.len();

                buf[cursor..cursor + 4].copy_from_slice(key_len);
                cursor += 4;

                buf[cursor] = 1;
            }
        }
    }
}

struct InnerBase<'b> {
    buf: &'b [u8],
}
impl<'b> From<&'b [u8]> for InnerBase<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}
impl<'b> InnerBase<'b> {
    fn find_child(self, target: &'b [u8]) -> (&'b [u8], PageId) {
        let num_entries = self.num_entries() as usize;
        let mut cursor = (12 * num_entries) + 10;
        for e in 0..num_entries {
            let key_len =
                u32::from_be_bytes(self.buf[10 + (e * 4)..10 + (e * 4) + 4].try_into().unwrap())
                    as usize;
            let key = &self.buf[cursor..cursor + key_len];
            if target <= key {
                let left_start = 10 + (num_entries * 4) + (e * 8);
                let left_end = left_start + 8;
                let left = u64::from_be_bytes(self.buf[left_start..left_end].try_into().unwrap());
                return (key, left);
            }
            cursor += key_len;
        }

        // NOTE: returning target (or anything for the key) is a bit meaningless here, but at least
        // right now it doesn't seem like it'll mess anything up, but keep and eye out
        (target, self.right_page_id())
    }
    #[inline]
    fn num_entries(&self) -> u16 {
        u16::from_be_bytes(self.buf[0..2].try_into().unwrap())
    }
    #[inline]
    fn right_page_id(&self) -> PageId {
        u64::from_be_bytes(self.buf[2..10].try_into().unwrap())
    }
}

pub struct InnerPageMut<'p> {
    buf: &'p mut [u8],
    top: usize,
    bottom: usize,
}
impl<'p> From<&'p mut [u8]> for InnerPageMut<'p> {
    fn from(buf: &'p mut [u8]) -> Self {
        Self {
            bottom: buf.len() - 9,
            top: 2,
            buf,
        }
    }
}
impl InnerPageMut<'_> {
    pub fn new(len: usize) -> Self {
        let layout = Layout::array::<u8>(len).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout)
        }
        let buf = unsafe { slice::from_raw_parts_mut(ptr, len) };
        Self {
            buf,
            bottom: len - 9,
            top: 2,
        }
    }
    pub fn compact(&mut self, other: InnerPage) {
        assert!(self.top == 2 && self.bottom == self.buf.len() - 9);

        let base = other.base();
        let num_entries = base.num_entries() as usize;

        // copy the header
        self.buf[0..2 + 8 + (12 * num_entries)]
            .copy_from_slice(&base.buf[0..2 + 8 + (12 * num_entries)]);
        self.top = 2 + 8 + (12 * num_entries);

        // copy the keys
        let keys = &base.buf[2 + 8 + (12 * num_entries)..];
        self.buf[self.bottom - keys.len()..self.bottom].copy_from_slice(keys);
        self.bottom -= keys.len();

        for delta in other.iter_deltas().rev() {
            self.apply_delta(&delta).unwrap();
        }
    }
    pub fn apply_delta(&mut self, delta: &InnerDelta) -> Result<(), ()> {
        match delta {
            InnerDelta::Set { key, left_page_id } => {
                // NOTE: pretty sure we never do an update, only insert, and the inserts should be
                // in the middle of the keys, never at the end, so that should make things simpler
                if self.bottom - self.top < key.len() + 12 {
                    return Err(());
                }

                let num_entries = u16::from_be_bytes(self.buf[0..2].try_into().unwrap()) as usize;
                let mut cursor = self.buf.len() - 9;
                for e in 0..num_entries {
                    let key_len = u32::from_be_bytes(
                        self.buf[10 + (e * 12)..10 + (e * 12) + 4]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    if *key <= &self.buf[cursor - key_len..cursor] {
                        self.buf
                            .copy_within(10 + (e * 12)..self.top, 10 + (e * 12) + 12);
                        self.buf[10 + (e * 12)..10 + (e * 12) + 4]
                            .copy_from_slice(&(key.len() as u32).to_be_bytes());
                        self.buf[10 + (e * 12) + 4..10 + (e * 12) + 12]
                            .copy_from_slice(&left_page_id.to_be_bytes());
                        self.top += 12;

                        self.buf
                            .copy_within(self.bottom..cursor, self.bottom - key.len());
                        self.buf[cursor - key.len()..cursor].copy_from_slice(key);
                        self.bottom -= key.len();
                        self.buf[0..2].copy_from_slice(&((num_entries + 1) as u16).to_be_bytes());
                        return Ok(());
                    }
                    cursor -= key_len;
                }
                // add it to the end
                self.buf[self.top..self.top + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                self.buf[self.top + 4..self.top + 12].copy_from_slice(&left_page_id.to_be_bytes());
                self.top += 12;

                self.buf[self.bottom - key.len()..self.bottom].copy_from_slice(key);
                self.bottom -= key.len();

                self.buf[0..2].copy_from_slice(&((num_entries + 1) as u16).to_be_bytes());
                return Ok(());
            }
        }
    }
    pub fn split_into(&mut self, other: &mut Self, out: &mut Vec<u8>) {
        let num_entries = u16::from_be_bytes(self.buf[0..2].try_into().unwrap()) as usize;
        let middle_entry = num_entries / 2;

        let mut cursor = self.buf.len() - 9;
        for e in 0..middle_entry {
            let key_len = u32::from_be_bytes(
                self.buf[10 + (e * 12)..10 + (e * 12) + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let left_page_id = u64::from_be_bytes(
                self.buf[10 + (e * 12) + 4..10 + (e * 12) + 12]
                    .try_into()
                    .unwrap(),
            );
            let key = &self.buf[cursor - key_len..cursor];
            let delta = InnerDelta::Set { key, left_page_id };
            other.apply_delta(&delta).unwrap();
            cursor -= key_len;
        }

        let middle_key_len = u32::from_be_bytes(
            self.buf[10 + (middle_entry * 12)..10 + (middle_entry * 12) + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let middle_left_page_id = u64::from_be_bytes(
            self.buf[10 + (middle_entry * 12) + 4..10 + (middle_entry * 12) + 12]
                .try_into()
                .unwrap(),
        );
        let middle_key = &self.buf[cursor - middle_key_len..cursor];
        out.extend(middle_key);
        self.buf[2..10].copy_from_slice(&middle_left_page_id.to_be_bytes());

        self.buf
            .copy_within(10 + (middle_entry * 12) + 12..self.top, 10);
        self.top -= (middle_entry * 12) + 12;
        self.buf.copy_within(
            self.bottom..cursor - middle_key_len,
            (self.buf.len() - 9) - ((cursor - middle_key_len) - self.bottom),
        );
        self.bottom = (self.buf.len() - 9) - ((cursor - middle_key_len) - self.bottom);
    }
    pub fn unpack<const PAGE_SIZE: usize>(self) -> PageBuffer<PAGE_SIZE> {
        assert!(self.buf.len() == PAGE_SIZE);
        self.buf.copy_within(0..self.top, self.bottom - self.top);
        let len = (self.top + (PAGE_SIZE - (9 + self.bottom))) as u64;
        self.buf[PAGE_SIZE - 9..PAGE_SIZE - 1].copy_from_slice(&len.to_be_bytes());
        PageBuffer::from_raw_buffer(self.buf, self.bottom - self.top)
    }
    pub fn set_right_page_id(&mut self, page_id: PageId) {
        self.buf[2..10].copy_from_slice(&page_id.to_be_bytes());
    }
}

// leaf page ======================================================================================
//
// TODO: get all these indexes in types or constants etc, easy to mess them up

pub struct LeafPage<'p> {
    buf: &'p [u8],
}
impl<'p> From<&'p [u8]> for LeafPage<'p> {
    fn from(buf: &'p [u8]) -> Self {
        Self { buf }
    }
}
impl<'p> LeafPage<'p> {
    pub fn get(self, target: &[u8]) -> Option<&'p [u8]> {
        for delta in self.iter_deltas() {
            match delta {
                LeafDelta::Set { key, val } => {
                    if target == key {
                        return Some(val);
                    }
                }
                LeafDelta::Del(key) => {
                    if target == key {
                        return None;
                    }
                }
            }
        }

        self.base().get(target)
    }
    fn iter_deltas(&self) -> LeafDeltaIter<'p> {
        LeafDeltaIter::from(
            &self.buf[self.buf.len() - (9 + self.base_len() as usize)..self.buf.len() - 9],
        )
    }
    fn base(&self) -> LeafBase<'p> {
        let base_len = self.base_len() as usize;
        LeafBase::from(&self.buf[self.buf.len() - (9 + base_len)..self.buf.len() - 9])
    }
    #[inline]
    fn base_len(&self) -> u64 {
        u64::from_be_bytes(
            self.buf[self.buf.len() - 9..self.buf.len() - 1]
                .try_into()
                .unwrap(),
        )
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
impl LeafDeltaIter<'_> {
    const SET_ID: u8 = 1;
    const DEL_ID: u8 = 2;

    // fn reset(&mut self) {
    //     self.top = 0;
    //     self.bottom = self.buf.len();
    // }
}
impl<'i> Iterator for LeafDeltaIter<'i> {
    type Item = LeafDelta<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        match self.buf[self.top] {
            Self::SET_ID => {
                let key_len =
                    u32::from_be_bytes(self.buf[self.top + 1..self.top + 5].try_into().unwrap())
                        as usize;
                let val_len =
                    u32::from_be_bytes(self.buf[self.top + 5..self.top + 9].try_into().unwrap())
                        as usize;
                let out = Some(LeafDelta::Set {
                    key: &self.buf[self.top + 9..self.top + 9 + key_len],
                    val: &self.buf[self.top + 9 + key_len..self.top + 9 + key_len + val_len],
                });
                self.top += 9 + key_len + val_len + 9;
                out
            }
            Self::DEL_ID => {
                let key_len =
                    u32::from_be_bytes(self.buf[self.top + 1..self.top + 5].try_into().unwrap())
                        as usize;
                let out = Some(LeafDelta::Del(
                    &self.buf[self.top + 5..self.top + 5 + key_len],
                ));
                self.top += 5 + key_len + 5;
                out
            }
            _ => panic!(),
        }
    }
}
impl<'i> DoubleEndedIterator for LeafDeltaIter<'i> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        match self.buf[self.bottom - 1] {
            Self::SET_ID => {
                let key_len = u32::from_be_bytes(
                    self.buf[self.bottom - 5..self.bottom - 1]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let val_len = u32::from_be_bytes(
                    self.buf[self.bottom - 9..self.bottom - 5]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let out = Some(LeafDelta::Set {
                    key: &self.buf
                        [self.bottom - (9 + key_len + val_len)..self.bottom - (9 + val_len)],
                    val: &self.buf[self.bottom - (9 + val_len)..self.bottom - 9],
                });
                self.bottom -= 9 + key_len + val_len + 9;
                out
            }
            Self::DEL_ID => {
                let key_len = u32::from_be_bytes(
                    self.buf[self.bottom - 5..self.bottom - 1]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let out = Some(LeafDelta::Del(
                    &self.buf[self.bottom - (5 + key_len)..self.bottom - 5],
                ));
                self.bottom -= 5 + key_len + 5;
                out
            }
            _ => panic!(),
        }
    }
}
pub enum LeafDelta<'c> {
    Set { key: &'c [u8], val: &'c [u8] },
    Del(&'c [u8]),
}
impl Delta for LeafDelta<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Set { key, val } => key.len() + val.len() + 18,
            Self::Del(key) => key.len() + 10,
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert!(self.len() == buf.len());
        let mut cursor = 0;
        match self {
            Self::Set { key, val } => {
                // top delta code
                buf[cursor] = 1;
                cursor += 1;

                // top lengths
                buf[cursor..cursor + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                buf[cursor + 4..cursor + 8].copy_from_slice(&(val.len() as u32).to_be_bytes());
                cursor += 8;

                // key and val
                buf[cursor..cursor + key.len()].copy_from_slice(key);
                buf[cursor + key.len()..cursor + key.len() + val.len()].copy_from_slice(val);
                cursor += key.len() + val.len();

                // bottom lengths
                buf[cursor..cursor + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                buf[cursor + 4..cursor + 8].copy_from_slice(&(val.len() as u32).to_be_bytes());
                cursor += 8;

                // bottom delta code
                buf[cursor] = 1;
            }
            Self::Del(key) => {
                // top delta code
                buf[cursor] = 2;
                cursor += 1;

                // top len
                buf[cursor..cursor + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                cursor += 4;

                // key
                buf[cursor..cursor + key.len()].copy_from_slice(key);
                cursor += key.len();

                // bottom len
                buf[cursor..cursor + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                cursor += 4;

                // bottom delta code
                buf[cursor] = 2;
            }
        }
    }
}

struct LeafBase<'b> {
    buf: &'b [u8],
}
impl<'b> From<&'b [u8]> for LeafBase<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}
impl<'b> LeafBase<'b> {
    fn get(self, target: &[u8]) -> Option<&'b [u8]> {
        let num_entries = self.num_entries() as usize;
        let mut cursor = 2 + (num_entries * 8);
        for e in 0..num_entries {
            let key_len =
                u32::from_be_bytes(self.buf[2 + (e * 8)..2 + (e * 8) + 4].try_into().unwrap())
                    as usize;
            let val_len = u32::from_be_bytes(
                self.buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let key = &self.buf[cursor..cursor + key_len];
            if target == key {
                return Some(&self.buf[cursor + key_len..cursor + key_len + val_len]);
            }
            cursor += key_len + val_len;
        }
        None
    }
    #[inline]
    fn num_entries(&self) -> u16 {
        u16::from_be_bytes(self.buf[1..2].try_into().unwrap())
    }
}

pub struct LeafPageMut<'p> {
    buf: &'p mut [u8],
    top: usize,
    bottom: usize,
}
impl<'p> From<&'p mut [u8]> for LeafPageMut<'p> {
    fn from(buf: &'p mut [u8]) -> Self {
        Self {
            top: 2,
            bottom: buf.len() - 9,
            buf,
        }
    }
}
impl LeafPageMut<'_> {
    /// NOTE: this is mostly temporary until [`crate::pool::Pool`] is finished
    pub fn new(len: usize) -> Self {
        let layout = Layout::array::<u8>(len).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout)
        }
        let buf = unsafe { slice::from_raw_parts_mut(ptr, len) };
        Self {
            buf,
            bottom: len - 9,
            top: 2,
        }
    }
    pub fn compact(&mut self, other: LeafPage) {
        // we should have a fresh one when we're compacting
        assert!(self.top == 2 && self.bottom == self.buf.len() - 9);

        let base = other.base();

        // copy the header
        let num_entries = base.num_entries() as usize;
        self.buf[0..2 + (num_entries * 8)].copy_from_slice(&base.buf[0..2 + (num_entries * 8)]);
        self.top = 2 + (num_entries * 8);

        // copy the actual data
        self.buf[self.bottom - (base.buf.len() - self.top)..self.bottom]
            .copy_from_slice(&base.buf[self.top..]);
        self.bottom -= base.buf.len() - self.top;

        // apply the deltas in reverse order
        for delta in other.iter_deltas().rev() {
            self.apply_delta(&delta).unwrap();
        }
    }
    pub fn apply_delta(&mut self, delta: &LeafDelta) -> Result<(), ()> {
        match delta {
            LeafDelta::Set { key, val } => {
                let num_entries = Self::num_entries(self.buf) as usize;
                let mut cursor = self.buf.len() - 9;
                for e in 0..num_entries {
                    let key_len = u32::from_be_bytes(
                        self.buf[2 + (e * 8)..2 + (e * 8) + 4].try_into().unwrap(),
                    ) as usize;
                    let val_len = u32::from_be_bytes(
                        self.buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let k = &self.buf[cursor - (key_len + val_len)..cursor - val_len];
                    if *key < k {
                        // insert

                        // check if we have room
                        if self.bottom - self.top < key.len() + val.len() + 8 {
                            return Err(());
                        }

                        // move the header and write the new slot data
                        self.buf.copy_within(2 + (e * 8)..self.top, 2 + (e * 8) + 8);
                        self.buf[2 + (e * 8)..2 + (e * 8) + 4]
                            .copy_from_slice(&(key.len() as u32).to_be_bytes());
                        self.buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
                            .copy_from_slice(&(val.len() as u32).to_be_bytes());
                        Self::set_num_entries(self.buf, (num_entries as u16) + 1);
                        self.top += 8;

                        // move the actual data and write the new data
                        self.buf.copy_within(
                            cursor - self.bottom..cursor,
                            self.bottom - (key.len() + val.len()),
                        );
                        self.buf[cursor - (key.len() + val.len())..cursor - val.len()]
                            .copy_from_slice(key);
                        self.buf[cursor - val.len()..cursor].copy_from_slice(val);
                        self.bottom -= key.len() + val.len();

                        return Ok(());
                    } else if *key == k {
                        // update
                        let gap = (key.len() + val.len()) as isize - (key_len + val_len) as isize;
                        if ((self.bottom - self.top) as isize) < gap {
                            return Err(());
                        }

                        // update the slot data
                        self.buf[2 + (e * 8)..2 + (e * 8) + 4]
                            .copy_from_slice(&(key.len() as u32).to_be_bytes());
                        self.buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
                            .copy_from_slice(&(val.len() as u32).to_be_bytes());

                        // update and move the actual data
                        let new_bottom = (self.bottom as isize - gap) as usize;
                        self.buf
                            .copy_within(self.bottom..cursor - (key_len + val_len), new_bottom);
                        self.buf[cursor - (key.len() + val.len())..cursor - val.len()]
                            .copy_from_slice(key);
                        self.buf[cursor - val.len()..cursor].copy_from_slice(val);
                        if let Some(s) = self.buf.get_mut(self.bottom..new_bottom) {
                            s.fill(0);
                        }
                        self.bottom = new_bottom;

                        return Ok(());
                    }
                    cursor -= key_len + val_len;
                }

                // key is greater than anything existing, so goes at the very end
                if self.bottom - self.top < key.len() + val.len() + 8 {
                    return Err(());
                }

                self.buf[self.top..self.top + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                self.buf[self.top + 4..self.top + 8]
                    .copy_from_slice(&(val.len() as u32).to_be_bytes());
                self.top += 8;
                Self::set_num_entries(self.buf, (num_entries as u16) + 1);

                self.buf[self.bottom - (key.len() + val.len())..self.bottom - val.len()]
                    .copy_from_slice(key);
                self.buf[self.bottom - val.len()..self.bottom].copy_from_slice(val);
                self.bottom -= key.len() + val.len();

                return Ok(());
            }
            LeafDelta::Del(key) => {
                let num_entries = Self::num_entries(self.buf) as usize;
                let mut cursor = self.buf.len() - 9;
                for e in 0..num_entries {
                    let key_len = u32::from_be_bytes(
                        self.buf[2 + (e * 8)..2 + (e * 8) + 4].try_into().unwrap(),
                    ) as usize;
                    let val_len = u32::from_be_bytes(
                        self.buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let k = &self.buf[cursor - (key_len + val_len)..cursor - val_len];
                    if *key == k {
                        self.buf.copy_within(2 + (e * 8) + 8..self.top, 2 + (e * 8));
                        self.buf[self.top..self.top + 8].fill(0);
                        Self::set_num_entries(self.buf, (num_entries as u16) - 1);
                        self.top -= 8;

                        self.buf.copy_within(
                            self.bottom..cursor - (key_len + val_len),
                            self.bottom + key_len + val_len,
                        );
                        self.buf[self.bottom..self.bottom + key_len + val_len].fill(0);
                        self.bottom += key_len + val_len;

                        return Ok(());
                    }

                    cursor -= key_len + val_len;
                }
                // we're trying to delete something that isn't in here
                panic!()
            }
        }
    }
    /// NOTE: we always split left
    pub fn split_into(&mut self, other: &mut Self, out: &mut Vec<u8>) {
        // we always want to split into a fresh buffer
        assert!(other.top == 2 && other.bottom == self.buf.len() - 9);
        let num_entries = Self::num_entries(self.buf) as usize;
        let middle_entry = num_entries / 2;

        // ingest data up to and including middle_entry into other
        let mut cursor = self.buf.len() - 9;
        for e in 0..middle_entry {
            let key_len =
                u32::from_be_bytes(self.buf[2 + (e * 8)..2 + (e * 8) + 4].try_into().unwrap())
                    as usize;
            let val_len = u32::from_be_bytes(
                self.buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
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

        let key_len = u32::from_be_bytes(
            self.buf[2 + (middle_entry * 8)..2 + (middle_entry * 8) + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let val_len = u32::from_be_bytes(
            self.buf[2 + (middle_entry * 8) + 4..2 + (middle_entry * 8) + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let delta = LeafDelta::Set {
            key: &self.buf[cursor - (key_len + val_len)..cursor - val_len],
            val: &self.buf[cursor - val_len..cursor],
        };
        other.apply_delta(&delta).unwrap();
        cursor -= key_len + val_len;
        out.extend(&self.buf[cursor - (key_len + val_len)..cursor - val_len]);

        // copy up header info
        self.buf
            .copy_within(2 + (8 * middle_entry) + 8..self.top, 2);
        self.top -= (8 * middle_entry) + 8;
        // copy down data
        self.buf
            .copy_within(self.bottom..cursor, self.bottom + (cursor - 9));
        self.bottom += cursor - 9;
        // zero out the middle
        self.buf[self.top..self.bottom].fill(0);
        Self::set_num_entries(self.buf, (num_entries - (middle_entry + 1)) as u16);
    }
    pub fn unpack<const PAGE_SIZE: usize>(self) -> PageBuffer<PAGE_SIZE> {
        assert!(self.buf.len() == PAGE_SIZE);
        self.buf.copy_within(0..self.top, self.bottom - self.top);
        let len = (self.top + (PAGE_SIZE - (9 + self.bottom))) as u64;
        self.buf[PAGE_SIZE - 9..PAGE_SIZE - 1].copy_from_slice(&len.to_be_bytes());
        PageBuffer::from_raw_buffer(self.buf, self.bottom - self.top)
    }
    #[inline]
    fn num_entries(buf: &mut [u8]) -> u16 {
        u16::from_be_bytes(buf[0..2].try_into().unwrap())
    }
    #[inline]
    fn set_num_entries(buf: &mut [u8], num_entries: u16) {
        buf[0..2].copy_from_slice(&num_entries.to_be_bytes());
    }
}
