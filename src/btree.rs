use crate::{PageId, buffer::PageBuffer, page_dir::PageDirectory};

use std::{
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
};

pub struct BTree<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> {
    page_dir: PageDirectory<BLOCK_SIZE, PAGE_SIZE>,
    root: AtomicU64,
}
impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> BTree<BLOCK_SIZE, PAGE_SIZE> {
    pub fn new() -> Self {
        todo!()
    }
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let root_id = self.root_id();
        let mut current = Page::from(self.page_dir.get(root_id));
        while let Page::Inner(inner_page) = current {
            current = Page::from(self.page_dir.get(inner_page.find_child(key)));
        }
        let leaf = current.unwrap_as_leaf();
        leaf.get(key)
    }
    pub fn set(
        &self,
        key: &[u8],
        _val: &[u8],
        path_buf: &mut Vec<(PageId, &PageBuffer<PAGE_SIZE>)>,
    ) -> bool {
        // traverse the tree and load up the path_buf with the path
        let root_id = self.root_id();
        let mut current = Page::from(self.page_dir.get(root_id));
        while let Page::Inner(inner_page) = current {
            let child_id = inner_page.find_child(key);
            path_buf.push(child_id);
            current = Page::from(self.page_dir.get(child_id));
        }

        todo!()
    }
    pub fn delete(&self, _key: &[u8]) -> bool {
        todo!()
    }
    pub fn iter_range(&self, _range: Range<&[u8]>) {
        todo!()
    }

    #[inline]
    fn root_id(&self) -> PageId {
        self.root.load(Ordering::Acquire)
    }
}

// ================================================================================================
// page access types
// ================================================================================================

enum Page<'p> {
    Leaf(LeafPage<'p>),
    Inner(InnerPage<'p>),
}
impl<'p> Page<'p> {
    const LEAF_ID: u8 = 0;
    const INNER_ID: u8 = 1;

    fn unwrap_as_leaf(self) -> LeafPage<'p> {
        match self {
            Page::Leaf(leaf) => leaf,
            _ => panic!(),
        }
    }
    fn unwrap_as_inner(self) -> InnerPage<'p> {
        match self {
            Page::Inner(inner) => inner,
            _ => panic!(),
        }
    }
}
impl<'p, const PAGE_SIZE: usize> From<&'p PageBuffer<PAGE_SIZE>> for Page<'p> {
    fn from(page_buff: &'p PageBuffer<PAGE_SIZE>) -> Self {
        let buf = page_buff.read();
        match *buf.last().unwrap() {
            Self::LEAF_ID => Self::Leaf(LeafPage::from(&buf[0..buf.len() - 1])),
            Self::INNER_ID => Self::Inner(InnerPage::from(&buf[0..buf.len() - 1])),
            _ => panic!(),
        }
    }
}

// inner page =====================================================================================
//
// NOTE: for now we're assuming the right page id will not be updated (at least for a given
// instance of a page/pagebuffer, but it may get updated after sealing/compaction etc)

struct InnerPage<'p> {
    buf: &'p [u8],
}
impl<'p> From<&'p [u8]> for InnerPage<'p> {
    fn from(buf: &'p [u8]) -> Self {
        Self { buf }
    }
}
impl<'p> InnerPage<'p> {
    fn find_child(&self, target: &[u8]) -> PageId {
        let mut best = None;
        for chunk in self.iter_chunks() {
            match chunk {
                InnerChunk::Set { key, left_page_id } => {
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
                InnerChunk::Base(base_page) => {
                    let base_best = base_page.find_child(target);
                    match best {
                        None => best = Some(base_best),
                        Some((k, _)) => {
                            if base_best.0 < k {
                                best = Some(base_best);
                            }
                        }
                    }
                }
                _ => panic!(),
            }
        }
        best.unwrap().1
    }
    fn iter_chunks(&self) -> InnerChunkIter<'p> {
        self.into()
    }
}

struct InnerChunkIter<'i> {
    buf: &'i [u8],
    top: usize,
    bottom: usize,
}
impl<'i> From<&InnerPage<'i>> for InnerChunkIter<'i> {
    fn from(page: &InnerPage<'i>) -> Self {
        Self {
            buf: page.buf,
            top: 0,
            bottom: page.buf.len(),
        }
    }
}
impl InnerChunkIter<'_> {
    const BASE_ID: u8 = 0;
    const SET_ID: u8 = 1;
    const DEL_ID: u8 = 2;
}
impl<'i> Iterator for InnerChunkIter<'i> {
    type Item = InnerChunk<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        match self.buf[self.top] {
            Self::BASE_ID => {
                let out = Some(InnerChunk::Base(InnerBase::from(
                    &self.buf[self.top + 1..self.buf.len() - 9],
                )));
                self.top = self.buf.len();
                out
            }
            Self::SET_ID => {
                let key_len =
                    u32::from_be_bytes(self.buf[self.top + 1..self.top + 5].try_into().unwrap())
                        as usize;
                let out = Some(InnerChunk::Set {
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
            Self::DEL_ID => todo!(),
            _ => panic!(),
        }
    }
}
impl DoubleEndedIterator for InnerChunkIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        if self.bottom == self.buf.len() {
            let base_len =
                u64::from_be_bytes(self.buf[self.buf.len() - 8..].try_into().unwrap()) as usize;
            let out = Some(InnerChunk::Base(InnerBase::from(
                &self.buf[self.buf.len() - (8 + base_len)..self.buf.len() - 8],
            )));
            self.bottom -= 1 + base_len + 8;
            return out;
        }
        match self.buf[self.bottom - 1] {
            Self::SET_ID => {
                let key_len = u32::from_be_bytes(
                    self.buf[self.bottom - 5..self.bottom - 1]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let out = Some(InnerChunk::Set {
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
            Self::DEL_ID => todo!(),
            _ => panic!(),
        }
    }
}

enum InnerChunk<'c> {
    Set { key: &'c [u8], left_page_id: PageId },
    Del(&'c [u8]),
    Base(InnerBase<'c>),
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

// leaf page ======================================================================================
//
// TODO: get all these indexes in types or constants etc, easy to mess them up

struct LeafPage<'p> {
    buf: &'p [u8],
}
impl<'p> From<&'p [u8]> for LeafPage<'p> {
    fn from(buf: &'p [u8]) -> Self {
        Self { buf }
    }
}
impl<'p> LeafPage<'p> {
    fn get(self, target: &[u8]) -> Option<&'p [u8]> {
        for chunk in self.iter_chunks() {
            match chunk {
                LeafChunk::Set { key, val } => {
                    if target == key {
                        return Some(val);
                    }
                }
                LeafChunk::Del(key) => {
                    if target == key {
                        return None;
                    }
                }
                LeafChunk::Base(leaf_base) => return leaf_base.get(target),
            }
        }
        None
    }
    fn iter_chunks(self) -> LeafChunkIter<'p> {
        self.into()
    }
}
struct LeafChunkIter<'i> {
    buf: &'i [u8],
    top: usize,
    bottom: usize,
}
impl<'i> From<LeafPage<'i>> for LeafChunkIter<'i> {
    fn from(page: LeafPage<'i>) -> Self {
        Self {
            buf: page.buf,
            top: 0,
            bottom: page.buf.len(),
        }
    }
}
impl LeafChunkIter<'_> {
    const BASE_ID: u8 = 0;
    const SET_ID: u8 = 1;
    const DEL_ID: u8 = 2;

    fn reset(&mut self) {
        self.top = 0;
        self.bottom = self.buf.len();
    }
}
impl<'i> Iterator for LeafChunkIter<'i> {
    type Item = LeafChunk<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        match self.buf[self.top] {
            Self::BASE_ID => {
                let out = Some(LeafChunk::Base(LeafBase::from(
                    &self.buf[self.top + 1..self.buf.len() - 9],
                )));
                self.top = self.buf.len();
                out
            }
            Self::SET_ID => {
                let key_len =
                    u32::from_be_bytes(self.buf[self.top + 1..self.top + 5].try_into().unwrap())
                        as usize;
                let val_len =
                    u32::from_be_bytes(self.buf[self.top + 5..self.top + 9].try_into().unwrap())
                        as usize;
                let out = Some(LeafChunk::Set {
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
                let out = Some(LeafChunk::Del(
                    &self.buf[self.top + 5..self.top + 5 + key_len],
                ));
                self.top += 5 + key_len + 5;
                out
            }
            _ => panic!(),
        }
    }
}
impl<'i> DoubleEndedIterator for LeafChunkIter<'i> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.top >= self.bottom {
            return None;
        }
        if self.bottom == self.buf.len() {
            let base_len = u64::from_be_bytes(
                self.buf[self.bottom - 9..self.bottom - 1]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let out = Some(LeafChunk::Base(LeafBase::from(
                &self.buf[self.bottom - (9 + base_len)..self.bottom - 9],
            )));
            self.bottom -= base_len + 9 + 1;
            return out;
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
                let out = Some(LeafChunk::Set {
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
                let out = Some(LeafChunk::Del(
                    &self.buf[self.bottom - (5 + key_len)..self.bottom - 5],
                ));
                self.bottom -= 5 + key_len + 5;
                out
            }
            _ => panic!(),
        }
    }
}
enum LeafChunk<'c> {
    Set { key: &'c [u8], val: &'c [u8] },
    Del(&'c [u8]),
    Base(LeafBase<'c>),
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
