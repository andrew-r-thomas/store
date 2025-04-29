use crate::{
    PageId,
    buffer::{Delta, PageBuffer, WriteRes},
    page_dir::PageDirectory,
};

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
    pub fn set(&self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        let root_id = self.root_id();
        let mut current = self.page_dir.get(root_id);

        // TODO: figure out right way to do this, if we need references, it seems like there's
        // gonna be a lifetime issue
        let mut path = Vec::new();
        path.push((root_id, current));

        while let Page::Inner(inner_page) = Page::from(current) {
            let child_id = inner_page.find_child(key);
            current = self.page_dir.get(child_id);
            path.push((child_id, current));
        }

        let delta = LeafDelta::Set { key, val };
        let (leaf_id, leaf) = path.pop().unwrap();
        match leaf.write_delta(&delta) {
            WriteRes::Ok => Ok(()),
            WriteRes::Sealed => Err(()),
            WriteRes::Sealer(old) => {
                // this part can get pretty hairry, so here's the basic idea
                //
                // first we compact the page
                //
                // then we try to apply the set in place to the compacted page
                // if that works, we can just update the page dir and call it a day
                //
                // if it doesn't, the page needs to be split, splitting the page may cascade all
                // the way up to the root, which is what we have path for, this section will need
                // to be a loop (or maybe recurrsive, but i doubt it). this is the part we know the
                // least about how it will look, so take your time, and don't be afraid to throw it
                // away and try again

                // first we compact the page
                let old_page = LeafPage::from(old);
                let new_buf = PageBuffer::<PAGE_SIZE>::new();
                let mut new_page = LeafPageMut::from(new_buf);
                new_page.compact(old_page);

                if let Err(()) = new_page.apply_delta(delta) {
                    // we need to do a split
                    let mut to_page = LeafPageMut::from(PageBuffer::<PAGE_SIZE>::new());
                    let to_page_id = self.page_dir.pop_free();

                    let mut middle_key = Vec::new();
                    new_page.split_into(&mut to_page, &mut middle_key);

                    let new_buf = new_page.unpack();
                    self.page_dir.set(leaf_id, new_buf);
                    let to_buf = to_page.unpack();
                    self.page_dir.set(to_page_id, to_buf);

                    let mut delta = InnerDelta::Set {
                        key: &middle_key,
                        left_page_id: to_page_id,
                    };

                    'split: loop {
                        match path.pop() {
                            Some((_parent_id, parent)) => 'attempt: loop {
                                match parent.write_delta(&delta) {
                                    WriteRes::Ok => break 'split,
                                    WriteRes::Sealed => continue 'attempt,
                                    WriteRes::Sealer(old) => {
                                        let old_parent = InnerPage::from(old);
                                        let mut new_parent =
                                            InnerPageMut::from(PageBuffer::<PAGE_SIZE>::new());
                                        new_parent.compact(old_parent);
                                        if let Err(()) = new_parent.apply_delta(delta) {
                                            todo!()
                                        } else {
                                            todo!()
                                        }
                                    }
                                }
                            },
                            None => todo!("we split the root"),
                        }
                    }
                } else {
                    let new_buf = new_page.unpack();
                    self.page_dir.set(leaf_id, new_buf);
                }

                Ok(())
            }
        }
    }
    pub fn delete(&self, _key: &[u8]) -> Result<(), ()> {
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
//
// TODO: pages seem to make the most sense sorted bottom to top, still easy for reads, and makes
// building the mutable pages much simpler, so i think we need to adjust some of the logic for that

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
    // fn unwrap_as_inner(self) -> InnerPage<'p> {
    //     match self {
    //         Page::Inner(inner) => inner,
    //         _ => panic!(),
    //     }
    // }
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
        InnerDeltaIter::from(
            &self.buf[self.buf.len() - (9 + self.base_len() as usize)..self.buf.len() - 9],
        )
    }
    fn base(&self) -> InnerBase<'p> {
        let base_len = self.base_len() as usize;
        InnerBase::from(&self.buf[self.buf.len() - (9 + base_len)..self.buf.len() - 9])
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
impl InnerDeltaIter<'_> {
    const SET_ID: u8 = 1;
    const DEL_ID: u8 = 2;
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
            Self::DEL_ID => todo!(),
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
            Self::DEL_ID => todo!(),
            _ => panic!(),
        }
    }
}

enum InnerDelta<'c> {
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

struct InnerPageMut<const PAGE_SIZE: usize> {
    page: PageBuffer<PAGE_SIZE>,
    top: usize,
    bottom: usize,
}
impl<const PAGE_SIZE: usize> From<PageBuffer<PAGE_SIZE>> for InnerPageMut<PAGE_SIZE> {
    fn from(page: PageBuffer<PAGE_SIZE>) -> Self {
        Self {
            page,
            top: 2,
            bottom: PAGE_SIZE - 9,
        }
    }
}
impl<const PAGE_SIZE: usize> InnerPageMut<PAGE_SIZE> {
    fn compact(&mut self, _other: InnerPage) {
        todo!()
    }
    fn apply_delta(&mut self, _delta: InnerDelta) -> Result<(), ()> {
        todo!()
    }
    fn split_into(&mut self, _other: &mut Self, _out: &mut Vec<u8>) {
        todo!()
    }
    fn unpack(self) -> PageBuffer<PAGE_SIZE> {
        todo!()
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
enum LeafDelta<'c> {
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

struct LeafPageMut<const PAGE_SIZE: usize> {
    page: PageBuffer<PAGE_SIZE>,
    top: usize,
    bottom: usize,
}
impl<const PAGE_SIZE: usize> From<PageBuffer<PAGE_SIZE>> for LeafPageMut<PAGE_SIZE> {
    fn from(page: PageBuffer<PAGE_SIZE>) -> Self {
        Self {
            page,
            top: 2,
            bottom: PAGE_SIZE - 9,
        }
    }
}
impl<const PAGE_SIZE: usize> LeafPageMut<PAGE_SIZE> {
    fn compact(&mut self, other: LeafPage) {
        // we should have a fresh one when we're compacting
        assert!(self.top == 2 && self.bottom == PAGE_SIZE - 9);

        let base = other.base();
        let buf = self.page.raw_buffer();

        // copy the header
        let num_entries = base.num_entries() as usize;
        buf[0..2 + (num_entries * 8)].copy_from_slice(&base.buf[0..2 + (num_entries * 8)]);
        self.top = 2 + (num_entries * 8);

        // copy the actual data
        buf[self.bottom - (base.buf.len() - self.top)..self.bottom]
            .copy_from_slice(&base.buf[self.top..]);
        self.bottom -= base.buf.len() - self.top;

        // apply the deltas in reverse order
        for delta in other.iter_deltas().rev() {
            self.apply_delta(delta).unwrap();
        }
    }
    fn apply_delta(&mut self, delta: LeafDelta) -> Result<(), ()> {
        let buf = self.page.raw_buffer();
        match delta {
            LeafDelta::Set { key, val } => {
                let num_entries = Self::num_entries(buf) as usize;
                let mut cursor = PAGE_SIZE - 9;
                for e in 0..num_entries {
                    let key_len =
                        u32::from_be_bytes(buf[2 + (e * 8)..2 + (e * 8) + 4].try_into().unwrap())
                            as usize;
                    let val_len = u32::from_be_bytes(
                        buf[2 + (e * 8) + 4..2 + (e * 8) + 8].try_into().unwrap(),
                    ) as usize;
                    let k = &buf[cursor - (key_len + val_len)..cursor - val_len];
                    if key < k {
                        // insert

                        // check if we have room
                        if self.bottom - self.top < key.len() + val.len() + 8 {
                            return Err(());
                        }

                        // move the header and write the new slot data
                        buf.copy_within(2 + (e * 8)..self.top, 2 + (e * 8) + 8);
                        buf[2 + (e * 8)..2 + (e * 8) + 4]
                            .copy_from_slice(&(key.len() as u32).to_be_bytes());
                        buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
                            .copy_from_slice(&(val.len() as u32).to_be_bytes());
                        Self::set_num_entries(buf, (num_entries as u16) + 1);
                        self.top += 8;

                        // move the actual data and write the new data
                        buf.copy_within(
                            cursor - self.bottom..cursor,
                            self.bottom - (key.len() + val.len()),
                        );
                        buf[cursor - (key.len() + val.len())..cursor - val.len()]
                            .copy_from_slice(key);
                        buf[cursor - val.len()..cursor].copy_from_slice(val);
                        self.bottom -= key.len() + val.len();

                        return Ok(());
                    } else if key == k {
                        // update
                        let gap = (key.len() + val.len()) as isize - (key_len + val_len) as isize;
                        if ((self.bottom - self.top) as isize) < gap {
                            return Err(());
                        }

                        // update the slot data
                        buf[2 + (e * 8)..2 + (e * 8) + 4]
                            .copy_from_slice(&(key.len() as u32).to_be_bytes());
                        buf[2 + (e * 8) + 4..2 + (e * 8) + 8]
                            .copy_from_slice(&(val.len() as u32).to_be_bytes());

                        // update and move the actual data
                        let new_bottom = (self.bottom as isize - gap) as usize;
                        buf.copy_within(self.bottom..cursor - (key_len + val_len), new_bottom);
                        buf[cursor - (key.len() + val.len())..cursor - val.len()]
                            .copy_from_slice(key);
                        buf[cursor - val.len()..cursor].copy_from_slice(val);
                        if let Some(s) = buf.get_mut(self.bottom..new_bottom) {
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

                buf[self.top..self.top + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                buf[self.top + 4..self.top + 8].copy_from_slice(&(val.len() as u32).to_be_bytes());
                self.top += 8;
                Self::set_num_entries(buf, (num_entries as u16) + 1);

                buf[self.bottom - (key.len() + val.len())..self.bottom - val.len()]
                    .copy_from_slice(key);
                buf[self.bottom - val.len()..self.bottom].copy_from_slice(val);
                self.bottom -= key.len() + val.len();

                return Ok(());
            }
            LeafDelta::Del(key) => {
                let num_entries = Self::num_entries(buf) as usize;
                let mut cursor = PAGE_SIZE - 9;
                for e in 0..num_entries {
                    let key_len =
                        u32::from_be_bytes(buf[2 + (e * 8)..2 + (e * 8) + 4].try_into().unwrap())
                            as usize;
                    let val_len = u32::from_be_bytes(
                        buf[2 + (e * 8) + 4..2 + (e * 8) + 8].try_into().unwrap(),
                    ) as usize;
                    let k = &buf[cursor - (key_len + val_len)..cursor - val_len];
                    if key == k {
                        buf.copy_within(2 + (e * 8) + 8..self.top, 2 + (e * 8));
                        buf[self.top..self.top + 8].fill(0);
                        Self::set_num_entries(buf, (num_entries as u16) - 1);
                        self.top -= 8;

                        buf.copy_within(
                            self.bottom..cursor - (key_len + val_len),
                            self.bottom + key_len + val_len,
                        );
                        buf[self.bottom..self.bottom + key_len + val_len].fill(0);
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
    fn split_into(&mut self, other: &mut Self, out: &mut Vec<u8>) {
        // we always want to split into a fresh buffer
        assert!(other.top == 2 && other.bottom == PAGE_SIZE - 9);
        let buf = self.page.raw_buffer();
        let num_entries = Self::num_entries(buf) as usize;
        let middle_entry = num_entries / 2;

        // ingest data up to and including middle_entry into other
        let mut cursor = PAGE_SIZE - 9;
        for e in 0..middle_entry {
            let key_len =
                u32::from_be_bytes(buf[2 + (e * 8)..2 + (e * 8) + 4].try_into().unwrap()) as usize;
            let val_len =
                u32::from_be_bytes(buf[2 + (e * 8) + 4..2 + (e * 8) + 8].try_into().unwrap())
                    as usize;
            let delta = LeafDelta::Set {
                key: &buf[cursor - (key_len + val_len)..cursor - val_len],
                val: &buf[cursor - val_len..cursor],
            };
            other.apply_delta(delta).unwrap();
            cursor -= key_len + val_len;
        }

        let key_len = u32::from_be_bytes(
            buf[2 + (middle_entry * 8)..2 + (middle_entry * 8) + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let val_len = u32::from_be_bytes(
            buf[2 + (middle_entry * 8) + 4..2 + (middle_entry * 8) + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let delta = LeafDelta::Set {
            key: &buf[cursor - (key_len + val_len)..cursor - val_len],
            val: &buf[cursor - val_len..cursor],
        };
        other.apply_delta(delta).unwrap();
        cursor -= key_len + val_len;
        out.extend(&buf[cursor - (key_len + val_len)..cursor - val_len]);

        // copy up header info
        buf.copy_within(2 + (8 * middle_entry) + 8..self.top, 2);
        self.top -= (8 * middle_entry) + 8;
        // copy down data
        buf.copy_within(self.bottom..cursor, self.bottom + (cursor - 9));
        self.bottom += cursor - 9;
        // zero out the middle
        buf[self.top..self.bottom].fill(0);
        Self::set_num_entries(buf, (num_entries - (middle_entry + 1)) as u16);
    }
    fn unpack(mut self) -> PageBuffer<PAGE_SIZE> {
        let buf = self.page.raw_buffer();
        buf.copy_within(0..self.top, self.bottom - self.top);
        let len = (self.top + (PAGE_SIZE - (9 + self.bottom))) as u64;
        buf[PAGE_SIZE - 9..PAGE_SIZE - 1].copy_from_slice(&len.to_be_bytes());
        self.page.set_top(self.bottom - self.top);
        self.page
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
