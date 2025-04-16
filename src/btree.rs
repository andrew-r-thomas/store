//  NOTE:
//  - what if we only did splits and merges at compaction time?? that seems so much nicer than
//    having to do deltas for SMOs, and more intuitive
//  - we always split to the left
//  - we always assume empty buffers are filled with 0s
//
//  TODO:
//  - write docs, including the structure of the pages
//  - sibling pointers
//  - right pointers
//  - binary search
//  - maybe set up some modules, especially for the finer details of the byte dancing

use crate::{
    buffer::{PageBuffer, ReserveResult},
    mapping_table::{FrameInner, Table},
    page_table::PageId,
};

use std::{ops::Range, sync::atomic::Ordering};

pub fn get<'g, const B: usize>(table: &'g Table<B>, key: &[u8]) -> Option<&'g [u8]> {
    let mut page_id = table.root.load(Ordering::Acquire);
    loop {
        let page_buffer = table.read(page_id).read();
        if *page_buffer.last().unwrap() > 0 {
            // inner page
            page_id = inner::search(page_buffer, key);
        } else {
            // leaf page
            return leaf::search(page_buffer, key);
        }
    }
}
pub fn set<const B: usize, const PAGE_CAP: usize, const SPLIT_POINT: usize>(
    table: &Table<B>,
    key: &[u8],
    val: &[u8],
    path: &mut Vec<PageId>,
    leaf_scratch: &mut Vec<(Range<usize>, Range<usize>)>,
    inner_scratch: &mut Vec<(Range<usize>, PageId)>,
) -> Result<(), ()> {
    // first we find the leaf page and keep track of our path
    let mut page_id = table.root.load(Ordering::Acquire);
    loop {
        let page_buffer = table.read(page_id);
        path.push(page_id);
        let buf = page_buffer.read();
        if *buf.last().unwrap() > 0 {
            // inner page
            page_id = inner::search(buf, key);
        } else {
            // leaf page
            break;
        }
    }

    // then we try to reserve space for the write
    let leaf_id = path.pop().unwrap();
    let leaf = table.read(leaf_id);
    match leaf.reserve(leaf::INSUP_HEADER_SIZE + key.len() + val.len()) {
        ReserveResult::Ok(write_guard) => {
            leaf::write_insup(write_guard.buf, key, val);
            Ok(())
        }
        ReserveResult::Sealed => Err(()), // buffer already sealed, just fail
        ReserveResult::Sealer(seal_guard) => {
            // we sealed the buffer and need to compact it
            let new_page = PageBuffer::new(PAGE_CAP);
            let new_buf = unsafe { new_page.raw_buffer() };
            let old_buf = seal_guard.wait_for_writers();
            match leaf::compact::<SPLIT_POINT>(old_buf, new_buf, leaf_scratch) {
                CompactResult::Ok(top) => {
                    // happy path for compaction, we just need to finish setting up our page buf,
                    // and do a store on the mapping table
                    new_page.read_offset.store(top, Ordering::Release);
                    new_page.write_offset.store(top as isize, Ordering::Release);
                    table.update(page_id, new_page);
                }
                CompactResult::NeedsSplit(top, split_idx) => {
                    // first we do the split for the leaf page
                    let to_page = PageBuffer::new(PAGE_CAP);
                    let to_buf = unsafe { to_page.raw_buffer() };
                    let to_id = table.pop();
                    let (from_top, to_top) = leaf::split(&mut new_buf[top..], to_buf, split_idx);

                    // then we need to add the middle key to the parent, this may cascade into a
                    // split all the way up to the root
                    let mut middle_key = leaf::last_key(&to_buf[to_top..]);
                    match path.pop() {
                        Some(parent_id) => loop {
                            match table.read(parent_id).reserve(middle_key.len() + 13) {
                                ReserveResult::Ok(write_guard) => {
                                    inner::write_insup(write_guard.buf, middle_key, to_id);
                                    break;
                                }
                                ReserveResult::Sealed => continue,
                                ReserveResult::Sealer(seal_guard) => {
                                    let new = PageBuffer::new(PAGE_CAP);
                                    let new_buf = unsafe { new.raw_buffer() };
                                    let old_buf = seal_guard.wait_for_writers();
                                    match inner::compact(old_buf, new_buf) {
                                        CompactResult::Ok(top) => {}
                                        CompactResult::NeedsSplit(top, split_idx) => {}
                                    }
                                    break;
                                }
                            }
                        },
                        None => {
                            // we just split the root, so we have to make a new one
                            let new_root = PageBuffer::new(PAGE_CAP);
                            let new_root_buf = unsafe { new_root.raw_buffer() };
                            let new_root_id = table.pop();
                            let new_root_top =
                                inner::build_root(new_root_buf, middle_key, to_id, leaf_id);
                            new_root.read_offset.store(new_root_top, Ordering::Release);
                            new_root
                                .write_offset
                                .store(new_root_top as isize, Ordering::Release);
                            table.update(new_root_id, new_root);
                        }
                    }
                }
            }

            // TODO: need to recycle old page here once we're ready to do that
            Ok(())
        }
    }
}

mod inner {
    use super::CompactResult;
    use crate::page_table::PageId;
    use std::{collections::BTreeMap, ops::Range};

    pub fn split<'s>(from: &'s mut [u8], _to: &'s mut [u8], _idx: usize) -> (usize, usize) {
        todo!()
    }
    pub fn compact(og: &[u8], _new: &mut [u8]) -> CompactResult {
        let mut entries = BTreeMap::new();
        let mut i = 0;
        loop {
            match og[i] {
                0 => {
                    // base page
                    let num_entries =
                        u16::from_be_bytes(og[i + 1..i + 3].try_into().unwrap()) as usize;
                    let mut cursor = og.len() - 1;
                    for e in 0..num_entries {
                        let key_len = u32::from_be_bytes(
                            og[i + 11 + (e * 12)..i + 11 + (e * 12) + 4]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        let key = &og[cursor - key_len..cursor];
                        let page_id = u64::from_be_bytes(
                            og[i + 11 + (e * 12) + 4..i + 11 + (e * 12) + 12]
                                .try_into()
                                .unwrap(),
                        );
                        entries.insert(key, page_id);
                        cursor -= key_len;
                    }
                    break;
                }
                1 => {
                    // insup
                    let key_len = u32::from_be_bytes(og[i + 1..i + 5].try_into().unwrap()) as usize;
                    let key = &og[i + 13..i + 13 + key_len];
                    let page_id = u64::from_be_bytes(og[i + 5..i + 13].try_into().unwrap());
                    entries.insert(key, page_id);
                    i += 13 + key_len;
                }
                _ => panic!(),
            }
        }

        todo!()
    }

    pub fn search(page_buf: &[u8], key: &[u8]) -> PageId {
        let mut best = None;
        let mut i = 0;
        loop {
            match page_buf[i] {
                0 => {
                    // base page
                    // TODO: make these indexing things way more clear
                    let num_entries =
                        u16::from_be_bytes(page_buf[i + 1..i + 3].try_into().unwrap()) as usize;
                    let mut cursor = page_buf.len() - 1;
                    for e in 0..num_entries {
                        let key_len = u32::from_be_bytes(
                            page_buf[i + 3 + (e * 12)..i + 3 + (e * 12) + 4]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        let k = &page_buf[cursor - key_len..cursor];
                        let page_id = u64::from_be_bytes(
                            page_buf[i + 3 + (e * 12) + 4..i + 3 + (e * 12) + 12]
                                .try_into()
                                .unwrap(),
                        );
                        if key <= k {
                            match best {
                                Some((b_k, b_pid)) => {
                                    if k < b_k {
                                        return page_id;
                                    } else {
                                        return b_pid;
                                    }
                                }
                                None => return page_id,
                            }
                        }
                        cursor -= key_len;
                    }

                    match best {
                        Some((_, pid)) => return pid,
                        None => {
                            return u64::from_be_bytes(
                                page_buf[i + 3..i + 3 + 8].try_into().unwrap(),
                            );
                        }
                    }
                }
                1 => {
                    // insert or update
                    let key_len =
                        u32::from_be_bytes(page_buf[i + 1..i + 5].try_into().unwrap()) as usize;
                    let k = &page_buf[i + 5 + 8..i + 5 + 8 + key_len];
                    let page_id =
                        u64::from_be_bytes(page_buf[i + 5..i + 5 + 8].try_into().unwrap());
                    if key <= k {
                        match best {
                            Some((b_k, _)) => {
                                if k < b_k {
                                    best = Some((k, page_id));
                                }
                            }
                            None => best = Some((k, page_id)),
                        }
                    }
                    i += 5 + 8 + key_len;
                }
                _ => panic!(),
            }
        }
    }

    pub const DELTA_ID: usize = 0;
    pub const INSUP_DELTA_ID: u8 = 1;

    pub const BASE_PAGE_NUM_ENTRIES: Range<usize> = 1..3;
    pub const BASE_PAGE_RIGHT_PTR: Range<usize> = 3..11;
    pub const BASE_PAGE_HEADER_SIZE: usize = 11;
    pub const BASE_PAGE_ENTRY_HEADER_SIZE: usize = 12;
    pub const BASE_PAGE_FOOTER_SIZE: usize = 1;

    pub const INSUP_KEY_LEN: Range<usize> = 1..5;
    pub const INSUP_PAGE_ID: Range<usize> = 5..13;
    pub const INSUP_HEADER_SIZE: usize = 13;

    #[inline]
    pub fn write_insup(buf: &mut [u8], key: &[u8], page_id: PageId) {
        buf[DELTA_ID] = INSUP_DELTA_ID;
        buf[INSUP_KEY_LEN].copy_from_slice(&(key.len() as u32).to_be_bytes());
        buf[INSUP_PAGE_ID].copy_from_slice(&page_id.to_be_bytes());
        buf[INSUP_HEADER_SIZE..].copy_from_slice(key);
    }

    /// [`buf`] here is the full page buffer, returns the top of the buffer after building
    pub fn build_root(
        buf: &mut [u8],
        key: &[u8],
        left_page_id: PageId,
        right_page_id: PageId,
    ) -> usize {
        let top = BASE_PAGE_FOOTER_SIZE + BASE_PAGE_HEADER_SIZE + BASE_PAGE_ENTRY_HEADER_SIZE;
        let b = &mut buf[top..];

        b[BASE_PAGE_NUM_ENTRIES].copy_from_slice(&(1 as u16).to_be_bytes());
        b[BASE_PAGE_RIGHT_PTR].copy_from_slice(&right_page_id.to_be_bytes());
        b[BASE_PAGE_HEADER_SIZE..BASE_PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&(key.len() as u32).to_be_bytes());
        b[BASE_PAGE_HEADER_SIZE + 4..BASE_PAGE_HEADER_SIZE + 12]
            .copy_from_slice(&left_page_id.to_be_bytes());
        b[BASE_PAGE_HEADER_SIZE + BASE_PAGE_ENTRY_HEADER_SIZE
            ..BASE_PAGE_HEADER_SIZE + BASE_PAGE_ENTRY_HEADER_SIZE + key.len()]
            .copy_from_slice(key);
        *b.last_mut().unwrap() = 1;

        top
    }
}

mod leaf {
    use super::CompactResult;
    use std::ops::Range;

    /// splits [`from`] "left", with everything less than or equal to [`idx`] going into [`to`],
    /// returns the new top offset of both pages
    pub fn split<'s>(from: &'s mut [u8], to: &'s mut [u8], idx: usize) -> (usize, usize) {
        // first we find the start and end of the section we're taking out (since slots are sorted in
        // ascending key order, and actual data is sorted in descending key order, we're keeping the
        // middle chunk in the og buffer, and copying the edges out)
        let from_entries = u16::from_be_bytes(from[1..3].try_into().unwrap()) as usize;
        let from_len = from.len();
        let from_start = 3 + (8 * idx);
        let mut from_end = from_len - 1;
        for i in 0..idx {
            let key_len =
                u32::from_be_bytes(from[3 + (8 * i)..3 + (8 * i) + 4].try_into().unwrap()) as usize;
            let val_len =
                u32::from_be_bytes(from[3 + (8 * i) + 4..3 + (8 * i) + 8].try_into().unwrap())
                    as usize;
            from_end -= key_len + val_len;
        }

        // then copy the stuff around that middle chunk to the bottom of the to buffer
        let to_len = to.len();
        // copy the actual data
        to[to_len - (from_len - from_end)..].copy_from_slice(&from[from_end..]);
        // copy the header info
        to[(to_len - (from_len - from_end)) - (3 + idx * 8)..to_len - (from_len - from_end)]
            .copy_from_slice(&from[..from_start]);
        // update num_entries
        to[(to_len - (from_len - from_end)) - (3 + idx * 8)
            ..(to_len - (from_len - from_end)) - (3 + idx * 8) + 2]
            .copy_from_slice(&(idx as u16).to_be_bytes());

        // then we update the from page
        // first we clear everything below what we're keeping
        from[from_end..].fill(0);
        // then we bump everything down
        from.copy_within(from_start..from_end, from_start + (from_end - from_start));
        // then we clear everything above
        from[..from_start + (from_end - from_start)].fill(0);
        // and update the num entries
        from[from_start + (from_end - from_start) - 2..from_start + (from_end - from_start)]
            .copy_from_slice(&((from_entries - idx) as u16).to_be_bytes());

        (
            (to_len - (from_len - from_end)) - (3 + idx * 8) - 1,
            from_start + (from_end - from_start) - 3,
        )
    }

    pub fn compact<'c, const SPLIT_POINT: usize>(
        og: &'c [u8],
        new: &mut [u8],
        scratch: &mut Vec<(Range<usize>, Range<usize>)>,
    ) -> CompactResult {
        // for now we will assume that there's enough physical space in the new
        // buffer for everything, but that may not be true in some cases

        // first we collect all the entries in key order
        let mut i = 0;
        loop {
            match og[i] {
                0 => {
                    // base page
                    let num_entries =
                        u16::from_be_bytes(og[i + 1..i + 3].try_into().unwrap()) as usize;
                    let mut cursor = og.len() - 1;
                    for e in 0..num_entries {
                        let key_len = u32::from_be_bytes(
                            og[i + 3 + (e * 8)..i + 3 + (e * 8) + 4].try_into().unwrap(),
                        ) as usize;
                        let val_len = u32::from_be_bytes(
                            og[i + 3 + (e * 8) + 4..i + 3 + (e * 8) + 8]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        scratch.push((
                            cursor - (key_len + val_len)..cursor - val_len,
                            cursor - val_len..cursor,
                        ));
                        cursor -= key_len + val_len;
                    }
                    break;
                }
                1 => {
                    // insup
                    let key_len = u32::from_be_bytes(og[i + 1..i + 5].try_into().unwrap()) as usize;
                    let val_len = u32::from_be_bytes(og[i + 5..i + 9].try_into().unwrap()) as usize;
                    scratch.push((
                        i + 9..i + 9 + key_len,
                        i + 9 + key_len..i + 9 + key_len + val_len,
                    ));
                    i += 9 + key_len + val_len;
                }
                _ => panic!(),
            }
        }

        // for writing the new page, we write the actual data from the bottom,
        // and we write the slots and header from the top, then memcpy the solts
        // down to the level of the data
        //
        // for the header/footer, everything is zeroed, so we only need to write in
        // the number of entries
        new[1..3].copy_from_slice(&(scratch.len() as u16).to_be_bytes());
        let mut top_i = 3;
        let mut bottom_i = new.len() - 1;
        for (key, val) in scratch.iter() {
            // copy in the new key and value at the bottom, and update the cursor
            new[bottom_i - val.len()..bottom_i].copy_from_slice(val);
            new[bottom_i - (val.len() + key.len())..bottom_i - val.len()].copy_from_slice(key);
            bottom_i -= key.len() + val.len();

            // write in the slot data, and update the cursor
            new[top_i..top_i + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
            new[top_i + 4..top_i + 8].copy_from_slice(&(val.len() as u32).to_be_bytes());
            top_i += 8;
        }

        // for now this will let us uphold our assumption that we have enough space
        // in the new buffer for everything
        assert!(top_i < bottom_i);
        let top = bottom_i - top_i;

        // copy down the slots + header to meet the actual data (and make a contiguous base page)
        new.copy_within(0..top_i, top);

        // then we check if we need to split
        if top < SPLIT_POINT {
            return CompactResult::NeedsSplit(top, scratch.len() / 2);
        }

        CompactResult::Ok(top)
    }

    pub fn search<'s>(leaf: &'s [u8], key: &[u8]) -> Option<&'s [u8]> {
        let mut i = 0;
        loop {
            match leaf[i] {
                0 => {
                    // base page
                    let num_entries =
                        u16::from_be_bytes(leaf[i + 1..i + 3].try_into().unwrap()) as usize;
                    let mut cursor = leaf.len() - 1;
                    for e in 0..num_entries {
                        let key_len = u32::from_be_bytes(
                            leaf[i + 3 + (e * 8)..i + 3 + (e * 8) + 4]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        let val_len = u32::from_be_bytes(
                            leaf[i + 3 + (e * 8) + 4..i + 3 + (e * 8) + 8]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        let k = &leaf[cursor - (key_len + val_len)..cursor - val_len];
                        if key < k {
                            return None;
                        } else if key == k {
                            return Some(&leaf[cursor - val_len..cursor]);
                        }
                        cursor -= key_len + val_len;
                    }
                    return None;
                }
                1 => {
                    // insup
                    let key_len =
                        u32::from_be_bytes(leaf[i + 1..i + 5].try_into().unwrap()) as usize;
                    let val_len =
                        u32::from_be_bytes(leaf[i + 5..i + 9].try_into().unwrap()) as usize;
                    let k = &leaf[i + 9..i + 9 + key_len];
                    if key == k {
                        return Some(&leaf[i + 9 + key_len..i + 9 + key_len + val_len]);
                    }
                    i += 9 + key_len + val_len;
                }
                _ => panic!(),
            }
        }
    }

    pub fn last_key(page: &[u8]) -> &[u8] {
        let num_entries = u16::from_be_bytes(page[1..3].try_into().unwrap()) as usize;
        let key_len = u32::from_be_bytes(
            page[3 + ((num_entries - 1) * 8)..3 + ((num_entries - 1) * 8) + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        &page[3 + ((num_entries - 1) * 8) + 8..3 + ((num_entries - 1) * 8) + 8 + key_len]
    }

    pub const DELTA_ID: usize = 0;
    pub const BASE_PAGE_ID: u8 = 0;
    pub const INSUP_DELTA_ID: u8 = 1;

    pub const INSUP_KEY_LEN: Range<usize> = 1..5;
    pub const INSUP_VAL_LEN: Range<usize> = 5..9;
    pub const INSUP_HEADER_SIZE: usize = 9;

    #[inline]
    pub fn write_insup(buf: &mut [u8], key: &[u8], val: &[u8]) {
        buf[DELTA_ID] = INSUP_DELTA_ID;
        buf[INSUP_KEY_LEN].copy_from_slice(&(key.len() as u32).to_be_bytes());
        buf[INSUP_VAL_LEN].copy_from_slice(&(val.len() as u32).to_be_bytes());
        buf[INSUP_HEADER_SIZE..INSUP_HEADER_SIZE + key.len()].copy_from_slice(key);
        buf[INSUP_HEADER_SIZE + key.len()..].copy_from_slice(val);
    }
}

/// first usize is the top index for the page
enum CompactResult {
    Ok(usize),
    /// this second usize is the middle key index for performing the split
    NeedsSplit(usize, usize),
}
