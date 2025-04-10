/*
    NOTE:
    - what if we only did splits and merges at compaction time?? that seems so
      much nicer than having to do deltas for SMOs, and more intuitive

    TODO:
    - write docs, including the structure of the pages
*/

use std::{
    any::Any,
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
};

use crate::{
    buffer::{PageBuffer, ReserveResult},
    mapping_table::{FrameInner, Table},
    page_table::PageId,
};

// NOTE: these will all be configuarable eventually
const B: usize = 1024; // size of the mapping table blocks
const PAGE_CAP: usize = 1024; // physical capacity of the pages
const SPLIT_POINT: usize = 512; // physical size at which a split is triggered

/// btree cursors are owned by a single thread, and are not concurrently
/// accessed, they just encode the btree logic
pub struct BTreeCursor {
    table: Arc<Table<B>>,
    // TODO: we probably want to include the actual buffer here if possible
    path: Vec<PageId>,
}

impl BTreeCursor {
    /// out is expected to be cleared and have some space allocated already
    pub fn get(&mut self, key: &[u8], out: &mut Vec<u8>) -> Option<()> {
        self.find_leaf(key);
        let leaf = self.table.read(*self.path.last().unwrap()).read();
        match search_leaf(leaf, key) {
            Some(val) => {
                // PERF: some kind of memcpy at least
                for v in val {
                    out.push(*v);
                }
                return Some(());
            }
            None => None,
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        self.find_leaf(key);
        match self
            .table
            .read(self.path.pop().unwrap())
            .reserve(9 + key.len() + val.len())
        {
            ReserveResult::Ok(write_guard) => {
                // regular happy path, write the new delta
                write_guard.write[0] = 1;
                write_guard.write[1..5].copy_from_slice(&(key.len() as u32).to_be_bytes());
                write_guard.write[5..9].copy_from_slice(&(val.len() as u32).to_be_bytes());
                write_guard.write[9..9 + key.len()].copy_from_slice(key);
                write_guard.write[9 + key.len()..9 + key.len() + val.len()].copy_from_slice(val);
                Ok(())
            }
            ReserveResult::Sealed => Err(()), // buffer already sealed, just fail
            ReserveResult::Sealer(seal_guard) => {
                // we sealed the buffer, so we need to compact, and might need to split
                // first, we get a fresh buffer to compact into
                let new_page = PageBuffer::new(PAGE_CAP);
                let new_buf = unsafe { new_page.raw_buffer() };

                // then wait for current writers to finish,
                // and get the old buf back
                let old_buf = seal_guard.wait_for_writers();

                // do the compaction
                match compact_leaf(old_buf, new_buf) {
                    CompactResult::Ok(top) => {
                        // compaction worked, so we just need to update the
                        // mapping table
                        new_page.read_offset.store(top, Ordering::Release);
                        new_page.write_offset.store(top as isize, Ordering::Release);
                        self.table
                            .update(*self.path.last().unwrap(), &FrameInner::Mem(new_page));
                        // TODO: we would also be recycling the old buffer here
                        Ok(())
                    }
                    CompactResult::NeedsSplit(top, split_idx) => {
                        // the page is compacted, but needs to be split
                        let (to_page_id, to_page) = self.table.pop();
                        let to_buf = unsafe { to_page.raw_buffer() };
                        // TODO: make sure we use these to update the page buffer atomic indices
                        let (from_top, to_top) = split_leaf(&mut new_buf[top..], to_buf, split_idx);
                        // then we find the middle key, which is the right most key of the "to"
                        // page
                        let middle_key = {
                            let num_entries = u16::from_be_bytes(
                                to_buf[to_top + 1..to_top + 3].try_into().unwrap(),
                            ) as usize;
                            let key_len = u32::from_be_bytes(
                                to_buf[to_top + 3 + ((num_entries - 1) * 8)
                                    ..to_top + 3 + ((num_entries - 1) * 8) + 4]
                                    .try_into()
                                    .unwrap(),
                            ) as usize;
                            &to_buf[to_top + 3 + ((num_entries - 1) * 8) + 8
                                ..to_top + 3 + ((num_entries - 1) * 8) + 8 + key_len]
                        };

                        // page has been split, now we need to add the new
                        // page id to the parent, this may trigger another
                        // compaction and/or split, all the way up the path
                        let parent_id = self.path.pop().unwrap();
                        let parent = self.table.read(parent_id);
                        match parent.reserve(13 + middle_key.len()) {
                            ReserveResult::Ok(write_guard) => {
                                write_guard.write[0] = 1;
                                write_guard.write[1..5]
                                    .copy_from_slice(&(middle_key.len() as u32).to_be_bytes());
                                write_guard.write[5..13].copy_from_slice(&to_page_id.to_be_bytes());
                                write_guard.write[13..].copy_from_slice(middle_key);
                            }
                            ReserveResult::Sealed => {
                                // need to figure out what to do here,
                                // failure may not be an option, and we might
                                // need to spin until we get it
                                todo!()
                            }
                            ReserveResult::Sealer(seal_guard) => {
                                // we need to compact the parent
                                todo!()
                            }
                        }
                        todo!()
                    }
                    CompactResult::NeedsMerge => panic!(),
                }
            }
        }
    }

    fn find_leaf(&mut self, key: &[u8]) {
        self.path.clear();
        let mut page_id = self.table.root.load(Ordering::Acquire);
        loop {
            self.path.push(page_id);
            let page_buffer = self.table.read(page_id).read();
            if page_buffer[page_buffer.len() - 1] > 0 {
                // inner page
                // so we gotta read the page to find the next one to go to
                page_id = search_inner(page_buffer, key);
            } else {
                // leaf page
                break;
            }
        }
    }
}

fn split_inner() {
    todo!()
}

// NOTE:
// - we always split to the left
// - we always assume empty buffers are filled with 0s
//
// TODO:
// - get rid of all the offset stuff

/// splits [`from`] "left", with everything less than or equal to [`idx`] going into [`to`],
/// returns the new top offset of both pages
fn split_leaf<'s>(from: &'s mut [u8], to: &'s mut [u8], idx: usize) -> (usize, usize) {
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
            u32::from_be_bytes(from[3 + (8 * i) + 4..3 + (8 * i) + 8].try_into().unwrap()) as usize;
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

    todo!()
}

/// first usize is the top index for the page
enum CompactResult {
    Ok(usize),
    /// this second usize is the middle key index for performing the split
    NeedsSplit(usize, usize),
    NeedsMerge,
}

fn compact_inner() {
    todo!()
}

fn compact_leaf(og: &[u8], new: &mut [u8]) -> CompactResult {
    // for now we will assume that there's enough physical space in the new
    // buffer for everything, but that may not be true in some cases

    // FIX: this is definitely not the right way to do this, but just trying
    // to get a sense of the logic first
    let mut entries = BTreeMap::new();

    // first we collect all the entries in key order
    let mut i = 0;
    loop {
        match og[i] {
            0 => {
                // base page
                let num_entries = u16::from_be_bytes(og[i + 1..i + 3].try_into().unwrap()) as usize;
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
                    let key = &og[cursor - (key_len + val_len)..cursor - val_len];
                    let val = &og[cursor - val_len..cursor];
                    entries.insert(key, val);
                    cursor -= key_len + val_len;
                }
                break;
            }
            1 => {
                // insup
                let key_len = u32::from_be_bytes(og[i + 1..i + 5].try_into().unwrap()) as usize;
                let val_len = u32::from_be_bytes(og[i + 5..i + 9].try_into().unwrap()) as usize;
                let key = &og[i + 9..i + 9 + key_len];
                let val = &og[i + 9 + key_len..i + 9 + key_len + val_len];
                entries.insert(key, val);
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
    new[1..3].copy_from_slice(&(entries.len() as u16).to_be_bytes());
    let mut top_i = 3;
    let mut bottom_i = new.len() - 1;
    for (key, val) in &entries {
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
        return CompactResult::NeedsSplit(top, entries.len() / 2);
    }

    CompactResult::Ok(top)
}

fn search_inner(page_buf: &[u8], key: &[u8]) -> PageId {
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
                    if key <= &page_buf[cursor - key_len..cursor] {
                        return u64::from_be_bytes(
                            page_buf[i + 3 + (e * 12) + 4..i + 3 + (e * 12) + 12]
                                .try_into()
                                .unwrap(),
                        );
                    }
                    cursor -= key_len;
                }

                return u64::from_be_bytes(page_buf[i + 3..i + 3 + 8].try_into().unwrap());
            }
            1 => {
                // insert or update
                let key_len =
                    u32::from_be_bytes(page_buf[i + 1..i + 5].try_into().unwrap()) as usize;
                // FIX: this won't work, if we find a delta for an entry super far to the right,
                // we'd return early even if there's an entry in a lower delta or base page that
                // would be closer (correct)
                if key <= &page_buf[i + 5 + 8..i + 5 + 8 + key_len] {
                    return u64::from_be_bytes(page_buf[i + 5..i + 5 + 8].try_into().unwrap());
                }
                i += 5 + 8 + key_len;
            }
            _ => panic!(),
        }
    }
}

// TODO: probably wanna do binary search at some point
fn search_leaf<'s>(leaf: &'s [u8], key: &[u8]) -> Option<&'s [u8]> {
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
                let key_len = u32::from_be_bytes(leaf[i + 1..i + 5].try_into().unwrap()) as usize;
                let val_len = u32::from_be_bytes(leaf[i + 5..i + 9].try_into().unwrap()) as usize;
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
