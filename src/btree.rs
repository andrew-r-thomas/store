/*
    NOTE:
    - what if we only did splits and merges at compaction time?? that seems so
      much nicer than having to do deltas for SMOs, and more intuitive

    TODO:
    - write docs, including the structure of the pages
*/

use std::{
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
                        let middle_key = split_leaf(&mut new_buf[top..], to_buf, split_idx);

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

/// returns the middle key from the split, this is the first key in the new page
fn split_leaf<'s>(from: &'s mut [u8], to: &'s mut [u8], idx: usize) -> &'s [u8] {
    todo!()
}

/// first usize is the top index for the page
enum CompactResult {
    Ok(usize),
    /// this second usize is the middle key index for performing the split
    NeedsSplit(usize, usize),
    NeedsMerge,
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
                for e in 0..num_entries {
                    let offset = u32::from_be_bytes(
                        og[i + 3 + (e * 12)..i + 3 + (e * 12) + 4]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let key_len = u32::from_be_bytes(
                        og[i + 3 + (e * 12) + 4..i + 3 + (e * 12) + 8]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let val_len = u32::from_be_bytes(
                        og[i + 3 + (e * 12) + 8..i + 3 + (e * 12) + 12]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let key = &og[i + offset..i + offset + key_len];
                    let val = &og[i + offset + key_len..i + offset + key_len + val_len];
                    entries.insert(key, val);
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
    let mut bottom_i = PAGE_CAP - 1;
    for (key, val) in &entries {
        // copy in the new key and value at the bottom, and update the cursor
        new[bottom_i - val.len()..bottom_i].copy_from_slice(val);
        new[bottom_i - val.len() - key.len()..bottom_i - val.len()].copy_from_slice(key);
        bottom_i -= key.len() + val.len();

        // write in the slot data, and update the cursor
        // FIX: this means we need to update the logic to use the offset as
        // "from the bottom" not "from the top"
        new[top_i..top_i + 4].copy_from_slice(&(bottom_i as u32).to_be_bytes());
        new[top_i + 4..top_i + 8].copy_from_slice(&(key.len() as u32).to_be_bytes());
        new[top_i + 8..top_i + 12].copy_from_slice(&(val.len() as u32).to_be_bytes());
        top_i += 12;
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
                for e in 0..num_entries {
                    let offset = u32::from_be_bytes(
                        page_buf[i + 3 + 8 + (e * 16)..i + 3 + 8 + (e * 16) + 4]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let key_len = u32::from_be_bytes(
                        page_buf[i + 3 + 8 + (e * 16) + 4..i + 3 + 8 + (e * 16) + 8]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    if key <= &page_buf[i + offset..i + offset + key_len] {
                        return u64::from_be_bytes(
                            page_buf[i + 3 + 8 + (e * 16) + 8..i + 3 + 8 + (e * 16) + 16]
                                .try_into()
                                .unwrap(),
                        );
                    }
                }

                // TODO: we need to go right at this point, which may have been in the
                // deltas maybe?
                return u64::from_be_bytes(page_buf[i + 3..i + 3 + 8].try_into().unwrap());
            }
            1 => {
                // insert or update
                let key_len =
                    u32::from_be_bytes(page_buf[i + 1..i + 5].try_into().unwrap()) as usize;
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
                for e in 0..num_entries {
                    let offset = u32::from_be_bytes(
                        leaf[i + 3 + (e * 12)..i + 3 + (e * 12) + 4]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let key_len = u32::from_be_bytes(
                        leaf[i + 3 + (e * 12) + 4..i + 3 + (e * 12) + 8]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    if key == &leaf[i + offset..i + offset + key_len] {
                        let val_len = u32::from_be_bytes(
                            leaf[i + 3 + (e * 12) + 8..i + 3 + (e * 12) + 12]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        return Some(&leaf[i + offset + key_len..i + offset + key_len + val_len]);
                    }
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
