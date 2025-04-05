//! # btree page structure
//! there are two major ways data in pages can be represented:
//! - base pages, and
//! - delta updates
//!
//! base pages have a footer (conceptually a header, but layed out like a
//! footer bc of how page buffers work) that contains the static information
//! about a page, this includes
//!
//! # footer
//! ```
//! |--------|--------|--------|--------|
//! | level  | ... idk some other stuff |
//! |--------|--------|--------|--------|
//! ```
//!
//! # deltas
//! ```
//! |--------|--------|--------|--------|
//! |delta id|
//! |--------|--------|--------|--------|
//! ```

use std::sync::{Arc, atomic::Ordering};

use crate::{mapping_table::Table, page_table::PageId};

const B: usize = 1024;

/// btree cursors are owned by a single thread, and are not concurrently
/// accessed, they just encode the btree logic
pub struct BTreeCursor {
    table: Arc<Table<B>>,
    path: Vec<PageId>,
}

impl BTreeCursor {
    pub fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        let mut page_id = self.table.root.load(Ordering::Acquire);
        'tree: loop {
            let page = self.table.read(page_id).read();
            let level = page[page.len() - 1];

            // tracking variables for page search
            let mut i = 0;
            let p = &page[i + 1..page.len() - 1];
            if level > 0 {
                // search inner page
                loop {
                    match page[i] {
                        0 => {
                            // base page
                            let entries = u16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
                            // PERF: probably replace this with binary search or simd cmps
                            // at some point in the future
                            for e in 0..entries {
                                let key_off_start = 2 + (e * 2);
                                let key_off_end = key_off_start + 2;
                                let key_offset = u16::from_be_bytes(
                                    p[key_off_start..key_off_end].try_into().unwrap(),
                                ) as usize;
                                let key_len = u32::from_be_bytes(
                                    p[key_offset..key_offset + 4].try_into().unwrap(),
                                ) as usize;
                                let k = &p[key_offset + 4..key_offset + 4 + key_len];
                                if key <= k {
                                    let ids = &p[p.len() - (entries * 8)..];
                                    page_id = u64::from_be_bytes(
                                        ids[e * 8..(e * 8) + 8].try_into().unwrap(),
                                    );
                                    continue 'tree;
                                }
                            }
                            // TODO: if we get here, we need to go right
                        }
                        1 => {
                            // insert or update
                            let key_len = u32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
                            if key <= &p[4..4 + key_len] {
                                // we can stop the page search early bc we found
                                // where we need to go
                                page_id = u64::from_be_bytes(
                                    p[4 + key_len..4 + key_len + 8].try_into().unwrap(),
                                );
                                continue 'tree;
                            } else {
                                // increment i to read next delta (or base page)
                                i += key_len + 12;
                            }
                        }
                        _ => panic!("invalid page node type?"),
                    }
                }
            } else {
                // search leaf page
                loop {
                    match page[i] {
                        0 => {
                            // base page
                            // TODO: seems kinda silly to have u16 offsets with
                            // u32 lengths for the actual data, figure out the
                            // right sizes for these
                            let entries = u16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
                            for e in 0..entries {
                                let key_off_start = 2 + (e * 2);
                                let key_off_end = key_off_start + 2;
                                let key_offset = u16::from_be_bytes(
                                    p[key_off_start..key_off_end].try_into().unwrap(),
                                ) as usize;
                                let key_len = u32::from_be_bytes(
                                    p[key_offset..key_offset + 4].try_into().unwrap(),
                                ) as usize;
                                let k = &p[key_offset + 4..key_offset + 4 + key_len];
                                if key < k {
                                    return None;
                                }
                                if key == k {
                                    let val_len = u32::from_be_bytes(
                                        p[key_offset + 4 + key_len..key_offset + 4 + key_len + 4]
                                            .try_into()
                                            .unwrap(),
                                    ) as usize;
                                    return Some(
                                        &p[key_offset + 4 + key_len + 4
                                            ..key_offset + 4 + key_len + 4 + val_len],
                                    );
                                }
                            }
                            return None;
                        }
                        1 => {
                            // update or insert
                            let key_len = u32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
                            let val_len = u32::from_be_bytes(
                                p[4 + key_len..4 + key_len + 4].try_into().unwrap(),
                            ) as usize;
                            let k = &p[4..4 + key_len];
                            if key < k {
                                return None;
                            } else if key == k {
                                return Some(&p[4 + key_len + 4..4 + key_len + 4 + val_len]);
                            } else {
                                i += key_len + val_len + 8;
                            }
                        }
                        _ => panic!("invalid page node type!"),
                    }
                }
            }
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8]) {
        // this is where the fun begins
    }
}
