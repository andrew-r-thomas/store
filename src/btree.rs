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

/*
    NOTE:
    - what if we only did splits and merges at compaction time?? that seems so
      much nicer than having to do deltas for SMOs, and more intuitive
*/

use std::sync::{Arc, atomic::Ordering};

use crate::{
    buffer::{PageBuffer, ReserveResult},
    mapping_table::Table,
    page_table::PageId,
};

const B: usize = 1024;
const PAGE_CAP: usize = 1024;

/// btree cursors are owned by a single thread, and are not concurrently
/// accessed, they just encode the btree logic
pub struct BTreeCursor {
    table: Arc<Table<B>>,
    path: Vec<PageId>,
}

impl BTreeCursor {
    pub fn get(&mut self, key: &[u8], out: &mut Vec<u8>) -> Option<()> {
        let mut page_id = self.table.root.load(Ordering::Acquire);
        loop {
            let page = Page::new(self.table.read(page_id).read());
            match page {
                Page::Inner(inner) => page_id = inner.search(key),
                Page::Leaf(leaf) => {
                    let res = leaf.search(key);
                    match res {
                        Some(o) => {
                            for x in o {
                                out.push(*x);
                            }
                            return Some(());
                        }
                        None => return None,
                    }
                }
            }
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        // this is where the fun begins
        let mut page_buf = self.table.read(self.table.root.load(Ordering::Acquire));
        loop {
            let page = Page::new(page_buf.read());
            match page {
                Page::Inner(inner) => page_buf = self.table.read(inner.search(key)),
                Page::Leaf(_) => break,
            }
        }
        // now we know page_buf is a leaf page
        match page_buf.reserve(key.len() + val.len() + 8) {
            // regular happy path
            ReserveResult::Ok(write_guard) => {
                write_guard.write[0..4].copy_from_slice(&(key.len() as u32).to_be_bytes());
                write_guard.write[4..4 + key.len()].copy_from_slice(key);
                write_guard.write[4 + key.len()..4 + key.len() + 4]
                    .copy_from_slice(&(val.len() as u32).to_be_bytes());
                write_guard.write[4 + key.len() + 4..].copy_from_slice(val);
            }

            // buffer is already sealed, just fail
            ReserveResult::Sealed => return Err(()),

            // we caused this buffer to be sealed, and need to perform compaction
            ReserveResult::Sealer(seal_guard) => {
                // so here we can prep whatever stuff we need to do,
                let new_page_buf = PageBuffer::new(PAGE_CAP);
                let buf = unsafe { new_page_buf.raw_buffer() };

                // and then when everything is t-ed up, we can:
                let old_buf = seal_guard.wait_for_writers();
                let old_page = LeafPage { buf: old_buf };

                // and then do the store on the mapping table and what not
            }
        }

        Ok(())
    }
}

enum Page<'p> {
    Inner(InnerPage<'p>),
    Leaf(LeafPage<'p>),
}
impl<'p> Page<'p> {
    pub fn new(buf: &'p [u8]) -> Self {
        match buf[0] {
            0 => Self::Leaf(LeafPage { buf: &buf[1..] }),
            _ => Self::Inner(InnerPage { buf: &buf[1..] }),
        }
    }
}
struct InnerPage<'p> {
    buf: &'p [u8],
}
impl InnerPage<'_> {
    pub fn search(&self, key: &[u8]) -> PageId {
        let mut i = 0;
        loop {
            let p = &self.buf[i + 1..];
            match self.buf[i] {
                0 => {
                    // base page
                    let entries = u16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
                    for e in 0..entries {
                        let key_off_start = 2 + (2 * e);
                        let key_off_end = key_off_start + 2;
                        let key_offset =
                            u16::from_be_bytes(p[key_off_start..key_off_end].try_into().unwrap())
                                as usize;
                        let key_len =
                            u32::from_be_bytes(p[key_offset..key_offset + 4].try_into().unwrap())
                                as usize;
                        if key <= &p[key_offset + 4..key_offset + 4 + key_len] {
                            let ids = &p[p.len() - (entries * 8)..];
                            return u64::from_be_bytes(ids[e * 8..(e * 8) + 8].try_into().unwrap());
                        }
                    }
                    todo!("if we get here, we gotta go right");
                }
                1 => {
                    // insert or update
                    let key_len = u32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
                    if key <= &p[4..4 + key_len] {
                        return u64::from_be_bytes(
                            p[4 + key_len..4 + key_len + 8].try_into().unwrap(),
                        );
                    }
                    i += 1 + 4 + key_len + 8;
                }
                _ => panic!(),
            }
        }
    }
}
struct LeafPage<'p> {
    buf: &'p [u8],
}
impl LeafPage<'_> {
    pub fn iter_chunks(&self) -> LeafChunkIter {
        LeafChunkIter { buf: self.buf }
    }
    pub fn search<'s>(&'s self, key: &'s [u8]) -> Option<&'s [u8]> {
        for chunk in self.iter_chunks() {
            match chunk {
                LeafChunk::InsUp { key: k, val } => {
                    if key < k {
                        return None;
                    }
                    if key == k {
                        return Some(val);
                    }
                }
                LeafChunk::Base(base) => return base.search(key),
            }
        }
        None
    }
}

struct LeafChunkIter<'i> {
    buf: &'i [u8],
}
impl<'i> Iterator for LeafChunkIter<'i> {
    type Item = LeafChunk<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() == 0 {
            return None;
        }
        match self.buf[0] {
            0 => {
                let out = Some(LeafChunk::Base(LeafBase {
                    buf: &self.buf[1..],
                }));
                self.buf = &self.buf[0..0];
                out
            }
            1 => {
                let p = &self.buf[1..];
                let key_len = u32::from_be_bytes(p[0..4].try_into().unwrap()) as usize;
                let val_len =
                    u32::from_be_bytes(p[4 + key_len..4 + key_len + 4].try_into().unwrap())
                        as usize;
                let key = &p[4..4 + key_len];
                let val = &p[4 + key_len + 4..4 + key_len + 4 + val_len];
                self.buf = &self.buf[1 + 4 + key_len + 4 + val_len..];
                Some(LeafChunk::InsUp { key, val })
            }
            _ => panic!(),
        }
    }
}
enum LeafChunk<'c> {
    InsUp { key: &'c [u8], val: &'c [u8] },
    Base(LeafBase<'c>),
}

struct LeafBase<'b> {
    buf: &'b [u8],
}
impl LeafBase<'_> {
    pub fn search(self, key: &[u8]) -> Option<&[u8]> {
        todo!()
    }
}
