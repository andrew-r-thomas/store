/*

    TODO:
    - make hard retry interface as well as simple "try" interface for looping on sealed buffers vs
      just failing, right now everything is the "try" interface

    NOTE:
    - i think we'll do extendible hashing for this, which is currently in progress
    - we won't ever shrink the hash table, that will be done by a vaccuum operation or something

*/

use crate::{
    buffer::{PageBuffer, ReserveResult},
    mapping_table::Table,
    page_table::PageId,
};

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::atomic::{AtomicU8, Ordering},
};

pub struct HashTable<const PAGE_SIZE: usize, const B: usize> {
    depth: AtomicU8,
    table: Table<B>,
}
impl<const PAGE_SIZE: usize, const B: usize> HashTable<PAGE_SIZE, B> {
    // public interface ===========================================================================

    pub fn get(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        let bucket = self.table.read(self.find_bucket(key)).read();
        match Self::search_bucket(bucket, key) {
            Some(val) => {
                out.extend(val);
                true
            }
            None => false,
        }
    }

    pub fn set(&self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        let delta_len = 9 + key.len() + val.len();
        match self.table.read(self.find_bucket(key)).reserve(delta_len) {
            ReserveResult::Ok(write_guard) => {
                Self::write_insup(write_guard.buf, key, val);
                Ok(())
            }
            ReserveResult::Sealed => Err(()),
            ReserveResult::Sealer(seal_guard) => {
                // we sealed the buffer and need to compact it,
                // first we do the compaction
                let new_page = PageBuffer::new(PAGE_SIZE);
                let new_buf = unsafe { new_page.raw_buffer() };
                let old_buf = seal_guard.wait_for_writers();
                let new_top = Self::compact_bucket(old_buf, new_buf);

                // then we check if there's room for our write after compacting

                // if not, we need to split the bucket

                todo!()
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), ()> {
        let delta_len = 5 + key.len();
        match self.table.read(self.find_bucket(key)).reserve(delta_len) {
            ReserveResult::Ok(write_guard) => {
                Self::write_delete(write_guard.buf, key);
                Ok(())
            }
            ReserveResult::Sealed => Err(()),
            ReserveResult::Sealer(seal_guard) => {
                // we sealed the buffer and need to compact it,
                // first we do the compaction
                let new_page = PageBuffer::new(PAGE_SIZE);
                let new_buf = unsafe { new_page.raw_buffer() };
                let old_buf = seal_guard.wait_for_writers();
                let new_top = Self::compact_bucket(old_buf, new_buf);

                // then we check if there's room for our write after compacting

                // if not, we need to split the bucket

                todo!()
            }
        }
    }

    // private functions ==========================================================================

    #[inline]
    fn depth(&self) -> u8 {
        self.depth.load(Ordering::Acquire)
    }
    #[inline]
    fn find_bucket(&self, key: &[u8]) -> PageId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let depth = self.depth();
        hash & ((1 << depth) - 1)
    }

    // utils ======================================================================================

    fn search_bucket<'s>(page: &'s [u8], key: &[u8]) -> Option<&'s [u8]> {
        let mut i = 0;
        loop {
            match page[i] {
                0 => {
                    // base page
                    let num_entries =
                        u16::from_be_bytes(page[i + 1..i + 3].try_into().unwrap()) as usize;
                    let mut cursor = i + 3 + (num_entries * 8);
                    for e in 0..num_entries {
                        let key_len = u32::from_be_bytes(
                            page[i + 3 + (e * 8)..i + 3 + (e * 8) + 4]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        let val_len = u32::from_be_bytes(
                            page[i + 3 + (e * 8) + 4..i + 3 + (e * 8) + 8]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        if key == &page[cursor..cursor + key_len] {
                            return Some(&page[cursor + key_len..cursor + key_len + val_len]);
                        }
                        cursor += key_len + val_len;
                    }
                    return None;
                }
                1 => {
                    // insup
                    let key_len =
                        u32::from_be_bytes(page[i + 1..i + 5].try_into().unwrap()) as usize;
                    let val_len =
                        u32::from_be_bytes(page[i + 5..i + 9].try_into().unwrap()) as usize;
                    if key == &page[i + 9..i + 9 + key_len] {
                        return Some(&page[i + 9 + key_len..i + 9 + key_len + val_len]);
                    }
                    i += 9 + key_len + val_len;
                }
                2 => {
                    // delete
                    let key_len =
                        u32::from_be_bytes(page[i + 1..i + 5].try_into().unwrap()) as usize;
                    if key == &page[i + 5..i + 5 + key_len] {
                        return None;
                    }
                    i += 5 + key_len;
                }
                _ => panic!(),
            }
        }
    }

    /// takes bucket to be compacted ([`old`]), and a zeroed complete page buffer to compact into
    /// ([`new`]). performs the compaction and returns the new "top" of the bucket
    fn compact_bucket(old: &[u8], new: &mut [u8]) -> usize {
        todo!()
    }

    #[inline]
    fn write_insup(buf: &mut [u8], key: &[u8], val: &[u8]) {
        buf[0] = 1;
        buf[1..5].copy_from_slice(&(key.len() as u32).to_be_bytes());
        buf[5..9].copy_from_slice(&(val.len() as u32).to_be_bytes());
        buf[9..9 + key.len()].copy_from_slice(key);
        buf[9 + key.len()..9 + key.len() + val.len()].copy_from_slice(val);
    }

    #[inline]
    fn write_delete(buf: &mut [u8], key: &[u8]) {
        buf[0] = 2;
        buf[1..5].copy_from_slice(&(key.len() as u32).to_be_bytes());
        buf[5..5 + key.len()].copy_from_slice(key);
    }
}
