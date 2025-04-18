/*

    TODO:
    - make hard retry interface as well as simple "try" interface for looping on sealed buffers vs
      just failing, right now everything is the "try" interface
    - right now, after we compact, we add writes as a delta, but we could just update the
      base page itself since we have exclusive access

    NOTE:
    - we won't ever shrink the hash table, that will be done by a vaccuum operation or something

*/

use crate::{
    buffer::{PageBuffer, ReserveResult},
    mapping_table::Table,
    page_table::PageId,
};

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::atomic::Ordering,
};

pub struct HashTable<const PAGE_SIZE: usize, const B: usize> {
    buckets: u64,
    table: Table<B>,
}
impl<const PAGE_SIZE: usize, const B: usize> HashTable<PAGE_SIZE, B> {
    // public interface ===========================================================================

    pub fn get(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        let bucket = self.table.read(self.find_bucket(key)).read();
        for chunk in ChunkIter::new(bucket) {
            match chunk {
                Chunk::Insup { key: k, val } => {
                    if key == k {
                        out.extend(val);
                        return true;
                    }
                }
                Chunk::Delete(k) => {
                    if key == k {
                        return false;
                    }
                }
                Chunk::Base(base_page) => {
                    for entry in BaseEntryIter::new(base_page) {
                        if entry.key == key {
                            out.extend(entry.val);
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    pub fn set(&self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        let delta_len = Self::insup_delta_len(key.len(), val.len());
        let bucket_id = self.find_bucket(key);
        match self.table.read(bucket_id).reserve(delta_len) {
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

                // now we check if there's room in the compacted buffer for our write
                let size = PAGE_SIZE - new_top;
                let out = {
                    if size + delta_len <= PAGE_SIZE {
                        Self::write_insup(&mut new_buf[new_top - delta_len..new_top], key, val);
                        Ok(())
                    } else {
                        Err(())
                    }
                };

                // need to update the ptrs for the page buffer, and replace the page in the
                // mapping table
                new_page
                    .read_offset
                    .store(new_top - delta_len, Ordering::Release);
                new_page
                    .write_offset
                    .store((new_top - delta_len) as isize, Ordering::Release);
                self.table.update(bucket_id, new_page);

                out
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), ()> {
        let delta_len = Self::delete_delta_len(key.len());
        let bucket_id = self.find_bucket(key);
        match self.table.read(bucket_id).reserve(delta_len) {
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

                let size = PAGE_SIZE - new_top;
                let out = {
                    if size + delta_len <= PAGE_SIZE {
                        Self::write_delete(&mut new_buf[new_top - delta_len..new_top], key);
                        Ok(())
                    } else {
                        Err(())
                    }
                };

                // need to update the ptrs for the page buffer, and replace the page in the
                // mapping table
                new_page
                    .read_offset
                    .store(new_top - delta_len, Ordering::Release);
                new_page
                    .write_offset
                    .store((new_top - delta_len) as isize, Ordering::Release);
                self.table.update(bucket_id, new_page);

                out
            }
        }
    }

    // private functions ==========================================================================

    #[inline]
    fn find_bucket(&self, key: &[u8]) -> PageId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        hash % self.buckets
    }

    // utils ======================================================================================

    /// takes bucket to be compacted ([`old`]), and a zeroed complete page buffer to compact into
    /// ([`new`]). performs the compaction and returns the new "top" of the bucket
    fn compact_bucket(old: &[u8], new: &mut [u8]) -> usize {
        // PERF: use a thread local scratch allocator for this, might need to use nightly or wait
        // until the allocator api is stable (ugh)
        let mut scratch = HashMap::new();

        let mut total_len: isize = 0;
        for chunk in ChunkIter::new(old) {
            match chunk {
                Chunk::Insup { key, val } => {
                    if let None = scratch.insert(key, val) {
                        total_len += (key.len() + val.len()) as isize;
                    }
                }
                Chunk::Delete(key) => {
                    if let Some(val) = scratch.remove(key) {
                        total_len -= (key.len() + val.len()) as isize;
                    }
                }
                Chunk::Base(base_page) => {
                    for entry in BaseEntryIter::new(base_page) {
                        if let None = scratch.insert(entry.key, entry.val) {
                            total_len += (entry.key.len() + entry.val.len()) as isize;
                        }
                    }
                }
            }
        }

        let local_level = *old.last().unwrap();
        let num_entries = scratch.len();
        let top = new.len() - (3 + (num_entries * 8) + total_len as usize + 1);

        new[top + 1..top + 3].copy_from_slice(&(num_entries as u16).to_be_bytes());
        *new.last_mut().unwrap() = local_level;

        let mut cursor = top + 3 + num_entries * 8;
        for ((key, val), i) in scratch.iter().zip(0..) {
            new[top + 3 + (i * 8)..top + 3 + (i * 8) + 4]
                .copy_from_slice(&(key.len() as u32).to_be_bytes());
            new[top + 3 + (i * 8) + 4..top + 3 + (i * 8) + 8]
                .copy_from_slice(&(val.len() as u32).to_be_bytes());
            new[cursor..cursor + key.len()].copy_from_slice(key);
            new[cursor + key.len()..cursor + key.len() + val.len()].copy_from_slice(val);
            cursor += key.len() + val.len();
        }

        top
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
    #[inline]
    fn insup_delta_len(key_len: usize, val_len: usize) -> usize {
        9 + key_len + val_len
    }
    #[inline]
    fn delete_delta_len(key_len: usize) -> usize {
        5 + key_len
    }
}

pub enum WriteResult {
    Ok,
    Sealed,
    Full,
}

// some quality of life iterators =================================================================

struct ChunkIter<'i> {
    buf: &'i [u8],
    i: usize,
}
enum Chunk<'c> {
    Insup { key: &'c [u8], val: &'c [u8] },
    Delete(&'c [u8]),
    Base(&'c [u8]),
}
struct BaseEntryIter<'i> {
    buf: &'i [u8],
    cursor: usize,
    entry: usize,
}
struct Entry<'e> {
    pub key: &'e [u8],
    pub val: &'e [u8],
}

impl<'i> ChunkIter<'i> {
    fn new(page: &'i [u8]) -> Self {
        Self { buf: page, i: 0 }
    }
}
impl<'i> Iterator for ChunkIter<'i> {
    type Item = Chunk<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.buf.len() {
            return None;
        }

        match self.buf[self.i] {
            0 => {
                // base page
                let out = Some(Chunk::Base(&self.buf[self.i..]));
                self.i = self.buf.len();
                out
            }
            1 => {
                // insup
                let key_len =
                    u32::from_be_bytes(self.buf[self.i + 1..self.i + 5].try_into().unwrap())
                        as usize;
                let val_len =
                    u32::from_be_bytes(self.buf[self.i + 5..self.i + 9].try_into().unwrap())
                        as usize;
                let out = Some(Chunk::Insup {
                    key: &self.buf[self.i + 9..self.i + 9 + key_len],
                    val: &self.buf[self.i + 9 + key_len..self.i + 9 + val_len],
                });
                self.i += 9 + key_len + val_len;
                out
            }
            2 => {
                // delete
                let key_len =
                    u32::from_be_bytes(self.buf[self.i + 1..self.i + 5].try_into().unwrap())
                        as usize;
                let out = Some(Chunk::Delete(&self.buf[self.i + 5..self.i + 5 + key_len]));
                self.i += 5 + key_len;
                out
            }
            _ => panic!(),
        }
    }
}

impl<'i> BaseEntryIter<'i> {
    pub fn new(buf: &'i [u8]) -> Self {
        let num_entries = u16::from_be_bytes(buf[1..3].try_into().unwrap()) as usize;
        Self {
            buf,
            entry: 0,
            cursor: 3 + num_entries * 8,
        }
    }
}
impl<'i> Iterator for BaseEntryIter<'i> {
    type Item = Entry<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.buf.len() {
            return None;
        }

        let key_len = u32::from_be_bytes(
            self.buf[3 + self.entry * 8..3 + (self.entry * 8) + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let val_len = u32::from_be_bytes(
            self.buf[3 + (self.entry * 8) + 4..3 + (self.entry * 8) + 8]
                .try_into()
                .unwrap(),
        ) as usize;

        let out = Some(Entry {
            key: &self.buf[self.cursor..self.cursor + key_len],
            val: &self.buf[self.cursor + key_len..self.cursor + key_len + val_len],
        });

        self.cursor += key_len + val_len;
        self.entry += 1;

        out
    }
}
