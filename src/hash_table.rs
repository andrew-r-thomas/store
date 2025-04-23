/*

    TODO:
    - make hard retry interface as well as simple "try" interface for looping on sealed buffers vs
      just failing, right now everything is the "try" interface
    - right now, after we compact, we add writes as a delta, but we could just update the
      base page itself since we have exclusive access

    NOTE:
    - we won't ever shrink the hash table, that will be done by a vaccuum operation or something
    - we could probably pull off extendible hashing if we have the directory kept as a page itself
      rather than just an in memory structure, this would also make it easier to just have one
      mapping table implementation

*/

use crate::{
    PageId,
    buffer::{PageBuffer, ReserveResult},
    page_dir::PageDirectory,
};

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::atomic::Ordering,
};

const DIR_PAGE: PageId = 1;

pub struct HashTable<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> {
    page_dir: PageDirectory<BLOCK_SIZE, PAGE_SIZE>,
}
impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> HashTable<BLOCK_SIZE, PAGE_SIZE> {
    // public interface ===========================================================================

    /// hmmm
    pub fn new(num_buckets: usize) -> Self {
        todo!()
    }

    pub fn get(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        let (bucket_id, _) = self.get_bucket_id(key);
        let bucket = Bucket::from(self.page_dir.get(bucket_id).read());
        for chunk in bucket.iter_chunks() {
            match chunk {
                BucketChunk::Insup {
                    key: k,
                    val,
                    hash: _,
                } => {
                    if key == k {
                        out.extend(val);
                        return true;
                    }
                }
                BucketChunk::Delete(k) => {
                    if key == k {
                        return false;
                    }
                }
                BucketChunk::Base(base_page) => {
                    for entry in base_page.iter_entries() {
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
        let (bucket_id, global_lvl) = self.get_bucket_id(key);
        let bucket_buffer = self.page_dir.get(bucket_id);
        let delta_len = Self::insup_delta_len(key.len(), val.len());
        match bucket_buffer.reserve(delta_len) {
            ReserveResult::Ok(write_guard) => {
                Self::write_insup(write_guard.buf, key, val);
                Ok(())
            }
            ReserveResult::Sealed => Err(()),
            ReserveResult::Sealer(seal_guard) => {
                // we sealed the buffer and need to compact it,
                // first we do the compaction
                let (new_page, new_buf) = PageBuffer::new();
                let old_buf = seal_guard.wait_for_writers();
                let new_top = Self::compact_bucket(old_buf, new_buf);

                // now we check if there's room in the compacted buffer for our write
                let size = PAGE_SIZE - new_top;
                let out = {
                    if size + delta_len <= PAGE_SIZE {
                        Self::write_insup(&mut new_buf[new_top - delta_len..new_top], key, val);
                        new_page
                            .read_offset
                            .store(new_top - delta_len, Ordering::Release);
                        new_page
                            .write_offset
                            .store((new_top - delta_len) as isize, Ordering::Release);
                        Ok(())
                    } else {
                        // there isn't room in the buffer still, so we need to do a split
                        let (to_page, to_buf) = PageBuffer::<PAGE_SIZE>::new();
                        let local_lvl = Self::split_bucket(&mut new_buf[new_top..], to_buf);
                        if local_lvl < global_lvl {
                            // we can split the page without growing the dir
                            todo!()
                        } else {
                            // we need to grow the dir
                            todo!()
                        }
                    }
                };

                self.page_dir.set(bucket_id, new_page);

                out
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), ()> {
        let (bucket_id, global_lvl) = self.get_bucket_id(key);
        let bucket_buffer = self.page_dir.get(bucket_id);
        let delta_len = Self::delete_delta_len(key.len());
        match bucket_buffer.reserve(delta_len) {
            ReserveResult::Ok(write_guard) => {
                Self::write_delete(write_guard.buf, key);
                Ok(())
            }
            ReserveResult::Sealed => Err(()),
            ReserveResult::Sealer(seal_guard) => {
                // we sealed the buffer and need to compact it,
                // first we do the compaction
                let (new_page, new_buf) = PageBuffer::new();
                let old_buf = seal_guard.wait_for_writers();
                let new_top = Self::compact_bucket(old_buf, new_buf);

                let size = PAGE_SIZE - new_top;
                let out = {
                    if size + delta_len <= PAGE_SIZE {
                        Self::write_delete(&mut new_buf[new_top - delta_len..new_top], key);
                        new_page
                            .read_offset
                            .store(new_top - delta_len, Ordering::Release);
                        new_page
                            .write_offset
                            .store((new_top - delta_len) as isize, Ordering::Release);
                        Ok(())
                    } else {
                        todo!()
                    }
                };

                self.page_dir.set(bucket_id, new_page);

                out
            }
        }
    }

    fn get_bucket_id(&self, key: &[u8]) -> (PageId, usize) {
        let mut dir = Dir::from(self.page_dir.get(DIR_PAGE).read());
        let mut global_lvl = None;
        let mut slot = None;
        'list: loop {
            for chunk in dir.iter_chunks() {
                match chunk {
                    DirChunk::Base(dir_base) => {
                        if let None = global_lvl {
                            global_lvl = Some(dir_base.global_lvl());
                        }
                        if let None = slot {
                            slot = Some(Self::find_slot(key, global_lvl.unwrap() as u64));
                        }
                        match dir_base.page_id_at(slot.unwrap()) {
                            Some(bucket_id) => return (bucket_id, global_lvl.unwrap()),
                            None => {
                                dir = Dir::from(self.page_dir.get(dir_base.next().unwrap()).read());
                                continue 'list;
                            }
                        }
                    }
                }
            }
        }
    }

    // utils ======================================================================================

    #[inline]
    fn find_slot(key: &[u8], lvl: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash & ((1 << lvl) - 1)) as usize
    }

    /// takes bucket to be compacted ([`old`]), and a zeroed complete page buffer to compact into
    /// ([`new`]). performs the compaction and returns the new "top" of the bucket
    fn compact_bucket(old: &[u8], new: &mut [u8]) -> usize {}

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

unsafe impl<const PAGE_SIZE: usize, const BLOCK_SIZE: usize> Send
    for HashTable<PAGE_SIZE, BLOCK_SIZE>
{
}
unsafe impl<const PAGE_SIZE: usize, const BLOCK_SIZE: usize> Sync
    for HashTable<PAGE_SIZE, BLOCK_SIZE>
{
}

// ================================================================================================
// page layout logic and quality of life utils
// ================================================================================================

// dir pages ======================================================================================

struct Dir<'d> {
    buf: &'d [u8],
}
impl<'d> From<&'d [u8]> for Dir<'d> {
    fn from(buf: &'d [u8]) -> Self {
        Self { buf }
    }
}
impl<'d> Dir<'d> {
    pub fn iter_chunks(&self) -> DirChunkIter {
        DirChunkIter::from(self.buf)
    }
}

struct DirChunkIter<'i> {
    buf: &'i [u8],
    i: usize,
}
impl<'i> From<&'i [u8]> for DirChunkIter<'i> {
    fn from(buf: &'i [u8]) -> Self {
        Self { buf, i: 0 }
    }
}
enum DirChunk<'c> {
    Base(DirBase<'c>),
}
impl<'i> Iterator for DirChunkIter<'i> {
    type Item = DirChunk<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.buf.len() {
            return None;
        }

        match self.buf[self.i] {
            0 => {
                let out = Some(DirChunk::Base(DirBase::from(&self.buf[self.i..])));
                self.i = self.buf.len();
                out
            }
            _ => panic!(),
        }
    }
}

struct DirBase<'b> {
    buf: &'b [u8],
}
impl<'b> From<&'b [u8]> for DirBase<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}
impl<'b> DirBase<'b> {
    #[inline]
    pub fn global_lvl(&self) -> usize {
        self.buf[1] as usize
    }
    #[inline]
    pub fn next(&self) -> Option<PageId> {
        let n = u64::from_be_bytes(self.buf[self.buf.len() - 8..].try_into().unwrap());
        if n == 0 { None } else { Some(n) }
    }
    #[inline]
    pub fn num_entries(&self) -> usize {
        u16::from_be_bytes(self.buf[2..4].try_into().unwrap()) as usize
    }
    #[inline]
    pub fn page_id_at(&self, slot: usize) -> Option<PageId> {
        if slot >= self.num_entries() {
            None
        } else {
            Some(u64::from_be_bytes(
                self.buf[4 + (slot * 8)..4 + (slot * 8) + 8]
                    .try_into()
                    .unwrap(),
            ))
        }
    }
}

// bucket pages ===================================================================================

struct Bucket<'b> {
    buf: &'b [u8],
}
impl<'b> From<&'b [u8]> for Bucket<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}
impl Bucket<'_> {
    pub fn iter_chunks(&self) -> BucketChunkIter {
        BucketChunkIter::from(self.buf)
    }
    #[inline]
    pub fn local_lvl(&self) -> usize {
        *self.buf.last().unwrap() as usize
    }
}

struct BucketChunkIter<'i> {
    buf: &'i [u8],
    i: usize,
}
impl<'i> From<&'i [u8]> for BucketChunkIter<'i> {
    fn from(buf: &'i [u8]) -> Self {
        Self { buf, i: 0 }
    }
}
enum BucketChunk<'c> {
    Insup {
        key: &'c [u8],
        val: &'c [u8],
        hash: u64,
    },
    Delete(&'c [u8]),
    Base(BucketBase<'c>),
}
impl<'i> Iterator for BucketChunkIter<'i> {
    type Item = BucketChunk<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.buf.len() {
            return None;
        }

        match self.buf[self.i] {
            0 => {
                // base page
                let out = Some(BucketChunk::Base(BucketBase::from(&self.buf[self.i..])));
                self.i = self.buf.len();
                out
            }
            1 => {
                // insup
                let hash = u64::from_be_bytes(self.buf[self.i + 1..self.i + 9].try_into().unwrap());
                let key_len =
                    u32::from_be_bytes(self.buf[self.i + 9..self.i + 13].try_into().unwrap())
                        as usize;
                let val_len =
                    u32::from_be_bytes(self.buf[self.i + 13..self.i + 17].try_into().unwrap())
                        as usize;
                let out = Some(BucketChunk::Insup {
                    key: &self.buf[self.i + 17..self.i + 17 + key_len],
                    val: &self.buf[self.i + 17 + key_len..self.i + 17 + key_len + val_len],
                    hash,
                });
                self.i += 17 + key_len + val_len;
                out
            }
            2 => {
                // delete
                let key_len =
                    u32::from_be_bytes(self.buf[self.i + 1..self.i + 5].try_into().unwrap())
                        as usize;
                let out = Some(BucketChunk::Delete(
                    &self.buf[self.i + 5..self.i + 5 + key_len],
                ));
                self.i += 5 + key_len;
                out
            }
            _ => panic!(),
        }
    }
}
struct BucketBase<'b> {
    buf: &'b [u8],
}
impl<'b> From<&'b [u8]> for BucketBase<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}
impl<'b> BucketBase<'b> {
    pub fn iter_entries(&self) -> BucketBaseEntryIter {
        BucketBaseEntryIter::from(self.buf)
    }
}
struct BucketBaseEntryIter<'i> {
    buf: &'i [u8],
    cursor: usize,
    entry: usize,
}
struct BucketBaseEntry<'e> {
    pub key: &'e [u8],
    pub val: &'e [u8],
    pub hash: u64,
}

impl<'i> From<&'i [u8]> for BucketBaseEntryIter<'i> {
    fn from(buf: &'i [u8]) -> Self {
        let num_entries = u16::from_be_bytes(buf[1..3].try_into().unwrap()) as usize;
        Self {
            buf,
            entry: 0,
            cursor: 3 + num_entries * 8,
        }
    }
}
impl<'i> Iterator for BucketBaseEntryIter<'i> {
    type Item = BucketBaseEntry<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.buf.len() {
            return None;
        }

        let hash = u64::from_be_bytes(
            self.buf[3 + (self.entry * 16)..3 + (self.entry * 16) + 8]
                .try_into()
                .unwrap(),
        );
        let key_len = u32::from_be_bytes(
            self.buf[3 + (self.entry * 16) + 8..3 + (self.entry * 16) + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let val_len = u32::from_be_bytes(
            self.buf[3 + (self.entry * 16) + 12..3 + (self.entry * 16) + 16]
                .try_into()
                .unwrap(),
        ) as usize;

        let out = Some(BucketBaseEntry {
            key: &self.buf[self.cursor..self.cursor + key_len],
            val: &self.buf[self.cursor + key_len..self.cursor + key_len + val_len],
            hash,
        });

        self.cursor += key_len + val_len;
        self.entry += 1;

        out
    }
}

/// this is assumed to be logically a base page,
/// with [`buf`] being a full, exclusively owned page buffer
struct BucketBaseMut<'b> {
    buf: &'b mut [u8],
    top: usize,
}
impl<'b> BucketBaseMut<'b> {
    fn compact_from(buf: &'b mut [u8], other: Bucket<'b>) -> Self {
        // PERF: use a thread local scratch allocator for this, might need to use nightly or wait
        // until the allocator api is stable (ugh)
        let mut scratch = HashMap::new();
        let mut deltas = Vec::new();

        let mut total_data_len: isize = 0;
        for chunk in other.iter_chunks() {
            match chunk {
                BucketChunk::Insup {
                    key: _,
                    val: _,
                    hash: _,
                }
                | BucketChunk::Delete(_) => deltas.push(chunk),
                BucketChunk::Base(base_page) => {
                    for entry in base_page.iter_entries() {
                        scratch.insert(entry.key, (entry.val, entry.hash));
                        total_data_len += (entry.key.len() + entry.val.len()) as isize;
                    }
                }
            }
        }
        while let Some(delta) = deltas.pop() {
            match delta {
                BucketChunk::Delete(key) => {
                    if let Some((val, _)) = scratch.remove(key) {
                        total_data_len -= (key.len() + val.len()) as isize;
                    }
                }
                BucketChunk::Insup { key, val, hash } => {
                    if let None = scratch.insert(key, (val, hash)) {
                        total_data_len += (key.len() + val.len()) as isize;
                    }
                }
                _ => panic!(),
            }
        }

        let num_entries = scratch.len();
        let top = buf.len() - (3 + (num_entries * 16) + total_data_len as usize + 1);

        buf[top + 1..top + 3].copy_from_slice(&(num_entries as u16).to_be_bytes());

        let mut cursor = top + 3 + num_entries * 8;
        for ((key, (val, hash)), i) in scratch.iter().zip(0..) {
            buf[top + 3 + (i * 16)..top + 3 + (i * 16) + 8].copy_from_slice(&hash.to_be_bytes());
            buf[top + 3 + (i * 16) + 8..top + 3 + (i * 16) + 12]
                .copy_from_slice(&(key.len() as u32).to_be_bytes());
            buf[top + 3 + (i * 16) + 12..top + 3 + (i * 16) + 16]
                .copy_from_slice(&(val.len() as u32).to_be_bytes());
            buf[cursor..cursor + key.len()].copy_from_slice(key);
            buf[cursor + key.len()..cursor + key.len() + val.len()].copy_from_slice(val);
            cursor += key.len() + val.len();
        }

        todo!()
    }
    pub fn as_base(&self) -> BucketBase {
        BucketBase::from(&self.buf[self.top..])
    }
    pub fn unwrap(self) -> (&'b mut [u8], usize) {
        (self.buf, self.top)
    }
}

// tests ==========================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use std::{sync::Arc, thread};

    use rand::Rng;

    #[test]
    fn scratch() {
        const PAGE_SIZE: usize = 1024;
        let hash_table = Arc::new(HashTable::<PAGE_SIZE, 8>::new(64));

        // ok we wanna rewrite this a bit so that we get contention on buckets, but not on keys,
        // that way we can have ~deterministic checking of values we know should be in there vs not
        let mut threads = Vec::new();
        for t in 0..4 {
            let table = hash_table.clone();
            threads.push(
                thread::Builder::new()
                    .name(format!("{t}"))
                    .spawn(move || {
                        let mut rng = rand::rng();
                        let mut get_vec = Vec::new();
                        for i in 0..100000 as u32 {
                            let d = &i.to_be_bytes();
                            if rng.random_bool(0.5) {
                                // get
                                get_vec.clear();
                                if table.get(d, &mut get_vec) {
                                    assert_eq!(&get_vec, d);
                                }
                            } else if rng.random_bool(0.75) {
                                // set
                                match table.set(d, d) {
                                    Ok(()) => {
                                        // bro idk i just need a breakpoint here
                                    }
                                    Err(()) => {
                                        // same here
                                    }
                                }
                            } else {
                                // delete
                                match table.delete(d) {
                                    Ok(()) => {
                                        //
                                    }
                                    Err(()) => {
                                        //
                                    }
                                }
                            }
                        }
                    })
                    .unwrap(),
            );
        }

        for t in threads {
            t.join().unwrap();
        }
    }
}
