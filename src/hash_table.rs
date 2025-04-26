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
    buffer::{Base, BaseMut, Delta, Page, PageBuffer, PageChunk, PageLayout, WriteRes},
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
        let (bucket_id, _, _) = self.get_bucket_id(key);
        let bucket_page = self.page_dir.get(bucket_id).read::<Bucket>();
        for chunk in bucket_page.iter_chunks() {
            match chunk {
                PageChunk::Delta(delta) => match delta {
                    BucketDelta::Set { key: k, val, .. } => {
                        if k == key {
                            out.extend(val);
                            return true;
                        }
                    }
                    BucketDelta::Del(k) => {
                        if k == key {
                            return false;
                        }
                    }
                },
                PageChunk::Base(base) => {
                    for entry in base.iter_entries() {
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

    // ok im making a big ole type mess, so i want to just wrap my head around the logic for a
    // mutation.
    //
    // - first, we find the bucket that we want to do the set on
    // - then, we try to add the delta to the bucket, if it works, or if it's already sealed, we
    //   just call it a day
    // - if we caused the buffer to be sealed, we create a new buffer, and compact the old one into
    //   it, then we try to apply our delta to the compacted buffer, if that works, we're done
    // - if it doesn't work, we need to split the compacted page into two
    //

    pub fn set(&self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        let (bucket_id, global_lvl, hash) = self.get_bucket_id(key);
        let bucket_buffer = self.page_dir.get(bucket_id);
        let delta = BucketDelta::Set { key, val, hash };
        match bucket_buffer.write_delta::<Bucket>(&delta) {
            WriteRes::Ok => Ok(()),
            WriteRes::Sealed => Err(()),
            WriteRes::Sealer(old_page) => {
                // we sealed the buffer and need to compact it,
                let (mut new_page, new_buf) = PageBuffer::new();
                let mut new_base = old_page.compact_into(new_buf);

                if let Err(()) = new_base.apply_delta(delta) {
                    // compacted buffer is still too full for our delta, so we need to split it
                    let (mut to_page, to_buf) = PageBuffer::new();
                    let mut to_base = BucketBaseMut::from(to_buf);
                    new_base.split_into(&mut to_base);

                    let (_, to_top) = to_base.unwrap();
                    to_page.set_top(to_top);
                    self.page_dir.push(to_page);

                    todo!()
                }

                let (_, new_top) = new_base.unwrap();
                new_page.set_top(new_top);
                self.page_dir.set(bucket_id, new_page);

                Ok(())
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), ()> {
        let (bucket_id, global_lvl, hash) = self.get_bucket_id(key);
        let bucket_buffer = self.page_dir.get(bucket_id);
        let delta = BucketDelta::Del(key);
        match bucket_buffer.write_delta::<Bucket>(&delta) {
            WriteRes::Ok => Ok(()),
            WriteRes::Sealed => Err(()),
            WriteRes::Sealer(old_page) => {
                // we sealed the buffer and need to compact it,
                // first we do the compaction
                let (new_page, new_buf) = PageBuffer::new();
                let mut new_base = BucketBaseMut::from(new_buf);
                old_page.compact_into(&mut new_base);

                if let Err(()) = new_base.apply_delta(delta) {
                    todo!()
                }

                self.page_dir.set(bucket_id, new_page);

                Ok(())
            }
        }
    }

    fn get_bucket_id(&self, key: &[u8]) -> (PageId, usize, u64) {
        let mut dir = self.page_dir.get(DIR_PAGE).read::<Dir>();
        let mut global_lvl = None;
        let mut slot = None;
        let mut hash = None;
        'list: loop {
            for chunk in dir.iter_chunks() {
                match chunk {
                    PageChunk::Base(dir_base) => {
                        if let None = global_lvl {
                            global_lvl = Some(dir_base.global_lvl());
                        }
                        if let None = slot {
                            let (s, h) = Self::find_slot(key, global_lvl.unwrap() as u64);
                            slot = Some(s);
                            hash = Some(h);
                        }
                        match dir_base.page_id_at(slot.unwrap()) {
                            Some(bucket_id) => {
                                return (bucket_id, global_lvl.unwrap(), hash.unwrap());
                            }
                            None => {
                                dir = self.page_dir.get(dir_base.next().unwrap()).read();
                                continue 'list;
                            }
                        }
                    }
                    PageChunk::Delta(_) => panic!(),
                }
            }
        }
    }

    // utils ======================================================================================

    #[inline]
    fn find_slot(key: &[u8], lvl: u64) -> (usize, u64) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        ((hash & ((1 << lvl) - 1)) as usize, hash)
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

enum Dir {}
impl<'d> PageLayout<'d> for Dir {
    type Delta = DirDelta;
    type Base = DirBase<'d>;
    type BaseMut = DirBaseMut;
}
struct DirBaseMut;
impl<'b> From<&'b mut [u8]> for DirBaseMut {
    fn from(_: &'b mut [u8]) -> Self {
        todo!()
    }
}
impl<'b> BaseMut<'b, Dir> for DirBaseMut {
    fn unwrap(self) -> (&'b mut [u8], usize) {
        todo!()
    }
    fn apply_base(&mut self, _base: DirBase) {
        todo!()
    }
    fn apply_delta(&mut self, _delta: DirDelta) -> Result<(), ()> {
        todo!()
    }
    fn split_into(&mut self, _to: &mut Self) {
        todo!()
    }
}
enum DirDelta {}
impl Delta<'_> for DirDelta {
    fn len(&self) -> usize {
        0
    }
    fn write_to_buf(&self, _buf: &mut [u8]) {}
}
impl<'d> From<(&'d [u8], u8)> for DirDelta {
    fn from(_data: (&'d [u8], u8)) -> Self {
        todo!()
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
impl<'b> Base<'b> for DirBase<'b> {}
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

enum Bucket {}
impl<'p> PageLayout<'p> for Bucket {
    type Base = BucketBase<'p>;
    type Delta = BucketDelta<'p>;
    type BaseMut = BucketBaseMut<'p>;
}
enum BucketDelta<'d> {
    Set {
        key: &'d [u8],
        val: &'d [u8],
        hash: u64,
    },
    Del(&'d [u8]),
}
impl<'d> Delta<'d> for BucketDelta<'d> {
    fn len(&self) -> usize {
        match self {
            Self::Del(key) => key.len(),
            Self::Set { key, val, .. } => 16 + key.len() + val.len(),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        debug_assert!(self.len() == buf.len());
        match self {
            Self::Del(key) => buf.copy_from_slice(key),
            Self::Set { key, val, hash } => {
                buf[0..8].copy_from_slice(&hash.to_be_bytes());
                buf[8..12].copy_from_slice(&(key.len()).to_be_bytes());
                buf[12..16].copy_from_slice(&(val.len()).to_be_bytes());
                buf[16..16 + key.len()].copy_from_slice(key);
                buf[16 + key.len()..].copy_from_slice(val);
            }
        }
    }
}
impl<'d> From<(&'d [u8], u8)> for BucketDelta<'d> {
    fn from(data: (&'d [u8], u8)) -> Self {
        match data.1 {
            1 => {
                // set
                let hash = u64::from_be_bytes(data.0[0..8].try_into().unwrap());
                let key_len = u32::from_be_bytes(data.0[8..12].try_into().unwrap()) as usize;
                let val_len = u32::from_be_bytes(data.0[12..16].try_into().unwrap()) as usize;
                Self::Set {
                    key: &data.0[16..16 + key_len],
                    val: &data.0[16 + key_len..16 + key_len + val_len],
                    hash,
                }
            }
            2 => Self::Del(data.0),
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
impl<'b> Base<'b> for BucketBase<'b> {}
impl<'b> BucketBase<'b> {
    pub fn iter_entries(&self) -> BucketBaseEntryIter {
        BucketBaseEntryIter::from(self.buf)
    }
    #[inline]
    pub fn num_entries(&self) -> u16 {
        u16::from_be_bytes(self.buf[1..3].try_into().unwrap())
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

struct BucketBaseMut<'b> {
    buf: &'b mut [u8],
    top: usize,
    bottom: usize,
}
impl<'b> From<(&'b mut [u8], BucketBase<'b>)> for BucketBaseMut<'b> {
    fn from((buf, base): (&'b mut [u8], BucketBase<'b>)) -> Self {
        todo!();
        Self {
            top: 0,
            bottom: buf.len(),
            buf,
        }
    }
}
impl<'b> BaseMut<'b, Bucket> for BucketBaseMut<'b> {
    fn unwrap(self) -> (&'b mut [u8], usize) {
        self.buf.copy_within(0..self.top, self.bottom - self.top);
        (self.buf, self.bottom - self.top)
    }
    fn apply_delta(&mut self, delta: BucketDelta) -> Result<(), ()> {
        match delta {
            BucketDelta::Del(key) => {}
            BucketDelta::Set { key, val, hash } => {}
        }
        todo!()
    }
    fn split_into(&mut self, _to: &mut Self) {
        todo!()
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
