/*

    TODO:
    - make buckets a pow of 2 and do bit manipulation for the bucket finding

*/

use crate::{buffer::ReserveResult, mapping_table::Table, page_table::PageId};

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};

pub struct HashTable<const PAGE_SIZE: usize, const B: usize> {
    buckets: AtomicU64,
    table: Table<B>,
}
impl<const PAGE_SIZE: usize, const B: usize> HashTable<PAGE_SIZE, B> {
    pub fn get(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        // first we hash the key
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // then we find the page based on the hash and the capacity
        let buckets = self.buckets();
        let page_id = (hash % buckets) + 1; // plus 1 bc page 0 is reserved for the free list

        let mut page = self.table.read(page_id).read();
        loop {
            match Self::search_page(page, key) {
                PageSearchResult::Some(val) => {
                    out.extend(val);
                    return true;
                }
                PageSearchResult::Next(next_page_id) => page = self.table.read(next_page_id).read(),
                PageSearchResult::None => return false,
            }
        }
    }
    pub fn set(&self, key: &[u8], val: &[u8]) -> Result<(), ()> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let buckets = self.buckets();
        let page_id = (hash % buckets) + 1;

        // TODO: ok we gotta figure out how to manage compacting vs looking into the next page in
        // the chain if we're doing chained hashing
        let delta_len = 9 + key.len() + val.len();
        match self.table.read(page_id).reserve(delta_len) {
            ReserveResult::Ok(write_guard) => {
                Self::write_insup(write_guard.buf, key, val);
                Ok(())
            }
            ReserveResult::Sealed => Err(()),
            ReserveResult::Sealer(_seal_guard) => {
                todo!()
            }
        }
    }
    pub fn delete() {}

    #[inline]
    pub fn buckets(&self) -> u64 {
        self.buckets.load(Ordering::Acquire)
    }

    fn search_page<'s>(page: &'s [u8], key: &[u8]) -> PageSearchResult<'s> {
        let mut i = 0;
        loop {
            match page[i] {
                0 => {
                    // base page
                    let num_entries =
                        u16::from_be_bytes(page[i + 1..i + 3].try_into().unwrap()) as usize;
                    let mut cursor = i + 3 + 8 * num_entries;
                    // TODO: probably a good idea to make these sorted and do binary search
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
                            return PageSearchResult::Some(
                                &page[cursor + key_len..cursor + key_len + val_len],
                            );
                        }
                        cursor += key_len + val_len;
                    }
                    // if we get here, check if there's a next pointer, if there is, return that,
                    // otherwise they key is not in the table
                    let next_ptr = u64::from_be_bytes(page[page.len() - 8..].try_into().unwrap());
                    if next_ptr == 0 {
                        return PageSearchResult::None;
                    } else {
                        return PageSearchResult::Next(next_ptr);
                    }
                }
                1 => {
                    // insup
                    let key_len =
                        u32::from_be_bytes(page[i + 1..i + 5].try_into().unwrap()) as usize;
                    let val_len =
                        u32::from_be_bytes(page[i + 5..i + 9].try_into().unwrap()) as usize;
                    if key == &page[i + 9..i + 9 + key_len] {
                        return PageSearchResult::Some(
                            &page[i + 9 + key_len..i + 9 + key_len + val_len],
                        );
                    }
                    i += 9 + key_len + val_len;
                }
                2 => {
                    // delete
                    todo!();
                }
                _ => panic!(),
            }
        }
    }
    #[inline]
    fn write_insup(buf: &mut [u8], key: &[u8], val: &[u8]) {
        buf[0] = 1;
        buf[1..5].copy_from_slice(&(key.len() as u32).to_be_bytes());
        buf[5..9].copy_from_slice(&(val.len() as u32).to_be_bytes());
        buf[9..9 + key.len()].copy_from_slice(key);
        buf[9 + key.len()..].copy_from_slice(val);
    }
}

enum PageSearchResult<'r> {
    Some(&'r [u8]),
    None,
    Next(PageId),
}
