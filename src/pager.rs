use std::collections::HashMap;

use crate::{cache::LRUKCache, io::FileIO};

pub struct Pager {
    page_map: HashMap<u32, usize>,
    pool: Vec<Page>,
    free_list: Vec<usize>,
    cache_tracker: LRUKCache,
    io: FileIO,
}
impl Pager {
    pub fn new(capacity: usize, page_size: usize, io: FileIO) -> Self {
        Self {
            pool: vec![
                Page {
                    buf: vec![0; page_size],
                    dirty: false,
                };
                capacity
            ],
            free_list: Vec::from_iter(0..capacity),
            page_map: HashMap::with_capacity(capacity),
            cache_tracker: LRUKCache::new(),
            io,
        }
    }

    pub fn get(&mut self, page_id: u32) -> Result<&mut Page, PagerError> {
        if let Some(i) = self.page_map.get(&page_id) {
            self.cache_tracker.hit(page_id);
            return Ok(&mut self.pool[*i]);
        }
        if let Some(free_frame) = self.free_list.pop() {
            self.io.read_page(page_id, &mut self.pool[free_frame].buf);
            self.cache_tracker.hit(page_id);
            self.page_map.insert(page_id, free_frame);
            return Ok(&mut self.pool[free_frame]);
        }
        let mut evict_iter = self.cache_tracker.evict();
        while let Some(to_evict) = evict_iter.next() {
            let page_idx = *self.page_map.get(&to_evict).unwrap();
            if !self.pool[page_idx].dirty {
                // evict the page
                self.page_map.remove(&to_evict);
                self.io.read_page(page_id, &mut self.pool[page_idx].buf);
                self.pool[page_idx].dirty = false;
                self.page_map.insert(page_id, page_idx);
            }
        }
        Err(PagerError::HotCache)
    }
}

#[derive(Debug)]
pub enum PagerError {
    HotCache,
}

const SLOT_START: usize = 7;
/// page assumes the code calling it's functions knows the context of what
/// kind of page it is
#[derive(Clone)] // don't like this
pub struct Page {
    buf: Vec<u8>,
    dirty: bool,
}
impl Page {
    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }
    pub fn level(&self) -> u8 {
        self.buf[0]
    }
    pub fn slots(&self) -> usize {
        u16::from_be_bytes(self.buf[1..3].try_into().unwrap()) as usize
    }
    pub fn right_ptr(&self) -> u32 {
        u32::from_be_bytes(self.buf[3..7].try_into().unwrap())
    }
    pub fn get_slot(&self, slot_num: usize) -> &[u8] {
        let offset_start = SLOT_START + (slot_num * 4);
        let offset_end = offset_start + 4;
        let offset =
            u32::from_be_bytes(self.buf[offset_start..offset_end].try_into().unwrap()) as usize;
        &self.buf[offset..]
    }
    pub fn iter_slots(&self) -> SlotIter {
        SlotIter {
            page: self,
            current_slot: 0,
        }
    }
}
pub struct SlotIter<'si> {
    page: &'si Page,
    current_slot: usize,
}
impl<'si> Iterator for SlotIter<'si> {
    type Item = &'si [u8];
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_slot >= self.page.slots() {
            return None;
        }
        let out = self.page.get_slot(self.current_slot);
        self.current_slot += 1;
        Some(out)
    }
}
