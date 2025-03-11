use std::collections::HashMap;

use crate::{cache::LRUKCache, io::FileIO};

pub struct Pager {
    page_map: HashMap<u32, usize>,
    pool: Vec<Vec<u8>>,
    free_list: Vec<usize>,
    cache_tracker: LRUKCache,
    io: FileIO,
}
impl Pager {
    pub fn new(capacity: usize, page_size: usize, io: FileIO) -> Self {
        Self {
            pool: vec![vec![0; page_size]; capacity],
            free_list: Vec::from_iter(0..capacity),
            page_map: HashMap::with_capacity(capacity),
            cache_tracker: LRUKCache::new(),
            io,
        }
    }

    pub fn get(&mut self, page_id: u32) -> Result<Page, PageError> {
        if let Some(i) = self.page_map.get(&page_id) {
            self.cache_tracker.hit(page_id);
            return Ok(Page {
                buf: &self.pool[*i],
            });
        }
        if let Some(free_frame) = self.free_list.pop() {
            self.io.read_page(page_id, &mut self.pool[free_frame]);
            self.cache_tracker.hit(page_id);
            self.page_map.insert(page_id, free_frame);
            return Ok(Page {
                buf: &self.pool[free_frame],
            });
        }
        let to_evict = self.cache_tracker.evict().next().unwrap();
        let idx = self.page_map.remove(&to_evict).unwrap();
        // TODO: we gotta check for dirty n stuff
        self.io.read_page(page_id, &mut self.pool[idx]);
        self.page_map.insert(page_id, idx);
        Ok(Page {
            buf: &self.pool[idx],
        })
    }

    // TODO:
    pub fn get_mut() {}
}

enum PageError {
    Uhhh,
}

pub enum PageType {
    Key {
        level: u32,
        left_sib: u32,
        right_sib: u32,
    },
    Val,
}
pub struct Page<'p> {
    buf: &'p [u8],
}
pub struct PageMut<'pm> {
    buf: &'pm mut [u8],
}
