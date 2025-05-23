use std::collections::HashMap;

use crate::PageId;

/// NOTE: gonna try to do a 2Q cache at first for this
pub struct Cache {
    pub id_map: HashMap<PageId, usize>,
}
impl Cache {
    pub fn new(cap: usize) -> Self {
        Self {
            id_map: HashMap::with_capacity(cap),
        }
    }
    pub fn get(&mut self, page_id: PageId) -> Option<usize> {
        let out = self.id_map.get(&page_id).copied();
        if let Some(_) = out {
            todo!()
        }
        out
    }
    pub fn insert(&mut self, page_id: PageId, frame: usize) {
        if let Some(_) = self.id_map.insert(page_id, frame) {
            panic!()
        }
    }
    pub fn evict(&mut self) -> (PageId, usize) {
        todo!()
    }
}
