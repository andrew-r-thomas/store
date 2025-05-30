use std::collections::HashMap;

use itertools::Itertools;

use crate::PageId;

pub struct Cache {
    pub id_map: HashMap<PageId, usize>,
    pub hits: Vec<[u64; 2]>,
    pub hit: u64,
}
impl Cache {
    pub fn new(cap: usize) -> Self {
        Self {
            id_map: HashMap::with_capacity(cap),
            hits: vec![[u64::MAX, 0]; cap],
            hit: 0,
        }
    }
    pub fn get(&mut self, page_id: PageId) -> Option<usize> {
        match self.id_map.get(&page_id) {
            Some(idx) => {
                self.hits[*idx][1] = self.hits[*idx][0];
                self.hits[*idx][0] = self.hit;
                self.hit += 1;
                Some(*idx)
            }
            None => None,
        }
    }
    pub fn insert(&mut self, page_id: PageId, idx: usize) {
        if let Some(_) = self.id_map.insert(page_id, idx) {
            panic!()
        }
        self.hits[idx][0] = self.hit;
        self.hits[idx][1] = 0;
        self.hit += 1;
    }
    pub fn remove(&mut self, page_id: PageId) {
        self.id_map.remove(&page_id).unwrap();
    }
    pub fn evict_cands(&self) -> impl Iterator<Item = (PageId, usize)> {
        self.id_map
            .iter()
            .map(|(pid, idx)| (self.hits[*idx][0] - self.hits[*idx][1], (*pid, *idx)))
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .rev()
            .map(|(_, out)| out)
    }
}
