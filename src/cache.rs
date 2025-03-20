use std::collections::{BinaryHeap, HashMap};

pub struct LRUKCache {
    map: HashMap<u32, [u64; 2]>,
    ts: u64,
}

impl LRUKCache {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            ts: 0,
        }
    }

    /// called whenever a page is accessed, records a "cache hit"
    pub fn hit(&mut self, page_id: u32) {
        self.ts += 1;
        match self.map.get_mut(&page_id) {
            Some(hits) => {
                hits[1] = hits[0];
                hits[0] = self.ts;
            }
            None => {
                self.map.insert(page_id, [self.ts, 0]);
            }
        }
    }

    /// called when the pager has determined it needs to evict something
    /// from the buffer pool, returns an iterator over pages ids from best
    /// eviction option to worst, so that pager can ignore pages it doesn't
    /// want to evict for some reason
    pub fn evict(&mut self) -> BinaryHeap<EvictOption> {
        let mut out = BinaryHeap::new();
        for item in &self.map {
            out.push(EvictOption {
                dist: item.1[0] - item.1[1],
                id: *item.0,
            });
        }
        out
    }

    /// remove an item from the cache, this means it will not show up when
    /// iterating over the evict options
    pub fn remove(&mut self, page_id: u32) {
        self.map.remove(&page_id);
    }
}

pub struct Evict<'e> {
    heap: &'e mut BinaryHeap<EvictOption>,
}
impl Iterator for Evict<'_> {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        match self.heap.pop() {
            Some(eo) => Some(eo.id),
            None => None,
        }
    }
}

#[derive(Ord, Eq)]
pub struct EvictOption {
    dist: u64,
    pub id: u32,
}
impl PartialEq for EvictOption {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}
impl PartialOrd for EvictOption {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.dist.partial_cmp(&other.dist)
    }
}
