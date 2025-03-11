use std::collections::HashMap;
use std::hash::Hash;

pub trait Cache<K, V> {
    fn get(&mut self, key: K) -> Option<V>;
    fn set(&mut self, key: K, val: V);
    fn evict(&mut self) -> K;
    fn remove(&mut self, key: K);
}

pub struct LRUCache<K, V>
where
    K: Hash + Eq,
{
    list: Vec<K>,
    map: HashMap<K, (V, usize)>,
}

impl<K, V> Cache<K, V> for LRUCache<K, V>
where
    K: Hash + Eq,
{
    fn get(&mut self, key: K) -> Option<V> {
        match self.map.get(&key) {
            Some((val, i)) => {}
            None => None,
        }
    }
    fn set(&mut self, key: K, val: V) {}
    fn evict(&mut self) -> K {}
    fn remove(&mut self, key: K) {}
}
