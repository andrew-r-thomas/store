/*

    NOTE:
    for now im just gonna do this in memory and write some tests to make sure
    i roughly know what im doing first

*/

use std::collections::HashMap;

/// B+ Tree
pub struct BTree<K, V, const PAGE_CAP: usize>
where
    K: Ord,
{
    root: Option<usize>,
    // just a hashmap for now to get a sense of things
    pages: HashMap<usize, Page<K, V, PAGE_CAP>>,
}

enum Page<K, V, const PAGE_CAP: usize> {
    Leaf { keys: Vec<K>, vals: Vec<V> },
    Inner { keys: Vec<K>, pointers: Vec<usize> },
}

impl<K, V, const PAGE_CAP: usize> BTree<K, V, PAGE_CAP>
where
    K: Ord,
{
    pub fn new() -> Self {
        Self {
            root: None,
            pages: HashMap::new(),
        }
    }

    pub fn get(&self, key: K) -> Option<&V> {
        if self.root.is_none() {
            return None;
        }

        let mut current_page = self.pages.get(&self.root.unwrap()).unwrap();

        'trav: loop {
            match current_page {
                Page::Leaf { keys, vals } => {
                    for (k, i) in keys.iter().zip(0..) {
                        if *k == key {
                            return Some(&vals[i]);
                        }
                    }
                    return None;
                }
                Page::Inner { keys, pointers } => {
                    for (k, i) in keys.iter().zip(0..) {
                        if key < *k {
                            current_page = self.pages.get(&pointers[i]).unwrap();
                            continue 'trav;
                        }
                    }
                    current_page = self.pages.get(pointers.last().unwrap()).unwrap();
                }
            }
        }
    }

    pub fn set(&mut self, key: K, val: V) {}

    pub fn delete(&mut self, key: K) {}
}
