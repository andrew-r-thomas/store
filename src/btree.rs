/*

    NOTE:
    for now im just gonna do this in memory and write some tests to make sure
    i roughly know what im doing first

*/

use std::{collections::HashMap, fmt::Display};

/// B+ Tree
pub struct BTree<K, V>
where
    K: Ord,
{
    root: usize,
    page_cap: usize,

    // just a hashmap for now to get a sense of things
    pages: HashMap<usize, Page<K, V>>,
}

// TODO: sibling pointers
enum Page<K, V> {
    Leaf { keys: Vec<K>, vals: Vec<V> },
    Inner { keys: Vec<K>, pointers: Vec<usize> },
}

impl<K, V> BTree<K, V>
where
    K: Ord,
{
    pub fn new(page_cap: usize) -> Self {
        let mut pages = HashMap::new();
        let keys = Vec::with_capacity(page_cap);
        let vals = Vec::with_capacity(page_cap);
        let root = Page::Leaf { keys, vals };
        pages.insert(0, root);
        Self {
            root: 0,
            page_cap,
            pages,
        }
    }

    pub fn get(&self, key: K) -> Option<&V> {
        let mut current_page = self.pages.get(&self.root).unwrap();

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

    pub fn set(&mut self, key: K, val: V) {
        let mut current_page_id = self.root;
        'trav: loop {
            let current_page = self.pages.get_mut(&current_page_id).unwrap();
            match current_page {
                Page::Inner { keys, pointers } => {
                    for (k, i) in keys.iter().zip(0..) {
                        if key < *k {
                            current_page_id = pointers[i];
                            continue 'trav;
                        }
                    }
                    current_page_id = *pointers.last().unwrap();
                }
                Page::Leaf { keys, vals } => {
                    for (k, i) in keys.iter().zip(0..) {
                        if key < *k {
                            // do insert
                            keys.insert(i, key);
                            vals.insert(i, val);
                            break 'trav;
                        }
                    }
                    // insert at end
                    keys.push(key);
                    vals.push(val);
                    break 'trav;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_set_get() {
        let mut btree = BTree::<&'static str, u32>::new(5);
        btree.set("one", 1);
        assert_eq!(btree.get("one"), Some(&1));
    }
}
