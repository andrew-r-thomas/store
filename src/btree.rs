/*

    NOTE:
    for now im just gonna do this in memory and write some tests to make sure
    i roughly know what im doing first

*/

use std::collections::HashMap;

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

    pub fn get(&self, key: &K) -> Option<&V> {
        let page_id = self.find_leaf_page(key);
        let page = self.pages.get(&page_id).unwrap();
        if let Page::Leaf { keys, vals } = page {
            for (k, i) in keys.iter().zip(0..) {
                if k == key {
                    return Some(&vals[i]);
                }
            }
        }
        None
    }

    pub fn insert(&mut self, key: K, val: V) {
        let page_id = self.find_leaf_page(&key);
        let page = self.pages.get_mut(&page_id).unwrap();
        if let Page::Leaf { keys, vals } = page {
            if keys.len() >= self.page_cap {
                // TODO: gonna need to do a split
            }
            let mut idx = None;
            for (k, i) in keys.iter().zip(0..) {
                if key < *k {
                    idx = Some(i);
                    break;
                }
            }
            match idx {
                Some(i) => {
                    keys.insert(i, key);
                    vals.insert(i, val);
                }
                None => {
                    keys.push(key);
                    vals.push(val);
                }
            }
        }
    }

    /// this method does not do tree rebalancing when an underflow occurs
    /// it will instead rely on future insertions or vaccuum operations to
    /// do the rebalancing
    pub fn remove(&mut self, key: &K) {
        let page_id = self.find_leaf_page(key);
        let page = self.pages.get_mut(&page_id).unwrap();
        if let Page::Leaf { keys, vals } = page {
            let i = {
                let mut out = None;
                for (k, i) in keys.iter().zip(0..) {
                    if k == key {
                        out = Some(i);
                        break;
                    }
                }
                out
            };
            if let Some(idx) = i {
                keys.remove(idx);
                vals.remove(idx);
            }
        }
    }

    fn find_leaf_page(&self, key: &K) -> usize {
        let mut current_page_id = self.root;
        'trav: while let Page::Inner { keys, pointers } = self.pages.get(&current_page_id).unwrap()
        {
            for (k, i) in keys.iter().zip(0..) {
                if key < k {
                    current_page_id = pointers[i];
                    continue 'trav;
                }
            }
        }
        current_page_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_set_get() {
        let mut btree = BTree::<&'static str, u32>::new(5);
        btree.insert("one", 1);
        btree.insert("two", 2);
        btree.insert("three", 3);
        btree.insert("four", 4);
        assert_eq!(btree.get(&"one"), Some(&1));
        btree.remove(&"one");
        assert_eq!(btree.get(&"one"), None);
        assert_eq!(btree.get(&"three"), Some(&3));
    }
}
