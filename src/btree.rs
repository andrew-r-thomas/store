/*

    NOTE:
    for now im just gonna do this in memory and write some tests to make sure
    i roughly know what im doing first

*/

use std::collections::HashMap;
use std::fmt::Debug;

/// B+ Tree
pub struct BTree<K, V>
where
    K: Ord + Copy + Debug,
    V: Debug,
{
    root: usize,
    page_cap: usize,

    // just a hashmap for now to get a sense of things
    pages: HashMap<usize, Page<K, V>>,

    // scratch buf for keeping track of node traversals,
    // makes splits and merges easier
    path: Vec<usize>,
}

// TODO: sibling pointers
struct Page<K, V> {
    keys: Vec<K>,
    page_type: PageType<V>,
}
enum PageType<V> {
    Leaf { vals: Vec<V> },
    Inner { pointers: Vec<usize> },
}
impl<K, V> Page<K, V>
where
    K: Ord + Copy + Debug,
    V: Debug,
{
    fn split(&mut self) -> Self {
        let mut new_keys = Vec::with_capacity(self.keys.capacity());
        new_keys.extend(self.keys.split_off(self.keys.len() / 2));
        match &mut self.page_type {
            PageType::Inner { pointers } => {
                let mut new_pointers = Vec::with_capacity(pointers.capacity());
                new_pointers.extend(pointers.drain(pointers.len() / 2..));
                Self {
                    keys: new_keys,
                    page_type: PageType::Inner {
                        pointers: new_pointers,
                    },
                }
            }
            PageType::Leaf { vals } => {
                let mut new_vals = Vec::with_capacity(vals.capacity());
                new_vals.extend(vals.split_off(vals.len() / 2));
                Self {
                    keys: new_keys,
                    page_type: PageType::Leaf { vals: new_vals },
                }
            }
        }
    }
    /// errors if there isn't enough room
    fn leaf_insert(&mut self, key: K, val: V) -> Result<(), (K, V)> {
        match &mut self.page_type {
            PageType::Leaf { vals } => {
                if self.keys.len() < self.keys.capacity() {
                    let mut idx = None;
                    for (k, i) in self.keys.iter().zip(0..) {
                        if &key < k {
                            idx = Some(i);
                            break;
                        }
                    }
                    match idx {
                        Some(i) => {
                            self.keys.insert(i, key);
                            vals.insert(i, val);
                        }
                        None => {
                            self.keys.push(key);
                            vals.push(val);
                        }
                    }
                    Ok(())
                } else {
                    Err((key, val))
                }
            }
            _ => panic!(),
        }
    }
    /// errors if there isn't enough room
    fn inner_insert(&mut self, key: K, pointer: usize) -> Result<(), (K, usize)> {
        match &mut self.page_type {
            PageType::Inner { pointers } => {
                if self.keys.len() < self.keys.capacity() {
                    let mut idx = None;
                    for (k, i) in self.keys.iter().zip(0..) {
                        if &key < k {
                            idx = Some(i);
                            break;
                        }
                    }
                    match idx {
                        Some(i) => {
                            self.keys.insert(i, key);
                            pointers.insert(i, pointer);
                        }
                        None => {
                            self.keys.push(key);
                            pointers.push(pointer);
                        }
                    }
                    Ok(())
                } else {
                    Err((key, pointer))
                }
            }
            _ => panic!(),
        }
    }
}

impl<K, V> BTree<K, V>
where
    K: Ord + Copy + Debug,
    V: Debug,
{
    pub fn new(page_cap: usize) -> Self {
        let mut pages = HashMap::new();
        let keys = Vec::with_capacity(page_cap);
        let vals = Vec::with_capacity(page_cap);
        let root = Page {
            keys,
            page_type: PageType::Leaf { vals },
        };
        pages.insert(0, root);
        Self {
            root: 0,
            page_cap,
            pages,
            path: Vec::new(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.find_leaf_page(key);
        let page = self.pages.get(&self.path.last().unwrap()).unwrap();
        for (k, i) in page.keys.iter().zip(0..) {
            if k == key {
                if let PageType::Leaf { vals } = &page.page_type {
                    return Some(&vals[i]);
                }
            }
        }
        None
    }

    // ok so for a split,
    //
    // first we split the page, with the larger keys going into the new page
    // then we put the key into the appropriate page depending on how the
    // split ended up.
    //
    // then, we take the first key of the new page, and insert it into the parent
    // with the new page id as the pointer, and do this recursively if we have
    // size issues as we go up
    pub fn insert(&mut self, key: K, val: V) {
        // get the right leaf page for the insert
        self.find_leaf_page(&key);

        let page_id = self.path.pop().unwrap();
        let page = self.pages.get_mut(&page_id).unwrap();
        if let Err((key, val)) = page.leaf_insert(key, val) {
            // need to do a split
            let mut right = page.split();
            let right_first = right.keys.first().copied().unwrap();

            if &key < right.keys.first().unwrap() {
                page.leaf_insert(key, val).unwrap();
            } else {
                right.leaf_insert(key, val).unwrap();
            }

            let right_id = self.pages.len();
            self.pages.insert(right_id, right);

            let mut id_to_insert = right_id;
            let mut key_to_insert = right_first;
            while let Some(parent_id) = self.path.pop() {
                let parent = self.pages.get_mut(&parent_id).unwrap();
                if let Err((err_key, err_id)) = parent.inner_insert(key_to_insert, id_to_insert) {
                    let mut right = parent.split();
                    let right_first = right.keys.first().copied().unwrap();
                    if err_key < right_first {
                        parent.inner_insert(err_key, err_id).unwrap();
                    } else {
                        right.inner_insert(err_key, err_id).unwrap();
                    }
                    let right_id = self.pages.len();
                    self.pages.insert(right_id, right);
                    id_to_insert = right_id;
                    key_to_insert = right_first;
                } else {
                    return;
                }
            }
            // if we reach this point, we split the root
            let mut new_root = Page {
                keys: Vec::with_capacity(self.page_cap),
                page_type: PageType::Inner {
                    pointers: Vec::with_capacity(self.page_cap),
                },
            };
            new_root.inner_insert(key_to_insert, id_to_insert).unwrap();
            new_root.inner_insert(key_to_insert, self.root).unwrap();
            let new_root_id = self.pages.len();
            self.pages.insert(new_root_id, new_root);
            self.root = new_root_id;
        }
    }

    /// this method does not do tree rebalancing when an underflow occurs
    /// it will instead rely on future insertions or vaccuum operations to
    /// do the rebalancing
    pub fn remove(&mut self, key: &K) {
        self.find_leaf_page(key);
        let page = self.pages.get_mut(&self.path.last().unwrap()).unwrap();
        let i = {
            let mut out = None;
            for (k, i) in page.keys.iter().zip(0..) {
                if k == key {
                    out = Some(i);
                    break;
                }
            }
            out
        };
        if let Some(idx) = i {
            page.keys.remove(idx);
            if let PageType::Leaf { vals } = &mut page.page_type {
                vals.remove(idx);
            }
        }
    }

    fn find_leaf_page(&mut self, key: &K) {
        self.path.clear();
        let mut current_page_id = self.root;
        'trav: loop {
            let page = self.pages.get(&current_page_id).unwrap();
            match &page.page_type {
                PageType::Leaf { vals: _ } => break 'trav,
                PageType::Inner { pointers } => {
                    self.path.push(current_page_id);
                    for (k, i) in page.keys.iter().zip(0..) {
                        if key < k {
                            current_page_id = pointers[i];
                            continue 'trav;
                        }
                    }
                }
            }
        }
        self.path.push(current_page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[test]
    fn basic_set_get() {
        let mut btree = BTree::<u32, u32>::new(3);
        btree.insert(1, 1);
        btree.insert(2, 2);
        btree.insert(3, 3);
        btree.insert(4, 4);
        btree.insert(5, 5);
        btree.insert(6, 6);
        assert_eq!(btree.get(&1), Some(&1));
        btree.remove(&1);
        assert_eq!(btree.get(&1), None);
        assert_eq!(btree.get(&3), Some(&3));
    }
}
