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
    fn inner_insert(&mut self, key: K, pointer: usize) -> Result<(), K> {
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
                    Err(key)
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

            let parent_id = self.path.pop();
            match parent_id {
                Some(pid) => {
                    let parent = self.pages.get_mut(&pid).unwrap();
                    // TODO: recursive
                    let _ = parent.inner_insert(right_first, right_id);
                }
                None => {}
            }
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

    // split uses the path variable to know where to do the split,
    fn split(&mut self) -> usize {
        // get all our bookkeeping data
        let page_id = self.path.pop().unwrap();
        let new_page_id = self.pages.len();
        let page = self.pages.get_mut(&page_id).unwrap();
        let new_keys = page.keys.split_off(page.keys.len() / 2);

        // make the new page
        match &mut page.page_type {
            PageType::Leaf { vals } => {
                let new_vals = vals.split_off(vals.len() / 2);
                self.pages.insert(
                    new_page_id,
                    Page {
                        keys: new_keys,
                        page_type: PageType::Leaf { vals: new_vals },
                    },
                );
            }
            PageType::Inner { pointers } => {
                todo!()
            }
        }

        // update the parent
        let parent_page_id = self.path.pop();
        match parent_page_id {
            Some(ppid) => {
                let parent = self.pages.get_mut(&ppid).unwrap();
                if parent.keys.len() < self.page_cap {
                } else {
                }
            }
            None => {
                // we split the root
            }
        }
        todo!()
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
