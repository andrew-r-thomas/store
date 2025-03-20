use crate::pager::{Cell, Page, Pager};

use std::{collections::HashMap, sync::Arc};

use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock};

/// btree cursors are single threaded, you will have one per transaction??
/// something like that idk man
pub struct BTreeCursor {
    pager: Arc<Pager>,
    write_lock_map: HashMap<u32, ArcRwLockWriteGuard<RawRwLock, Page>>,
    read_lock_map: HashMap<u32, ArcRwLockReadGuard<RawRwLock, Page>>,
    path: Vec<u32>,
}
impl BTreeCursor {
    pub fn new(pager: Arc<Pager>) -> Self {
        Self {
            pager,
            write_lock_map: HashMap::new(),
            read_lock_map: HashMap::new(),
            path: Vec::new(),
        }
    }
    /// navigate to the leaf node that would contain [`key`] optimistically
    /// (taking read locks)
    pub fn search_opt(&mut self, key: &[u8]) {
        self.path.clear();
        let mut current_page_id = self.pager.get_root();
        let mut searching = true;
        while searching {
            let current_page = self.pager.get(current_page_id);
            if current_page.level() == 0 {
                searching = false;
            } else {
                match current_page.get(key) {
                    Some(cell) => {
                        if let Cell::InnerCell { key: _, left_ptr } = cell {
                            current_page_id = left_ptr;
                        }
                    }
                    None => current_page_id = current_page.right_ptr(),
                }
            }
            self.path.push(current_page_id);
            self.read_lock_map.insert(current_page_id, current_page);
        }
    }
    /// navigate to the leaf node that would contain [`key`] pessemistically
    /// (taking write locks)
    pub fn search_pes(&mut self, key: &[u8]) {
        self.path.clear();
        let mut current_page_id = self.pager.get_root();
        let mut searching = true;
        while searching {
            let current_page = self.pager.get_mut(current_page_id);
            if current_page.level() == 0 {
                searching = false;
            } else {
                match current_page.get(key) {
                    Some(cell) => {
                        if let Cell::InnerCell { key: _, left_ptr } = cell {
                            current_page_id = left_ptr;
                        }
                    }
                    None => current_page_id = current_page.right_ptr(),
                }
            }
            self.path.push(current_page_id);
            self.write_lock_map.insert(current_page_id, current_page);
        }
    }

    /// attempt to get the value indexed by [`key`], and write it into [`buf`]
    /// the returned boolean indicates whether the value was found
    /// idk if this api should be part of the cursor, or live above it
    pub fn get(&mut self, key: &[u8], buf: &mut Vec<u8>) -> bool {
        self.search_opt(key);
        let leaf_id = self.path.last().unwrap();
        let leaf = self.read_lock_map.get(&leaf_id).unwrap();
        let out = match leaf.get(key) {
            Some(cell) => {
                if let Cell::LeafCell { key: k, val } = cell {
                    if key == k {
                        for v in val {
                            buf.push(*v);
                        }
                        true
                    } else {
                        false
                    }
                } else {
                    panic!(
                        "non leaf cell in leaf page, this should really be part of the type system"
                    );
                }
            }
            None => false,
        };

        self.read_lock_map.clear();

        out
    }

    pub fn set(&mut self, key: &[u8], val: &[u8]) {
        self.search_pes(key);
        let cell = Cell::LeafCell { key, val };
        let leaf_id = *self.path.last().unwrap();
        if let Err(cell) = self.write_lock_map.get_mut(&leaf_id).unwrap().set(cell) {
            let (middle_key, other) = self.split();
            if key <= &middle_key {
                self.write_lock_map
                    .get_mut(&other)
                    .unwrap()
                    .set(cell)
                    .unwrap();
            } else {
                self.write_lock_map
                    .get_mut(&leaf_id)
                    .unwrap()
                    .set(cell)
                    .unwrap();
            }
        }

        self.write_lock_map.clear();
    }

    pub fn split(&mut self) -> (Vec<u8>, u32) {
        let from_id = self.path.pop().unwrap();
        let from_page = self.write_lock_map.get_mut(&from_id).unwrap();
        let (to_id, mut to_page) = self.pager.create_page();
        let mut scratch = Page {
            buf: vec![0; from_page.buf.capacity()],
            dirty: false,
        };

        // FIX: i think we're getting deadlocks, and this is a good culprit
        // along with the haphazardly mutex'd stuff in the pager
        match from_page.left_sib() {
            0 => {
                from_page.set_left_sib(to_id);
                to_page.set_right_sib(from_id);
            }
            n => {
                let mut left = self.pager.get_mut(n);
                left.set_right_sib(to_id);
                to_page.set_left_sib(n);
                from_page.set_left_sib(to_id);
                to_page.set_right_sib(from_id);
            }
        }
        // i know, i know, just trying to figure this shit out
        let middle_key = from_page.split_into(&mut to_page, &mut scratch);
        println!(
            "middle key: {}",
            u32::from_be_bytes(middle_key.clone().try_into().unwrap())
        );
        let new_parent_cell = Cell::InnerCell {
            key: &middle_key,
            left_ptr: to_id,
        };

        let parent_id = self.path.last();
        match parent_id {
            Some(parent_id) => {
                println!("updating parent: {parent_id}");
                let pid = *parent_id; // dumb
                if let Err(cell) = self
                    .write_lock_map
                    .get_mut(&pid)
                    .unwrap()
                    .set(new_parent_cell)
                {
                    println!("recursive split incoming");
                    let (m_key, o) = self.split();
                    if cell.key() <= &m_key {
                        self.write_lock_map.get_mut(&o).unwrap().set(cell).unwrap();
                    } else {
                        self.write_lock_map
                            .get_mut(&pid)
                            .unwrap()
                            .set(cell)
                            .unwrap();
                    }
                }
            }
            None => {
                let (new_root_id, mut new_root) = self.pager.create_root();
                println!("split root, new id is {new_root_id}");
                new_root.set_right_ptr(from_id);
                new_root.set_level(from_page.level() + 1);
                new_root.set(new_parent_cell).unwrap();
            }
        }

        self.write_lock_map.insert(to_id, to_page);
        (middle_key, to_id)
    }
}
