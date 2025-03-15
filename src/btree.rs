use std::sync::Arc;

use crate::pager::{Cell, Page, Pager};

/// NOTE:
/// so there's a trade off here between splitting nodes, and opting to insert
/// a key as an overflow page kinda thing, this should be profiled
pub struct BTree {
    pager: Pager,
    root_page_id: u32,
    page_stack: Vec<u32>,
    val_buf: Vec<u8>,
}
impl BTree {
    pub fn new(pager: Pager, root_page_id: u32) -> Self {
        Self {
            pager,
            root_page_id,
            page_stack: Vec::new(),
            val_buf: Vec::new(),
        }
    }
    /// the buf passed to this function should be empty, and will be filled with
    /// the target value if it is found
    pub fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        self.find_leaf(key);
        let leaf_page = self.pager.get(*self.page_stack.last().unwrap()).unwrap();
        match Self::search_leaf_page(key, &leaf_page) {
            Some(val) => {
                self.val_buf.clear();
                // PERF: here, there, everywhere
                for v in val {
                    self.val_buf.push(*v);
                }
                return Some(&self.val_buf);
            }
            None => return None,
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8]) {
        self.find_leaf(key);
        let leaf_page = self.pager.get(*self.page_stack.last().unwrap()).unwrap();
        let new_cell = Cell::LeafCell { key, val };
        for (cell, slot) in leaf_page.iter_cells().zip(0..) {
            if let Cell::LeafCell { key: k, val: _ } = cell {
                if key < k {
                    if let Err(bad_cell) = leaf_page.insert_cell(slot, new_cell) {
                        let new_page_id = self.pager.create_page();
                        self.split(new_page_id);
                        leaf_page.insert_cell(slot, bad_cell).unwrap();
                    }
                    return;
                } else if key == k {
                    if let Err(_) = leaf_page.update_cell(slot, val) {
                        let new_page_id = self.pager.create_page();
                        self.split(new_page_id);
                        leaf_page.update_cell(slot, val).unwrap();
                    }
                    return;
                }
            }
        }
        // new val goes at the end, and is an insert
        if let Err(bad_cell) = leaf_page.push_cell(new_cell) {
            let new_page_id = self.pager.create_page();
            self.split(new_page_id);
            leaf_page.push_cell(bad_cell).unwrap();
        }
    }
    pub fn delete(&mut self) {
        // NOTE: for now i think we'll just do compaction on deletion, to keep
        // things simple, but will probably want to switch to an sqlite style
        // "freeblock" situation, and compact on insert if it gives us enough
        // space
        unimplemented!()
    }

    fn find_leaf(&mut self, key: &[u8]) {
        self.page_stack.clear();

        let mut current_page_id = self.root_page_id;
        loop {
            self.page_stack.push(current_page_id);
            let current_page = self.pager.get(current_page_id).unwrap();
            if current_page.level() == 0 {
                break;
            }
            current_page_id = Self::search_inner_page(key, &current_page);
        }
    }

    fn search_inner_page(target: &[u8], page: &Page) -> u32 {
        for cell in page.iter_cells() {
            if let Cell::InnerCell { key, left_ptr } = cell {
                if target < key {
                    return left_ptr;
                }
            }
        }
        page.right_ptr()
    }

    fn search_leaf_page<'s>(target: &[u8], page: &'s Page) -> Option<&'s [u8]> {
        for cell in page.iter_cells() {
            if let Cell::LeafCell { key, val } = cell {
                if key == target {
                    return Some(val);
                }
            }
        }
        None
    }

    /// assumes the caller will add the new key
    fn split(&mut self, page_id: u32) {
        match self.page_stack.pop() {
            Some(from_page_id) => {
                // split the pages
                let from_page = self.pager.get(from_page_id).unwrap();
                let to_page = self.pager.get(page_id).unwrap();
                from_page.split_into(to_page);

                // check if parent need a split
            }
            None => {
                // we split the root, and need to create a new one
            }
        }
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::io::FileIO;

    use super::*;

    #[test]
    fn basic_set_get() {
        let io = FileIO::create("temp.store", 4 * 1024).unwrap();
        let pager = Pager::new(10, 4 * 1024, io);
        let mut btree = BTree::new(pager, 1);
        btree.set("one".as_bytes(), &1_u32.to_be_bytes());
        btree.set("two".as_bytes(), &2_u32.to_be_bytes());
        btree.set("three".as_bytes(), &3_u32.to_be_bytes());

        assert_eq!(
            btree.get("three".as_bytes()),
            Some(&3_u32.to_be_bytes()[..])
        );
        assert_eq!(btree.get("two".as_bytes()), Some(&2_u32.to_be_bytes()[..]));
        assert_eq!(btree.get("one".as_bytes()), Some(&1_u32.to_be_bytes()[..]));

        std::fs::remove_file("temp.store").unwrap();
    }
}
