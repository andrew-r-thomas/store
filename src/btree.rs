use crate::pager::{Cell, Pager};

/// NOTE:
/// so there's a trade off here between splitting nodes, and opting to insert
/// a key as an overflow page kinda thing, this should be profiled
pub struct BTree {
    pager: Pager,
    root_page_id: u32,
    page_stack: Vec<u32>,
}

impl BTree {
    pub fn new(pager: Pager, root_page_id: u32) -> Self {
        Self {
            pager,
            root_page_id,
            page_stack: Vec::new(),
        }
    }
    /// the buf passed to this function should be empty, and will be filled with
    /// the target value if it is found
    pub fn get(&mut self, key: &[u8], buf: &mut Vec<u8>) -> Option<()> {
        self.find_leaf(key);
        let leaf_page = self.pager.get(*self.page_stack.last().unwrap()).unwrap();
        match leaf_page.borrow().get(key) {
            Some(cell) => {
                if let Cell::LeafCell { key: k, val } = cell {
                    if key == k {
                        for v in val {
                            buf.push(*v);
                        }
                        return Some(());
                    }
                }
                None
            }
            None => None,
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8]) {
        self.find_leaf(key);
        let new_cell = Cell::LeafCell { key, val };
        let leaf_page = self.pager.get(*self.page_stack.last().unwrap()).unwrap();
        if let Err(cell) = { leaf_page.borrow_mut().set(new_cell) } {
            self.split();
            leaf_page.borrow_mut().set(cell).unwrap();
        }
    }

    fn find_leaf(&mut self, key: &[u8]) {
        self.page_stack.clear();

        let mut current_page_id = self.root_page_id;
        loop {
            self.page_stack.push(current_page_id);
            let current_page = self.pager.get(current_page_id).unwrap();
            if current_page.borrow().level() == 0 {
                break;
            }
            match current_page.borrow().get(key) {
                Some(cell) => {
                    if let Cell::InnerCell { key: _, left_ptr } = cell {
                        current_page_id = left_ptr;
                    }
                }
                None => {
                    current_page_id = current_page.borrow().right_ptr();
                }
            }
        }
    }

    /// assumes the caller will add the new key
    fn split(&mut self) {
        // do the split
        let from_id = self.page_stack.pop().unwrap();
        let from_page = self.pager.get(from_id).unwrap();
        let to_page_id = self.pager.create_page();
        let to_page = self.pager.get(to_page_id).unwrap();
        let scratch = self.pager.get_scratch();
        from_page
            .borrow_mut()
            .split_into(&mut to_page.borrow_mut(), &mut scratch.borrow_mut());

        // try to update parent, it should get the to page id as the val,
        // and the last key of the to page
        let to_page_borrow = to_page.borrow();
        let last_slot = to_page_borrow.slots() as usize - 1;
        let last_cell = to_page_borrow.get_cell(last_slot);
        let key = last_cell.key();
        let new_parent_cell = Cell::InnerCell {
            key,
            left_ptr: to_page_id,
        };

        let parent_id = self.page_stack.last();
        match parent_id {
            Some(pid) => {
                let parent = self.pager.get(*pid).unwrap();
                if let Err(cell) = { parent.borrow_mut().set(new_parent_cell) } {
                    self.split();
                    parent.borrow_mut().set(cell).unwrap();
                }
            }
            None => {
                // we just split the root
                let new_root_id = self.pager.create_page();
                let new_root = self.pager.get(new_root_id).unwrap();
                let mut new_root_mut = new_root.borrow_mut();
                new_root_mut.set_right_ptr(from_id);
                new_root_mut.set_level(from_page.borrow().level() + 1);
                new_root_mut.set(new_parent_cell).unwrap();
                self.root_page_id = new_root_id;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::io::FileIO;

    use super::*;

    #[ignore]
    #[test]
    fn basic_set_get() {
        let io = FileIO::create("temp.store", 4 * 1024).unwrap();
        let pager = Pager::new(10, 4 * 1024, io);
        let mut btree = BTree::new(pager, 1);
        btree.set("one".as_bytes(), &1_u32.to_be_bytes());
        btree.set("two".as_bytes(), &2_u32.to_be_bytes());
        btree.set("three".as_bytes(), &3_u32.to_be_bytes());

        let mut val_buf = Vec::new();
        assert_eq!(Some(()), btree.get("three".as_bytes(), &mut val_buf));
        assert_eq!(val_buf, &3_u32.to_be_bytes());
        val_buf.clear();
        assert_eq!(Some(()), btree.get("two".as_bytes(), &mut val_buf));
        assert_eq!(val_buf, &2_u32.to_be_bytes());
        val_buf.clear();
        assert_eq!(Some(()), btree.get("one".as_bytes(), &mut val_buf));
        assert_eq!(val_buf, &1_u32.to_be_bytes());

        std::fs::remove_file("temp.store").unwrap();
    }
}
