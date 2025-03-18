use crate::pager::{Cell, Page, Pager};

use std::{cell::RefCell, rc::Rc};

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
        let leaf_page = self.pager.get(self.page_stack.pop().unwrap()).unwrap();
        match leaf_page.borrow().get(key) {
            Some(cell) => match cell {
                Cell::InnerCell {
                    key: _,
                    left_ptr: _,
                } => {
                    println!("ahhh!!! inner cell in leaf node");
                    None
                }
                Cell::LeafCell { key: k, val } => {
                    if key == k {
                        for v in val {
                            buf.push(*v);
                        }
                        Some(())
                    } else {
                        println!("got wrong val");
                        None
                    }
                }
            },
            None => {
                println!("it just straight up don't exist");
                None
            }
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8]) {
        self.find_leaf(key);
        let new_cell = Cell::LeafCell { key, val };
        let leaf_page = self.pager.get(*self.page_stack.last().unwrap()).unwrap();
        if let Err(cell) = { leaf_page.borrow_mut().set(new_cell) } {
            let (middle_key, other) = self.split();
            if key <= &middle_key {
                self.pager
                    .get(other)
                    .unwrap()
                    .borrow_mut()
                    .set(cell)
                    .unwrap();
            } else {
                leaf_page.borrow_mut().set(cell).unwrap();
            }
        }
    }

    pub fn get_first_page(&mut self) -> Rc<RefCell<Page>> {
        let mut current_page = self.pager.get(self.root_page_id).unwrap();
        while current_page.borrow().level() > 0 {
            let left = {
                let page_borrow = current_page.borrow();
                let left = page_borrow.get_cell(0);
                if let Cell::InnerCell { key: _, left_ptr } = left {
                    left_ptr
                } else {
                    panic!();
                }
            };
            current_page = self.pager.get(left).unwrap();
        }
        current_page
    }
    pub fn get_page(&mut self, page_id: u32) -> Rc<RefCell<Page>> {
        self.pager.get(page_id).unwrap()
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
                    println!("going right");
                    current_page_id = current_page.borrow().right_ptr();
                }
            }
        }
    }

    /// assumes the caller will add the new key
    fn split(&mut self) -> (Vec<u8>, u32) {
        // do the split
        let from_id = self.page_stack.pop().unwrap();
        let to_page_id = self.pager.create_page();
        let from_page_guarded = self.pager.get(from_id).unwrap();
        let mut from_page = from_page_guarded.borrow_mut();
        let to_page_guarded = self.pager.get(to_page_id).unwrap();
        let mut to_page = to_page_guarded.borrow_mut();
        let mut scratch = Page {
            buf: vec![0; to_page.buf.capacity()],
            dirty: true,
        };
        // get sibling pointers correct
        match from_page.left_sib() {
            0 => {
                from_page.set_left_sib(to_page_id);
                to_page.set_right_sib(from_id);
            }
            n => {
                let left = self.pager.get(n).unwrap();
                left.borrow_mut().set_right_sib(to_page_id);
                to_page.set_left_sib(n);
                from_page.set_left_sib(to_page_id);
                to_page.set_right_sib(from_id);
            }
        }
        // i know, i know, just trying to figure this shit out
        let middle_key = from_page.split_into(&mut to_page, &mut scratch);

        // try to update parent, it should get the to page id as the val,
        // and the last key of the to page
        let new_parent_cell = Cell::InnerCell {
            key: &middle_key,
            left_ptr: to_page_id,
        };

        let parent_id = self.page_stack.last();
        match parent_id {
            Some(pid) => {
                println!("split inner");
                let parent = self.pager.get(*pid).unwrap();
                if let Err(cell) = { parent.borrow_mut().set(new_parent_cell) } {
                    // so i think the problem is that we always try to add the cell
                    // to the original page when we split, but bc of the split,
                    // it might need to go in the new one
                    let (m_key, other) = self.split();
                    if cell.key() <= &m_key {
                        self.pager
                            .get(other)
                            .unwrap()
                            .borrow_mut()
                            .set(cell)
                            .unwrap();
                    } else {
                        parent.borrow_mut().set(cell).unwrap();
                    }
                }
                (middle_key, to_page_id)
            }
            None => {
                // we just split the root
                println!("split root");
                let new_root_id = self.pager.create_page();
                let new_root = self.pager.get(new_root_id).unwrap();
                let mut new_root_mut = new_root.borrow_mut();
                new_root_mut.set_right_ptr(from_id);
                new_root_mut.set_level(from_page.level() + 1);
                new_root_mut.set(new_parent_cell).unwrap();
                self.root_page_id = new_root_id;
                (middle_key, to_page_id)
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
