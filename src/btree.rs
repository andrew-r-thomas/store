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
enum SetOption {
    Insert(usize),
    Push,
    Update(usize),
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
        match Self::search_leaf_page(key, &leaf_page.borrow()) {
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
        let new_cell = Cell::LeafCell { key, val };
        let leaf_page = self.pager.get(*self.page_stack.last().unwrap()).unwrap();
        let opt = {
            let mut out = SetOption::Push;
            for (cell, slot) in leaf_page.borrow().iter_cells().zip(0..) {
                if let Cell::LeafCell { key: k, val: _ } = cell {
                    if key < k {
                        out = SetOption::Insert(slot);
                        break;
                    } else if key == k {
                        out = SetOption::Update(slot);
                        break;
                    }
                }
            }
            out
        };
        match opt {
            SetOption::Insert(slot) => {
                let res = { leaf_page.borrow_mut().insert_cell(slot, new_cell) };
                if let Err(new_cell) = res {
                    let new_page_id = self.pager.create_page();
                    self.split(new_page_id);
                    leaf_page.borrow_mut().insert_cell(slot, new_cell).unwrap();
                }
            }
            SetOption::Update(slot) => {
                let res = { leaf_page.borrow_mut().update_cell(slot, val) };
                if let Err(_) = res {
                    let new_page_id = self.pager.create_page();
                    self.split(new_page_id);
                    leaf_page.borrow_mut().update_cell(slot, val).unwrap();
                }
            }
            SetOption::Push => {
                let res = { leaf_page.borrow_mut().push_cell(new_cell) };
                if let Err(new_cell) = res {
                    let new_page_id = self.pager.create_page();
                    self.split(new_page_id);
                    leaf_page.borrow_mut().push_cell(new_cell).unwrap();
                }
            }
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
            if current_page.borrow().level() == 0 {
                break;
            }
            current_page_id = Self::search_inner_page(key, &current_page.borrow());
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

    fn find_slot(page: &Page, key: &[u8]) -> TargetSlot {
        // PERF: gotta rework these cell types, can't be matching all the time
        for (cell, slot) in page.iter_cells().zip(0..) {
            let k = match cell {
                Cell::InnerCell { key, left_ptr: _ } => key,
                Cell::LeafCell { key, val: _ } => key,
            };
            if key < k {
                return TargetSlot::Lt(slot);
            } else if key == k {
                return TargetSlot::Eq(slot);
            }
        }

        TargetSlot::Gt
    }

    /// assumes the caller will add the new key
    fn split(&mut self, page_id: u32) {
        let from_page_id = self.page_stack.pop().unwrap();
        // split the pages
        let from_page = self.pager.get(from_page_id).unwrap();
        let to_page = self.pager.get(page_id).unwrap();
        from_page.borrow_mut().split_into(
            &mut to_page.borrow_mut(),
            &mut self.pager.get_scratch().borrow_mut(),
        );

        // check if parent need a split
        match self.page_stack.last() {
            Some(parent_id) => {
                let parent = self.pager.get(*parent_id).unwrap();
                let to_borrow = to_page.borrow();
                let cell = to_borrow.get_cell(0);

                // find right slot
                if let Cell::InnerCell { key, left_ptr: _ } = cell {
                    let target_slot = Self::find_slot(&parent.borrow(), key);
                    let res = match target_slot {
                        TargetSlot::Lt(slot) | TargetSlot::Eq(slot) => {
                            parent.borrow_mut().insert_cell(slot, cell)
                        }
                        TargetSlot::Gt => parent.borrow_mut().push_cell(cell),
                    };
                    if let Err(cell) = res {
                        // we need to split some more
                        self.split(*parent_id);
                        match target_slot {
                            TargetSlot::Lt(slot) | TargetSlot::Eq(slot) => {
                                parent.borrow_mut().insert_cell(slot, cell).unwrap();
                            }
                            TargetSlot::Gt => parent.borrow_mut().push_cell(cell).unwrap(),
                        }
                    }
                }
            }
            None => {
                // we split the root
                let new_root_id = self.pager.create_page();
                let new_root = self.pager.get(new_root_id).unwrap();
                let mut new_root_mut = new_root.borrow_mut();
                new_root_mut.set_level(from_page.borrow().level());
                new_root_mut.set_right_ptr(page_id);
                new_root_mut.set_cells_start(self.pager.page_size as u16);
                let from_page_borrow = from_page.borrow();
                match from_page_borrow.get_cell(from_page_borrow.slots() as usize) {
                    Cell::InnerCell { key, left_ptr: _ } => new_root_mut
                        .push_cell(Cell::InnerCell {
                            key,
                            left_ptr: from_page_id,
                        })
                        .unwrap(),
                    Cell::LeafCell { key, val: _ } => new_root_mut
                        .push_cell(Cell::InnerCell {
                            key,
                            left_ptr: from_page_id,
                        })
                        .unwrap(),
                }
                self.root_page_id = new_root_id;
                // TODO: gotta make sure this update gets to disk manager
            }
        }
    }
}

enum TargetSlot {
    Lt(usize),
    Eq(usize),
    Gt,
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
