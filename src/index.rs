use crate::{
    PageId,
    log::PageDir,
    page::{Delta, PageMut, SetDelta, SplitDelta},
};

use std::ops::Range;

pub struct Index {
    pub page_dir: PageDir,
    pub page_mut: PageMut,
    pub path: Vec<(PageId, usize)>,
}
impl Index {
    pub fn get(&mut self, key: &[u8], buf: &mut Vec<u8>) -> bool {
        let root_idx = self.page_dir.get_root();
        let mut current = self.page_dir.buf_pool[root_idx].read();
        while current.is_inner() {
            let idx = self.page_dir.get(current.search_inner(key));
            current = self.page_dir.buf_pool[idx].read();
        }

        match current.search_leaf(key) {
            Some(val) => {
                buf.extend(val);
                true
            }
            None => false,
        }
    }
    // FIX: lots of messy with constantly calling into the page dir to get pages, should save them
    // locally when we first read them, might need move semantics for this to work
    pub fn set(&mut self, key: &[u8], val: &[u8]) {
        self.path.clear();
        self.path
            .push((self.page_dir.root, self.page_dir.get_root()));
        loop {
            let mut current = self.page_dir.buf_pool[self.path.last().unwrap().1].read();
            if current.is_inner() {
                let id = current.search_inner(key);
                self.path.push((id, self.page_dir.get(id)));
            } else {
                break;
            }
        }

        let delta = SetDelta { key, val };
        let (leaf_id, leaf_idx) = self.path.pop().unwrap();
        if !self.page_dir.buf_pool[leaf_idx].write_delta(&Delta::Set(delta)) {
            // first we compact the page
            let old_page = self.page_dir.buf_pool[leaf_idx].read();
            let new_page = &mut self.page_mut;
            new_page.clear();
            new_page.compact(old_page);

            // then we try to apply the delta to the newly compacted page
            if !new_page.apply_delta(&Delta::Set(delta)) {
                // if it fails, we need to do a split
                let mut middle_key = Vec::new();
                let mut to_page = new_page.split_leaf(&mut middle_key);

                // then we apply the delta to the correct split page
                if key <= &middle_key {
                    to_page.apply_delta(&Delta::Set(delta));
                } else {
                    new_page.apply_delta(&Delta::Set(delta));
                }

                // then we update the page dir with the new pages
                let (to_page_id, to_page_buf) = self.page_dir.new_page();
                to_page.pack(to_page_buf);
                new_page.pack(&mut self.page_dir.buf_pool[leaf_idx]);

                // then we need to update the parent with the new key, which may cascade up to
                // the root, so we do it in a loop
                let mut parent_delta = SplitDelta {
                    middle_key: &middle_key,
                    left_pid: to_page_id,
                };
                let mut right = leaf_id;
                'split: loop {
                    match self.path.pop() {
                        Some((parent_id, parent_idx)) => {
                            // try to write the delta to the parent
                            if !self.page_dir.buf_pool[parent_idx]
                                .write_delta(&Delta::Split(parent_delta))
                            {
                                // compact the parent
                                let old_parent = self.page_dir.buf_pool[parent_idx].read();
                                let new_parent = &mut self.page_mut;
                                new_parent.clear();
                                new_parent.compact(old_parent);

                                // try to apply the delta to the compacted parent
                                if !new_parent.apply_delta(&Delta::Split(parent_delta)) {
                                    // if it fails, we need to split the parent
                                    // FIX: hot garbage
                                    let mut temp_key = Vec::new();
                                    let mut to_parent = new_parent.split_inner(&mut temp_key);

                                    if &middle_key <= &temp_key {
                                        to_parent.apply_delta(&Delta::Split(parent_delta));
                                    } else {
                                        new_parent.apply_delta(&Delta::Split(parent_delta));
                                    }

                                    let (to_parent_id, to_parent_buf) = self.page_dir.new_page();
                                    to_parent.pack(to_parent_buf);
                                    // TODO: same here
                                    new_parent.pack(&mut self.page_dir.buf_pool[parent_idx]);

                                    middle_key.clear();
                                    middle_key.extend(temp_key);
                                    parent_delta = SplitDelta {
                                        middle_key: &middle_key,
                                        left_pid: to_parent_id,
                                    };
                                    right = parent_id;

                                    continue 'split;
                                } else {
                                    new_parent.pack(&mut self.page_dir.buf_pool[parent_idx]);
                                    break 'split;
                                }
                            } else {
                                break 'split;
                            }
                        }
                        None => {
                            // we split the root
                            let new_root = &mut self.page_mut;
                            new_root.clear();
                            new_root.set_right_pid(right);
                            new_root.set_left_pid(u64::MAX);
                            new_root.apply_delta(&Delta::Split(parent_delta));

                            let (_, new_root_buf) = self.page_dir.new_root();
                            new_root.pack(new_root_buf);

                            break 'split;
                        }
                    }
                }
            } else {
                new_page.pack(&mut self.page_dir.buf_pool[leaf_idx]);
            }
        }
    }

    pub fn delete(&mut self, _key: &[u8]) -> Result<(), ()> {
        todo!()
    }
    pub fn iter_range(&self, _range: Range<&[u8]>) -> RangeIter {
        todo!()
    }
}

pub struct RangeIter {}

pub fn get(key: &[u8], val_buf: &mut Vec<u8>) -> bool {
    todo!()
}
