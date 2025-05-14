use crate::{
    PageId,
    page::{Delta, PageBuffer, PageMut, SetDelta, SplitDelta},
};

use std::{collections::HashMap, ops::Range};

pub struct Index {
    pub buf_pool: Vec<PageBuffer>,
    pub id_map: HashMap<PageId, usize>,

    pub root: PageId,
    pub next_pid: PageId,
    pub page_size: usize,

    pub page_mut: PageMut,
}
impl Index {
    pub fn get(&self, key: &[u8], buf: &mut Vec<u8>) -> bool {
        let mut current = self.buf_pool[*self.id_map.get(&self.root).unwrap()].read();
        while current.is_inner() {
            let child_id = current.search_inner(key);
            current = self.buf_pool[*self.id_map.get(&child_id).unwrap()].read();
        }

        match current.search_leaf(key) {
            Some(val) => {
                buf.extend(val);
                true
            }
            None => false,
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8], path: &mut Vec<(PageId, usize)>) {
        let mut current_idx = *self.id_map.get(&self.root).unwrap();
        path.push((self.root, current_idx));
        let mut current = self.buf_pool[current_idx].read();

        while current.is_inner() {
            let child_id = current.search_inner(key);

            current_idx = *self.id_map.get(&child_id).unwrap();
            current = self.buf_pool[current_idx].read();
            path.push((child_id, current_idx));
        }

        let delta = SetDelta { key, val };
        let (leaf_id, leaf_idx) = path.pop().unwrap();
        if !self.buf_pool[leaf_idx].write_delta(&Delta::Set(delta)) {
            // first we compact the page
            let old_page = self.buf_pool[leaf_idx].read();
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
                let to_page_id = self.next_pid;
                self.next_pid += 1;
                let to_page_idx = self.buf_pool.len();
                let mut to_page_buf = PageBuffer::new(self.page_size);
                to_page.pack(&mut to_page_buf);
                self.buf_pool.push(to_page_buf);
                self.id_map.insert(to_page_id, to_page_idx);
                // TODO: we could probably just reuse the old page buf here
                let mut new_page_buf = PageBuffer::new(self.page_size);
                new_page.pack(&mut new_page_buf);
                self.buf_pool[leaf_idx] = new_page_buf;

                // then we need to update the parent with the new key, which may cascade up to
                // the root, so we do it in a loop
                let mut parent_delta = SplitDelta {
                    middle_key: &middle_key,
                    left_pid: to_page_id,
                };
                let mut right = leaf_id;
                'split: loop {
                    match path.pop() {
                        Some((parent_id, parent_idx)) => {
                            // try to write the delta to the parent
                            if !self.buf_pool[*self.id_map.get(&parent_id).unwrap()]
                                .write_delta(&Delta::Split(parent_delta))
                            {
                                // compact the parent
                                let old_parent = self.buf_pool[parent_idx].read();
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

                                    let to_parent_id = self.next_pid;
                                    self.next_pid += 1;
                                    let to_parent_idx = self.buf_pool.len();
                                    let mut to_parent_buf = PageBuffer::new(self.page_size);
                                    to_parent.pack(&mut to_parent_buf);
                                    self.buf_pool.push(to_parent_buf);
                                    self.id_map.insert(to_parent_id, to_parent_idx);
                                    let mut new_parent_buf = PageBuffer::new(self.page_size);
                                    new_parent.pack(&mut new_parent_buf);
                                    self.buf_pool[parent_idx] = new_parent_buf;

                                    middle_key.clear();
                                    middle_key.extend(temp_key);
                                    parent_delta = SplitDelta {
                                        middle_key: &middle_key,
                                        left_pid: to_parent_id,
                                    };
                                    right = parent_id;

                                    continue 'split;
                                } else {
                                    let mut new_parent_buf = PageBuffer::new(self.page_size);
                                    new_parent.pack(&mut new_parent_buf);
                                    self.buf_pool[parent_idx] = new_parent_buf;
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
                            new_root.apply_delta(&Delta::Split(parent_delta));
                            new_root.set_right_pid(right);
                            new_root.set_left_pid(u64::MAX);

                            let new_root_id = self.next_pid;
                            self.next_pid += 1;
                            let new_root_idx = self.buf_pool.len();
                            let mut new_root_buf = PageBuffer::new(self.page_size);
                            new_root.pack(&mut new_root_buf);
                            self.buf_pool.push(new_root_buf);
                            self.id_map.insert(new_root_id, new_root_idx);
                            self.root = new_root_id;

                            break 'split;
                        }
                    }
                }
            } else {
                let mut new_page_buf = PageBuffer::new(self.page_size);
                new_page.pack(&mut new_page_buf);
                self.buf_pool[leaf_idx] = new_page_buf;
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
