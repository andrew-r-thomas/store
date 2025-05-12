use crate::{
    PageId,
    page::{
        self, PageBuffer, PageMut,
        inner::{self, InnerDelta, SplitDelta},
        leaf::{self, LeafDelta, SetDelta},
    },
};

use std::{collections::HashMap, ops::Range};

pub struct Index {
    pub buf_pool: Vec<PageBuffer>,
    pub id_map: HashMap<PageId, usize>,

    pub root: PageId,
    pub next_pid: PageId,
    pub page_size: usize,
}
impl Index {
    pub fn get(&self, key: &[u8], buf: &mut Vec<u8>) -> bool {
        let mut current = self.buf_pool[*self.id_map.get(&self.root).unwrap()].read();
        while page::is_inner(current) {
            let child_id = inner::find_child(current, key);
            current = self.buf_pool[*self.id_map.get(&child_id).unwrap()].read();
        }

        match leaf::get(current, key) {
            Some(val) => {
                buf.extend(val);
                true
            }
            None => false,
        }
    }
    pub fn set(&mut self, key: &[u8], val: &[u8], path: &mut Vec<(PageId, usize)>) {
        let mut current_idx = *self.id_map.get(&self.root).unwrap();
        let mut current = self.buf_pool[current_idx].read();
        path.push((self.root, current_idx));

        while page::is_inner(current) {
            let child_id = inner::find_child(current, key);

            current_idx = *self.id_map.get(&child_id).unwrap();
            current = self.buf_pool[current_idx].read();
            path.push((child_id, current_idx));
        }

        let delta = SetDelta { key, val };
        let (leaf_id, leaf_idx) = path.pop().unwrap();
        if !self.buf_pool[leaf_idx].write_delta(&delta) {
            // first we compact the page
            let old_page = self.buf_pool[leaf_idx].read();
            let mut new_page = PageBuffer::new(self.page_size);
            let mut new_page_mut = leaf::compact(old_page, new_page.raw_buffer());

            // then we try to apply the delta to the newly compacted page
            if !leaf::apply_delta(&mut new_page_mut, LeafDelta::Set(&delta)) {
                // if it fails, we need to do a split
                let mut to_page = PageBuffer::new(self.page_size);
                let mut middle_key = Vec::new();
                let mut to_page_mut =
                    leaf::split(&mut new_page_mut, to_page.raw_buffer(), &mut middle_key);

                // then we apply the delta to the correct split page
                if key <= &middle_key {
                    leaf::apply_delta(&mut to_page_mut, LeafDelta::Set(&delta));
                } else {
                    leaf::apply_delta(&mut new_page_mut, LeafDelta::Set(&delta));
                }

                // then we update the page dir with the new pages
                let to_page_id = self.next_pid;
                self.next_pid += 1;
                let to_page_idx = self.buf_pool.len();
                // TODO: i don't love the pattern of always having to remember to
                // pack the PageMuts
                to_page_mut.pack();
                self.buf_pool.push(to_page);
                self.id_map.insert(to_page_id, to_page_idx);
                new_page_mut.pack();
                self.buf_pool[leaf_idx] = new_page;

                // then we need to update the parent with the new key, which may cascade up to
                // the root, so we do it in a loop
                let mut parent_delta = SplitDelta {
                    middle_key: &middle_key,
                    left_page_id: to_page_id,
                };
                let mut right = leaf_id;
                'split: loop {
                    match path.pop() {
                        Some((parent_id, parent_idx)) => {
                            // try to write the delta to the parent
                            if !self.buf_pool[*self.id_map.get(&parent_id).unwrap()]
                                .write_delta(&parent_delta)
                            {
                                // compact the parent
                                let old_parent = self.buf_pool[parent_idx].read();
                                let mut new_parent = PageBuffer::new(self.page_size);
                                let mut new_parent_mut =
                                    inner::compact(old_parent, new_parent.raw_buffer());

                                // try to apply the delta to the compacted parent
                                if !inner::apply_delta(
                                    &mut new_parent_mut,
                                    InnerDelta::Split(&parent_delta),
                                ) {
                                    // if it fails, we need to split the parent
                                    let mut to_parent = PageBuffer::new(self.page_size);
                                    // FIX: hot garbage
                                    let mut temp_key = Vec::new();
                                    let mut to_parent_mut = inner::split(
                                        &mut new_parent_mut,
                                        to_parent.raw_buffer(),
                                        &mut temp_key,
                                    );

                                    if &middle_key <= &temp_key {
                                        inner::apply_delta(
                                            &mut to_parent_mut,
                                            InnerDelta::Split(&parent_delta),
                                        );
                                    } else {
                                        inner::apply_delta(
                                            &mut new_parent_mut,
                                            InnerDelta::Split(&parent_delta),
                                        );
                                    }

                                    let to_parent_id = self.next_pid;
                                    self.next_pid += 1;
                                    let to_parent_idx = self.buf_pool.len();
                                    to_parent_mut.pack();
                                    self.buf_pool.push(to_parent);
                                    self.id_map.insert(to_parent_id, to_parent_idx);
                                    new_parent_mut.pack();
                                    self.buf_pool[parent_idx] = new_parent;

                                    middle_key.clear();
                                    middle_key.extend(temp_key);
                                    parent_delta = SplitDelta {
                                        middle_key: &middle_key,
                                        left_page_id: to_parent_id,
                                    };
                                    right = parent_id;

                                    continue 'split;
                                } else {
                                    new_parent_mut.pack();
                                    self.buf_pool[parent_idx] = new_parent;
                                    break 'split;
                                }
                            } else {
                                break 'split;
                            }
                        }
                        None => {
                            // we split the root
                            let mut new_root = PageBuffer::new(self.page_size);
                            let mut new_root_mut = PageMut {
                                buf: new_root.raw_buffer(),
                                bottom: self.page_size, // FIX:
                            };
                            inner::apply_delta(&mut new_root_mut, InnerDelta::Split(&parent_delta));
                            inner::set_right_pid(new_root_mut.buf, right);

                            let new_root_id = self.next_pid;
                            self.next_pid += 1;
                            let new_root_idx = self.buf_pool.len();
                            new_root_mut.pack();
                            self.buf_pool.push(new_root);
                            self.id_map.insert(new_root_id, new_root_idx);
                            self.root = new_root_id;

                            break 'split;
                        }
                    }
                }
            } else {
                new_page_mut.pack();
                self.buf_pool[leaf_idx] = new_page;
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
