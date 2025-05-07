use crate::{
    PageId,
    page::{InnerDelta, InnerPageMut, LeafDelta, LeafPageMut, Page, PageBuffer},
};

use std::{collections::HashMap, ops::Range};

pub fn get<const PAGE_SIZE: usize>(
    buf_pool: &Vec<PageBuffer<PAGE_SIZE>>,
    page_dir: &HashMap<PageId, usize>,
    root: PageId,

    key: &[u8],
    buf: &mut Vec<u8>,
) -> bool {
    let mut current = buf_pool[*page_dir.get(&root).unwrap()].read();
    while let Page::Inner(inner_page) = current {
        current = buf_pool[*page_dir.get(&inner_page.find_child(key)).unwrap()].read();
    }

    let leaf = current.unwrap_as_leaf();
    match leaf.get(key) {
        Some(val) => {
            buf.extend(val);
            true
        }
        None => false,
    }
}
pub fn set<const PAGE_SIZE: usize>(
    buf_pool: &mut Vec<PageBuffer<PAGE_SIZE>>,
    page_dir: &mut HashMap<PageId, usize>,
    root: &mut PageId,
    next_pid: &mut PageId,

    key: &[u8],
    val: &[u8],
    path: &mut Vec<(PageId, usize)>,
) {
    let mut current_idx = *page_dir.get(root).unwrap();
    path.push((*root, current_idx));

    while let Page::Inner(inner_page) = buf_pool[current_idx].read() {
        let child_id = inner_page.find_child(key);
        current_idx = *page_dir.get(&child_id).unwrap();
        path.push((child_id, current_idx));
    }

    let delta = LeafDelta::Set { key, val };
    let (leaf_id, leaf_idx) = path.pop().unwrap();
    if let Err(()) = buf_pool[leaf_idx].write_delta(&delta) {
        // first we compact the page
        let old_page = buf_pool[leaf_idx].read().unwrap_as_leaf();
        let mut new_page = LeafPageMut::new(PAGE_SIZE);
        new_page.compact(old_page);

        // then we try to apply the delta to the newly compacted page
        if let Err(()) = new_page.apply_delta(&delta) {
            // if it fails, we need to do a split
            let mut to_page = LeafPageMut::new(PAGE_SIZE);
            let mut middle_key = Vec::new();
            new_page.split_into(&mut to_page, &mut middle_key);

            // then we apply the delta to the correct split page
            if key <= &middle_key {
                to_page.apply_delta(&delta).unwrap();
            } else {
                new_page.apply_delta(&delta).unwrap();
            }

            // then we update the page dir with the new pages
            let to_page_id = *next_pid;
            *next_pid += 1;
            let to_page_idx = buf_pool.len();
            buf_pool.push(to_page.unpack());
            page_dir.insert(to_page_id, to_page_idx);
            buf_pool[leaf_idx] = new_page.unpack();

            // then we need to update the parent with the new key, which may cascade up to
            // the root, so we do it in a loop
            let mut delta = InnerDelta::Set {
                key: &middle_key,
                left_page_id: to_page_id,
            };
            'split: loop {
                match path.pop() {
                    Some((parent_id, parent_idx)) => {
                        // try to write the delta to the parent
                        if let Err(()) =
                            buf_pool[*page_dir.get(&parent_id).unwrap()].write_delta(&delta)
                        {
                            // compact the parent
                            let old_parent = buf_pool[parent_idx].read().unwrap_as_inner();
                            let mut new_parent = InnerPageMut::new(PAGE_SIZE);
                            new_parent.compact(old_parent);

                            // try to apply the delta to the compacted parent
                            if let Err(()) = new_parent.apply_delta(&delta) {
                                // if it fails, we need to split the parent
                                let mut to_parent = InnerPageMut::new(PAGE_SIZE);
                                // FIX: hot garbage
                                let mut temp_key = Vec::new();
                                new_parent.split_into(&mut to_parent, &mut temp_key);

                                if &middle_key <= &temp_key {
                                    to_parent.apply_delta(&delta).unwrap();
                                } else {
                                    new_parent.apply_delta(&delta).unwrap();
                                }

                                let to_parent_id = *next_pid;
                                *next_pid += 1;
                                let to_parent_idx = buf_pool.len();
                                buf_pool.push(to_parent.unpack());
                                page_dir.insert(to_parent_id, to_parent_idx);
                                buf_pool[parent_idx] = new_parent.unpack();

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                delta = InnerDelta::Set {
                                    key: &middle_key,
                                    left_page_id: to_parent_id,
                                };

                                continue 'split;
                            } else {
                                buf_pool[parent_idx] = new_parent.unpack();
                                break 'split;
                            }
                        }
                    }
                    None => {
                        // we split the root
                        let mut new_root = InnerPageMut::new(PAGE_SIZE);
                        new_root.apply_delta(&delta).unwrap();
                        new_root.set_right_page_id(leaf_id);

                        let new_root_id = *next_pid;
                        *next_pid += 1;
                        let new_root_idx = buf_pool.len();
                        buf_pool.push(new_root.unpack());
                        page_dir.insert(new_root_id, new_root_idx);
                        *root = new_root_id;

                        break 'split;
                    }
                }
            }
        } else {
            buf_pool[leaf_idx] = new_page.unpack();
        }
    }
}

pub fn delete(_key: &[u8]) -> Result<(), ()> {
    todo!()
}
pub fn iter_range(_range: Range<&[u8]>) {
    todo!()
}
