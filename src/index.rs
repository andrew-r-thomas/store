use crate::{
    PageId,
    page::{InnerDelta, InnerPageMut, LeafDelta, LeafPageMut, Page, PageBuffer},
};

use std::{collections::HashMap, ops::Range};

pub fn get<const PAGE_SIZE: usize>(
    page_dir: &PageDir<PAGE_SIZE>,

    key: &[u8],
    buf: &mut Vec<u8>,
) -> bool {
    let mut current = page_dir.buf_pool[*page_dir.id_map.get(&page_dir.root).unwrap()].read();
    while let Page::Inner(inner_page) = current {
        let child_id = &inner_page.find_child(key);
        current = page_dir.buf_pool[*page_dir.id_map.get(child_id).unwrap()].read();
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
    page_dir: &mut PageDir<PAGE_SIZE>,

    key: &[u8],
    val: &[u8],
    path: &mut Vec<(PageId, usize)>,
) {
    let mut current_idx = *page_dir.id_map.get(&page_dir.root).unwrap();
    path.push((page_dir.root, current_idx));

    while let Page::Inner(inner_page) = page_dir.buf_pool[current_idx].read() {
        let child_id = inner_page.find_child(key);
        current_idx = *page_dir.id_map.get(&child_id).unwrap();
        path.push((child_id, current_idx));
    }

    let delta = LeafDelta::Set { key, val };
    let (leaf_id, leaf_idx) = path.pop().unwrap();
    if let Err(()) = page_dir.buf_pool[leaf_idx].write_delta(&delta) {
        // first we compact the page
        let old_page = page_dir.buf_pool[leaf_idx].read().unwrap_as_leaf();
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
            let to_page_id = page_dir.next_pid;
            page_dir.next_pid += 1;
            let to_page_idx = page_dir.buf_pool.len();
            page_dir.buf_pool.push(to_page.unpack());
            page_dir.id_map.insert(to_page_id, to_page_idx);
            page_dir.buf_pool[leaf_idx] = new_page.unpack();

            // then we need to update the parent with the new key, which may cascade up to
            // the root, so we do it in a loop
            let mut parent_delta = InnerDelta::Set {
                key: &middle_key,
                left_page_id: to_page_id,
            };
            let mut right = leaf_id;
            'split: loop {
                match path.pop() {
                    Some((parent_id, parent_idx)) => {
                        // try to write the delta to the parent
                        if let Err(()) = page_dir.buf_pool
                            [*page_dir.id_map.get(&parent_id).unwrap()]
                        .write_delta(&parent_delta)
                        {
                            // compact the parent
                            let old_parent = page_dir.buf_pool[parent_idx].read().unwrap_as_inner();
                            let mut new_parent = InnerPageMut::new(PAGE_SIZE);
                            new_parent.compact(old_parent);

                            // try to apply the delta to the compacted parent
                            if let Err(()) = new_parent.apply_delta(&parent_delta) {
                                // if it fails, we need to split the parent
                                let mut to_parent = InnerPageMut::new(PAGE_SIZE);
                                // FIX: hot garbage
                                let mut temp_key = Vec::new();
                                new_parent.split_into(&mut to_parent, &mut temp_key);

                                if &middle_key <= &temp_key {
                                    to_parent.apply_delta(&parent_delta).unwrap();
                                } else {
                                    new_parent.apply_delta(&parent_delta).unwrap();
                                }

                                let to_parent_id = page_dir.next_pid;
                                page_dir.next_pid += 1;
                                let to_parent_idx = page_dir.buf_pool.len();
                                page_dir.buf_pool.push(to_parent.unpack());
                                page_dir.id_map.insert(to_parent_id, to_parent_idx);
                                page_dir.buf_pool[parent_idx] = new_parent.unpack();

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                parent_delta = InnerDelta::Set {
                                    key: &middle_key,
                                    left_page_id: to_parent_id,
                                };
                                right = parent_id;

                                continue 'split;
                            } else {
                                page_dir.buf_pool[parent_idx] = new_parent.unpack();
                                break 'split;
                            }
                        } else {
                            break 'split;
                        }
                    }
                    None => {
                        // we split the root
                        let mut new_root = InnerPageMut::new(PAGE_SIZE);
                        new_root.apply_delta(&parent_delta).unwrap();
                        new_root.set_right_page_id(right);

                        let new_root_id = page_dir.next_pid;
                        page_dir.next_pid += 1;
                        let new_root_idx = page_dir.buf_pool.len();
                        page_dir.buf_pool.push(new_root.unpack());
                        page_dir.id_map.insert(new_root_id, new_root_idx);
                        page_dir.root = new_root_id;

                        break 'split;
                    }
                }
            }
        } else {
            page_dir.buf_pool[leaf_idx] = new_page.unpack();
        }
    }
}

pub fn delete(_key: &[u8]) -> Result<(), ()> {
    todo!()
}
pub fn iter_range(_range: Range<&[u8]>) -> RangeIter {
    todo!()
}

pub struct RangeIter {}

pub struct PageDir<const PAGE_SIZE: usize> {
    pub buf_pool: Vec<PageBuffer<PAGE_SIZE>>,
    pub id_map: HashMap<PageId, usize>,
    pub root: PageId,
    pub next_pid: PageId,
}
