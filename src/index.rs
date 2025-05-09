use crate::{
    PageId,
    page::{
        self, PageBuffer, SetDelta,
        inner::{self, SplitDelta},
        leaf,
    },
};

use std::{collections::HashMap, ops::Range};

pub fn get<const PAGE_SIZE: usize>(
    page_dir: &PageDir<PAGE_SIZE>,

    key: &[u8],
    buf: &mut Vec<u8>,
) -> bool {
    let mut current = page_dir.buf_pool[*page_dir.id_map.get(&page_dir.root).unwrap()].read();
    while page::is_inner(current) {
        let child_id = inner::find_child(current, key);
        current = page_dir.buf_pool[*page_dir.id_map.get(&child_id).unwrap()].read();
    }

    match leaf::get(current, key) {
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
    let mut current = page_dir.buf_pool[current_idx].read();
    path.push((page_dir.root, current_idx));

    while page::is_inner(current) {
        let child_id = inner::find_child(current, key);

        current_idx = *page_dir.id_map.get(&child_id).unwrap();
        current = page_dir.buf_pool[current_idx].read();
        path.push((child_id, current_idx));
    }

    let delta = SetDelta { key, val };
    let (leaf_id, leaf_idx) = path.pop().unwrap();
    if !page_dir.buf_pool[leaf_idx].write_delta(&delta) {
        // first we compact the page
        let old_page = page_dir.buf_pool[leaf_idx].read();
        let mut new_page = PageBuffer::new(PAGE_SIZE);
        let new_page_mut = leaf::compact(old_page, new_page.raw_buffer());

        // then we try to apply the delta to the newly compacted page
        if !leaf::apply_delta(&mut new_page_mut, &delta) {
            // if it fails, we need to do a split
            let mut to_page = PageBuffer::new(PAGE_SIZE);
            let mut middle_key = Vec::new();
            let to_page_mut = leaf::split(&mut new_page_mut, to_page.raw_buffer(), &mut middle_key);

            // then we apply the delta to the correct split page
            if key <= &middle_key {
                leaf::apply_delta(&mut to_page_mut, &delta);
            } else {
                leaf::apply_delta(&mut new_page_mut, &delta);
            }

            // then we update the page dir with the new pages
            let to_page_id = page_dir.next_pid;
            page_dir.next_pid += 1;
            let to_page_idx = page_dir.buf_pool.len();
            // TODO: i don't love the pattern of always having to remember to
            // pack the PageMuts
            to_page_mut.pack();
            page_dir.buf_pool.push(to_page);
            page_dir.id_map.insert(to_page_id, to_page_idx);
            new_page_mut.pack();
            page_dir.buf_pool[leaf_idx] = new_page;

            // then we need to update the parent with the new key, which may cascade up to
            // the root, so we do it in a loop
            let mut parent_delta = SplitDelta {/Users/andrewthomas/Desktop/mashas\ stuff\ that\ was\ on\ the\ flash\ drive
                middle_key: &middle_key,
                left_page_id: to_page_id,
            };
            let mut right = leaf_id;
            'split: loop {
                match path.pop() {
                    Some((parent_id, parent_idx)) => {
                        // try to write the delta to the parent
                        if !page_dir.buf_pool[*page_dir.id_map.get(&parent_id).unwrap()]
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
    pub buf_pool: Vec<PageBuffer>,
    pub id_map: HashMap<PageId, usize>,
    pub root: PageId,
    pub next_pid: PageId,
}
