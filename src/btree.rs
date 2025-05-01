use crate::{
    page::{InnerDelta, InnerPageMut, LeafDelta, LeafPageMut, Page, WriteRes},
    page_dir::PageDirectory,
};

use std::{
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
};

pub fn get<'g, const BLOCK_SIZE: usize, const PAGE_SIZE: usize>(
    root: &AtomicU64,
    page_dir: &'g PageDirectory<BLOCK_SIZE, PAGE_SIZE>,
    key: &[u8],
) -> Option<&'g [u8]> {
    let root_id = root.load(Ordering::Acquire);
    let mut current = page_dir.get(root_id).read();
    while let Page::Inner(inner_page) = current {
        current = page_dir.get(inner_page.find_child(key)).read();
    }
    let leaf = current.unwrap_as_leaf();
    leaf.get(key)
}
pub fn set<const BLOCK_SIZE: usize, const PAGE_SIZE: usize>(
    root: &AtomicU64,
    page_dir: &PageDirectory<BLOCK_SIZE, PAGE_SIZE>,
    key: &[u8],
    val: &[u8],
) -> Result<(), ()> {
    let root_id = root.load(Ordering::Acquire);
    let mut current = page_dir.get(root_id);

    let mut path = Vec::new(); // PERF:
    path.push((root_id, current));

    while let Page::Inner(inner_page) = current.read() {
        let child_id = inner_page.find_child(key);
        current = page_dir.get(child_id);
        path.push((child_id, current));
    }

    let delta = LeafDelta::Set { key, val };
    let (leaf_id, leaf) = path.pop().unwrap();
    match leaf.write_delta(&delta) {
        WriteRes::Ok => Ok(()),
        WriteRes::Sealed => Err(()),
        WriteRes::Sealer(old) => {
            // first we compact the page
            let old_page = old.unwrap_as_leaf();
            let mut new_page = LeafPageMut::new(PAGE_SIZE);
            new_page.compact(old_page);

            // then we try to apply the delta to the newly compacted page
            if let Err(()) = new_page.apply_delta(&delta) {
                // if it failes, we need to do a split
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
                page_dir.set(leaf_id, new_page.unpack());
                let to_page_id = page_dir.push(to_page.unpack());

                // then we need to update the parent with the new key, which may cascade up to
                // the root, so we do it in a loop
                let mut delta = InnerDelta::Set {
                    key: &middle_key,
                    left_page_id: to_page_id,
                };
                'split: loop {
                    match path.pop() {
                        Some((parent_id, parent)) => 'attempt: loop {
                            // try to write the delta to the parent
                            match parent.write_delta(&delta) {
                                // if it succeeds, then we're done
                                WriteRes::Ok => break 'split,
                                // if it's sealed we need to keep trying
                                WriteRes::Sealed => continue 'attempt,
                                // if it's sealed, we need to do the whole dance again
                                WriteRes::Sealer(old) => {
                                    // compact the parent
                                    let old_parent = old.unwrap_as_inner();
                                    let mut new_parent = InnerPageMut::new(PAGE_SIZE);
                                    new_parent.compact(old_parent);

                                    // try to apply the delta to the compacted parent
                                    if let Err(()) = new_parent.apply_delta(&delta) {
                                        // if it fails, we need to split the parent
                                        let mut to_parent = InnerPageMut::new(PAGE_SIZE);
                                        middle_key.clear();
                                        new_parent.split_into(&mut to_parent, &mut middle_key);

                                        page_dir.set(parent_id, new_parent.unpack());
                                        let to_parent_id = page_dir.push(to_parent.unpack());

                                        delta = InnerDelta::Set {
                                            key: &middle_key,
                                            left_page_id: to_parent_id,
                                        };

                                        continue 'split;
                                    } else {
                                        page_dir.set(parent_id, new_parent.unpack());
                                        break 'split;
                                    }
                                }
                            }
                        },
                        None => {
                            // we split the root
                            let mut new_root = InnerPageMut::new(PAGE_SIZE);
                            new_root.apply_delta(&delta).unwrap();
                            new_root.set_right_page_id(leaf_id);

                            let new_root_id = page_dir.push(new_root.unpack());
                            root.store(new_root_id, Ordering::Release);
                        }
                    }
                }
            } else {
                page_dir.set(leaf_id, new_page.unpack());
            }

            Ok(())
        }
    }
}
pub fn delete(_key: &[u8]) -> Result<(), ()> {
    todo!()
}
pub fn iter_range(_range: Range<&[u8]>) {
    todo!()
}
