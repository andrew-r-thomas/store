use crate::{
    PageId,
    page::{InnerDelta, InnerPageMut, LeafDelta, LeafPageMut, Page, WriteRes},
    page_dir::PageDirectory,
};

use std::{
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
};

pub struct Index<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> {
    page_dir: PageDirectory<BLOCK_SIZE, PAGE_SIZE>,
    root: AtomicU64,
}

impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> Index<BLOCK_SIZE, PAGE_SIZE> {
    pub fn new(capacity: usize) -> Self {
        let page_dir = PageDirectory::new(capacity);
        let root_page = LeafPageMut::new(PAGE_SIZE);
        let root = AtomicU64::new(page_dir.push(root_page.unpack()));
        Self { page_dir, root }
    }
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let root_id = self.root.load(Ordering::Acquire);
        let mut current = self.page_dir.get(root_id).read();
        while let Page::Inner(inner_page) = current {
            current = self.page_dir.get(inner_page.find_child(key)).read();
        }
        let leaf = current.unwrap_as_leaf();
        leaf.get(key)
    }
    pub fn set(&self, key: &[u8], val: &[u8], path: &mut Vec<PageId>) -> Result<(), ()> {
        let root_id = self.root.load(Ordering::Acquire);
        let mut current = self.page_dir.get(root_id);

        path.push(root_id);
        while let Page::Inner(inner_page) = current.read() {
            let child_id = inner_page.find_child(key);
            current = self.page_dir.get(child_id);
            path.push(child_id);
        }

        let delta = LeafDelta::Set { key, val };
        let leaf_id = path.pop().unwrap();
        match self.page_dir.get(leaf_id).write_delta(&delta) {
            WriteRes::Ok => Ok(()),
            WriteRes::Sealed => Err(()),
            WriteRes::Sealer(old) => {
                // first we compact the page
                let old_page = old.unwrap_as_leaf();
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
                    self.page_dir.set(leaf_id, new_page.unpack());
                    let to_page_id = self.page_dir.push(to_page.unpack());

                    // then we need to update the parent with the new key, which may cascade up to
                    // the root, so we do it in a loop
                    let mut delta = InnerDelta::Set {
                        key: &middle_key,
                        left_page_id: to_page_id,
                    };
                    'split: loop {
                        match path.pop() {
                            Some(parent_id) => 'attempt: loop {
                                // try to write the delta to the parent
                                match self.page_dir.get(parent_id).write_delta(&delta) {
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
                                            // FIX: hot garbage
                                            let mut temp_key = Vec::new();
                                            new_parent.split_into(&mut to_parent, &mut temp_key);

                                            if &middle_key <= &temp_key {
                                                to_parent.apply_delta(&delta).unwrap();
                                            } else {
                                                new_parent.apply_delta(&delta).unwrap();
                                            }

                                            self.page_dir.set(parent_id, new_parent.unpack());
                                            let to_parent_id =
                                                self.page_dir.push(to_parent.unpack());

                                            middle_key.clear();
                                            middle_key.extend(temp_key);
                                            delta = InnerDelta::Set {
                                                key: &middle_key,
                                                left_page_id: to_parent_id,
                                            };

                                            continue 'split;
                                        } else {
                                            self.page_dir.set(parent_id, new_parent.unpack());
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

                                let new_root_id = self.page_dir.push(new_root.unpack());
                                self.root.store(new_root_id, Ordering::Release);
                                break 'split;
                            }
                        }
                    }
                } else {
                    self.page_dir.set(leaf_id, new_page.unpack());
                }

                Ok(())
            }
        }
    }
    pub fn delete(&self, _key: &[u8]) -> Result<(), ()> {
        todo!()
    }
    pub fn iter_range(&self, _range: Range<&[u8]>) {
        todo!()
    }
}

unsafe impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> Send for Index<BLOCK_SIZE, PAGE_SIZE> {}
unsafe impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> Sync for Index<BLOCK_SIZE, PAGE_SIZE> {}
