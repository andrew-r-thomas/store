use crate::page_dir::PageDirectory;

use std::{ops::Range, sync::atomic::AtomicU64};

pub struct BTree<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> {
    page_dir: PageDirectory<BLOCK_SIZE, PAGE_SIZE>,
    root: AtomicU64,
}
impl<const BLOCK_SIZE: usize, const PAGE_SIZE: usize> BTree<BLOCK_SIZE, PAGE_SIZE> {
    pub fn new() -> Self {
        todo!()
    }
    pub fn get(&self, _key: &[u8], _out: &mut Vec<u8>) -> bool {
        todo!()
    }
    pub fn set(&self, _key: &[u8], _val: &[u8]) -> bool {
        todo!()
    }
    pub fn delete(&self, _key: &[u8]) -> bool {
        todo!()
    }
    pub fn iter_range(&self, _range: Range<&[u8]>) {
        todo!()
    }
}
