use std::path::Path;

use crate::{
    btree::BTree,
    io::{FileIO, IOError},
    pager::Pager,
};

pub struct Store {
    pub btree: BTree,
}

const DEFAULT_PAGE_SIZE: u32 = 1024 * 4;
const DEFAULT_POOL_CAP: usize = 1024 * 1024;

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IOError> {
        let io = FileIO::open(path)?;
        let root_id = io.root_id;
        let page_size = io.page_size;
        let pager = Pager::new(DEFAULT_POOL_CAP, page_size as usize, io);
        let btree = BTree::new(pager, root_id);

        Ok(Self { btree })
    }
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, IOError> {
        let io = FileIO::create(path, DEFAULT_PAGE_SIZE)?;
        let root_id = io.root_id;
        let pager = Pager::new(DEFAULT_POOL_CAP, DEFAULT_PAGE_SIZE as usize, io);
        let btree = BTree::new(pager, root_id);

        Ok(Self { btree })
    }
}
