use std::{path::Path, sync::Arc};

use crate::{
    btree::BTreeCursor,
    io::{FileIO, IOError},
    pager::Pager,
};

pub struct Store {
    pager: Arc<Pager>,
}

const DEFAULT_PAGE_SIZE: u32 = 1024 * 4;
const DEFAULT_POOL_CAP: usize = 1024;

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IOError> {
        let io = FileIO::open(path)?;
        let pager = Arc::new(Pager::new(DEFAULT_POOL_CAP, io));
        Ok(Self { pager })
    }
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, IOError> {
        let io = FileIO::create(path, DEFAULT_PAGE_SIZE)?;
        let pager = Arc::new(Pager::new(DEFAULT_POOL_CAP, io));
        Ok(Self { pager })
    }
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut cursor = BTreeCursor::new(self.pager.clone());
        // for now
        let mut buf = Vec::new();
        if cursor.get(key, &mut buf) {
            Some(buf)
        } else {
            None
        }
    }
    pub fn set(&self, key: &[u8], val: &[u8]) {
        let mut cursor = BTreeCursor::new(self.pager.clone());
        cursor.set(key, val);
    }
}
