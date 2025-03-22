use std::{marker::PhantomData, path::Path, sync::Arc};

use parking_lot::{ArcRwLockReadGuard, RawRwLock};

use crate::{
    btree::{BTreeCursor, PageIter},
    io::{FileIO, IOError},
    pager::{Cell, CellsIter, OwnedCell, Page, Pager},
};

pub struct Store {
    pager: Arc<Pager>,
}

// i know it's like, really not a good idea, but im *this* close to scrapping
// everything and rewriting this as a bw-tree

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
        let cursor = BTreeCursor::new(self.pager.clone());
        // for now
        let mut buf = Vec::new();
        if cursor.get(key, &mut buf) {
            Some(buf)
        } else {
            None
        }
    }
    pub fn set(&self, key: &[u8], val: &[u8]) {
        let cursor = BTreeCursor::new(self.pager.clone());
        cursor.set(key, val);
    }
    pub fn iter(&self) -> EntryIter {
        let mut cursor = BTreeCursor::new(self.pager.clone());
        cursor.search_opt(&[]);
        let mut page_iter = cursor.iter();
        let current_page = page_iter.next().unwrap();
        EntryIter {
            page_iter,
            current_page,
            current_cell: 0,
        }
    }
}

pub struct EntryIter {
    page_iter: PageIter,
    current_page: ArcRwLockReadGuard<RawRwLock, Page>,
    current_cell: usize,
}
impl Iterator for EntryIter {
    type Item = OwnedCell;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_cell < self.current_page.slots() as usize {
            let out = self.current_page.get_cell(self.current_cell);
            self.current_cell += 1;
            Some(OwnedCell::from_cell(out))
        } else {
            match self.page_iter.next() {
                Some(page) => {
                    self.current_page = page;
                    self.current_cell = 1;
                    Some(OwnedCell::from_cell(self.current_page.get_cell(0)))
                }
                None => None,
            }
        }
    }
}
