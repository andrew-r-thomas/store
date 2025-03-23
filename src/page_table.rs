use std::{
    ops::Range,
    sync::atomic::{AtomicPtr, Ordering},
};

pub struct PageTable {
    buf: Vec<AtomicPtr<Option<Page>>>,
}
impl PageTable {
    pub fn new(capacity: usize) -> Self {
        let mut buf = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buf.push(AtomicPtr::new(&mut None));
        }
        Self { buf }
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        unimplemented!()
    }
    pub fn read(&self, page_id: u64) -> *mut Option<Page> {
        self.buf[page_id as usize].load(Ordering::Acquire)
    }
    pub fn update(
        &self,
        page_id: u64,
        current: &mut Option<Page>,
        new: &mut Option<Page>,
    ) -> Result<*mut Option<Page>, *mut Option<Page>> {
        self.buf[page_id as usize].compare_exchange(
            current,
            new,
            Ordering::Release,
            Ordering::Relaxed,
        )
    }
}

pub enum Page {
    Base(BasePage),
    Disk(DiskPage),
    Delta(PageDelta),
}
const LEVEL: usize = 0;
const ENTRIES: Range<usize> = 1..5;
const HEADER_OFFSET: usize = 5;
pub struct BasePage {
    buf: Vec<u8>,
}
impl BasePage {
    pub fn level(&self) -> u8 {
        self.buf[LEVEL]
    }
    pub fn entries(&self) -> u32 {
        u32::from_be_bytes(self.buf[ENTRIES].try_into().unwrap())
    }
    pub fn iter_entries_leaf(&self) -> LeafEntries {
        LeafEntries {
            buf: &self.buf,
            offset: HEADER_OFFSET,
            entries: self.entries() as usize,
            entry: 0,
        }
    }
    pub fn iter_entries_inner(&self) -> InnerEntries {}
}
pub struct LeafEntries<'e> {
    buf: &'e [u8],
    offset: usize,
    entry: usize,
    entries: usize,
}
impl<'e> Iterator for LeafEntries<'e> {
    type Item = (&'e [u8], &'e [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        if self.entry < self.entries {
            let key_len =
                u16::from_be_bytes(self.buf[self.offset..self.offset + 2].try_into().unwrap())
                    as usize;
            let val_len = u16::from_be_bytes(
                self.buf[self.offset + 2..self.offset + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let out = (
                &self.buf[self.offset + 4..self.offset + 4 + key_len],
                &self.buf[self.offset + 4 + key_len..self.offset + 4 + key_len + val_len],
            );
            self.offset += 4 + key_len + val_len;
            self.entry += 1;
            Some(out)
        } else {
            None
        }
    }
}
pub struct InnerEntries<'e> {
    buf: &'e [u8],
    entry: usize,
    entries: usize,
    key_offset: usize,
}
impl<'e> Iterator for InnerEntries<'e> {
    type Item = (&'e [u8], u64);
    fn next(&mut self) -> Option<Self::Item> {
        if self.entry < self.entries {
            let ptr = u64::from_be_bytes(
                self.buf[HEADER_OFFSET + (entry * 8)..HEADER_OFFSET + ((entry + 1) * 8)]
                    .try_into()
                    .unwrap(),
            );
            let key_len = 
        } else {
            None
        }
    }
}
pub struct DiskPage {}
pub struct PageDelta {}
