use std::{collections::HashMap, ops::Range};

use crate::{cache::LRUKCache, io::FileIO};

pub struct Pager {
    page_map: HashMap<u32, usize>,
    pool: Vec<Page>,
    free_list: Vec<usize>,
    cache_tracker: LRUKCache,
    io: FileIO,
}
impl Pager {
    pub fn new(capacity: usize, page_size: usize, io: FileIO) -> Self {
        Self {
            pool: vec![
                Page {
                    buf: vec![0; page_size],
                    dirty: false,
                };
                capacity
            ],
            free_list: Vec::from_iter(0..capacity),
            page_map: HashMap::with_capacity(capacity),
            cache_tracker: LRUKCache::new(),
            io,
        }
    }

    pub fn get(&mut self, page_id: u32) -> Result<&mut Page, PagerError> {
        if let Some(i) = self.page_map.get(&page_id) {
            self.cache_tracker.hit(page_id);
            return Ok(&mut self.pool[*i]);
        }

        if let Some(free_frame) = self.free_list.pop() {
            self.io.read_page(page_id, &mut self.pool[free_frame].buf);
            self.cache_tracker.hit(page_id);
            self.page_map.insert(page_id, free_frame);
            return Ok(&mut self.pool[free_frame]);
        }

        // if we reach this point, we need to evict something
        // we probably want a separate list of dirty pages instead of storing
        // the info in the page type itself

        Err(PagerError::HotCache)
    }
}

#[derive(Debug)]
pub enum PagerError {
    HotCache,
}

#[derive(Clone)] // don't like this
pub struct Page {
    buf: Vec<u8>,
    dirty: bool,
}
const LEVEL: usize = 0;
const LEFT_SIB: Range<usize> = 1..5;
const RIGHT_SIB: Range<usize> = 5..9;
const SLOTS: Range<usize> = 10..12;
const CELLS_START: Range<usize> = 12..14;
const RIGHT_PTR: Range<usize> = 14..18;
const HEADER_OFFSET: usize = 18;
impl Page {
    pub fn level(&self) -> u8 {
        self.buf[LEVEL]
    }
    pub fn left_sib(&self) -> u32 {
        u32::from_be_bytes(self.buf[LEFT_SIB].try_into().unwrap())
    }
    pub fn right_sib(&self) -> u32 {
        u32::from_be_bytes(self.buf[RIGHT_SIB].try_into().unwrap())
    }
    pub fn slots(&self) -> u16 {
        u16::from_be_bytes(self.buf[SLOTS].try_into().unwrap())
    }
    pub fn cells_start(&self) -> u16 {
        u16::from_be_bytes(self.buf[CELLS_START].try_into().unwrap())
    }
    pub fn right_ptr(&self) -> u32 {
        u32::from_be_bytes(self.buf[RIGHT_PTR].try_into().unwrap())
    }

    pub fn free_space(&self) -> usize {
        self.cells_start() as usize - HEADER_OFFSET - (self.slots() as usize * 2)
    }
    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn get_cell(&self, slot: usize) -> Cell<'_> {
        let slot_start = HEADER_OFFSET + (2 * slot);
        let slot_end = slot_start + 2;
        let cell_offset =
            u16::from_be_bytes(self.buf[slot_start..slot_end].try_into().unwrap()) as usize;
        if self.level() > 0 {
            let key_len =
                u16::from_be_bytes(self.buf[cell_offset..cell_offset + 2].try_into().unwrap())
                    as usize;
            let key = &self.buf[cell_offset + 2..cell_offset + 2 + key_len];
            let left_ptr = u32::from_be_bytes(
                self.buf[cell_offset + 2 + key_len..cell_offset + 2 + key_len + 4]
                    .try_into()
                    .unwrap(),
            );
            Cell::InnerCell { key, left_ptr }
        } else {
            let key_len =
                u16::from_be_bytes(self.buf[cell_offset..cell_offset + 2].try_into().unwrap())
                    as usize;
            let val_len = u16::from_be_bytes(
                self.buf[cell_offset + 2..cell_offset + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let key = &self.buf[cell_offset + 4..cell_offset + 4 + key_len];
            let val = &self.buf[cell_offset + 4 + key_len..cell_offset + 4 + key_len + val_len];
            Cell::LeafCell { key, val }
        }
    }
    pub fn iter_cells(&self) -> CellsIter {
        CellsIter {
            page: self,
            current_slot: 0,
        }
    }
    pub fn insert_cell(&mut self, slot: usize, cell: Cell<'_>) -> Result<(), ()> {
        if self.free_space() < cell.len() + 2 {
            return Err(());
        }

        let cell_end = self.cells_start() as usize;
        let cell_start = cell_end - cell.len();
        cell.copy_to_buf(&mut self.buf[cell_start..cell_end]);

        let slots = self.slots();
        let slots_start = HEADER_OFFSET + (2 * slot);
        let slots_end = slots_start + (2 * (slots as usize - slot));
        self.buf
            .copy_within(slots_start..slots_end, slots_start + 2);
        self.buf[slots_start..slots_start + 2].copy_from_slice(&(cell_start as u16).to_be_bytes());

        self.buf[SLOTS].copy_from_slice(&(slots + 1).to_be_bytes());
        let current_cells_start = self.cells_start();
        self.buf[CELLS_START]
            .copy_from_slice(&(current_cells_start - cell.len() as u16).to_be_bytes());

        self.dirty = true;

        Ok(())
    }
    pub fn push_cell(&mut self, cell: Cell<'_>) -> Result<(), ()> {
        if self.free_space() < cell.len() + 2 {
            return Err(());
        }

        let cell_end = self.cells_start() as usize;
        let cell_start = cell_end - cell.len();
        cell.copy_to_buf(&mut self.buf[cell_start..cell_end]);

        let slots = self.slots();
        let slot_start = HEADER_OFFSET + (2 * slots as usize);
        let slot_end = slot_start + 2;
        self.buf[slot_start..slot_end].copy_from_slice(&(cell_start as u16).to_be_bytes());

        // update cell start and num slots
        self.buf[SLOTS].copy_from_slice(&(slots + 1).to_be_bytes());
        let current_cells_start = self.cells_start();
        self.buf[CELLS_START]
            .copy_from_slice(&(current_cells_start - cell.len() as u16).to_be_bytes());

        self.dirty = true;

        Ok(())
    }
    pub fn update_cell(&mut self, slot: usize, val: &[u8]) -> Result<(), ()> {
        let slot_start = HEADER_OFFSET + (2 * slot);
        let slot_end = slot_start + 2;
        let cell_offset =
            u16::from_be_bytes(self.buf[slot_start..slot_end].try_into().unwrap()) as usize;
        let key_len =
            u16::from_be_bytes(self.buf[cell_offset..cell_offset + 2].try_into().unwrap()) as usize;
        if self.level() > 0 {
            // inner page
            self.buf[cell_offset + 2 + key_len..cell_offset + 2 + key_len + 4].copy_from_slice(val);
            self.dirty = true;
            Ok(())
        } else {
            // leaf page
            let val_len = u16::from_be_bytes(
                self.buf[cell_offset + 2..cell_offset + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            if val_len >= val.len() {
                // we can just overwrite
                self.buf[cell_offset + 4 + key_len..cell_offset + 4 + key_len + val.len()]
                    .copy_from_slice(val);
                // TODO: figure out what we wanna do if we swap a really large value for a really
                // small one
                self.buf
                    [cell_offset + 4 + key_len + val.len()..cell_offset + 4 + key_len + val_len]
                    .fill(0); // for posterity, or whatever
            } else {
                // FIX: so right now this is not the best option, this error
                // will trigger the btree to do a split, but we might still
                // have some room to just push the new cell on top, and set
                // the current one to 0s, or something, this will be figured
                // out via the compaction policy.
                // in general i think it makes sense to be aggresive about
                // compaction, especially if we already have what we need in
                // memory, but this will have to get figured out, i'll probably
                // wait until we've added overflow pages
                return Err(());
            }
            self.dirty = true;
            Ok(())
        }
    }
}

pub enum Cell<'c> {
    InnerCell { key: &'c [u8], left_ptr: u32 },
    LeafCell { key: &'c [u8], val: &'c [u8] },
}
impl Cell<'_> {
    pub fn len(&self) -> usize {
        match self {
            Cell::InnerCell { key, left_ptr: _ } => 6 + key.len(),
            Cell::LeafCell { key, val } => 4 + key.len() + val.len(),
        }
    }
    pub fn copy_to_buf(&self, buf: &mut [u8]) {
        match self {
            Cell::InnerCell { key, left_ptr } => {
                buf[0..2].copy_from_slice(&(key.len() as u16).to_be_bytes());
                buf[2..2 + key.len()].copy_from_slice(key);
                buf[2 + key.len()..2 + key.len() + 4].copy_from_slice(&left_ptr.to_be_bytes());
            }
            Cell::LeafCell { key, val } => {
                buf[0..2].copy_from_slice(&(key.len() as u16).to_be_bytes());
                buf[2..4].copy_from_slice(&(val.len() as u16).to_be_bytes());
                buf[4..4 + key.len()].copy_from_slice(key);
                buf[4 + key.len()..4 + key.len() + val.len()].copy_from_slice(val);
            }
        }
    }
}

pub struct CellsIter<'ci> {
    page: &'ci Page,
    current_slot: usize,
}
impl<'ci> Iterator for CellsIter<'ci> {
    type Item = Cell<'ci>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_slot >= self.page.slots() as usize {
            return None;
        }
        let out = self.page.get_cell(self.current_slot);
        self.current_slot += 1;
        Some(out)
    }
}
