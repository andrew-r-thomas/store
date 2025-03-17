use std::{cell::RefCell, collections::HashMap, ops::Range, rc::Rc};

use crate::{cache::LRUKCache, io::FileIO};

pub struct Pager {
    page_map: HashMap<u32, usize>,
    pool: Vec<Rc<RefCell<Page>>>,
    free_list: Vec<usize>,
    cache_tracker: LRUKCache,
    io: FileIO,
    pub page_size: usize,
}

impl Pager {
    pub fn new(capacity: usize, page_size: usize, io: FileIO) -> Self {
        let mut pool = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            pool.push(Rc::new(RefCell::new(Page {
                buf: vec![0; page_size],
                dirty: false,
            })));
        }
        Self {
            pool,
            free_list: Vec::from_iter(0..capacity),
            page_map: HashMap::with_capacity(capacity),
            cache_tracker: LRUKCache::new(),
            io,
            page_size,
        }
    }

    pub fn get(&mut self, page_id: u32) -> Result<Rc<RefCell<Page>>, PagerError> {
        if let Some(i) = self.page_map.get(&page_id) {
            self.cache_tracker.hit(page_id);
            // TODO: might want to return a guard here or something instead
            return Ok(self.pool[*i].clone());
        }

        if let Some(free_frame) = self.free_list.pop() {
            {
                let page = &mut self.pool[free_frame].borrow_mut();
                self.io.read_page(page_id, &mut page.buf);
                self.cache_tracker.hit(page_id);
                self.page_map.insert(page_id, free_frame);
            }
            return Ok(self.pool[free_frame].clone());
        }

        // if we reach this point, we need to evict something
        // we probably want a separate list of dirty pages instead of storing
        // the info in the page type itself
        todo!("evict")
    }
    pub fn get_scratch(&mut self) -> Rc<RefCell<Page>> {
        let idx = self.free_list.pop().unwrap();
        let page = self.pool[idx].clone();
        let mut guard = page.borrow_mut();
        guard.set_cells_start(self.page_size as u16);
        drop(guard);
        page
    }

    pub fn create_page(&mut self) -> u32 {
        if let Some(free_frame) = self.free_list.pop() {
            let page = &self.pool[free_frame].borrow();
            let page_id = self.io.create_page(&page.buf);
            self.page_map.insert(page_id, free_frame);
            self.cache_tracker.hit(page_id);
            return page_id;
        }

        // evict something
        todo!("evict")
    }
}

#[derive(Debug)]
pub enum PagerError {
    HotCache,
}

// pub enum Page_ {
//     Overflow(OverflowPage),
//     Leaf(LeafPage),
//     Inner(InnerPage),
// }

enum SetOption {
    Gt,
    Lt(usize),
    Eq(usize),
}

#[derive(Clone)] // don't like this
pub struct Page {
    buf: Vec<u8>,
    pub dirty: bool,
}
const LEVEL: usize = 0;
const LEFT_SIB: Range<usize> = 1..5;
const RIGHT_SIB: Range<usize> = 5..9;
const SLOTS: Range<usize> = 9..11;
const CELLS_START: Range<usize> = 11..13;
const RIGHT_PTR: Range<usize> = 13..17;
const HEADER_OFFSET: usize = 17;
impl Page {
    // methods for reading the header
    pub fn level(&self) -> i8 {
        i8::from_be_bytes([self.buf[LEVEL]])
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
        match u16::from_be_bytes(self.buf[CELLS_START].try_into().unwrap()) {
            0 => self.buf.len() as u16,
            n => n,
        }
    }
    pub fn right_ptr(&self) -> u32 {
        u32::from_be_bytes(self.buf[RIGHT_PTR].try_into().unwrap())
    }

    // methods for updating the header
    pub fn set_level(&mut self, level: i8) {
        self.buf[LEVEL] = level.to_be_bytes()[0];
        self.dirty = true;
    }
    pub fn set_left_sib(&mut self, left_sib: u32) {
        self.buf[LEFT_SIB].copy_from_slice(&left_sib.to_be_bytes());
        self.dirty = true;
    }
    pub fn set_right_sib(&mut self, right_sib: u32) {
        self.buf[RIGHT_SIB].copy_from_slice(&right_sib.to_be_bytes());
        self.dirty = true;
    }
    pub fn set_right_ptr(&mut self, ptr: u32) {
        self.buf[RIGHT_PTR].copy_from_slice(&ptr.to_be_bytes());
        self.dirty = true;
    }
    pub fn set_slots(&mut self, slots: u16) {
        self.buf[SLOTS].copy_from_slice(&slots.to_be_bytes());
        self.dirty = true;
    }
    pub fn set_cells_start(&mut self, cells_start: u16) {
        self.buf[CELLS_START].copy_from_slice(&cells_start.to_be_bytes());
        self.dirty = true;
    }

    pub fn set<'s>(&mut self, cell: Cell<'s>) -> Result<(), Cell<'s>> {
        let key = cell.key();
        let set_option = {
            let mut out = SetOption::Gt;
            for (cell, slot) in self.iter_cells().zip(0..) {
                let k = cell.key();
                if key < k {
                    out = SetOption::Lt(slot);
                    break;
                } else if key == k {
                    out = SetOption::Eq(slot);
                    break;
                }
            }
            out
        };

        match set_option {
            SetOption::Lt(slot) => {
                // insert
                if self.free_space() < cell.len() + 2 {
                    return Err(cell);
                }
                // push the cell in
                let cell_end = self.cells_start() as usize;
                let cell_start = cell_end - cell.len();
                cell.copy_to_buf(&mut self.buf[cell_start..cell_end]);
                self.set_cells_start(cell_start as u16);

                // adjust slots
                let slot_start = HEADER_OFFSET + (slot * 2);
                let slots_end = HEADER_OFFSET + (self.slots() as usize * 2);
                self.buf.copy_within(slot_start..slots_end, slot_start + 2);
                self.buf[slot_start..slot_start + 2]
                    .copy_from_slice(&(cell_start as u16).to_be_bytes());
                self.set_slots(self.slots() + 1);

                Ok(())
            }
            SetOption::Eq(slot) => {
                // update
                let cell_start = self.get_slot(slot) as usize;
                match cell {
                    Cell::InnerCell { key, left_ptr } => {
                        self.buf[cell_start + key.len()..cell_start + key.len() + 4]
                            .copy_from_slice(&left_ptr.to_be_bytes());
                        Ok(())
                    }
                    Cell::LeafCell { key: _, val } => {
                        let val_len = u16::from_be_bytes(
                            self.buf[cell_start + 2..cell_start + 4].try_into().unwrap(),
                        ) as usize;
                        if val.len() > val_len {
                            Err(cell)
                        } else {
                            cell.copy_to_buf(&mut self.buf[cell_start..cell_start + cell.len()]);
                            Ok(())
                        }
                    }
                }
            }
            SetOption::Gt => {
                // push
                if self.free_space() < cell.len() + 2 {
                    return Err(cell);
                }
                // push the cell on
                let cell_end = self.cells_start() as usize;
                let cell_start = cell_end - cell.len();
                cell.copy_to_buf(&mut self.buf[cell_start..cell_end]);
                self.set_cells_start(cell_start as u16);

                // push the slot on
                let slot_start = HEADER_OFFSET + (self.slots() as usize * 2);
                let slot_end = slot_start + 2;
                self.buf[slot_start..slot_end].copy_from_slice(&(cell_start as u16).to_be_bytes());
                self.set_slots(self.slots() + 1);

                Ok(())
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Cell<'_>> {
        for cell in self.iter_cells() {
            if key <= cell.key() {
                return Some(cell);
            }
        }
        None
    }

    // utilities
    pub fn free_space(&self) -> usize {
        let cells_start = self.cells_start() as usize;
        let slots = self.slots() as usize;
        cells_start - (HEADER_OFFSET + (slots * 2))
    }

    // slot methods
    pub fn inc_slot(&mut self, slot: usize, by: u16) {
        let slot_start = HEADER_OFFSET + (2 * slot);
        let slot_end = slot_start + 2;
        let slot_val = u16::from_be_bytes(self.buf[slot_start..slot_end].try_into().unwrap());
        self.buf[slot_start..slot_end].copy_from_slice(&(slot_val + by).to_be_bytes());
    }
    pub fn get_slot(&self, slot: usize) -> u16 {
        let slot_start = HEADER_OFFSET + (2 * slot);
        let slot_end = slot_start + 2;
        u16::from_be_bytes(self.buf[slot_start..slot_end].try_into().unwrap())
    }
    pub fn delete_slot(&mut self, slot: usize) {
        let slot_start = HEADER_OFFSET + (2 * slot);
        let slot_end = slot_start + 2;
        let slots_end = HEADER_OFFSET + (2 * self.slots() as usize);
        self.buf.copy_within(slot_end..slots_end, slot_start);
        self.set_slots(self.slots() - 1);
        self.dirty = true;
    }
    // FIX:
    pub fn iter_slots_sorted(&self) -> Vec<u16> {
        let mut out = (0..self.slots())
            .into_iter()
            .map(|slot| self.get_slot(slot as usize))
            .collect::<Vec<u16>>();
        out.sort();
        out
    }

    pub fn get_cell(&self, slot: usize) -> Cell<'_> {
        let cell_offset = self.get_slot(slot) as usize;
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

    // TODO: so we can't do trick where we push everything down, bc cells are
    // not necessarily same order as slots
    pub fn delete_cell(&mut self, slot: usize) {
        let cell_offset = self.get_slot(slot) as usize;
        let cell_len = {
            if self.level() > 0 {
                // inner node
                u16::from_be_bytes(self.buf[cell_offset..cell_offset + 2].try_into().unwrap())
                    as usize
                    + 6
            } else {
                // leaf node
                (u16::from_be_bytes(self.buf[cell_offset..cell_offset + 2].try_into().unwrap())
                    + u16::from_be_bytes(
                        self.buf[cell_offset + 2..cell_offset + 4]
                            .try_into()
                            .unwrap(),
                    )) as usize
                    + 4
            }
        };

        // erase cell, and shift stuff
        self.buf[cell_offset..cell_offset + cell_len].fill(0);
        if cell_offset == self.cells_start() as usize {
            // we need to find the correct amount to shift this by
            let new_cells_start = *self
                .iter_slots_sorted()
                .get(1)
                .unwrap_or(&(self.buf.len() as u16));
            self.set_cells_start(new_cells_start);
        }

        self.delete_slot(slot);
        self.dirty = true;
    }

    // TODO: ok so this is where we left off, on some splits, we fail to move
    // stuff around correctly, and the next get fails, with a None, bc the best
    // key found is not equal, zoinks, why am i so bad at this
    //
    /// this function splits left
    pub fn split_into(&mut self, other: &mut Self, scratch: &mut Self) {
        let lvl = self.level();
        other.set_level(lvl);
        other.set_cells_start(self.buf.len() as u16);
        scratch.set_level(lvl);
        let middle_slot = (self.slots() / 2) as usize;
        for slot in 0..middle_slot {
            other.set(self.get_cell(slot)).unwrap();
        }
        for _ in 0..middle_slot {
            self.delete_cell(0);
        }
        if let Cell::InnerCell { key: _, left_ptr } = self.get_cell(0) {
            other.set_right_ptr(left_ptr);
        }
        self.compact(scratch);
    }

    pub fn compact(&mut self, scratch: &mut Self) {
        // something got fucked here
        for cell in self.iter_cells() {
            scratch.set(cell).unwrap();
        }
        for _ in 0..self.slots() as usize {
            self.delete_cell(0);
        }
        for cell in scratch.iter_cells() {
            self.set(cell).unwrap();
        }
        scratch.clear();
    }

    pub fn clear(&mut self) {
        self.buf.fill(0);
    }
}

#[derive(Debug)]
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
    pub fn key(&self) -> &[u8] {
        match self {
            Cell::LeafCell { key, val: _ } => key,
            Cell::InnerCell { key, left_ptr: _ } => key,
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
