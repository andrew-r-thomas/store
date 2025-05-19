use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    ops::Range,
    path::Path,
};

use crate::{PageId, page::PageBuffer};

const MAGIC_NUM: [u8; 5] = [b'S', b'T', b'O', b'R', b'E'];
const VERSION: [u8; 3] = [0, 0, 1];

const MAGIC_NUM_RANGE: Range<usize> = 0..5;
const VERSION_RANGE: Range<usize> = 5..8;
const PAGE_SIZE_RANGE: Range<usize> = 8..16;
const ROOT_RANGE: Range<usize> = 16..24;

const HEADER_SIZE: usize = 24;
const PID_SIZE: usize = 8;
const OFFSET_SIZE: usize = 8;
const OFFSET_SLOT_SIZE: usize = PID_SIZE + OFFSET_SIZE;

const INITIAL_ROOT: PageId = 1;
const INITIAL_NUM_PAGES: usize = 8;

pub struct PageDir {
    file: File,

    buf_pool: Vec<PageBuffer>,
    free_list: Vec<usize>,
    page_meta: HashMap<PageId, PageMeta>,

    pub root: PageId,
    next_pid: PageId,
    access: u64,
}
impl PageDir {
    pub fn create(path: &Path, buf_pool_size: usize, page_size: usize) -> Self {
        let mut file = File::create(path).unwrap();

        // write the header
        file.write_all(&MAGIC_NUM).unwrap();
        file.write_all(&VERSION).unwrap();
        file.write_all(&page_size.to_be_bytes()).unwrap();
        file.write_all(&INITIAL_ROOT.to_be_bytes()).unwrap();

        // TODO:

        // write initial offset table

        // write initial data blocks

        let buf_pool = vec![PageBuffer::new(page_size); buf_pool_size];
        let free_list = Vec::from_iter(0..buf_pool_size);
        let page_meta = HashMap::new();

        Self {
            file,

            buf_pool,
            free_list,
            page_meta,

            root: INITIAL_ROOT,
            next_pid: INITIAL_ROOT + 1,
            access: 0,
        }
    }
    pub fn open(path: &Path, buf_pool_size: usize) -> Self {
        let mut file = File::open(path).unwrap();

        // read header
        let mut header_buf = vec![0; HEADER_SIZE];
        file.read_exact(&mut header_buf).unwrap();
        assert_eq!(header_buf[MAGIC_NUM_RANGE], MAGIC_NUM);
        assert_eq!(header_buf[VERSION_RANGE], VERSION);
        let page_size =
            u64::from_be_bytes(header_buf[PAGE_SIZE_RANGE].try_into().unwrap()) as usize;
        let root = u64::from_be_bytes(header_buf[ROOT_RANGE].try_into().unwrap());

        // calculate offset table region from file size
        let file_size = file.metadata().unwrap().len() as usize;
        let num_pages = (file_size - HEADER_SIZE) / (OFFSET_SLOT_SIZE + page_size);
        let offset_table_size = OFFSET_SLOT_SIZE * num_pages;

        // read and parse the offset table
        let mut offset_table_buf = vec![0; offset_table_size];
        file.read_exact(&mut offset_table_buf).unwrap();
        let mut page_meta = HashMap::new();
        let mut cursor = 0;
        let mut max_page_id = 0;
        while cursor < offset_table_buf.len() {
            let page_id = u64::from_be_bytes(
                offset_table_buf[cursor..cursor + PID_SIZE]
                    .try_into()
                    .unwrap(),
            );
            cursor += PID_SIZE;
            if page_id == 0 {
                break;
            }
            if page_id > max_page_id {
                max_page_id = page_id;
            }

            let offset = u64::from_be_bytes(
                offset_table_buf[cursor..cursor + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );
            cursor += OFFSET_SIZE;

            page_meta.insert(
                page_id,
                PageMeta {
                    loc: PageLoc::Disk(offset),
                    accesses: [0; 2],
                },
            );
        }

        Self {
            file,

            buf_pool: vec![PageBuffer::new(page_size); buf_pool_size],
            free_list: Vec::from_iter(0..buf_pool_size),
            page_meta,

            root,
            next_pid: max_page_id + 1,
            access: 0,
        }
    }
    pub fn get(&mut self, page_id: PageId) -> &mut PageBuffer {
        let meta = self.page_meta.get_mut(&page_id).unwrap();
        match meta.loc {
            PageLoc::Mem(idx) => {
                meta.accesses[1] = meta.accesses[0];
                meta.accesses[0] = self.access;
                self.access += 1;

                &mut self.buf_pool[idx]
            }
            PageLoc::Disk(_offset) => todo!("cache eviction"),
        }
    }
    #[inline]
    pub fn get_root(&mut self) -> &mut PageBuffer {
        self.get(self.root)
    }

    pub fn new_page(&mut self) -> (PageId, &mut PageBuffer) {
        todo!()
    }
    pub fn new_root(&mut self) -> (PageId, &mut PageBuffer) {
        todo!()
    }
}

pub struct PageMeta {
    loc: PageLoc,
    accesses: [u64; 2],
}

pub enum PageLoc {
    Mem(usize),
    Disk(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch() {
        {
            PageDir::create(Path::new("temp.store"), 16, 1024);
        }
        {
            PageDir::open(Path::new("temp.store"), 16);
        }
    }
}
