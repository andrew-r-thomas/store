/*

    NOTE:
    - we assume all page chunks exist within a single data block, since data blocks are page sized,
      there is a small chance that a very full page gets evicted and there is not room in the write
      buffer for the next offset and chunk len, for now, let's just panic in that case, and figure
      out the best way to manage that case later

*/
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
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
const CHUNK_LEN_SIZE: usize = 8;

const INITIAL_ROOT: PageId = 1;
const INITIAL_NUM_PAGES: usize = 8;

pub struct PageDir {
    file: File,
    data_region_start: u64,
    read_buf: Vec<u8>,

    buf_pool: Vec<PageBuffer>,
    free_list: Vec<usize>,
    mapping_table: HashMap<PageId, PageLoc>,

    pub root: PageId,
    next_pid: PageId,
}
impl PageDir {
    pub fn create(path: &Path, buf_pool_size: usize, page_size: usize) -> Self {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();

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
            mapping_table: page_meta,

            root: INITIAL_ROOT,
            next_pid: INITIAL_ROOT + 1,
            data_region_start: 0, // TODO:
            read_buf: vec![0; page_size],
        }
    }
    pub fn open(path: &Path, buf_pool_size: usize) -> Self {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();

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

            page_meta.insert(page_id, PageLoc::Disk(offset));
        }

        Self {
            file,

            buf_pool: vec![PageBuffer::new(page_size); buf_pool_size],
            free_list: Vec::from_iter(0..buf_pool_size),
            mapping_table: page_meta,

            root,
            next_pid: max_page_id + 1,
            data_region_start: 0, // TODO:
            read_buf: vec![0; page_size],
        }
    }
    pub fn get(&mut self, page_id: PageId) -> &mut PageBuffer {
        match self.mapping_table.get(&page_id).unwrap() {
            PageLoc::Mem(idx) => &mut self.buf_pool[*idx],
            PageLoc::Disk(offset) => match self.free_list.pop() {
                Some(idx) => {
                    self.read_page(*offset, idx);
                    self.mapping_table.insert(page_id, PageLoc::Mem(idx));
                    &mut self.buf_pool[idx]
                }
                None => todo!("need to evict something"),
            },
        }
    }
    #[inline]
    pub fn get_root(&mut self) -> &mut PageBuffer {
        self.get(self.root)
    }

    // TODO: may need to allocate space in file
    pub fn new_page(&mut self) -> (PageId, &mut PageBuffer) {
        let page_id = self.next_pid;
        self.next_pid += 1;
        match self.free_list.pop() {
            Some(idx) => (page_id, &mut self.buf_pool[idx]),
            None => todo!("need to evict something"),
        }
    }
    pub fn new_root(&mut self) -> (PageId, &mut PageBuffer) {
        todo!()
    }

    pub fn read_page(&mut self, offset: u64, buf_idx: usize) {
        // we need to calculate the data block to read based on the offset and page
        // size (data blocks are page size chunks of the file)
        let page_size = self.read_buf.len() as u64;

        // PERF: page size is always a power of 2 so we can do this with bit manips
        let mut data_block = offset / page_size;
        self.file
            .seek(SeekFrom::Start(
                self.data_region_start + (data_block * page_size),
            ))
            .unwrap();
        self.read_buf.fill(0);
        self.file.read_exact(&mut self.read_buf).unwrap();

        let mut read_cursor = (offset % page_size) as usize;
        let mut next_chunk_offset = u64::from_be_bytes(
            self.read_buf[read_cursor..read_cursor + OFFSET_SIZE]
                .try_into()
                .unwrap(),
        );
        read_cursor += OFFSET_SIZE;
        let chunk_len = u64::from_be_bytes(
            self.read_buf[read_cursor..read_cursor + CHUNK_LEN_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;
        read_cursor += CHUNK_LEN_SIZE;

        let page = &mut self.buf_pool[buf_idx];
        let page_buf = page.raw_buffer_mut();
        let mut page_cursor = 0;
        page_buf[page_cursor..page_cursor + chunk_len]
            .copy_from_slice(&self.read_buf[read_cursor..read_cursor + chunk_len]);
        page_cursor += chunk_len;

        while next_chunk_offset != 0 {
            // need to keep reading in more chunks
            // first we need to check if the next chunk exists within the current read buf
            let next_data_block = next_chunk_offset / page_size;
            read_cursor = (next_chunk_offset % page_size) as usize;

            if next_data_block != data_block {
                // need to read a different block in
                self.file
                    .seek(SeekFrom::Start(
                        self.data_region_start + (next_data_block * page_size),
                    ))
                    .unwrap();
                self.read_buf.fill(0);
                self.file.read_exact(&mut self.read_buf).unwrap();
            }

            next_chunk_offset = u64::from_be_bytes(
                self.read_buf[read_cursor..read_cursor + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );
            read_cursor += OFFSET_SIZE;
            let chunk_len = u64::from_be_bytes(
                self.read_buf[read_cursor..read_cursor + CHUNK_LEN_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;
            read_cursor += CHUNK_LEN_SIZE;

            page_buf[page_cursor..page_cursor + chunk_len]
                .copy_from_slice(&self.read_buf[read_cursor..read_cursor + chunk_len]);
            page_cursor += chunk_len;

            data_block = next_data_block;
        }

        let top = page_buf.len() - page_cursor;
        page_buf.copy_within(0..page_cursor, top);
        page.top = top;
    }
}

pub enum PageLoc {
    Mem(usize),
    /// contains the offset from the start of the data block region for the most recent write of
    /// this page
    Disk(u64),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn scratch() {
        {
            PageDir::create(Path::new("temp.store"), 16, 1024);
        }
        {
            PageDir::open(Path::new("temp.store"), 16);
        }

        fs::remove_file("temp.store").unwrap();
    }
}
