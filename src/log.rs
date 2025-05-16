use std::{
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

const INITIAL_ROOT: [u8; 8] = 1_u64.to_be_bytes();
const INITIAL_NUM_PAGES: usize = 8;

pub struct DiskManager {
    file: File,

    root: PageId,
    page_size: usize,

    write_buf: Vec<u8>,
}
impl DiskManager {
    pub fn open(path: &Path) -> Self {
        let mut file = File::open(path).unwrap();

        let mut header_buf = vec![0; 24];
        file.read_exact(&mut header_buf).unwrap();

        assert_eq!(&MAGIC_NUM, &header_buf[MAGIC_NUM_RANGE]);
        assert_eq!(&VERSION, &header_buf[VERSION_RANGE]);
        let page_size =
            u64::from_be_bytes(header_buf[PAGE_SIZE_RANGE].try_into().unwrap()) as usize;
        let root = u64::from_be_bytes(header_buf[ROOT_RANGE].try_into().unwrap());

        todo!()
    }
    pub fn create(path: &Path, page_size: u64) -> Self {
        let mut file = File::create(path).unwrap();

        // write the header
        file.write_all(&MAGIC_NUM).unwrap();
        file.write_all(&VERSION).unwrap();
        file.write_all(&page_size.to_be_bytes()).unwrap();
        file.write_all(&INITIAL_ROOT).unwrap();

        todo!()
    }
    pub fn read(&mut self, page_id: PageId, page_buf: &mut PageBuffer) {
        todo!()
    }
    pub fn write(&mut self, data: &[u8]) {
        todo!()
    }
}
