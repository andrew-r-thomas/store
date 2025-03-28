use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    path::Path,
};

const CURRENT_VERSION: [u8; 3] = [0, 0, 1];
const HEADER_OFFSET: u64 = 16;

#[derive(Debug)]
pub enum IOError {
    IO(io::Error),
    Format,
}

pub struct FileIO {
    file: File,
    pub v_num: [u8; 3],
    next_page: u32,
    pub root_id: u32,
    pub page_size: u32,
}
impl FileIO {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IOError> {
        // open file
        let mut file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(f) => f,
            Err(e) => return Err(IOError::IO(e)),
        };

        // calculate next_page from file size
        let size = file.metadata().unwrap().size();
        let all_pages_size = size - HEADER_OFFSET;

        // read fixed header
        let mut fixed_header = [0; HEADER_OFFSET as usize];
        if let Err(e) = file.read_exact(&mut fixed_header) {
            return Err(IOError::IO(e));
        }

        // check magic num
        if &fixed_header[0..5] != b"StOrE" {
            return Err(IOError::Format);
        }

        // get version number
        let v_num: [u8; 3] = fixed_header[5..8].try_into().unwrap();

        // get page size
        let page_size = u32::from_be_bytes(fixed_header[8..12].try_into().unwrap());
        let next_page = (all_pages_size as u32 / page_size) + 1;

        // get rood id
        let root_id = u32::from_be_bytes(fixed_header[12..16].try_into().unwrap());

        Ok(Self {
            file,
            root_id,
            page_size,
            next_page,
            v_num,
        })
    }
    pub fn create<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, IOError> {
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(f) => f,
            Err(e) => return Err(IOError::IO(e)),
        };

        let mut fixed_header = [0; HEADER_OFFSET as usize];

        // magic num
        fixed_header[0..5].copy_from_slice(b"StOrE");

        // version number
        fixed_header[5..8].copy_from_slice(&CURRENT_VERSION);

        // page size
        fixed_header[8..12].copy_from_slice(&page_size.to_be_bytes());

        // root number (page nums start at 1)
        fixed_header[12..16].copy_from_slice(&1_u32.to_be_bytes());

        if let Err(e) = file.write_all(&fixed_header) {
            return Err(IOError::IO(e));
        }

        // TODO: might do this with bufpool
        let mut root = vec![0; page_size as usize];
        // lmao this is so janky
        root[11..13].copy_from_slice(&(page_size as u16).to_be_bytes());
        file.write_all(&root).unwrap();

        Ok(Self {
            file,
            root_id: 1,
            page_size,
            next_page: 2,
            v_num: CURRENT_VERSION,
        })
    }
    // lol this won't go terribly at all
    pub fn read_page(&mut self, page_id: u32, buf: &mut [u8]) {
        self.file
            .seek(io::SeekFrom::Start(
                HEADER_OFFSET + ((page_id - 1) * self.page_size) as u64,
            ))
            .unwrap();
        self.file.read_exact(buf).unwrap();
    }
    pub fn write_page(&mut self, page_id: u32, page: &[u8]) {
        self.file
            .seek(io::SeekFrom::Start(
                HEADER_OFFSET + ((page_id - 1) * self.page_size) as u64,
            ))
            .unwrap();
        self.file.write_all(page).unwrap();
    }
    pub fn create_page(&mut self, empty_buf: &[u8]) -> u32 {
        let out = self.next_page;
        self.file.seek(io::SeekFrom::End(0)).unwrap();
        self.file.write_all(empty_buf).unwrap();
        self.next_page += 1;
        out
    }
    pub fn update_root(&mut self, page_id: u32) {
        self.root_id = page_id;
        self.file.seek(io::SeekFrom::Start(10)).unwrap();
        self.file.write_all(&page_id.to_be_bytes()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    pub const KB: u32 = 1024;
    #[ignore]
    #[test]
    fn basic_fixed_header() {
        {
            // create file
            FileIO::create("temp.store", 4 * KB).unwrap();
        }
        {
            let store = FileIO::open("temp.store").unwrap();
            assert_eq!(store.page_size, 4 * KB);
            assert_eq!(store.v_num, CURRENT_VERSION);
            assert_eq!(store.root_id, 1);
            assert_eq!(store.next_page, 2);
        }
        fs::remove_file("temp.store").unwrap();
    }

    #[ignore]
    #[test]
    fn basic_write_read() {
        let page_size = 4 * KB;
        {
            let mut f = FileIO::create("temp.store", page_size).unwrap();
            let root_id = f.root_id;
            let mut root: Vec<u8> = vec![0; page_size as usize];
            root[0..4].copy_from_slice(root_id.to_be_bytes().as_slice());
            f.write_page(root_id, &root);
        }
        {
            let mut f = FileIO::open("temp.store").unwrap();
            let root_id = f.root_id;
            let mut root = vec![0; page_size as usize];
            f.read_page(root_id, &mut root);

            println!("{:?}", root);

            let serialized_id = u32::from_be_bytes(root[0..4].try_into().unwrap());
            assert_eq!(serialized_id, 1);
        }
        fs::remove_file("temp.store").unwrap();
    }
}
