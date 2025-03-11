use std::{
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

const CURRENT_VERSION: u8 = 0;
const HEADER_OFFSET: usize = 16;
pub const KB: u32 = 1024;
pub const MB: u32 = 1024 * KB;

pub trait IO<StoreKey>: Sized {
    fn open(_: StoreKey) -> Result<Self, IOError>;
    fn create(_: StoreKey, page_size: u32) -> Result<Self, IOError>;
    fn read_page(&self, page_id: u32) -> &[u8];
    fn write_page(&mut self, page_id: u32, page: &[u8]);
    fn create_page(&mut self) -> u32;
    fn update_root(&mut self, page_id: u32);
}

#[derive(Debug)]
pub enum IOError {
    IO(io::Error),
    Format,
}

pub struct FileIO {
    v_num: u8,
    file: File,
    root_id: u32,
    page_size: u32,
}
impl<P: AsRef<Path>> IO<P> for FileIO {
    fn open(path: P) -> Result<Self, IOError> {
        // open file
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) => return Err(IOError::IO(e)),
        };

        // read fixed header
        let mut fixed_header = [0; HEADER_OFFSET];
        if let Err(e) = file.read_exact(&mut fixed_header) {
            return Err(IOError::IO(e));
        }

        // check magic num
        if &fixed_header[0..5] != b"StOrE" {
            return Err(IOError::Format);
        }

        // get version number
        let v_num = fixed_header[5];

        // get page size
        let page_size = u32::from_be_bytes(fixed_header[6..10].try_into().unwrap());

        // get rood id
        let root_id = u32::from_be_bytes(fixed_header[10..14].try_into().unwrap());

        Ok(Self {
            file,
            root_id,
            page_size,
            v_num,
        })
    }
    fn create(path: P, page_size: u32) -> Result<Self, IOError> {
        let mut file = match File::create(path) {
            Ok(f) => f,
            Err(e) => return Err(IOError::IO(e)),
        };

        let mut fixed_header = [0; HEADER_OFFSET];

        // magic num
        fixed_header[0..5].copy_from_slice(b"StOrE");

        // version number
        fixed_header[5] = CURRENT_VERSION;

        // page size
        fixed_header[6..10].copy_from_slice(&page_size.to_be_bytes());

        // root number (page nums start at 1)
        fixed_header[10..14].copy_from_slice(&1_u32.to_be_bytes());

        if let Err(e) = file.write_all(&fixed_header) {
            return Err(IOError::IO(e));
        }

        // TODO: write root page

        Ok(Self {
            file,
            root_id: 1,
            page_size,
            v_num: CURRENT_VERSION,
        })
    }
    fn read_page(&self, page_id: u32) -> &[u8] {
        unimplemented!()
    }
    fn write_page(&mut self, page_id: u32, page: &[u8]) {
        unimplemented!()
    }
    fn create_page(&mut self) -> u32 {
        unimplemented!()
    }
    fn update_root(&mut self, page_id: u32) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

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
        }
        fs::remove_file("temp.store").unwrap();
    }
}
