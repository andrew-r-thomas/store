use std::collections::HashMap;

use crate::io::IO;

pub struct BufferPool<S>
where
    S: IO,
{
    map: HashMap<u32, usize>,
    free_list: Vec<usize>,
    pool: Vec<Vec<u8>>,
    io: S,
}

impl<S> BufferPool<S>
where
    S: IO,
{
    pub fn new(capacity: usize, page_size: usize, io: S) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            free_list: Vec::from_iter(0..capacity),
            pool: vec![vec![0; page_size]; capacity],
            io,
        }
    }

    pub fn get(&mut self, page_id: u32) -> Page<'_> {
        match self.map.get(&page_id) {
            Some(i) => {
                // page is in cache, hand it out
                Page {
                    buf: &mut self.pool[*i],
                }
            }
            None => {
                // page is not in cache
                //
                // first check if we have space for a new page
                match self.free_list.pop() {
                    Some(i) => {
                        // we have a free page to write into
                        self.io.read_page(page_id, &mut self.pool[i]);
                        self.map.insert(page_id, i);
                        Page {
                            buf: &mut self.pool[i],
                        }
                    }
                    None => {
                        todo!()
                    }
                }
            }
        }
    }
}

pub struct Page<'p> {
    buf: &'p mut [u8],
}
