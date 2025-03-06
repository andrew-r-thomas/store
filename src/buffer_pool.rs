pub struct BufferPool {
    pool: Vec<u8>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(capacity: usize, page_size: usize) -> Self {
        Self {
            pool: Vec::with_capacity(capacity * page_size),
            page_size,
        }
    }
}
