use std::{
    alloc::{Layout, alloc_zeroed},
    fs::File,
    os::unix::fs::FileExt,
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

pub struct Log {
    file: File,
    current_file_offset: AtomicU64,
    current_buf: AtomicUsize,
    buffers: Vec<BlockBuffer>,
    block_size: usize,
}

impl Log {
    pub fn new(block_size: usize, num_blocks: usize, file: File) -> Self {
        let mut buffers = Vec::with_capacity(num_blocks);
        for _ in 0..num_blocks {
            buffers.push(BlockBuffer::new(block_size));
        }
        Self {
            file,
            buffers,
            block_size,
            current_buf: AtomicUsize::new(0),
            current_file_offset: AtomicU64::new(0),
        }
    }
    /// reserve space to write in the current block buffer,
    /// if successful, the Ok variant of the Result will return a tuple
    /// containing the idx for the current block, and the offset to write to
    pub fn reserve(&self, len: usize) -> Result<(usize, usize), ()> {
        let curr_buf_idx = self.current_buf.load(Ordering::SeqCst);
        let idx = curr_buf_idx % self.buffers.len();
        let block = &self.buffers[idx];
        let block_state = block.state.load(Ordering::SeqCst);
        if BlockBuffer::is_sealed(block_state) {
            // failing is the simplest thing to do here, this just means that
            // we got a buffer in between it being sealed and the current buf
            // index being incremented, which will be fixed in like,
            // a couple more instructions by the thread that sealed the buf
            return Err(());
        }

        let offset = BlockBuffer::offset(block_state);
        let active_writers = BlockBuffer::active_writers(block_state);
        if offset + len <= self.block_size {
            if let Err(_) = block.state.compare_exchange(
                block_state,
                BlockBuffer::new_state(false, active_writers + 1, offset + len),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return Err(());
            }
        } else {
            // here we return an err to retry again because it keeps things simple

            // we first try to seal the buffer, if that fails, we just return early
            if let Err(_) = block.state.compare_exchange(
                block_state,
                BlockBuffer::new_state(true, active_writers, offset),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return Err(());
            }
            // then we store the next logical buffer index
            self.current_buf.store(curr_buf_idx + 1, Ordering::SeqCst);

            if active_writers == 0 {
                // we've sealed the buffer, and it has no writers, we need to flush it
                self.flush_buffer(block);
            }

            return Err(());
        }

        Ok((idx, offset))
    }
    pub fn write_to_buffer(&self, buf: &[u8], idx: usize, offset: usize) {
        let block = &self.buffers[idx];

        let block_buf = unsafe { std::slice::from_raw_parts_mut(block.buf, self.block_size) };
        block_buf[offset..offset + buf.len()].copy_from_slice(buf);

        if loop {
            let block_state = block.state.load(Ordering::SeqCst);
            let active_writers = BlockBuffer::active_writers(block_state);
            let sealed = BlockBuffer::is_sealed(block_state);
            let buf_offset = BlockBuffer::offset(block_state);
            if let Ok(_) = block.state.compare_exchange_weak(
                block_state,
                BlockBuffer::new_state(sealed, active_writers - 1, buf_offset),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break sealed && (active_writers - 1 == 0);
            }
        } {
            self.flush_buffer(block);
        }
    }

    /// a thread that causes a buffer's state to be both sealed and have
    /// 0 active writers, is responsible for calling this function, it should
    /// not be called at any other time
    fn flush_buffer(&self, block: &BlockBuffer) {
        println!("flushing a buffer");
        let file_offset = self
            .current_file_offset
            .fetch_add(self.block_size as u64, Ordering::SeqCst);

        let block_buf = unsafe { std::slice::from_raw_parts_mut(block.buf, self.block_size) };
        let block_state = block.state.load(Ordering::SeqCst);

        self.file
            .write_all_at(block_buf, file_offset)
            .expect("gotta figure out what to do if the file write fails");
        self.file.sync_all().expect("fsync failed");

        block_buf.fill(0);
        block
            .state
            .compare_exchange(
                block_state,
                BlockBuffer::new_state(false, 0, 0),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .expect("CAS failed on buffer state after a buffer flush");
    }
}

unsafe impl Send for Log {}
unsafe impl Sync for Log {}

pub struct BlockBuffer {
    buf: *mut u8,
    state: AtomicU32,
}
impl BlockBuffer {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 1024).unwrap();
        let buf = unsafe { alloc_zeroed(layout) };
        Self {
            buf,
            state: AtomicU32::new(0),
        }
    }

    pub fn is_sealed(state: u32) -> bool {
        (0x80000000 & state) != 0
    }
    pub fn offset(state: u32) -> usize {
        (0x00FFFFFF & state) as usize
    }
    pub fn active_writers(state: u32) -> u8 {
        ((state >> 24) & 0x7F) as u8
    }

    pub fn new_state(sealed: bool, active_writers: u8, offset: usize) -> u32 {
        ((sealed as u32) << 31)
            | (((active_writers & 0x7F) as u32) << 24)
            | ((offset as u32) & 0x00FF_FFFF)
    }
}
