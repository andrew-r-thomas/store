use std::cell::UnsafeCell;

pub struct Log {
    file: Arc<File>,
    current_buf: AtomicU8,
}

pub struct Block {
    buf: UnsafeCell<*mut u8>,
    state: AtomicU32,
}
