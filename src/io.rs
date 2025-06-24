use std::{collections, mem};

use crate::{page, shard};

/// this is just the "index" of the block
///
/// TODO
/// - for some reason im having a hard time deciding if this
///   should be the index, or the offset in the file, and whether to use a u64 vs usize, idk if it
///   matters that much but, yanno 🤷
pub type BlockId = usize;

/// this is basically just a trait that captures the io_uring interface, at a slightly higher level
/// of abstraction, contextual to our use case. the main purpose of the trait is to allow our test
/// harness to hook into our main system without too much trouble
pub trait IOFace {
    fn register_sub(&mut self, sub: Sub);
    fn submit(&mut self);
    fn poll(&mut self) -> Vec<Comp>;
}
pub enum Sub {
    TcpRead { buf: Vec<u8>, conn_id: u32 },
    TcpWrite { buf: Vec<u8>, conn_id: u32 },
    FileRead { buf: Vec<u8>, offset: u64 },
    FileWrite { buf: Vec<u8>, offset: u64 },
}
#[derive(Clone)]
pub enum Comp {
    TcpRead {
        buf: Vec<u8>,
        conn_id: u32,
        res: i32,
        bytes: usize,
    },
    TcpWrite {
        buf: Vec<u8>,
        conn_id: u32,
        res: i32,
    },
    FileRead {
        buf: Vec<u8>,
        offset: u64,
    },
    FileWrite {
        buf: Vec<u8>,
    },
}

pub enum Req<'r> {
    Get(GetReq<'r>),
    Set(SetReq<'r>),
    Del(DelReq<'r>),
}
pub struct GetReq<'r> {
    pub key: &'r [u8],
}
pub struct SetReq<'r> {
    pub key: &'r [u8],
    pub val: &'r [u8],
}
pub struct DelReq<'r> {
    pub key: &'r [u8],
}
impl Req<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::Get(get_req) => 1 + 4 + get_req.key.len(),
            Self::Set(set_req) => 1 + 4 + 4 + set_req.key.len() + set_req.val.len(),
            Self::Del(del_req) => 1 + 4 + del_req.key.len(),
        }
    }
}

const GET_CODE: u8 = 1;
const SET_CODE: u8 = 2;
const DEL_CODE: u8 = 3;

pub fn parse_req(req: &[u8]) -> Result<Req, ()> {
    match req.first() {
        Some(code) => match *code {
            GET_CODE => {
                if req.len() < 5 {
                    return Err(());
                }
                let key_len = u32::from_be_bytes(req[1..5].try_into().unwrap()) as usize;
                if req.len() < 5 + key_len {
                    return Err(());
                }
                let key = &req[5..5 + key_len];
                Ok(Req::Get(GetReq { key }))
            }
            SET_CODE => {
                if req.len() < 9 {
                    return Err(());
                }
                let key_len = u32::from_be_bytes(req[1..5].try_into().unwrap()) as usize;
                let val_len = u32::from_be_bytes(req[5..9].try_into().unwrap()) as usize;
                if req.len() < 9 + key_len + val_len {
                    return Err(());
                }
                let key = &req[9..9 + key_len];
                let val = &req[9 + key_len..9 + key_len + val_len];
                Ok(Req::Set(SetReq { key, val }))
            }
            DEL_CODE => {
                if req.len() < 5 {
                    return Err(());
                }
                let key_len = u32::from_be_bytes(req[1..5].try_into().unwrap()) as usize;
                if req.len() < 5 + key_len {
                    return Err(());
                }
                let key = &req[5..5 + key_len];
                Ok(Req::Del(DelReq { key }))
            }
            n => panic!("{n} is not an op code"),
        },
        None => Err(()),
    }
}

pub const CHUNK_LEN_SIZE: usize = 8;

pub struct DiskManager {
    pub block_bufs: Vec<Vec<u8>>,
    pub flush_buf: Vec<u8>,
    pub flush_off: usize,

    pub offset_table: collections::BTreeMap<crate::PageId, Vec<u64>>,

    pub blocks: Vec<Block>,
    pub gc: bool,
    pub head: BlockId,
    pub tail: BlockId,
}
impl DiskManager {
    pub fn new(block_size: usize, num_block_bufs: usize) -> Self {
        Self {
            block_bufs: Vec::from_iter((0..num_block_bufs).map(|_| vec![0; block_size])),
            flush_buf: vec![0; block_size],
            flush_off: 0,

            offset_table: collections::BTreeMap::new(),

            blocks: Vec::new(),
            gc: false,
            head: 0,
            tail: 0,
        }
    }
    pub fn inc_prio(&mut self, pid: crate::PageId) {
        todo!()
    }
    pub fn flush_page<IO: IOFace>(
        &mut self,
        page: &mut page::PageBuffer,
        pid: crate::PageId,
        io: &mut IO,
    ) {
        let flush_len = page.flush_len();
        if self.flush_buf.len() - self.flush_off < flush_len + CHUNK_LEN_SIZE {
            // not enough room in current write buffer
            let mut flush_buf = self.block_bufs.pop().unwrap();
            mem::swap(&mut flush_buf, &mut self.flush_buf);
            self.flush_off = 0;

            // TODO: ok this is wrong, we gotta wrap around and stuff, redo everything in this
            // branch
            let space = {
                if self.head >= self.tail {
                    (self.flush_buf.len() * self.blocks.len()) - (self.head - self.tail)
                } else {
                    self.tail - self.head
                }
            };
            let gc_thresh = self.blocks.len() / 5; // ~20%
            if space <= gc_thresh && !self.gc {
                // trigger gc
                // FIX: we need to check if this is already being read for normal query purposes
                io.register_sub(Sub::FileRead {
                    buf: self.block_bufs.pop().unwrap(),
                    offset: (self.tail * self.flush_buf.len()) as u64,
                });
                self.gc = true;
            }
            if space == 0 {
                // grow
                self.tail = 0;
                self.head = self.blocks.len();
                self.blocks.push(Block {
                    pend_pages: Vec::new(),
                });
            }

            io.register_sub(Sub::FileWrite {
                buf: flush_buf,
                offset: (self.head * self.flush_buf.len()) as u64,
            });
            self.head += 1;
        }

        // update offset table
        let offset = ((self.head * self.flush_buf.len()) + self.flush_off) as u64;
        match self.offset_table.get_mut(&pid) {
            Some(offs) => {
                if page.flush == page.cap {
                    offs.clear();
                }
                offs.push(offset);
            }
            None => {
                self.offset_table.insert(pid, vec![offset]);
            }
        }

        // write out the page chunk
        self.flush_buf[self.flush_off..self.flush_off + CHUNK_LEN_SIZE]
            .copy_from_slice(&(flush_len as u64).to_be_bytes());
        self.flush_off += CHUNK_LEN_SIZE;
        page.flush(&mut self.flush_buf[self.flush_off..self.flush_off + flush_len]);
        self.flush_off += flush_len;
    }
    pub fn tick_reads<IO: IOFace>(&mut self, io: &mut IO) {
        todo!()
    }
}

pub struct Block {
    pend_pages: Vec<PendPage>,
}
pub struct PendPage {
    pid: crate::PageId,
    idx: usize,
    len: usize,
    offs: Vec<usize>,
}

pub fn read_block(
    block: Vec<u8>,
    block_id: BlockId,
    disk_manager: &mut DiskManager,
    buf_pool: &mut Vec<page::PageBuffer>,
    page_dir: &mut shard::PageDir,
) {
    // read any chunks that we need
    while let Some(mut pend_page) = disk_manager.blocks[block_id].pend_pages.pop() {
        let page = &mut buf_pool[pend_page.idx];
        let buf = page.raw_buffer_mut();

        // read everything we can for this page from this block
        loop {
            let off = pend_page.offs.pop().unwrap();
            let mut cursor = off - (block_id * block.len());
            let chunk_len =
                u64::from_be_bytes(block[cursor..cursor + CHUNK_LEN_SIZE].try_into().unwrap())
                    as usize;
            cursor += CHUNK_LEN_SIZE;

            buf[pend_page.len..pend_page.len + chunk_len]
                .copy_from_slice(&block[cursor..cursor + chunk_len]);
            pend_page.len += chunk_len;

            match pend_page.offs.last() {
                Some(o) => {
                    // if next offset is out of bounds of this block, break
                    if *o < block_id * block.len() && *o >= (block_id * block.len()) + block.len() {
                        // tee up pend page in next block
                        let next_block = o / block.len();
                        disk_manager.blocks[next_block].pend_pages.push(pend_page);
                        break;
                    }
                }
                None => {
                    // done reading page
                    buf.copy_within(0..pend_page.len, buf.len() - pend_page.len);
                    page.top = buf.len() - pend_page.len;
                    page.flush = page.top;
                    page_dir.insert(pend_page.pid, pend_page.idx);
                    break;
                }
            }
        }
    }

    // check if we need to gc
    if disk_manager.gc && block_id == disk_manager.tail {
        // so we have all of the offsets in memory in the offset table, the problem here is we'd
        // have to iterate through all of them, and check which ones are within this block, maybe
        // thats the best approach initially, but seems inefficient. maybe we build a list of pages
        // in a block when we do a gc
        todo!()
    }
}
