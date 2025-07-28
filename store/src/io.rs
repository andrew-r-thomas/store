use std::{collections, mem, ops::Deref};

use crate::{
    format::{self, Op},
    page, shard,
};

/// this is basically just a trait that captures the io_uring interface, at a slightly higher level
/// of abstraction, contextual to our use case. the main purpose of the trait is to allow our test
/// harness to hook into our main system without too much trouble
pub trait IOFace {
    fn register_sub(&mut self, sub: Sub);
    fn submit(&mut self);
    fn poll(&mut self) -> Vec<Comp>;
}
pub enum Sub {
    Accept {},
    TcpRead {
        buf: Vec<u8>,
        conn_id: crate::ConnId,
    },
    TcpWrite {
        buf: Vec<u8>,
        conn_id: crate::ConnId,
    },
    FileRead {
        buf: Vec<u8>,
        offset: u64,
    },
    FileWrite {
        buf: Vec<u8>,
        offset: u64,
    },
}
#[derive(Clone, Debug)]
pub enum Comp {
    Accept {
        conn_id: crate::ConnId,
    },
    TcpRead {
        buf: Vec<u8>,
        conn_id: crate::ConnId,
        res: i32,
        bytes: usize,
    },
    TcpWrite {
        buf: Vec<u8>,
        conn_id: crate::ConnId,
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

pub struct Conn {
    pub id: crate::ConnId,
    pub buf: Vec<u8>,
    pub txns: collections::BTreeMap<crate::ConnTxnId, TxnQueue>,
}
impl Conn {
    pub fn new(id: crate::ConnId) -> Self {
        Self {
            id,
            buf: Vec::new(),
            txns: collections::BTreeMap::new(),
        }
    }
    pub fn write(&mut self, buf: &[u8]) {
        self.buf.extend_from_slice(buf);
    }
    pub fn tick<IO: IOFace>(&mut self, io: &mut IO, net_bufs: &mut Vec<Vec<u8>>) {
        io.register_sub(Sub::TcpRead {
            buf: net_bufs.pop().unwrap(),
            conn_id: self.id,
        });

        if self.buf.len() > 0 {
            let mut cursor = 0;
            // TODO: error handling
            // (eof means we just stop reading,
            // but we also need to handle thing like malformed requests, etc)
            while let Ok(request) = format::Request::from_bytes(&self.buf[cursor..]) {
                cursor += request.len();
                match self.txns.get_mut(&request.txn_id) {
                    Some(txn_buf) => {
                        let old_len = txn_buf.from.len();
                        txn_buf.from.resize(old_len + request.op.len(), 0);
                        request.op.write_to_buf(&mut txn_buf.from[old_len..]);
                    }
                    None => {
                        // new txn
                        let mut buf = vec![0; request.op.len()];
                        request.op.write_to_buf(&mut buf);
                        self.txns.insert(
                            request.txn_id,
                            TxnQueue {
                                from: buf,
                                from_head: 0,
                                to: Vec::new(),
                                to_head: 0,
                            },
                        );
                    }
                }
            }

            self.buf.drain(..cursor);
        }

        todo!("send responses here too");
    }
}

pub struct TxnQueue {
    from: Vec<u8>,
    from_head: usize,
    to: Vec<u8>,
    to_head: usize,
}
impl TxnQueue {
    pub fn iter(&self) -> format::OpIter<format::RequestOp> {
        format::OpIter::<format::RequestOp>::from(&self.from[self.from_head..])
    }
    pub fn push(&mut self, response: Result<format::OkOp, format::ErrOp>) {}
}

/// this is just the offset in the file of the start of the block
pub type BlockId = u64;
pub const CHUNK_LEN_SIZE: usize = mem::size_of::<u64>();
pub const PID_SIZE: usize = mem::size_of::<u64>();

pub struct StorageManager {
    pub flush_buf: FlushBuf,
    pub block_bufs: Vec<Vec<u8>>,
    pub offset_table: collections::BTreeMap<crate::PageId, Vec<u64>>,
    pub pend_blocks: PendingBlocks,
}
impl StorageManager {
    pub fn new(block_size: usize, num_block_bufs: usize, cap: u64) -> Self {
        Self {
            flush_buf: FlushBuf {
                buf: vec![0; block_size],
                off: 0,
            },
            block_bufs: Vec::from_iter((0..num_block_bufs).map(|_| vec![0; block_size])),
            offset_table: collections::BTreeMap::new(),
            pend_blocks: PendingBlocks {
                block_requests: collections::BTreeMap::new(),
                page_requests: collections::BTreeMap::new(),
                head: 0,
                tail: 0,
                cap,
                pend_block: None,
            },
        }
    }
    /// so we want to find out if we need to read anything,
    /// writing happens in line with write_chunk,
    /// triggering gc happens in write_chunk,
    /// and read_block makes progress on anything we can with the new block of data
    pub fn tick<IO: IOFace>(
        &mut self,
        io: &mut IO,
        buf_pool: &mut Vec<page::PageBuffer>,
        page_dir: &mut shard::PageDir,
    ) {
        // check the flush buffer first,
        if self
            .pend_blocks
            .block_requests
            .contains_key(&self.pend_blocks.head)
        {
            self.pend_blocks
                .read_block(self.pend_blocks.head, &self.flush_buf, buf_pool, page_dir);
        }

        // check if we need to issue any reads
        if self.pend_blocks.pend_block.is_none() && !self.pend_blocks.page_requests.is_empty() {
            let (block_id, _) = self
                .pend_blocks
                .block_requests
                .iter()
                .map(|(block_id, block_request)| {
                    (
                        block_id,
                        block_request
                            .iter()
                            .map(|pid| self.pend_blocks.page_requests.get(&pid).unwrap().prio)
                            .sum::<u32>(),
                    )
                })
                .max_by(|a, b| a.1.cmp(&b.1))
                .unwrap();

            io.register_sub(Sub::FileRead {
                buf: self.block_bufs.pop().unwrap(),
                offset: *block_id,
            });

            self.pend_blocks.pend_block = Some(*block_id);
        }
    }
    pub fn inc_prio(&mut self, pid: crate::PageId, free_list: &mut Vec<usize>) {
        match self.pend_blocks.page_requests.get_mut(&pid) {
            Some(page_request) => page_request.prio += 1,
            None => {
                let offs = self.offset_table.get(&pid).unwrap().clone();
                let block_id = (offs.last().unwrap() / self.flush_buf.cap()) * self.flush_buf.cap();

                self.pend_blocks
                    .block_requests
                    .entry(block_id)
                    .and_modify(|block_request| {
                        block_request.push(pid);
                    })
                    .or_insert(vec![pid]);

                self.pend_blocks.page_requests.insert(
                    pid,
                    PageRequest {
                        prio: 1,
                        idx: free_list.pop().unwrap(),
                        len: 0,
                        offs: offs,
                    },
                );
            }
        }
    }
    pub fn write_chunk<IO: IOFace>(
        &mut self,
        pid: crate::PageId,
        chunk: &[u8],
        io: &mut IO,
        base: bool,
    ) {
        let off =
            self.pend_blocks
                .write_chunk(pid, chunk, &mut self.flush_buf, &mut self.block_bufs, io);
        self.offset_table
            .entry(pid)
            .and_modify(|offs| {
                if base {
                    offs.clear();
                }
                offs.push(off);
            })
            .or_insert(vec![off]);
    }
    pub fn read_block(
        &mut self,
        block: &[u8],
        block_id: BlockId,
        buf_pool: &mut Vec<page::PageBuffer>,
        page_dir: &mut shard::PageDir,
    ) {
        self.pend_blocks
            .read_block(block_id, block, buf_pool, page_dir);
    }
}
pub struct FlushBuf {
    pub buf: Vec<u8>,
    pub off: usize,
}
impl FlushBuf {
    pub fn cap(&self) -> u64 {
        self.buf.len() as u64
    }
    pub fn write_chunk(&mut self, pid: crate::PageId, chunk: &[u8]) -> Result<u64, ()> {
        if self.buf.len() - self.off < PID_SIZE + CHUNK_LEN_SIZE + chunk.len() {
            return Err(());
        }

        // save the offset before we bump it
        let offset = self.off as u64;

        // write out the page chunk
        self.buf[self.off..self.off + PID_SIZE].copy_from_slice(&pid.0.to_be_bytes());
        self.off += PID_SIZE;
        self.buf[self.off..self.off + CHUNK_LEN_SIZE]
            .copy_from_slice(&(chunk.len() as u64).to_be_bytes());
        self.off += CHUNK_LEN_SIZE;
        self.buf[self.off..self.off + chunk.len()].copy_from_slice(chunk);
        self.off += chunk.len();

        Ok(offset)
    }
    pub fn swap(&mut self, new: Vec<u8>) -> Vec<u8> {
        assert_eq!(new.len(), self.buf.len());
        self.off = 0;
        mem::replace(&mut self.buf, new)
    }
}
impl Deref for FlushBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.buf[..]
    }
}
pub struct PendingBlocks {
    pub page_requests: collections::BTreeMap<crate::PageId, PageRequest>,
    pub block_requests: collections::BTreeMap<BlockId, Vec<crate::PageId>>,

    pub pend_block: Option<BlockId>,

    pub head: BlockId,
    pub tail: BlockId,
    pub cap: u64,
}
impl PendingBlocks {
    /// ## PERF
    /// right now this function only reads data that we've requested, which means gc is done in a
    /// separate pass when we read a block in from disk, it may be an optimization to try and do
    /// these in one go
    pub fn read_block(
        &mut self,
        block_id: BlockId,
        block: &[u8],
        buf_pool: &mut Vec<page::PageBuffer>,
        page_dir: &mut shard::PageDir,
    ) {
        if let Some(block_request) = self.block_requests.remove(&block_id) {
            for pid in block_request {
                let page_request = self.page_requests.get_mut(&pid).unwrap();
                let page = &mut buf_pool[page_request.idx];
                let buf = page.raw_buffer_mut();
                loop {
                    let offset = page_request.offs.pop().unwrap();
                    let mut cursor = offset as usize % block.len();
                    assert_eq!(
                        pid.0,
                        u64::from_be_bytes(block[cursor..cursor + PID_SIZE].try_into().unwrap())
                    );
                    cursor += PID_SIZE;
                    let chunk_len = u64::from_be_bytes(
                        block[cursor..cursor + CHUNK_LEN_SIZE].try_into().unwrap(),
                    ) as usize;
                    cursor += CHUNK_LEN_SIZE;

                    buf[page_request.len..page_request.len + chunk_len]
                        .copy_from_slice(&block[cursor..cursor + chunk_len]);
                    page_request.len += chunk_len;

                    match page_request.offs.last() {
                        Some(o) => {
                            if *o < block_id || *o >= block_id + block.len() as u64 {
                                // if next offset is outside of bounds of this block, tee up chunk
                                // reads for next block
                                let next_block = (*o / block.len() as u64) * block.len() as u64;
                                match self.block_requests.get_mut(&next_block) {
                                    Some(block_request) => {
                                        block_request.push(pid);
                                    }
                                    None => {
                                        self.block_requests.insert(next_block, vec![pid]);
                                    }
                                }
                                break;
                            }
                        }
                        None => {
                            // done reading page
                            buf.copy_within(0..page_request.len, buf.len() - page_request.len);
                            page.top = buf.len() - page_request.len;
                            page.flush = page.top;
                            page_dir.insert(pid, page_request.idx);
                            self.page_requests.remove(&pid);
                            break;
                        }
                    }
                }
            }

            if let Some(pend_block) = self.pend_block {
                if pend_block == block_id {
                    self.pend_block = None;
                }
            }
        }
    }

    pub fn write_chunk<IO: IOFace>(
        &mut self,
        pid: crate::PageId,
        chunk: &[u8],
        flush_buf: &mut FlushBuf,
        block_bufs: &mut Vec<Vec<u8>>,
        io: &mut IO,
    ) -> u64 {
        match flush_buf.write_chunk(pid, chunk) {
            Ok(o) => self.head + o,
            Err(_) => {
                // flush_buf is full, need to write it out
                let flush = flush_buf.swap(block_bufs.pop().unwrap());

                io.register_sub(Sub::FileWrite {
                    buf: flush,
                    offset: self.head,
                });

                self.head += flush_buf.len() as u64;
                if self.head == self.cap {
                    self.head = 0;
                }
                if self.head == self.tail {
                    panic!("head caught up to tail")
                }

                self.head + flush_buf.write_chunk(pid, chunk).unwrap()
            }
        }
    }
}

pub struct PageRequest {
    prio: u32,
    idx: usize,
    len: usize,
    offs: Vec<u64>,
}
