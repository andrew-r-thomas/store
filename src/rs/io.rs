use crate::format::{ChainChunk, ChunkLoc, Commit, Entries, Format, Iter, Serialize, Smop, Write};

use std::{
    collections::HashMap,
    fs::File,
    marker::PhantomData,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::{
    self,
    sync::{mpsc, oneshot},
};

pub struct IoCoordinator {
    io_chan: mpsc::Receiver<IOReq>,

    block_pool: BlockPool,

    pending_io: HashMap<u64, PendingIO>,
    pending_reads: HashMap<BlockId, u64>,
    next_ud: u64,
}
impl IoCoordinator {
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(req) = self.io_chan.recv() => {
                    self.process_req(req);
                    self.tick();
                }
                Ok(_guard) = self.uring_ready.readable() => self.tick(),
            }
        }
    }
    fn tick(&mut self) {
        // process requests
        while let Ok(req) = self.io_chan.try_recv() {
            self.process_req(req);
        }

        // process completions
        for entry in self.uring.completion() {
            let ud = entry.user_data();
            match self.pending_io.remove(&ud).unwrap() {
                PendingIO::Read(pending_read) => {
                    assert_eq!(self.block_pool.block_size(), entry.result() as usize);
                    self.pending_reads.remove(&pending_read.block_id).unwrap();

                    self.block_pool
                        .cache(pending_read.block_id, pending_read.buf_idx);
                    let bytes = self
                        .block_pool
                        .try_get_cached(pending_read.block_id)
                        .unwrap();

                    for resp_send in pending_read.resp_sends {
                        resp_send.send(bytes.clone()).unwrap();
                    }
                }

                PendingIO::Write(_) => todo!(),
            }
        }

        if !self.uring.submission().is_empty() {
            // submit io
            self.uring.submit().unwrap();
        }
    }

    fn process_req(&mut self, req: IOReq) {
        match req {
            IOReq::ReadBlock(read_req) => {
                // check if block was put in cache since request was sent
                if let Some(bytes) = self.block_pool.try_get_cached(read_req.block_id) {
                    read_req.resp_send.send(bytes).unwrap();
                    return;
                }

                // check if we're already reading the block
                if let Some(ud) = self.pending_reads.get(&read_req.block_id) {
                    match self.pending_io.get_mut(&ud).unwrap() {
                        PendingIO::Read(pending_read) => {
                            pending_read.resp_sends.push(read_req.resp_send);
                        }
                        _ => panic!(),
                    }
                    return;
                }

                // issue the read for the block
                let buf_idx = self.block_pool.pop_free().unwrap();
                let ud = self.new_ud();
                let read = opcode::ReadFixed::new(
                    self.level_fds[read_req.block_id.level as usize],
                    self.block_pool.bufs[buf_idx].as_ptr() as *mut u8,
                    self.block_pool.block_size() as u32,
                    buf_idx as u16,
                )
                .offset(read_req.block_id.offset(self.block_pool.block_size()))
                .build()
                .user_data(ud);

                self.pending_io.insert(
                    ud,
                    PendingIO::Read(PendingRead {
                        block_id: read_req.block_id,
                        buf_idx,
                        resp_sends: vec![read_req.resp_send],
                    }),
                );
                self.pending_reads.insert(read_req.block_id, ud);
                unsafe {
                    self.uring.submission().push(&read).unwrap();
                }
            }
            IOReq::WriteChunk(_) => todo!(),
            IOReq::AppendWal(_) => todo!(),
        }
    }

    #[inline]
    fn new_ud(&mut self) -> u64 {
        let out = self.next_ud;
        self.next_ud += 1;
        out
    }
}

pub enum IOReq {
    ReadBlock(ReadBlockReq),
    WriteChunk(WriteChunkReq),
    AppendWal(AppendWalReq),
}
pub struct ReadBlockReq {
    pub block_id: BlockId,
    pub resp_send: oneshot::Sender<Bytes>,
}
pub struct WriteChunkReq {
    pub level: u8,
    pub chunk: Box<dyn Serialize + Send>,
    pub resp_send: oneshot::Sender<u64>,
}
pub struct AppendWalReq {
    pub commit_ts: u64,
    pub write_batch: Vec<Write>,
    pub resp_send: oneshot::Sender<()>,
}

enum PendingIO {
    Read(PendingRead),
    Write(PendingWrite),
}
struct PendingRead {
    buf_idx: usize,
    block_id: BlockId,
    resp_sends: Vec<oneshot::Sender<Bytes>>,
}
struct PendingWrite {}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct BlockId {
    pub level: u8,
    pub block_num: u32,
}
pub struct CachedBlock {
    hot: AtomicBool,
    buf_idx: usize,
    buf: Weak<[u8]>,
}
impl CachedBlock {
    pub fn hit(&self) -> Bytes {
        self.hot.store(true, Ordering::Release);
        Bytes::from_owner(self.buf.upgrade().unwrap())
    }
}
impl BlockId {
    pub fn offset(&self, block_size: usize) -> u64 {
        (block_size * self.block_num as usize) as u64
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct PageId {
    pub pid: u64,
    pub level: u8,
}
impl BlockPool {
    fn cache(&mut self, block_id: BlockId, buf_idx: usize) {
        let buf = Arc::downgrade(&self.bufs[buf_idx]);
        let prev = self.cached_blocks.insert(
            block_id,
            CachedBlock {
                hot: AtomicBool::new(true),
                buf_idx,
                buf,
            },
        );
        assert!(prev.is_none());
        self.is_cached[buf_idx] = Some(block_id);
    }
    fn try_get_cached(&self, block_id: BlockId) -> Option<Bytes> {
        match self.cached_blocks.get(&block_id) {
            Some(entry) => {
                let block = entry.value();
                Some(block.hit())
            }
            None => None,
        }
    }
    fn pop_free(&mut self) -> Result<usize, ()> {
        if let Some(idx) = self.free_list.pop() {
            return Ok(idx);
        }

        let mut num_attempts = 0;
        while num_attempts < self.num_blocks() * 2 {
            self.clock_hand += 1;
            if self.clock_hand > self.num_blocks() {
                self.clock_hand = 0;
            }
            let Some(block_id) = self.is_cached[self.clock_hand] else {
                num_attempts += 1;
                continue;
            };

            let (_, cached_block) = self.cached_blocks.remove(&block_id).unwrap();
            if cached_block.hot.load(Ordering::Acquire) {
                // hot
                cached_block.hot.store(false, Ordering::Release);
                self.cached_blocks.insert(block_id, cached_block);
                num_attempts += 1;
                continue;
            }
            if cached_block.buf.strong_count() > 1 {
                // pinned
                self.cached_blocks.insert(block_id, cached_block);
                num_attempts += 1;
                continue;
            }

            self.is_cached[self.clock_hand] = None;
            return Ok(cached_block.buf_idx);
        }

        return Err(());
    }

    #[inline]
    fn block_size(&self) -> usize {
        self.bufs[0].len()
    }
    #[inline]
    fn num_blocks(&self) -> usize {
        self.bufs.len()
    }
}

pub struct BlockPool {
    cached_blocks: DashMap<BlockId, usize>,
    bufs: Vec<Frame>,
}
pub struct Frame {
    hot: AtomicBool,
    buf: Arc<[u8]>,
}
impl Frame {
    fn hit(&self) -> Bytes {
        self.hot.store(true, Ordering::Release);
        Bytes::from_owner(self.buf.clone())
    }
}
impl BlockPool {
    fn get_block(&self, block_id: BlockId) -> Option<Bytes> {
        match self.cached_blocks.get(&block_id) {
            Some(entry) => Some(self.bufs[*entry.value()].hit()),
            None => None,
        }
    }
    fn evict(&self) -> Result<usize, ()> {
        todo!()
    }
}

#[derive(Clone)]
pub struct PageServer {
    block_pool: Arc<BlockPool>,
    levels: Arc<DashMap<u8, Level>>,
}
impl PageServer {
    pub async fn get_block(&self, block_id: BlockId) -> Bytes {
        match self.try_get_block(block_id) {
            Some(bytes) => bytes,
            None => {
                let (resp_send, resp_recv) = oneshot::channel();
                self.io_chan
                    .send(IOReq::ReadBlock(ReadBlockReq {
                        block_id,
                        resp_send,
                    }))
                    .await
                    .unwrap();
                resp_recv.await.unwrap()
            }
        }
    }
    pub fn try_get_block(&self, block_id: BlockId) -> Option<Bytes> {
        match self.cached_blocks.get(&block_id) {
            Some(entry) => {
                let block = entry.value();
                Some(block.hit())
            }
            None => None,
        }
    }

    pub fn num_levels(&self) -> usize {
        self.levels.len()
    }

    pub fn new_page(&self, level: u8) -> u64 {
        self.levels
            .get(&level)
            .unwrap()
            .next_pid
            .fetch_add(1, Ordering::AcqRel)
    }

    pub fn get_commits(&self, page_id: PageId) -> Chain<Commit> {
        let loc = self
            .levels
            .get(&page_id.level)
            .unwrap()
            .offset_table
            .get(&page_id.pid)
            .unwrap()
            .commits;
        Chain::new(page_id.level, loc, self)
    }
    pub fn get_smops(&self, page_id: PageId) -> Chain<Smop> {
        assert_ne!(page_id.level, 0); // leaf pages don't have smops
        let loc = self
            .levels
            .get(&page_id.level)
            .unwrap()
            .offset_table
            .get(&page_id.pid)
            .unwrap()
            .smops;
        Chain::new(page_id.level, loc, self)
    }
    pub async fn get_entries<E: Entries>(&self, page_id: PageId) -> E {
        let chunk_loc = self
            .levels
            .get(&page_id.level)
            .unwrap()
            .offset_table
            .get(&page_id.pid)
            .unwrap()
            .entries;
        let block = self
            .get_block(BlockId {
                block_num: chunk_loc.block_num,
                level: page_id.level,
            })
            .await;
        E::from(block.slice(chunk_loc.block_off as usize..))
    }
}

pub struct Level {
    offset_table: DashMap<u64, PageLoc>,
    next_pid: AtomicU64,
    file: File,
}
pub struct PageLoc {
    commits: Option<ChunkLoc>,
    smops: Option<ChunkLoc>,
    entries: ChunkLoc,
}

pub struct Chain<'c, F: Format> {
    level: u8,
    chunks: Vec<Iter<F>>,
    current_chunk: usize,
    next_loc: Option<ChunkLoc>,
    io_handle: &'c PageServer,
    _ph: PhantomData<F>,
}
impl<'c, F: Format> Chain<'c, F> {
    pub fn new(level: u8, next_loc: Option<ChunkLoc>, io_handle: &'c PageServer) -> Self {
        Self {
            level,
            next_loc,
            chunks: Vec::new(),
            current_chunk: 0,
            io_handle,
            _ph: PhantomData,
        }
    }
    pub async fn next(&mut self) -> Option<F> {
        if self.current_chunk < self.chunks.len() {}
        match self.next_loc {
            Some(loc) => {
                let block = self
                    .io_handle
                    .get_block(BlockId {
                        level: self.level,
                        block_num: loc.block_num,
                    })
                    .await;
                let chunk = ChainChunk::from(block.slice(loc.block_off as usize..));
                self.next_loc = chunk.next;
                Some(Iter::from(chunk.data))
            }
            None => None,
        }
    }
}
