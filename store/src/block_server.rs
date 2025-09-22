use std::{
    alloc::{self, Layout},
    ops::Index,
    pin::Pin,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
};

use crate::format::{Entries, Format, PageChunkData, PageChunkHeader};

#[derive(Clone)]
pub struct PageServer {
    pub block_pool: Arc<BlockPool>,
    pub levels: Arc<RCUVec<Level, 1024>>,
    pub bg_chan: mpsc::Sender<BgMsg>,
}
impl PageServer {
    pub fn get_page(&self, pid: u64, level: usize) -> PageIter<'_> {
        let level = &self.levels[level];
        let offset = level.offset_table[pid as usize].load(Ordering::Acquire);
        PageIter::new(offset, level, &self.block_pool, &self.bg_chan)
    }
}

pub struct BlockPool {
    frames: Vec<Arc<[u8]>>,
    bid_map: Vec<AtomicEntry>,
}
impl BlockPool {
    pub fn get_block(
        self: &Arc<Self>,
        block_id: BlockId,
        bg_chan: &mpsc::Sender<BgMsg>,
    ) -> BlockFuture {
        BlockFuture {
            block_pool: self.clone(),
            bg_chan: bg_chan.clone(),
            block_id,
        }
    }

    fn try_get_block(&self, block_id: BlockId) -> Option<Arc<[u8]>> {
        let hash = block_id.hash();
        let start_frame = hash as usize & (self.frames.len() - 1);
        let mut frame = start_frame;
        loop {
            let entry = &self.bid_map[frame];

            let stored_hash = entry.hash.load(Ordering::Acquire);
            if stored_hash == 0 {
                return None;
            }
            if stored_hash == hash {
                return Some(self.frames[entry.frame.load(Ordering::Acquire)].clone());
            }

            frame = (frame + 1) & (self.frames.len() - 1);
            if frame == start_frame {
                return None;
            }
        }
    }

    /// ## Safety
    /// must be called only from a single thread
    pub unsafe fn set_mapping(&self, block_id: BlockId, frame: usize) {
        let hash = block_id.hash();
        let start_frame = hash as usize & (self.frames.len() - 1);
        let mut curr_frame = start_frame;
        loop {
            let entry = &self.bid_map[curr_frame];

            let stored_hash = entry.hash.load(Ordering::Relaxed);
            if stored_hash == 0 {
                entry.frame.store(frame, Ordering::Release);
                entry.hash.store(hash, Ordering::Release);
                return;
            }

            curr_frame = (curr_frame + 1) & (self.frames.len() - 1);
            assert!(curr_frame != start_frame);
        }
    }

    pub fn block_size(&self) -> usize {
        self.frames[0].len()
    }
}

pub struct BlockFuture {
    block_pool: Arc<BlockPool>,
    bg_chan: mpsc::Sender<BgMsg>,
    block_id: BlockId,
}
impl Future for BlockFuture {
    type Output = Arc<[u8]>;
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        match self.block_pool.try_get_block(self.block_id) {
            Some(block) => Poll::Ready(block),
            None => {
                self.bg_chan
                    .send(BgMsg::BlockReq {
                        block_id: self.block_id,
                        waker: ctx.waker().clone(),
                    })
                    .unwrap();
                Poll::Pending
            }
        }
    }
}

struct AtomicEntry {
    hash: AtomicU64,
    frame: AtomicUsize,
}

#[derive(Clone, Copy)]
pub struct BlockId {
    level: u8,
    offset: u64,
}
impl BlockId {
    pub fn hash(&self) -> u64 {
        self.offset + self.level as u64
    }
}

pub struct Level {
    offset_table: RCUVec<AtomicU64, 1024>,
    level: u8,
}

pub struct PageIter<'p> {
    next_off: Option<u64>,
    level: &'p Level,
    block_pool: &'p Arc<BlockPool>,
    bg_chan: &'p mpsc::Sender<BgMsg>,
}
impl<'p> PageIter<'p> {
    pub fn new(
        offset: u64,
        level: &'p Level,
        block_pool: &'p Arc<BlockPool>,
        bg_chan: &'p mpsc::Sender<BgMsg>,
    ) -> Self {
        Self {
            next_off: Some(offset),
            level,
            block_pool,
            bg_chan,
        }
    }
    pub async fn next(&mut self) -> Result<Option<PageChunkRef>, ()> {
        let off = match self.next_off {
            Some(o) => o,
            None => return Ok(None),
        };

        let block_size = self.block_pool.block_size() as u64;
        let block_id = BlockId {
            level: self.level.level,
            offset: off & !(block_size - 1),
        };
        let block = self.block_pool.get_block(block_id, self.bg_chan).await;

        let start = (off - block_id.offset) as usize;
        let chunk_ref = PageChunkRef::new(block, start);

        self.next_off = chunk_ref.header.next;
        self.bg_chan.send(BgMsg::CacheHit { block_id }).unwrap();

        Ok(Some(chunk_ref))
    }
}

pub struct PageChunkRef {
    block: Arc<[u8]>,
    start: usize,
    header: PageChunkHeader,
}
impl PageChunkRef {
    pub fn new(block: Arc<[u8]>, start: usize) -> Self {
        Self {
            header: PageChunkHeader::from_bytes(&block[start..]),
            block,
            start,
        }
    }
    pub fn data<'p, E: Entries<'p>>(&'p self) -> PageChunkData<'p, E> {
        PageChunkData::from_bytes(&self.block[self.start + self.header.size()..])
    }
}

pub enum BgMsg {
    CacheHit { block_id: BlockId },
    BlockReq { block_id: BlockId, waker: Waker },
}

pub struct RCUVec<T, const BLOCK_SIZE: usize> {
    ptr: AtomicPtr<Arc<[NonNull<T>]>>,
    len: AtomicUsize,
    cap: AtomicUsize,
}
impl<T, const BLOCK_SIZE: usize> RCUVec<T, BLOCK_SIZE> {
    pub fn new(num_blocks: usize) -> Self {
        let mut ptrs = Arc::new_uninit_slice(num_blocks);
        let ptrs_data = Arc::get_mut(&mut ptrs).unwrap();

        let layout = Layout::array::<T>(BLOCK_SIZE).unwrap();
        for i in 0..num_blocks {
            let block = unsafe { alloc::alloc(layout) } as *mut T;
            ptrs_data[i].write(NonNull::new(block).unwrap());
        }
        let ptrs = unsafe { ptrs.assume_init() };

        Self {
            ptr: AtomicPtr::new(Box::into_raw(Box::new(ptrs))),
            len: AtomicUsize::new(0),
            cap: AtomicUsize::new(num_blocks * BLOCK_SIZE),
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// ## Safety
    /// should only be called by one thread at a time
    pub unsafe fn push(&mut self, item: T) {
        let len = self.len.load(Ordering::Relaxed);
        let cap = self.cap.load(Ordering::Relaxed);
        if len == cap {
            unsafe { self.expand() };
        }
        let block_idx = len >> BLOCK_SIZE.trailing_zeros();
        let item_idx = len & (BLOCK_SIZE - 1);
        unsafe {
            self.ptr.load(Ordering::Relaxed).as_ref().unwrap()[block_idx]
                .add(item_idx)
                .write(item)
        };
        self.len.store(len + 1, Ordering::Release);
    }

    unsafe fn expand(&self) {
        let old_ptrs = unsafe { Box::from_raw(self.ptr.load(Ordering::Relaxed)) };
        let mut new_ptrs = Arc::new_uninit_slice(old_ptrs.len() * 2);
        let new_ptrs_data = Arc::get_mut(&mut new_ptrs).unwrap();

        for i in 0..old_ptrs.len() {
            new_ptrs_data[i].write(old_ptrs[i]);
        }

        let layout = Layout::array::<T>(BLOCK_SIZE).unwrap();
        for i in old_ptrs.len()..old_ptrs.len() * 2 {
            let block = unsafe { alloc::alloc(layout) } as *mut T;
            new_ptrs_data[i].write(NonNull::new(block).unwrap());
        }
        let new_ptrs = unsafe { new_ptrs.assume_init() };

        self.ptr
            .store(Box::into_raw(Box::new(new_ptrs)), Ordering::Release);
        self.cap
            .store(old_ptrs.len() * BLOCK_SIZE * 2, Ordering::Release);
    }
}
impl<T, const BLOCK_SIZE: usize> Index<usize> for RCUVec<T, BLOCK_SIZE> {
    type Output = T;
    fn index(&self, index: usize) -> &Self::Output {
        let len = self.len.load(Ordering::Acquire);
        assert!(index < len);
        let block_idx = index >> BLOCK_SIZE.trailing_zeros();
        let item_idx = index & (BLOCK_SIZE - 1);
        unsafe {
            &self.ptr.load(Ordering::Acquire).as_ref().unwrap().clone()[block_idx]
                .add(item_idx)
                .as_ref()
        }
    }
}
impl<T, const BLOCK_SIZE: usize> Drop for RCUVec<T, BLOCK_SIZE> {
    fn drop(&mut self) {
        todo!()
    }
}
