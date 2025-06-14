use crate::{PageId, page, shard};

/// a [`BlockId`] is just the offset of the start of a block in the file
pub type BlockId = u64;

pub trait IO {
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
}
pub struct GetReq<'r> {
    pub key: &'r [u8],
}
pub struct SetReq<'r> {
    pub key: &'r [u8],
    pub val: &'r [u8],
}
impl Req<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::Get(get_req) => 1 + 4 + get_req.key.len(),
            Self::Set(set_req) => 1 + 4 + 4 + set_req.key.len() + set_req.val.len(),
        }
    }
}

const GET_CODE: u8 = 1;
const SET_CODE: u8 = 2;

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
            n => panic!("{n} is not an op code"),
        },
        None => Err(()),
    }
}

pub struct BlockDir {
    pub blocks: std::collections::BTreeMap<BlockId, PendBlock>,
    pub page_prios: std::collections::BTreeMap<PageId, u32>,
    pub offset_table: std::collections::BTreeMap<PageId, Vec<u64>>,
}
impl BlockDir {
    pub fn pop_best(&mut self) -> (BlockId, PendBlock) {
        let best = *self
            .blocks
            .iter()
            .max_by(|a, b| {
                a.1.pending_pages
                    .keys()
                    .map(|k| *self.page_prios.get(k).unwrap())
                    .sum::<u32>()
                    .cmp(
                        &b.1.pending_pages
                            .keys()
                            .map(|k| *self.page_prios.get(k).unwrap())
                            .sum(),
                    )
            })
            .unwrap()
            .0;
        let block = self.blocks.remove(&best).unwrap();
        (best, block)
    }
}

pub struct PendBlock {
    pub pending_pages: std::collections::BTreeMap<PageId, PendingPage>,
}
pub struct PendingPage {
    pub offsets: Vec<u64>,
    pub len: usize,
    pub idx: usize,
}

pub const CHUNK_LEN_SIZE: usize = 8;

pub fn read_block(
    block: &[u8],
    block_id: BlockId,
    pend: PendBlock,
    block_dir: &mut BlockDir,
    buf_pool: &mut Vec<page::PageBuffer>,
    page_dir: &mut shard::PageDir,
) {
    for (pid, mut pend_page) in pend.pending_pages {
        let page = &mut buf_pool[pend_page.idx];
        let buf = page.raw_buffer_mut();

        if pid == 7135 {
            println!("reading {pid} from {block_id}");
        }
        loop {
            let offset = pend_page.offsets.pop().unwrap();
            let mut cursor = (offset - block_id) as usize;
            let chunk_len =
                u64::from_be_bytes(block[cursor..cursor + CHUNK_LEN_SIZE].try_into().unwrap())
                    as usize;
            cursor += CHUNK_LEN_SIZE;
            let chunk = &block[cursor..cursor + chunk_len];
            if pid == 7135 {
                println!(
                    "  offset: {offset}, chunk len: {chunk_len}, pend page len: {}",
                    pend_page.len
                );
            }

            buf[pend_page.len..pend_page.len + chunk.len()].copy_from_slice(chunk);
            pend_page.len += chunk.len();

            match pend_page.offsets.last() {
                Some(o) => {
                    if *o >= block_id && *o < block_id + block.len() as u64 {
                        // next page chunk still in this block
                        continue;
                    } else {
                        // next page chunk in another block, clean up the tracking
                        let b_idx = (o / block.len() as u64) * block.len() as u64;
                        if pid == 7135 {
                            println!("  next chunk in {b_idx}");
                        }
                        match block_dir.blocks.get_mut(&b_idx) {
                            Some(pend_block) => {
                                pend_block.pending_pages.insert(pid, pend_page);
                            }
                            None => {
                                block_dir.blocks.insert(
                                    b_idx,
                                    PendBlock {
                                        pending_pages: std::collections::BTreeMap::from([(
                                            pid, pend_page,
                                        )]),
                                    },
                                );
                            }
                        }
                        break;
                    }
                }
                None => {
                    if pid == 7135 {
                        println!("  finished reading {pid}");
                    }
                    // finished reading page
                    buf.copy_within(0..pend_page.len, buf.len() - pend_page.len);
                    page.top = buf.len() - pend_page.len;
                    page.flush = page.top;

                    page_dir.insert(pid, pend_page.idx);
                    block_dir.page_prios.remove(&pid).unwrap();
                    break;
                }
            }
        }
    }
}
