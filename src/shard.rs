use crate::{PageId, io, page};

// NOTE: for now using BTreeMap bc the iteration order is deterministic, need a better solution
// eventually though
//
// TODO: probably gonna try to track *all* offsets for a page in the offset table, should make gc
// wayyyyyy faster, and more simple, lose a bit of memory, but proabably not a huge deal
pub struct Shard<I: io::IO, M: Mesh> {
    // page cache
    pub page_dir: PageDir,
    pub buf_pool: Vec<page::PageBuffer>,
    pub free_list: Vec<usize>,

    // === io ===
    pub io: I,
    // disk
    pub offset_table: std::collections::BTreeMap<PageId, u64>,
    pub pend_blocks: io::PendBlocks,
    pub pend_block: Option<io::PendBlock>,
    pub write_buf: Vec<u8>,
    pub block_bufs: Vec<Vec<u8>>,
    pub write_offset: usize,
    pub write_block: usize,
    pub block_size: usize,
    pub free_cap_target: usize,
    // network
    pub conns: std::collections::BTreeMap<u32, Conn>,
    pub net_bufs: Vec<Vec<u8>>,
    pub net_buf_size: usize,

    // sys stuff
    pub mesh: M,

    // scratch space (per pipeline)
    pub ops: Ops,

    pub runs: usize,
}
impl<I: io::IO, M: Mesh> Shard<I, M> {
    pub fn new(
        page_size: usize,
        buf_pool_size: usize,
        block_size: usize,
        num_block_bufs: usize,
        num_net_bufs: usize,
        net_buf_size: usize,
        free_cap_target: usize,
        io: I,
        mesh: M,
    ) -> Self {
        let mut page_dir = PageDir::new(buf_pool_size, 1, 2);
        let mut ops = Ops::new(page_size);
        let mut buf_pool: Vec<page::PageBuffer> = (0..buf_pool_size)
            .map(|_| page::PageBuffer::new(page_size))
            .collect();
        let mut free_list = Vec::from_iter(0..buf_pool_size);

        // make root
        let root_idx = free_list.pop().unwrap();
        ops.scratch.page_mut.pack(&mut buf_pool[root_idx]);
        page_dir.insert(page_dir.root, root_idx);

        Self {
            page_dir,
            buf_pool,
            free_list,

            io,

            offset_table: std::collections::BTreeMap::new(),
            pend_blocks: io::PendBlocks {
                blocks: std::collections::BTreeMap::new(),
                page_prios: std::collections::BTreeMap::new(),
            },
            pend_block: None,
            write_buf: vec![0; block_size],
            block_bufs: Vec::from_iter((0..num_block_bufs).map(|_| vec![0; block_size])),
            write_block: 0,
            write_offset: 0,
            block_size,
            free_cap_target,

            conns: std::collections::BTreeMap::new(),
            net_bufs: Vec::from_iter((0..num_net_bufs).map(|_| vec![0; net_buf_size])),
            net_buf_size,

            mesh,

            ops,

            runs: 0,
        }
    }

    pub fn run_pipeline(&mut self) {
        // read inputs
        //
        // from other threads
        for msg in self.mesh.poll() {
            match msg {
                Msg::NewConn(conn_id) => {
                    let mut buf = self.net_bufs.pop().unwrap();
                    buf.clear();

                    self.conns.insert(
                        conn_id,
                        Conn {
                            buf,
                            op: OpState::Reading,
                        },
                    );

                    self.io.register_sub(io::Sub::TcpRead {
                        buf: self.net_bufs.pop().unwrap(),
                        conn_id,
                    });
                }
            }
        }
        // from io
        for comp in self.io.poll() {
            match comp {
                io::Comp::TcpRead {
                    mut buf,
                    conn_id,
                    res,
                    bytes,
                } => {
                    if res <= 0 {
                        // error, client disconnected
                        self.conns.remove(&conn_id).unwrap();
                    } else {
                        let conn = self.conns.get_mut(&conn_id).unwrap();
                        if let OpState::Pending = conn.op {
                            panic!()
                        }

                        conn.buf.extend(&buf[..bytes]);
                        match io::parse_req(&conn.buf) {
                            Ok(_) => conn.op = OpState::Pending,
                            Err(()) => self.io.register_sub(io::Sub::TcpRead {
                                buf: self.net_bufs.pop().unwrap(),
                                conn_id,
                            }),
                        }
                    }

                    // reset the buf and add it back to the pool
                    buf.fill(0);
                    buf.resize(self.net_buf_size, 0);
                    self.net_bufs.push(buf);
                }
                io::Comp::TcpWrite {
                    mut buf,
                    conn_id,
                    res,
                } => {
                    if res <= 0 {
                        // error, client disconnected
                        self.conns.remove(&conn_id).unwrap();
                    }

                    // reset the buf and add it back to the pool
                    buf.fill(0);
                    buf.resize(self.net_buf_size, 0);
                    self.net_bufs.push(buf);
                }
                io::Comp::FileRead { mut buf, offset } => {
                    let pending_block = self.pend_block.take().unwrap();
                    io::read_block(
                        &buf,
                        offset,
                        pending_block,
                        &mut self.pend_blocks,
                        &mut self.buf_pool,
                        &mut self.page_dir,
                    );

                    buf.fill(0);
                    self.block_bufs.push(buf);
                }
                io::Comp::FileWrite { mut buf } => {
                    buf.fill(0);
                    self.block_bufs.push(buf);
                }
            }
        }

        // find ops that can be completed in this pipeline
        for (conn_id, conn) in self.conns.iter_mut() {
            if let OpState::Pending = conn.op {
                let req = io::parse_req(&conn.buf).unwrap();
                let req_len = req.len();
                match req {
                    io::Req::Get(get_req) => {
                        match find_leaf(get_req.key, &mut self.page_dir, &mut self.buf_pool) {
                            Ok((pid, idx)) => {
                                match self.ops.reads.get_mut(&pid) {
                                    Some(group) => {
                                        group.reads.push((*conn_id, req_len));
                                    }
                                    None => {
                                        self.ops.reads.insert(
                                            pid,
                                            ReadGroup {
                                                page_idx: idx,
                                                reads: vec![(*conn_id, req_len)],
                                            },
                                        );
                                    }
                                }

                                conn.op = OpState::Reading;
                                self.io.register_sub(io::Sub::TcpRead {
                                    conn_id: *conn_id,
                                    buf: self.net_bufs.pop().unwrap(),
                                });
                            }
                            Err(pid) => match self.pend_blocks.page_prios.get_mut(&pid) {
                                Some(prio) => *prio += 1,
                                None => {
                                    self.pend_blocks.page_prios.insert(pid, 1);
                                    let offset = *self.offset_table.get(&pid).unwrap() as usize;
                                    let block_id =
                                        (offset / self.block_size * self.block_size) as io::BlockId;
                                    match self.pend_blocks.blocks.get_mut(&block_id) {
                                        Some(pend_block) => {
                                            pend_block.pending_pages.insert(
                                                pid,
                                                io::PendingPage {
                                                    idx: self.free_list.pop().unwrap(),
                                                    len: 0,
                                                    offset,
                                                },
                                            );
                                        }
                                        None => {
                                            self.pend_blocks.blocks.insert(
                                                block_id,
                                                io::PendBlock {
                                                    pending_pages: std::collections::BTreeMap::from(
                                                        [(
                                                            pid,
                                                            io::PendingPage {
                                                                idx: self.free_list.pop().unwrap(),
                                                                len: 0,
                                                                offset,
                                                            },
                                                        )],
                                                    ),
                                                },
                                            );
                                        }
                                    }
                                }
                            },
                        }
                    }
                    io::Req::Set(set_req) => {
                        if let Err(pid) =
                            find_leaf(set_req.key, &mut self.page_dir, &mut self.buf_pool)
                        {
                            match self.pend_blocks.page_prios.get_mut(&pid) {
                                Some(prio) => *prio += 1,
                                None => {
                                    self.pend_blocks.page_prios.insert(pid, 1);
                                    let offset = *self.offset_table.get(&pid).unwrap() as usize;
                                    let block_id =
                                        (offset / self.block_size * self.block_size) as io::BlockId;
                                    match self.pend_blocks.blocks.get_mut(&block_id) {
                                        Some(pend_block) => {
                                            pend_block.pending_pages.insert(
                                                pid,
                                                io::PendingPage {
                                                    idx: self.free_list.pop().unwrap(),
                                                    len: 0,
                                                    offset,
                                                },
                                            );
                                        }
                                        None => {
                                            self.pend_blocks.blocks.insert(
                                                block_id,
                                                io::PendBlock {
                                                    pending_pages: std::collections::BTreeMap::from(
                                                        [(
                                                            pid,
                                                            io::PendingPage {
                                                                idx: self.free_list.pop().unwrap(),
                                                                len: 0,
                                                                offset,
                                                            },
                                                        )],
                                                    ),
                                                },
                                            );
                                        }
                                    }
                                }
                            }
                        } else {
                            self.ops.writes.push((*conn_id, req_len));

                            conn.op = OpState::Reading;
                            self.io.register_sub(io::Sub::TcpRead {
                                conn_id: *conn_id,
                                buf: self.net_bufs.pop().unwrap(),
                            });
                        }
                    }
                }
            }
        }

        // process reads
        let reads = std::mem::replace(&mut self.ops.reads, std::collections::BTreeMap::new());
        let read_len = reads.len();
        for (_, group) in reads {
            let mut page = self.buf_pool[group.page_idx].read();

            for (conn_id, req_len) in group.reads {
                let conn_buf = &mut self.conns.get_mut(&conn_id).unwrap().buf;
                let key = {
                    match io::parse_req(conn_buf).unwrap() {
                        io::Req::Get(get_req) => get_req.key,
                        _ => panic!(),
                    }
                };

                match page.search_leaf(key) {
                    Some(val) => {
                        let mut buf = self.net_bufs.pop().unwrap();
                        buf.clear();
                        buf.push(0);
                        buf.extend(&(val.len() as u32).to_be_bytes());
                        buf.extend(val);

                        self.io.register_sub(io::Sub::TcpWrite { buf, conn_id });
                    }
                    None => {
                        let mut buf = self.net_bufs.pop().unwrap();
                        buf.clear();
                        buf.push(1);

                        self.io.register_sub(io::Sub::TcpWrite { buf, conn_id });
                    }
                }

                conn_buf.drain(0..req_len);
            }
        }

        // process writes
        let writes = self.ops.writes.len();
        for (conn_id, req_len) in self.ops.writes.drain(..) {
            let conn_buf = &mut self.conns.get_mut(&conn_id).unwrap().buf;
            let delta = page::Delta::from_top(conn_buf);

            self.ops.scratch.clear();
            apply_write(
                &mut self.page_dir,
                &mut self.buf_pool,
                &mut self.free_list,
                &mut self.ops.scratch,
                delta,
            );

            let mut buf = self.net_bufs.pop().unwrap();
            buf.clear();
            buf.push(0);

            self.io.register_sub(io::Sub::TcpWrite { buf, conn_id });

            conn_buf.drain(0..req_len);
        }

        // evict enough to have a nice full free list
        let mut evicts = Vec::new(); // PERF:
        {
            let mut evict_cands = self.page_dir.evict_cands();
            while self.free_list.len() < self.free_cap_target {
                let (pid, idx) = evict_cands.next().unwrap();
                if self.pend_blocks.page_prios.contains_key(&pid) {
                    continue;
                }
                // for now we'll just evict the best candidates, but we may have some constraints
                // later from things like txns that prevent us from evicting certain pages
                let page = &mut self.buf_pool[idx];
                let flush_len = page.flush_len();

                if self.block_size - self.write_offset < flush_len + 16 {
                    // not enough room in current write buffer
                    let mut write_buf = self.block_bufs.pop().unwrap();
                    std::mem::swap(&mut write_buf, &mut self.write_buf);
                    self.io.register_sub(io::Sub::FileWrite {
                        buf: write_buf,
                        offset: (self.write_block * self.block_size) as u64,
                    });
                    self.write_block += 1;
                    self.write_offset = 0;
                }

                let prev_offset = {
                    if page.flush == page.cap {
                        // includes base page
                        u64::MAX
                    } else {
                        // just deltas
                        *self.offset_table.get(&pid).unwrap()
                    }
                };
                self.write_buf[self.write_offset..self.write_offset + 8]
                    .copy_from_slice(&prev_offset.to_be_bytes());
                self.write_buf[self.write_offset + 8..self.write_offset + 16]
                    .copy_from_slice(&(flush_len as u64).to_be_bytes());
                page.flush(
                    &mut self.write_buf[self.write_offset + 16..self.write_offset + 16 + flush_len],
                );
                self.offset_table.insert(
                    pid,
                    ((self.write_block * self.block_size) + self.write_offset) as u64,
                );
                self.write_offset += flush_len + 16;
                page.clear();
                self.free_list.push(idx);

                evicts.push(pid);
            }
        }
        for pid in evicts.drain(..) {
            self.page_dir.remove(pid);
        }

        // read anything in the write buffer if we need to (might want to do this somewhere else)
        // TODO: gotta think about this one some more
        if let Some(pend_block) = self
            .pend_blocks
            .blocks
            .remove(&((self.write_block * self.block_size) as io::BlockId))
        {
            io::read_block(
                &self.write_buf,
                (self.write_block * self.block_size) as io::BlockId,
                pend_block,
                &mut self.pend_blocks,
                &mut self.buf_pool,
                &mut self.page_dir,
            );
        }

        // tee up best page read if we can/need to
        if self.pend_block.is_none() && !self.pend_blocks.blocks.is_empty() {
            let (offset, pend_block) = self.pend_blocks.pop_best();
            self.io.register_sub(io::Sub::FileRead {
                buf: self.block_bufs.pop().unwrap(),
                offset: offset as u64,
            });
            self.pend_block = Some(pend_block);
        }

        // submit io
        self.io.submit();

        self.runs += 1;
        println!(
            "completed pipeline {}, {read_len} reads, {writes} writes",
            self.runs,
        );
    }
}

pub struct PageDir {
    pub id_map: std::collections::BTreeMap<PageId, usize>,
    pub hits: Vec<[u64; 2]>,
    pub root: PageId,
    pub next_pid: PageId,
    pub hit: u64,
}
impl PageDir {
    pub fn new(buf_pool_size: usize, root: PageId, next_pid: PageId) -> Self {
        Self {
            id_map: std::collections::BTreeMap::new(),
            hits: vec![[u64::MAX, 0]; buf_pool_size],
            root,
            next_pid,
            hit: 0,
        }
    }
    pub fn new_page_id(&mut self) -> PageId {
        let pid = self.next_pid;
        self.next_pid += 1;
        pid
    }
    pub fn get(&mut self, page_id: PageId) -> Option<usize> {
        match self.id_map.get(&page_id) {
            Some(idx) => {
                self.hits[*idx][1] = self.hits[*idx][0];
                self.hits[*idx][0] = self.hit;
                self.hit += 1;
                Some(*idx)
            }
            None => None,
        }
    }
    pub fn insert(&mut self, page_id: PageId, idx: usize) {
        if let Some(_) = self.id_map.insert(page_id, idx) {
            panic!()
        }
        self.hits[idx][0] = self.hit;
        self.hits[idx][1] = 0;
        self.hit += 1;
    }
    pub fn remove(&mut self, page_id: PageId) {
        self.id_map.remove(&page_id).unwrap();
    }
    pub fn evict_cands(&self) -> impl Iterator<Item = (PageId, usize)> {
        itertools::Itertools::sorted_by(
            self.id_map
                .iter()
                .map(|(pid, idx)| (self.hits[*idx][0] - self.hits[*idx][1], (*pid, *idx))),
            |a, b| a.0.cmp(&b.0),
        )
        .rev()
        .map(|(_, out)| out)
    }
}

pub struct WriteScratch {
    pub page_mut: page::PageMut,
    pub path: Vec<(PageId, usize)>,
}
impl WriteScratch {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_mut: page::PageMut::new(page_size),
            path: Vec::new(),
        }
    }
    pub fn clear(&mut self) {
        self.page_mut.clear();
        self.path.clear();
    }
}

pub struct Ops {
    pub reads: std::collections::BTreeMap<PageId, ReadGroup>,
    pub writes: Vec<(u32, usize)>,
    pub scratch: WriteScratch,
}
impl Ops {
    pub fn new(page_size: usize) -> Self {
        Self {
            reads: std::collections::BTreeMap::new(),
            writes: Vec::new(),
            scratch: WriteScratch::new(page_size),
        }
    }
}
pub struct ReadGroup {
    page_idx: usize,
    reads: Vec<(u32, usize)>,
}

fn find_leaf(
    target: &[u8],
    page_dir: &mut PageDir,
    buf_pool: &Vec<page::PageBuffer>,
) -> Result<(PageId, usize), PageId> {
    let (mut current, mut current_id, mut current_idx) = match page_dir.get(page_dir.root) {
        Some(idx) => (buf_pool[idx].read(), page_dir.root, idx),
        None => return Err(page_dir.root),
    };

    while current.is_inner() {
        current_id = current.search_inner(target);
        current = match page_dir.get(current_id) {
            Some(idx) => {
                current_idx = idx;
                buf_pool[idx].read()
            }
            None => return Err(current_id),
        };
    }

    Ok((current_id, current_idx))
}

pub fn apply_write(
    page_dir: &mut PageDir,
    buf_pool: &mut Vec<page::PageBuffer>,
    free_list: &mut Vec<usize>,
    scratch: &mut WriteScratch,
    delta: page::Delta,
) {
    let key = delta.key();

    scratch
        .path
        .push((page_dir.root, page_dir.get(page_dir.root).unwrap()));
    loop {
        let mut current = buf_pool[scratch.path.last().unwrap().1].read();
        if current.is_inner() {
            let id = current.search_inner(key);
            scratch.path.push((id, page_dir.get(id).unwrap()));
        } else {
            break;
        }
    }

    let (leaf_id, leaf_idx) = scratch.path.pop().unwrap();
    if !buf_pool[leaf_idx].write_delta(&delta) {
        let new_page = &mut scratch.page_mut;
        new_page.clear();
        new_page.compact(buf_pool[leaf_idx].read());

        if !new_page.apply_delta(&delta) {
            let mut middle_key = Vec::new();
            let mut to_page = new_page.split_leaf(&mut middle_key);

            if key <= &middle_key {
                to_page.apply_delta(&delta);
            } else {
                new_page.apply_delta(&delta);
            }

            let to_page_id = page_dir.new_page_id();
            let to_page_idx = free_list.pop().unwrap();
            to_page.pack(&mut buf_pool[to_page_idx]);
            page_dir.insert(to_page_id, to_page_idx);

            new_page.pack(&mut buf_pool[leaf_idx]);

            let mut parent_delta = page::SplitDelta {
                middle_key: &middle_key,
                left_pid: to_page_id,
            };
            let mut right = leaf_id;

            'split: loop {
                match scratch.path.pop() {
                    Some((parent_id, parent_idx)) => {
                        if !buf_pool[parent_idx].write_delta(&page::Delta::Split(parent_delta)) {
                            let new_parent = &mut scratch.page_mut;
                            new_parent.clear();
                            new_parent.compact(buf_pool[parent_idx].read());

                            if !new_parent.apply_delta(&page::Delta::Split(parent_delta)) {
                                let mut temp_key = Vec::new();
                                let mut to_parent = new_parent.split_inner(&mut temp_key);

                                if &middle_key <= &temp_key {
                                    to_parent.apply_delta(&page::Delta::Split(parent_delta));
                                } else {
                                    new_parent.apply_delta(&page::Delta::Split(parent_delta));
                                }

                                let to_parent_id = page_dir.new_page_id();
                                let to_parent_idx = free_list.pop().unwrap();
                                to_parent.pack(&mut buf_pool[to_parent_idx]);
                                page_dir.insert(to_parent_id, to_parent_idx);

                                new_parent.pack(&mut buf_pool[parent_idx]);

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                parent_delta = page::SplitDelta {
                                    middle_key: &middle_key,
                                    left_pid: to_parent_id,
                                };
                                right = parent_id;

                                continue 'split;
                            } else {
                                new_parent.pack(&mut buf_pool[parent_idx]);
                                break 'split;
                            }
                        } else {
                            break 'split;
                        }
                    }
                    None => {
                        let new_root = &mut scratch.page_mut;
                        new_root.clear();
                        new_root.set_right_pid(right);
                        new_root.set_left_pid(u64::MAX);
                        new_root.apply_delta(&page::Delta::Split(parent_delta));

                        let new_root_id = page_dir.new_page_id();
                        let new_root_idx = free_list.pop().unwrap();
                        page_dir.root = new_root_id;

                        new_root.pack(&mut buf_pool[new_root_idx]);
                        page_dir.insert(new_root_id, new_root_idx);

                        break 'split;
                    }
                }
            }
        } else {
            new_page.pack(&mut buf_pool[leaf_idx]);
        }
    }
}

pub struct Conn {
    buf: Vec<u8>,
    op: OpState,
}
pub enum OpState {
    Pending,
    Reading,
}

pub trait Mesh {
    fn poll(&mut self) -> Vec<Msg>;
}
#[derive(Clone)]
pub enum Msg {
    NewConn(u32),
}
