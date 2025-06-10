use crate::{
    PageId,
    cache::Cache,
    io,
    net_proto::{Req, parse_req},
    page::{Delta, PageBuffer, PageMut, SplitDelta},
};

use std::collections::BTreeMap;

// NOTE: for now using BTreeMap bc the iteration order is deterministic, need a better solution
// eventually though
pub struct Shard<I: IO, M: Mesh> {
    pub page_dir: PageDir,

    pub conns: BTreeMap<u32, Conn>,
    pub net_bufs: Vec<Vec<u8>>,
    pub ops: Ops,
    pub net_buf_size: usize,

    pub block_size: usize,
    pub free_cap_target: usize,
    pub pending_read: Option<PendingRead>,
    pub write_block: usize,
    pub write_offset: usize,
    pub block_bufs: Vec<Vec<u8>>,
    pub write_buf: Vec<u8>,
    pub read_prios: BTreeMap<PageId, (u32, u32)>,
    pub offset_table: BTreeMap<PageId, u64>,
    pub io: I,

    pub mesh: M,
    pub runs: usize,
}
impl<I: IO, M: Mesh> Shard<I, M> {
    pub fn new(
        page_size: usize,
        buf_pool_size: usize,
        block_size: usize,
        num_write_bufs: usize,
        num_conn_bufs: usize,
        net_buf_size: usize,
        free_cap_target: usize,
        io: I,
        mesh: M,
    ) -> Self {
        let mut page_dir = PageDir::new(page_size, buf_pool_size, 1, 2);
        let mut ops = Ops::new(page_size);

        // make root
        let root_idx = page_dir.free_list.pop().unwrap();
        ops.scratch.page_mut.pack(&mut page_dir.buf_pool[root_idx]);
        page_dir.cache.insert(page_dir.root, root_idx);

        Self {
            page_dir,

            ops,

            conns: BTreeMap::new(),
            net_bufs: Vec::from_iter((0..num_conn_bufs).map(|_| vec![0; net_buf_size])),
            net_buf_size,

            block_size,
            write_block: 0,
            write_offset: 0,
            free_cap_target,
            pending_read: None,
            block_bufs: Vec::from_iter((0..num_write_bufs).map(|_| vec![0; block_size])),
            write_buf: vec![0; block_size],
            read_prios: BTreeMap::new(),
            offset_table: BTreeMap::new(),
            io,

            mesh,
            runs: 0,
        }
    }

    // we're not resetting the offset table after compaction
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

                    self.io.register_sub(Sub::TcpRead {
                        buf: self.net_bufs.pop().unwrap(),
                        conn_id,
                    });
                }
            }
        }
        // from io
        for comp in self.io.poll() {
            match comp {
                Comp::TcpRead {
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
                        match parse_req(&conn.buf) {
                            Ok(_) => conn.op = OpState::Pending,
                            Err(()) => self.io.register_sub(Sub::TcpRead {
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
                Comp::TcpWrite {
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
                Comp::FileRead { mut buf } => {
                    let pr = self.pending_read.as_mut().unwrap();

                    let page = &mut self.page_dir.buf_pool[pr.idx];
                    let page_buf = page.raw_buffer_mut();

                    match io::read_page_from_block(
                        &buf,
                        &mut page_buf[pr.len..],
                        pr.off,
                        (pr.off / self.block_size) * self.block_size,
                    ) {
                        Ok(written) => {
                            // successfully read entire page
                            pr.len += written;
                            page_buf.copy_within(0..pr.len, page_buf.len() - pr.len);

                            // ===checking base len valid===
                            let complete_page = &page_buf[page_buf.len() - pr.len..];
                            let base_len = u64::from_be_bytes(
                                complete_page[complete_page.len() - 8..].try_into().unwrap(),
                            ) as usize;

                            if 8 + base_len > complete_page.len() {
                                panic!(
                                    "Corrupted page data read from disk for page {}: base_len {} + 8 > page_len {}",
                                    pr.pid,
                                    base_len,
                                    complete_page.len()
                                );
                            }
                            // ===

                            page.top = page_buf.len() - pr.len;
                            page.flush = page.top;

                            self.page_dir.cache.insert(pr.pid, pr.idx);
                            self.pending_read = None;
                        }
                        Err((written, next_off)) => {
                            // need to read more chunks in
                            pr.len += written;
                            pr.off = next_off;
                            let next_block_start = (next_off / self.block_size) * self.block_size;
                            self.io.register_sub(Sub::FileRead {
                                buf: self.block_bufs.pop().unwrap(),
                                offset: next_block_start as u64,
                            });
                        }
                    }

                    buf.fill(0);
                    self.block_bufs.push(buf);
                }
                Comp::FileWrite { mut buf } => {
                    buf.fill(0);
                    self.block_bufs.push(buf);
                }
            }
        }

        // find ops that can be completed in this pipeline
        for (conn_id, conn) in self.conns.iter_mut() {
            if let OpState::Pending = conn.op {
                let req = parse_req(&conn.buf).unwrap();
                let req_len = req.len();
                match req {
                    Req::Get(get_req) => match find_leaf(get_req.key, &mut self.page_dir) {
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
                            self.io.register_sub(Sub::TcpRead {
                                conn_id: *conn_id,
                                buf: self.net_bufs.pop().unwrap(),
                            });
                        }
                        Err(pid) => match &self.pending_read {
                            Some(pr) => {
                                if pr.pid != pid {
                                    match self.read_prios.get_mut(&pid) {
                                        Some((count, _)) => *count += 1,
                                        None => {
                                            self.read_prios.insert(pid, (1, *conn_id));
                                        }
                                    }
                                }
                            }
                            None => match self.read_prios.get_mut(&pid) {
                                Some((count, _)) => *count += 1,
                                None => {
                                    self.read_prios.insert(pid, (1, *conn_id));
                                }
                            },
                        },
                    },
                    Req::Set(set_req) => {
                        if let Err(pid) = find_leaf(set_req.key, &mut self.page_dir) {
                            match &self.pending_read {
                                Some(pr) => {
                                    if pr.pid != pid {
                                        match self.read_prios.get_mut(&pid) {
                                            Some((count, _)) => *count += 1,
                                            None => {
                                                self.read_prios.insert(pid, (1, *conn_id));
                                            }
                                        }
                                    }
                                }
                                None => match self.read_prios.get_mut(&pid) {
                                    Some((count, _)) => *count += 1,
                                    None => {
                                        self.read_prios.insert(pid, (1, *conn_id));
                                    }
                                },
                            }
                        } else {
                            self.ops.writes.push((*conn_id, req_len));

                            conn.op = OpState::Reading;
                            self.io.register_sub(Sub::TcpRead {
                                conn_id: *conn_id,
                                buf: self.net_bufs.pop().unwrap(),
                            });
                        }
                    }
                }
            }
        }

        // process reads
        let reads = std::mem::replace(&mut self.ops.reads, BTreeMap::new());
        let read_len = reads.len();
        for (_, group) in reads {
            let mut page = self.page_dir.buf_pool[group.page_idx].read();

            for (conn_id, req_len) in group.reads {
                let conn_buf = &mut self.conns.get_mut(&conn_id).unwrap().buf;
                let key = {
                    match parse_req(conn_buf).unwrap() {
                        Req::Get(get_req) => get_req.key,
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

                        self.io.register_sub(Sub::TcpWrite { buf, conn_id });
                    }
                    None => {
                        let mut buf = self.net_bufs.pop().unwrap();
                        buf.clear();
                        buf.push(1);

                        self.io.register_sub(Sub::TcpWrite { buf, conn_id });
                    }
                }

                conn_buf.drain(0..req_len);
            }
        }

        // process writes
        let write_len = self.ops.writes.len();
        for (conn_id, req_len) in self.ops.writes.drain(..) {
            let conn_buf = &mut self.conns.get_mut(&conn_id).unwrap().buf;
            let delta = Delta::from_top(conn_buf);

            self.ops.scratch.clear();
            apply_write(&mut self.page_dir, &mut self.ops.scratch, delta);

            let mut buf = self.net_bufs.pop().unwrap();
            buf.clear();
            buf.push(0);

            self.io.register_sub(Sub::TcpWrite { buf, conn_id });

            conn_buf.drain(0..req_len);
        }

        // evict enough to have a nice full free list
        let mut evicts = Vec::new(); // PERF:
        {
            let mut evict_cands = self.page_dir.cache.evict_cands();
            while self.page_dir.free_list.len() < self.free_cap_target {
                let (pid, idx) = evict_cands.next().unwrap();
                if self.read_prios.contains_key(&pid) {
                    continue;
                }
                // for now we'll just evict the best candidates, but we may have some constraints
                // later from things like txns that prevent us from evicting certain pages
                let page = &mut self.page_dir.buf_pool[idx];
                let flush_len = page.flush_len();

                if self.block_size - self.write_offset < flush_len + 16 {
                    // not enough room in current write buffer
                    let mut write_buf = self.block_bufs.pop().unwrap();
                    std::mem::swap(&mut write_buf, &mut self.write_buf);
                    self.io.register_sub(Sub::FileWrite {
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
                self.page_dir.free_list.push(idx);

                // // In eviction logic, right after flushing page 13113:
                // if pid == 13113 {
                //     let flushed_data =
                //         &self.write_buf[self.write_offset + 16..self.write_offset + 16 + flush_len];
                //     let base_len = u64::from_be_bytes(
                //         flushed_data[flushed_data.len() - 8..].try_into().unwrap(),
                //     ) as usize;
                //     println!(
                //         "DEBUG: Writing page 13113 - flush_len={}, base_len_in_data={}",
                //         flush_len, base_len
                //     );
                // }

                evicts.push(pid);
            }
        }
        for pid in evicts.drain(..) {
            self.page_dir.cache.remove(pid);
        }

        // tee up best page read if we can/need to
        if self.pending_read.is_none() && !self.read_prios.is_empty() {
            let best = *self
                .read_prios
                .iter()
                .max_by(|(_, a), (_, b)| a.cmp(b))
                .unwrap()
                .0;
            self.read_prios.remove(&best).unwrap();

            let offset = *self.offset_table.get(&best).unwrap() as usize;

            // // When starting to read page 13113:
            // if best == 13113 {
            //     println!("DEBUG: Starting to read page 13113 from offset {}", offset);
            // }
            let idx = self.page_dir.free_list.pop().unwrap();
            let page = &mut self.page_dir.buf_pool[idx];
            let page_buf = page.raw_buffer_mut();

            match io::read_page_from_block(
                &self.write_buf,
                &mut page_buf[..],
                offset,
                self.write_block * self.block_size,
            ) {
                Ok(written) => {
                    // successfully read entire page
                    page_buf.copy_within(0..written, page_buf.len() - written);

                    page.top = page_buf.len() - written;
                    page.flush = page.top;
                    self.page_dir.cache.insert(best, idx);
                }
                Err((written, next_off)) => {
                    // need to read more chunks in
                    let next_block_start = (next_off / self.block_size) * self.block_size;

                    self.io.register_sub(Sub::FileRead {
                        buf: self.block_bufs.pop().unwrap(),
                        offset: next_block_start as u64,
                    });
                    self.pending_read = Some(PendingRead {
                        pid: best,
                        idx,
                        len: written,
                        off: next_off,
                    });
                }
            }
        }

        // submit io
        self.io.submit();

        self.runs += 1;
        println!(
            "RAN PIPELINE {} with {} reads, {} writes, and {} pages in prio queue",
            self.runs,
            read_len,
            write_len,
            self.read_prios.len()
        );
    }
}

pub struct PageDir {
    pub buf_pool: Vec<PageBuffer>,
    pub cache: Cache,
    pub free_list: Vec<usize>,
    pub root: PageId,
    pub next_pid: PageId,
}
impl PageDir {
    pub fn new(page_size: usize, buf_pool_size: usize, root: PageId, next_pid: PageId) -> Self {
        Self {
            buf_pool: (0..buf_pool_size)
                .map(|_| PageBuffer::new(page_size))
                .collect(),
            free_list: Vec::from_iter(0..buf_pool_size),
            cache: Cache::new(buf_pool_size),
            root,
            next_pid,
        }
    }
    pub fn new_page(&mut self) -> Result<(PageId, usize), ()> {
        match self.free_list.pop() {
            Some(idx) => {
                let pid = self.next_pid;
                self.next_pid += 1;
                Ok((pid, idx))
            }
            None => Err(()),
        }
    }
}

pub struct WriteScratch {
    pub page_mut: PageMut,
    pub path: Vec<(PageId, usize)>,
}
impl WriteScratch {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_mut: PageMut::new(page_size),
            path: Vec::new(),
        }
    }
    pub fn clear(&mut self) {
        self.page_mut.clear();
        self.path.clear();
    }
}

pub struct Ops {
    pub reads: BTreeMap<PageId, ReadGroup>,
    pub writes: Vec<(u32, usize)>,
    pub scratch: WriteScratch,
}
impl Ops {
    pub fn new(page_size: usize) -> Self {
        Self {
            reads: BTreeMap::new(),
            writes: Vec::new(),
            scratch: WriteScratch::new(page_size),
        }
    }
}
pub struct ReadGroup {
    page_idx: usize,
    reads: Vec<(u32, usize)>,
}

fn find_leaf(target: &[u8], page_dir: &mut PageDir) -> Result<(PageId, usize), PageId> {
    let (mut current, mut current_id, mut current_idx) = match page_dir.cache.get(page_dir.root) {
        Some(idx) => (page_dir.buf_pool[idx].read(), page_dir.root, idx),
        None => return Err(page_dir.root),
    };

    while current.is_inner() {
        current_id = current.search_inner(target);
        current = match page_dir.cache.get(current_id) {
            Some(idx) => {
                current_idx = idx;
                page_dir.buf_pool[idx].read()
            }
            None => return Err(current_id),
        };
    }

    Ok((current_id, current_idx))
}

fn apply_write(page_dir: &mut PageDir, scratch: &mut WriteScratch, delta: Delta) {
    let key = delta.key();

    scratch
        .path
        .push((page_dir.root, page_dir.cache.get(page_dir.root).unwrap()));
    loop {
        let mut current = page_dir.buf_pool[scratch.path.last().unwrap().1].read();
        if current.is_inner() {
            let id = current.search_inner(key);
            scratch.path.push((id, page_dir.cache.get(id).unwrap()));
        } else {
            break;
        }
    }

    let (leaf_id, leaf_idx) = scratch.path.pop().unwrap();
    if !page_dir.buf_pool[leaf_idx].write_delta(&delta) {
        let new_page = &mut scratch.page_mut;
        new_page.clear();
        new_page.compact(page_dir.buf_pool[leaf_idx].read());

        if !new_page.apply_delta(&delta) {
            let mut middle_key = Vec::new();
            let mut to_page = new_page.split_leaf(&mut middle_key);

            if key <= &middle_key {
                to_page.apply_delta(&delta);
            } else {
                new_page.apply_delta(&delta);
            }

            let (to_page_id, to_page_idx) = page_dir.new_page().unwrap();
            to_page.pack(&mut page_dir.buf_pool[to_page_idx]);
            page_dir.cache.insert(to_page_id, to_page_idx);

            new_page.pack(&mut page_dir.buf_pool[leaf_idx]);

            let mut parent_delta = SplitDelta {
                middle_key: &middle_key,
                left_pid: to_page_id,
            };
            let mut right = leaf_id;

            'split: loop {
                match scratch.path.pop() {
                    Some((parent_id, parent_idx)) => {
                        if !page_dir.buf_pool[parent_idx].write_delta(&Delta::Split(parent_delta)) {
                            let new_parent = &mut scratch.page_mut;
                            new_parent.clear();
                            new_parent.compact(page_dir.buf_pool[parent_idx].read());

                            if !new_parent.apply_delta(&Delta::Split(parent_delta)) {
                                let mut temp_key = Vec::new();
                                let mut to_parent = new_parent.split_inner(&mut temp_key);

                                if &middle_key <= &temp_key {
                                    to_parent.apply_delta(&Delta::Split(parent_delta));
                                } else {
                                    new_parent.apply_delta(&Delta::Split(parent_delta));
                                }

                                let (to_parent_id, to_parent_idx) = page_dir.new_page().unwrap();
                                to_parent.pack(&mut page_dir.buf_pool[to_parent_idx]);
                                page_dir.cache.insert(to_parent_id, to_parent_idx);

                                new_parent.pack(&mut page_dir.buf_pool[parent_idx]);

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                parent_delta = SplitDelta {
                                    middle_key: &middle_key,
                                    left_pid: to_parent_id,
                                };
                                right = parent_id;

                                continue 'split;
                            } else {
                                new_parent.pack(&mut page_dir.buf_pool[parent_idx]);
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
                        new_root.apply_delta(&Delta::Split(parent_delta));

                        let (new_root_id, new_root_idx) = page_dir.new_page().unwrap();
                        page_dir.root = new_root_id;

                        new_root.pack(&mut page_dir.buf_pool[new_root_idx]);
                        page_dir.cache.insert(new_root_id, new_root_idx);

                        break 'split;
                    }
                }
            }
        } else {
            new_page.pack(&mut page_dir.buf_pool[leaf_idx]);
        }
    }
}

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
    },
    FileWrite {
        buf: Vec<u8>,
    },
}

pub struct Conn {
    buf: Vec<u8>,
    op: OpState,
}
pub enum OpState {
    Pending,
    Reading,
}
pub struct PendingRead {
    pid: PageId,
    idx: usize,
    len: usize,
    off: usize,
}

pub trait Mesh {
    fn poll(&mut self) -> Vec<Msg>;
}
#[derive(Clone)]
pub enum Msg {
    NewConn(u32),
}
