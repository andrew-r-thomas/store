//! # SHARD PIPELINE
//!
//! ## PHASE 0: SETUP
//!
//! ## PHASE 1: INPUT COLLECTION
//!
//! ## PHASE 2: OP PROCESSING
//!
//! ## PHASE 3: OUTPUT SHIPPING

use crate::{
    PageId,
    cache::Cache,
    net_proto::{Req, parse_req},
    page::{Delta, PageBuffer, PageMut, SplitDelta},
};

use std::{collections::HashMap, path::Path};

pub struct Shard<io: IO> {
    pub page_dir: PageDir,

    pub ops: Ops,

    pub conns: HashMap<u32, Conn>,
    pub conn_bufs: Vec<Vec<u8>>,

    pub pending_read: Option<()>,
    pub write_bufs: Vec<Vec<u8>>,
    pub read_prios: HashMap<PageId, u32>,
    pub offset_table: HashMap<PageId, u64>,
    pub io: io,
}
impl<io: IO> Shard<io> {
    pub fn new(
        page_size: usize,
        buf_pool_size: usize,
        write_buf_size: usize,
        num_write_bufs: usize,
        num_conn_bufs: usize,
        conn_buf_size: usize,
        path: &Path,
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

            conns: HashMap::new(),
            conn_bufs: Vec::from_iter((0..num_conn_bufs).map(|_| vec![0; conn_buf_size])),

            pending_read: None,
            write_bufs: Vec::from_iter((0..num_write_bufs).map(|_| vec![0; write_buf_size])),
            read_prios: HashMap::new(),
            offset_table: HashMap::new(),
            io: io::create(path),
        }
    }

    pub fn run_pipeline(&mut self) {
        // read inputs
        for comp in self.io.poll() {
            match comp {
                Comp::TcpRead { buf, conn_id } => {
                    let conn = self.conns.get_mut(&conn_id).unwrap();
                    match conn.op {
                        OpState::Reading(b) => {
                            b.extend(buf);
                            buf.fill(0);
                            self.conn_bufs.push(buf);
                            match parse_req(&b) {
                                Ok(_) => conn.op = OpState::Pending(b),
                                Err(()) => self.io.register_sub(Sub::TcpRead {
                                    buf: self.conn_bufs.pop().unwrap(),
                                    conn_id,
                                }),
                            }
                        }
                        OpState::None => match parse_req(&buf) {
                            Ok(_) => conn.op = OpState::Pending(buf),
                            Err(()) => {
                                conn.op = OpState::Reading(buf);
                                self.io.register_sub(Sub::TcpRead {
                                    buf: self.conn_bufs.pop().unwrap(),
                                    conn_id,
                                });
                            }
                        },
                        _ => panic!(),
                    }
                }
                Comp::TcpWrite { buf } => self.conn_bufs.push(buf),
                Comp::FileRead { .. } => todo!(),
                Comp::FileWrite { .. } => todo!(),
            }
        }

        for (conn_id, conn) in self.conns.iter_mut() {
            if let OpState::Pending(buf) = conn.op {
                match parse_req(&buf).unwrap() {
                    Req::Get(get_req) => match find_leaf(get_req.key, &mut self.page_dir) {
                        Ok((pid, idx)) => match self.ops.reads.get_mut(&pid) {
                            Some(group) => {
                                group.reads.push((*conn_id, buf));
                            }
                            None => {
                                self.ops.reads.insert(
                                    pid,
                                    ReadGroup {
                                        page_idx: idx,
                                        reads: vec![(*conn_id, buf)],
                                    },
                                );
                            }
                        },
                        Err(pid) => *self.read_prios.entry(pid).or_insert(0) += 1,
                    },
                    Req::Set(set_req) => {}
                }
            }
        }

        // load up ops
        for (conn_id, mut req) in self.ops.pending.drain(..) {
            match *req.first().unwrap() {
                1 => {
                    // get
                    // would have key length parsing in here as well
                    req.drain(0..5);
                    match find_leaf(&req, &mut self.page_dir) {
                        Ok((pid, idx)) => match self.reads.get_mut(&pid) {
                            Some(group) => {
                                group.reads.push((conn_id, req));
                            }
                            None => {
                                self.reads.insert(
                                    pid,
                                    ReadGroup {
                                        page_idx: idx,
                                        reads: vec![(conn_id, req)],
                                    },
                                );
                            }
                        },
                        Err(pid) => *self.read_prios.entry(pid).or_insert(0) += 1,
                    }
                }
                2 => {
                    // set
                    if let Err(pid) = find_leaf(Delta::from_top(&req).key(), &mut self.page_dir) {
                        *self.read_prios.entry(pid).or_insert(0) += 1;
                    } else {
                        self.writes.push((conn_id, req));
                    }
                }
                _ => panic!(),
            }
        }

        // process reads
        for (_, group) in self.ops.reads.drain() {
            let mut page = self.page_dir.buf_pool[group.page_idx].read();
            for (conn_id, req) in group.reads {
                match page.search_leaf(&req) {
                    Some(val) => {
                        let mut resp = Vec::new();
                        resp.push(0);
                        resp.extend(&(val.len() as u32).to_be_bytes());
                        resp.extend(val);
                        resps.push((conn_id, resp));
                    }
                    None => resps.push((conn_id, vec![1])),
                }
            }
        }

        // process writes
        for (conn_id, buf) in self.writes.drain(..) {
            self.scratch.clear();

            let delta = Delta::from_top(&buf);
            apply_write(&mut self.page_dir, &mut self.scratch, delta);

            resps.push((conn_id, vec![0]));
        }
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
    pub reads: HashMap<PageId, ReadGroup>,
    pub writes: Vec<(u32, Vec<u8>)>,
    pub scratch: WriteScratch,
}
impl Ops {
    pub fn new(page_size: usize) -> Self {
        Self {
            reads: HashMap::new(),
            writes: Vec::new(),
            scratch: WriteScratch::new(page_size),
        }
    }
}
pub struct ReadGroup {
    page_idx: usize,
    reads: Vec<(u32, Vec<u8>)>,
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
    fn create(path: &Path) -> Self;
    fn register_sub(&mut self, sub: Sub);
    fn submit(&mut self);
    fn poll(&mut self) -> impl Iterator<Item = Comp>;
}
pub enum Sub {
    TcpRead { buf: Vec<u8>, conn_id: u32 },
    TcpWrite { buf: Vec<u8>, conn_id: u32 },
    FileRead { buf: Vec<u8>, offset: u64 },
    FileWrite { buf: Vec<u8>, offset: u64 },
}
pub enum Comp {
    TcpRead { buf: Vec<u8>, conn_id: u32 },
    TcpWrite { buf: Vec<u8> },
    FileRead { buf: Vec<u8> },
    FileWrite { buf: Vec<u8> },
}

pub struct Conn {
    op: OpState,
}
pub enum OpState {
    Pending(Vec<u8>),
    Reading(Vec<u8>),
    None,
}
