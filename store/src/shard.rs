use std::collections;

use crate::{
    PageId,
    format::{self, Op},
    io, mesh, page,
};

const CENTRAL_ID: usize = 0;

/// ## NOTE
/// for now using BTreeMap bc the iteration order is deterministic, need a better solution
/// eventually though
///
/// need to make sure we're keeping the ordering of txn ops, wrt to responses too
pub struct Shard<I: io::IOFace, M: mesh::Mesh> {
    // page cache
    pub page_dir: PageDir,
    pub buf_pool: Vec<page::PageBuffer>,
    pub free_list: Vec<usize>,
    pub free_cap_target: usize,
    pub page_size: usize,

    // io
    pub io: I,
    pub storage_manager: io::StorageManager,
    pub conns: collections::BTreeMap<crate::ConnId, io::Conn>,
    pub net_bufs: Vec<Vec<u8>>,
    pub net_buf_size: usize,

    // sys stuff
    pub mesh: M,
    pub runs: usize,
}
impl<I: io::IOFace, M: mesh::Mesh> Shard<I, M> {
    pub fn new(
        page_size: usize,
        buf_pool_size: usize,
        block_size: usize,
        num_block_bufs: usize,
        num_net_bufs: usize,
        net_buf_size: usize,
        free_cap_target: usize,
        block_cap: u64,
        io: I,
        mesh: M,
    ) -> Self {
        let mut page_dir = PageDir::new(buf_pool_size, crate::PageId(1), crate::PageId(2));
        let mut buf_pool: Vec<page::PageBuffer> = (0..buf_pool_size)
            .map(|_| page::PageBuffer::new(page_size))
            .collect();
        let mut free_list = Vec::from_iter(0..buf_pool_size);

        // make root
        let root_idx = free_list.pop().unwrap();
        page::PageMut::new(page_size).pack(&mut buf_pool[root_idx]);
        page_dir.insert(page_dir.root, root_idx);

        Self {
            page_dir,
            buf_pool,
            free_list,
            free_cap_target,
            page_size,

            io,
            storage_manager: io::StorageManager::new(block_size, num_block_bufs, block_cap),
            conns: collections::BTreeMap::new(),
            net_bufs: Vec::from_iter((0..num_net_bufs).map(|_| vec![0; net_buf_size])),
            net_buf_size,

            mesh,

            runs: 0,
        }
    }

    /// ## TODO
    /// - some stuff that can just be a "ticker":
    ///     - io
    ///     - mesh
    ///     - maybe op processing, although that pretty much happens once per tick
    ///     - maybe cache, for evictions
    ///     - future:
    ///         - gc
    ///
    pub fn tick(&mut self) {
        // read inputs
        //
        // from other threads
        for msgs in self.mesh.poll() {
            for msg in msgs {
                match msg {
                    mesh::Msg::NewConnection(conn_id) => {
                        self.conns.insert(conn_id, io::Conn::new(conn_id));
                    }
                    mesh::Msg::CommitResponse { txn, success } => todo!(),
                    mesh::Msg::WriteRequest { txn_id, deltas } => todo!(),
                    m => panic!("invalid mesh msg in shard: {:?}", m),
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
                        self.conns.get_mut(&conn_id).unwrap().write(&buf[..bytes]);
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

                    todo!("need to tell conns about successful writes");

                    // reset the buf and add it back to the pool
                    buf.fill(0);
                    buf.resize(self.net_buf_size, 0);
                    self.net_bufs.push(buf);
                }
                io::Comp::FileRead { mut buf, offset } => {
                    self.storage_manager.read_block(
                        &buf,
                        offset,
                        &mut self.buf_pool,
                        &mut self.page_dir,
                    );
                    buf.fill(0);
                    self.storage_manager.block_bufs.push(buf);
                }
                io::Comp::FileWrite { mut buf } => {
                    buf.fill(0);
                    self.storage_manager.block_bufs.push(buf);
                }
                _ => {}
            }
        }

        // find ops that can be completed in this pipeline
        let mut ops = Ops::new();
        {
            for (conn_id, conn) in self.conns.iter() {
                for (txn_id, txn_queue) in &conn.txns {
                    if let Some(op) = txn_queue.iter().next() {
                        match op {
                            format::RequestOp::Read(read) => {
                                match find_leaf(
                                    read.key().unwrap(),
                                    &mut self.page_dir,
                                    &mut self.buf_pool,
                                ) {
                                    Ok((pid, idx)) => match ops.reads.get_mut(&pid) {
                                        Some(read_group) => {
                                            read_group.reads.push((
                                                crate::ShardTxnId {
                                                    conn_txn_id: *txn_id,
                                                    conn_id: *conn_id,
                                                },
                                                read.to_vec(),
                                            ));
                                        }
                                        None => {
                                            ops.reads.insert(
                                                pid,
                                                ReadGroup {
                                                    page_idx: idx,
                                                    reads: vec![(
                                                        crate::ShardTxnId {
                                                            conn_txn_id: *txn_id,
                                                            conn_id: *conn_id,
                                                        },
                                                        read.to_vec(),
                                                    )],
                                                },
                                            );
                                        }
                                    },
                                    Err(pid) => {
                                        self.storage_manager.inc_prio(pid, &mut self.free_list)
                                    }
                                }
                            }
                            format::RequestOp::Write(write) => {
                                match find_leaf(
                                    write.key().unwrap(),
                                    &mut self.page_dir,
                                    &mut self.buf_pool,
                                ) {
                                    Ok(_) => {
                                        ops.writes.push((
                                            crate::ShardTxnId {
                                                conn_txn_id: *txn_id,
                                                conn_id: *conn_id,
                                            },
                                            write.to_vec(),
                                        ));
                                    }
                                    Err(pid) => {
                                        self.storage_manager.inc_prio(pid, &mut self.free_list)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // process reads
        let read_len = ops.reads.len();
        for (_, group) in ops.reads {
            let mut page = self.buf_pool[group.page_idx].read();

            for (txn_id, read) in group.reads {
                self.conns
                    .get_mut(&txn_id.conn_id)
                    .unwrap()
                    .txns
                    .get_mut(&txn_id.conn_txn_id)
                    .unwrap()
                    .push(Ok(format::OkOp::Get(page.search_leaf(
                        format::ReadOp::from_bytes(&read).unwrap().key().unwrap(),
                    ))));
            }
        }

        // process writes
        let writes = ops.writes.len();
        for (txn_id, write) in ops.writes {
            apply_write(
                &mut self.page_dir,
                &mut self.buf_pool,
                &mut self.free_list,
                format::WriteOp::from_bytes(&write).unwrap(),
                self.page_size,
            );

            self.conns
                .get_mut(&txn_id.conn_id)
                .unwrap()
                .txns
                .get_mut(&txn_id.conn_txn_id)
                .unwrap()
                .push(Ok(format::OkOp::Success));
        }

        // evict enough to have a nice full free list
        let mut evicts = Vec::new(); // PERF:
        {
            let mut evict_cands = self.page_dir.evict_cands();
            while self.free_list.len() < self.free_cap_target {
                let (pid, idx) = evict_cands.next().unwrap();
                // for now we'll just evict the best candidates, but we may have some constraints
                // later from things like txns that prevent us from evicting certain pages
                let page = &mut self.buf_pool[idx];
                let total = page.cap - page.top;
                let chunk = page.flush();
                let base = chunk.len() == total;

                self.storage_manager
                    .write_chunk(pid, chunk, &mut self.io, base);

                // cleanup
                page.clear();
                self.free_list.push(idx);
                evicts.push(pid);
            }
        }
        for pid in evicts.drain(..) {
            self.page_dir.remove(pid);
        }

        // queue up any reads that we may want to do
        self.storage_manager
            .tick(&mut self.io, &mut self.buf_pool, &mut self.page_dir);

        // submit io
        self.io.submit();

        self.runs += 1;
        println!(
            "completed pipeline {}, {read_len} reads, {writes} writes, {} queued pages",
            self.runs,
            self.storage_manager.pend_blocks.page_requests.len(),
        );
    }
}

pub struct PageDir {
    pub id_map: std::collections::BTreeMap<crate::PageId, usize>,
    pub hits: Vec<[u64; 2]>,
    pub root: crate::PageId,
    pub next_pid: crate::PageId,
    pub hit: u64,
}
impl PageDir {
    pub fn new(buf_pool_size: usize, root: crate::PageId, next_pid: crate::PageId) -> Self {
        Self {
            id_map: std::collections::BTreeMap::new(),
            hits: vec![[u64::MAX, 0]; buf_pool_size],
            root,
            next_pid,
            hit: 0,
        }
    }
    pub fn new_page_id(&mut self) -> crate::PageId {
        let pid = self.next_pid;
        self.next_pid.0 += 1;
        pid
    }
    pub fn get(&mut self, page_id: crate::PageId) -> Option<usize> {
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
    pub fn insert(&mut self, page_id: crate::PageId, idx: usize) {
        if let Some(_) = self.id_map.insert(page_id, idx) {
            panic!()
        }
        self.hits[idx][0] = self.hit;
        self.hits[idx][1] = 0;
        self.hit += 1;
    }
    pub fn remove(&mut self, page_id: crate::PageId) {
        self.id_map.remove(&page_id).unwrap();
    }
    pub fn evict_cands(&self) -> impl Iterator<Item = (crate::PageId, usize)> {
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

pub struct Ops {
    reads: collections::BTreeMap<crate::PageId, ReadGroup>,
    writes: Vec<(crate::ShardTxnId, Vec<u8>)>,
}
impl Ops {
    pub fn new() -> Self {
        Self {
            reads: collections::BTreeMap::new(),
            writes: Vec::new(),
        }
    }
}
/// a bunch of [`format::ReadOp`]s for keys in the same page
pub struct ReadGroup {
    page_idx: usize,
    reads: Vec<(crate::ShardTxnId, Vec<u8>)>,
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
    write: format::WriteOp,
    page_size: usize,
) {
    let mut path = Vec::new();
    let mut page_mut = page::PageMut::new(page_size);

    let op = format::PageOp::Write(write);
    let key = write.key().unwrap();

    path.push((page_dir.root, page_dir.get(page_dir.root).unwrap()));
    loop {
        let mut current = buf_pool[path.last().unwrap().1].read();
        if current.is_inner() {
            let id = current.search_inner(key);
            path.push((id, page_dir.get(id).unwrap()));
        } else {
            break;
        }
    }

    let (leaf_id, leaf_idx) = path.pop().unwrap();
    if !buf_pool[leaf_idx].write(&op) {
        let new_page = &mut page_mut;
        new_page.clear();
        new_page.compact(buf_pool[leaf_idx].read());

        if !new_page.apply_op(op) {
            let mut middle_key = Vec::new();
            let mut to_page = new_page.split_leaf(&mut middle_key);

            if key <= &middle_key {
                to_page.apply_op(op);
            } else {
                new_page.apply_op(op);
            }

            let to_page_id = page_dir.new_page_id();
            let to_page_idx = free_list.pop().unwrap();
            to_page.pack(&mut buf_pool[to_page_idx]);
            page_dir.insert(to_page_id, to_page_idx);

            new_page.pack(&mut buf_pool[leaf_idx]);

            let mut parent_delta = format::SplitOp {
                middle_key: format::Key(&middle_key),
                left_pid: to_page_id,
            };
            let mut right = leaf_id;

            'split: loop {
                match path.pop() {
                    Some((parent_id, parent_idx)) => {
                        if !buf_pool[parent_idx]
                            .write(&format::PageOp::SMO(format::SMOp::Split(parent_delta)))
                        {
                            let new_parent = &mut page_mut;
                            new_parent.clear();
                            new_parent.compact(buf_pool[parent_idx].read());

                            if !new_parent
                                .apply_op(format::PageOp::SMO(format::SMOp::Split(parent_delta)))
                            {
                                let mut temp_key = Vec::new();
                                let mut to_parent = new_parent.split_inner(&mut temp_key);

                                if &middle_key <= &temp_key {
                                    to_parent.apply_op(format::PageOp::SMO(format::SMOp::Split(
                                        parent_delta,
                                    )));
                                } else {
                                    new_parent.apply_op(format::PageOp::SMO(format::SMOp::Split(
                                        parent_delta,
                                    )));
                                }

                                let to_parent_id = page_dir.new_page_id();
                                let to_parent_idx = free_list.pop().unwrap();
                                to_parent.pack(&mut buf_pool[to_parent_idx]);
                                page_dir.insert(to_parent_id, to_parent_idx);

                                new_parent.pack(&mut buf_pool[parent_idx]);

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                parent_delta = format::SplitOp {
                                    middle_key: format::Key(&middle_key),
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
                        let new_root = &mut page_mut;
                        new_root.clear();
                        new_root.set_right_pid(right);
                        new_root.set_left_pid(crate::PageId(u64::MAX));
                        new_root.apply_op(format::PageOp::SMO(format::SMOp::Split(parent_delta)));

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
