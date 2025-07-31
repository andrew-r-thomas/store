use std::{
    collections,
    sync::{self, atomic},
};

use crate::{
    PageId, central,
    format::{self, Format},
    io, mesh, page,
};

#[cfg(test)]
use crate::test;

const CENTRAL_ID: usize = 0;

/// ## NOTE
/// for now using BTreeMap bc the iteration order is deterministic, need a better solution
/// eventually though
///
/// need to make sure we're keeping the ordering of txn ops, wrt to responses too
pub struct Shard<I: io::IOFace> {
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
    pub txns: collections::BTreeMap<crate::ShardTxnId, Txn>,
    pub net_bufs: Vec<Vec<u8>>,
    pub net_buf_size: usize,

    // sys stuff
    pub mesh: mesh::Mesh,
    pub runs: usize,
    pub committed_ts: sync::Arc<atomic::AtomicU64>,
    pub oldest_active_ts: sync::Arc<atomic::AtomicU64>,
    pub pending_commits: collections::BTreeMap<crate::Timestamp, Vec<central::Commit>>,
}
impl<I: io::IOFace> Shard<I> {
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
        mesh: mesh::Mesh,
        committed_ts: sync::Arc<atomic::AtomicU64>,
        oldest_active_ts: sync::Arc<atomic::AtomicU64>,
    ) -> Self {
        let mut page_dir = PageDir::new(buf_pool_size, crate::PageId(1), crate::PageId(2));
        let mut buf_pool: Vec<page::PageBuffer> = (0..buf_pool_size)
            .map(|_| page::PageBuffer::new(page_size))
            .collect();
        let mut free_list = Vec::from_iter(0..buf_pool_size);

        // make root
        let root_idx = free_list.pop().unwrap();
        page::PageMut::new(page_size, crate::PageId(1)).pack(&mut buf_pool[root_idx]);
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
            txns: collections::BTreeMap::new(),
            net_bufs: Vec::from_iter((0..num_net_bufs).map(|_| vec![0; net_buf_size])),
            net_buf_size,

            mesh,

            runs: 0,
            committed_ts,
            oldest_active_ts,
            pending_commits: collections::BTreeMap::new(),
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
                        self.io.register_sub(io::Sub::TcpRead {
                            buf: self.net_bufs.pop().unwrap(),
                            conn_id,
                        });
                    }
                    mesh::Msg::CommitResponse { .. } => todo!(),
                    mesh::Msg::WriteRequest(commit) => {
                        match self.pending_commits.get_mut(&commit.commit_ts) {
                            Some(v) => v.push(commit),
                            None => {
                                self.pending_commits.insert(commit.commit_ts, vec![commit]);
                            }
                        }
                    }
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
                        self.conns.remove(&conn_id);
                    } else {
                        self.io.register_sub(io::Sub::TcpRead {
                            buf: self.net_bufs.pop().unwrap(),
                            conn_id,
                        });
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
                        self.conns.remove(&conn_id);
                    }

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

        // write anything out from conns
        for (_, conn) in self.conns.iter_mut() {
            conn.tick(&mut self.io, &mut self.net_bufs);
        }

        let tick_committed_ts = crate::Timestamp(self.committed_ts.load(atomic::Ordering::Acquire));
        let tick_oldest_active_ts =
            crate::Timestamp(self.oldest_active_ts.load(atomic::Ordering::Acquire));
        // find ops that can be completed in this pipeline
        let mut reads = collections::BTreeMap::<crate::PageId, ReadGroup>::new();
        {
            for (conn_id, conn) in self.conns.iter_mut() {
                for (txn_id, txn_queue) in &mut conn.txns {
                    let txn_id = crate::ShardTxnId {
                        conn_txn_id: *txn_id,
                        conn_id: *conn_id,
                    };
                    let txn = match self.txns.get_mut(&txn_id) {
                        Some(t) => t,
                        None => {
                            self.txns.insert(
                                txn_id,
                                Txn {
                                    start_ts: tick_committed_ts,
                                    writes: Vec::new(),
                                },
                            );
                            self.mesh
                                .push(mesh::Msg::TxnStart(tick_committed_ts), CENTRAL_ID);
                            self.txns.get_mut(&txn_id).unwrap()
                        }
                    };
                    if let Some(op) = txn_queue.iter_from().next() {
                        match op {
                            format::RequestOp::Read(read) => {
                                // first check if we can fulfill the read from the local txn writes
                                let mut locals = format::FormatIter::<'_, format::WriteOp>::from(
                                    &txn.writes[..],
                                )
                                .collect::<Vec<_>>();
                                let mut resp = None;
                                while let Some(local_write) = locals.pop() {
                                    if local_write.key() == read.key() {
                                        match local_write {
                                            format::WriteOp::Set(set) => {
                                                resp = Some(Ok(format::Resp::Get(Some(set.val))));
                                            }
                                            format::WriteOp::Del(_) => {
                                                resp = Some(Ok(format::Resp::Get(None)));
                                            }
                                        }
                                        break;
                                    }
                                }
                                if let Some(r) = resp {
                                    txn_queue.push_to(r);
                                    txn_queue.pop_from();
                                    continue;
                                }

                                // if we can't, queue it up for searching the actual index
                                match find_leaf(read.key(), &mut self.page_dir, &mut self.buf_pool)
                                {
                                    Ok((pid, idx)) => match reads.get_mut(&pid) {
                                        Some(read_group) => {
                                            read_group.reads.push((
                                                txn_id,
                                                txn.start_ts,
                                                read.to_vec(),
                                            ));
                                        }
                                        None => {
                                            reads.insert(
                                                pid,
                                                ReadGroup {
                                                    page_idx: idx,
                                                    reads: vec![(
                                                        txn_id,
                                                        txn.start_ts,
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
                                let old_len = txn.writes.len();
                                txn.writes.resize(old_len + write.len(), 0);
                                write.write_to_buf(&mut txn.writes[old_len..]);
                                txn_queue.push_to(Ok(format::Resp::Success));
                                txn_queue.pop_from();
                            }
                            format::RequestOp::Commit => {
                                txn_queue.pop_from();
                                self.mesh.push(
                                    mesh::Msg::CommitRequest {
                                        txn_id,
                                        start_ts: txn.start_ts,
                                        writes: txn.writes.clone(),
                                    },
                                    CENTRAL_ID,
                                );
                                todo!("implement commit")
                            }
                        }
                    }
                }
            }
        }

        // process reads
        let read_len = reads.len();
        for (_, group) in reads {
            let mut page = self.buf_pool[group.page_idx].read();

            for (txn_id, start_ts, read) in group.reads {
                let queue = self
                    .conns
                    .get_mut(&txn_id.conn_id)
                    .unwrap()
                    .txns
                    .get_mut(&txn_id.conn_txn_id)
                    .unwrap();
                queue.push_to(Ok(format::Resp::Get(page.search_leaf(
                    format::ReadOp::from_bytes(&read).unwrap().key(),
                    start_ts,
                ))));
                queue.pop_from();
            }
        }

        // find any commits that can be completed this tick
        // we want to make sure that we process all commits for the oldest timestamp to completion
        // before we move on to the next timestamp, we also want to process everything that we can
        // within a tick.
        //
        // for now, we'll keep this super simple and not optimized, but there will be lots of
        // refining to do later
        let mut commit_len = 0;
        if let Some((ts, commits)) = self.pending_commits.pop_first() {
            if {
                // check if we have all the pages in memory to do these writes
                let mut out = true;
                for commit in &commits {
                    for write in format::FormatIter::<format::WriteOp>::from(&commit.writes[..]) {
                        if let Err(pid) = find_leaf(write.key(), &mut self.page_dir, &self.buf_pool)
                        {
                            out = false;
                            self.storage_manager.inc_prio(pid, &mut self.free_list);
                        }
                    }
                }
                out
            } {
                commit_len = commits.len();

                // process commits
                for commit in commits {
                    for write_op in format::FormatIter::<format::WriteOp>::from(&commit.writes[..])
                    {
                        let write = format::PageWrite {
                            ts: commit.commit_ts,
                            write: write_op,
                        };
                        apply_write(
                            &mut self.page_dir,
                            &mut self.buf_pool,
                            &mut self.free_list,
                            write,
                            tick_oldest_active_ts,
                            self.page_size,
                        );
                    }

                    self.mesh.push(mesh::Msg::WriteResponse(commit), CENTRAL_ID);
                }
            } else {
                self.pending_commits.insert(ts, commits);
            }
        }

        // evict enough to have a nice full free list
        let mut evicts = Vec::new();
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
            "completed pipeline {}, {read_len} reads, {commit_len} commits",
            self.runs
        );
    }
}

pub struct Txn {
    start_ts: crate::Timestamp,
    /// these are in chronological (not reverse chron) order
    /// the oldest write is first
    writes: Vec<u8>,
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

/// a bunch of [`format::ReadOp`]s for keys in the same page
pub struct ReadGroup {
    page_idx: usize,
    reads: Vec<(crate::ShardTxnId, crate::Timestamp, Vec<u8>)>,
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
    write: format::PageWrite,
    oldest_active_ts: crate::Timestamp,
    page_size: usize,
) {
    let mut path = Vec::new();
    let mut page_mut = page::PageMut::new(page_size, crate::PageId(0));

    let op = format::PageOp::Write(write);
    let key = write.key();

    path.push((page_dir.root, page_dir.get(page_dir.root).unwrap()));
    loop {
        let (_, current_idx) = path.last().unwrap();
        let mut current = buf_pool[*current_idx].read();
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
        new_page.pid = leaf_id;
        new_page.compact(buf_pool[leaf_idx].read(), oldest_active_ts);

        if let Err(()) = new_page.write_op(op) {
            let mut middle_key = Vec::new();

            let to_page_id = page_dir.new_page_id();
            let mut to_page = new_page.split_leaf(&mut middle_key, to_page_id);

            if key <= &middle_key {
                to_page.write_op(op).unwrap();
            } else {
                new_page.write_op(op).unwrap();
            }

            let to_page_idx = free_list.pop().unwrap();
            to_page.pack(&mut buf_pool[to_page_idx]);
            page_dir.insert(to_page_id, to_page_idx);

            new_page.pack(&mut buf_pool[leaf_idx]);

            let mut parent_op = format::SplitOp {
                middle_key: &middle_key,
                left_pid: to_page_id,
            };
            let mut right = leaf_id;

            'split: loop {
                match path.pop() {
                    Some((parent_id, parent_idx)) => {
                        if !buf_pool[parent_idx]
                            .write(&format::PageOp::SMO(format::SMOp::Split(parent_op)))
                        {
                            let new_parent = &mut page_mut;
                            new_parent.clear();
                            new_parent.pid = parent_id;
                            new_parent.compact(buf_pool[parent_idx].read(), oldest_active_ts);

                            if let Err(()) = new_parent
                                .write_op(format::PageOp::SMO(format::SMOp::Split(parent_op)))
                            {
                                let to_parent_id = page_dir.new_page_id();
                                let mut temp_key = Vec::new();
                                let mut to_parent =
                                    new_parent.split_inner(&mut temp_key, to_parent_id);

                                if &middle_key <= &temp_key {
                                    to_parent
                                        .write_op(format::PageOp::SMO(format::SMOp::Split(
                                            parent_op,
                                        )))
                                        .unwrap();
                                } else {
                                    new_parent
                                        .write_op(format::PageOp::SMO(format::SMOp::Split(
                                            parent_op,
                                        )))
                                        .unwrap();
                                }

                                let to_parent_idx = free_list.pop().unwrap();
                                to_parent.pack(&mut buf_pool[to_parent_idx]);
                                page_dir.insert(to_parent_id, to_parent_idx);

                                new_parent.pack(&mut buf_pool[parent_idx]);

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                parent_op = format::SplitOp {
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
                        let new_root = &mut page_mut;
                        new_root.clear();
                        let new_root_id = page_dir.new_page_id();
                        new_root.pid = new_root_id;
                        new_root.set_right_pid(right);
                        new_root.set_left_pid(crate::PageId(u64::MAX));
                        new_root
                            .write_op(format::PageOp::SMO(format::SMOp::Split(parent_op)))
                            .unwrap();

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
