use std::{
    collections,
    sync::{self, atomic},
};

use format::{Format, net, op};

use crate::{central, config, io, mesh, page, page_cache};

const CENTRAL_ID: usize = 0;

/// ## NOTE
/// for now using BTreeMap bc the iteration order is deterministic, need a better solution
/// eventually though
///
/// need to make sure we're keeping the ordering of txn ops, wrt to responses too
pub struct Shard<I: io::IOFace> {
    pub config: config::Config,

    pub page_cache: page_cache::PageCache,

    // io
    pub io: I,
    pub page_store: io::PageStore,
    pub conns: collections::BTreeMap<crate::ConnId, io::Conn>,
    pub txns: collections::BTreeMap<crate::ShardTxnId, Txn>,
    pub net_bufs: Vec<Vec<u8>>,

    // sys stuff
    pub mesh: mesh::Mesh,
    pub runs: usize,
    pub committed_ts: sync::Arc<atomic::AtomicU64>,
    pub oldest_active_ts: sync::Arc<atomic::AtomicU64>,
    pub pending_commits: collections::BTreeMap<format::Timestamp, Vec<central::Commit>>,
}
impl<I: io::IOFace> Shard<I> {
    pub fn new(
        config: config::Config,
        io: I,
        mesh: mesh::Mesh,
        committed_ts: sync::Arc<atomic::AtomicU64>,
        oldest_active_ts: sync::Arc<atomic::AtomicU64>,
    ) -> Self {
        let mut page_cache = page_cache::PageCache::new(
            config.page_size,
            config.buf_pool_size,
            config.free_cap_target,
        );

        // make root
        let root_idx = page_cache.pop_free();
        page::PageMut::new(config.page_size, format::PageId(1)).pack(&mut page_cache[root_idx]);
        page_cache.insert(format::PageId(1), root_idx);

        Self {
            page_cache,
            io,
            page_store: io::PageStore::new(
                config.block_size,
                config.num_block_bufs,
                config.block_cap,
            ),
            conns: collections::BTreeMap::new(),
            txns: collections::BTreeMap::new(),
            net_bufs: Vec::from_iter(
                (0..config.num_net_bufs).map(|_| vec![0; config.net_buf_size]),
            ),

            mesh,

            runs: 0,
            committed_ts,
            oldest_active_ts,
            pending_commits: collections::BTreeMap::new(),

            config,
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
    ///         - maybe compaction
    ///
    #[tracing::instrument(skip(self))]
    pub fn tick(&mut self) {
        tracing::event!(tracing::Level::INFO, "shard ticked");

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
                    mesh::Msg::CommitResponse { txn_id, res } => {
                        let conn = self.conns.get_mut(&txn_id.conn_id).unwrap();
                        let txn_queue = conn.txns.get_mut(&txn_id.conn_txn_id).unwrap();
                        match res {
                            Ok(()) => {
                                txn_queue.push_to(Ok(net::Resp::Success));
                            }
                            Err(()) => {
                                txn_queue.push_to(Err(format::Error::Conflict));
                            }
                        }
                        txn_queue.pop_from();
                        txn_queue.sealed = false;
                        self.txns.remove(&txn_id).unwrap();
                    }
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
                    buf.resize(self.config.net_buf_size, 0);
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
                    buf.resize(self.config.net_buf_size, 0);
                    self.net_bufs.push(buf);
                }
                io::Comp::FileRead { mut buf, offset } => {
                    self.page_store
                        .read_block(&buf, offset, &mut self.page_cache);
                    buf.fill(0);
                    self.page_store.block_bufs.push(buf);
                }
                io::Comp::FileWrite { mut buf } => {
                    buf.fill(0);
                    self.page_store.block_bufs.push(buf);
                }
                _ => {}
            }
        }

        // write anything out from conns
        for (_, conn) in self.conns.iter_mut() {
            conn.tick(&mut self.io, &mut self.net_bufs);
        }

        let tick_committed_ts =
            format::Timestamp(self.committed_ts.load(atomic::Ordering::Acquire));
        let tick_oldest_active_ts =
            format::Timestamp(self.oldest_active_ts.load(atomic::Ordering::Acquire));
        // find ops that can be completed in this pipeline
        let mut reads = collections::BTreeMap::<format::PageId, ReadGroup>::new();
        {
            for (conn_id, conn) in self.conns.iter_mut() {
                for (txn_id, txn_queue) in &mut conn.txns {
                    if txn_queue.sealed {
                        continue;
                    }
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
                            net::RequestOp::Read(read) => {
                                // first check if we can fulfill the read from the local txn writes
                                let mut locals =
                                    format::FormatIter::<'_, op::WriteOp>::from(&txn.writes[..])
                                        .collect::<Vec<_>>();
                                let mut resp = None;
                                while let Some(local_write) = locals.pop() {
                                    if local_write.key() == read.key() {
                                        match local_write {
                                            op::WriteOp::Set(set) => {
                                                resp = Some(Ok(net::Resp::Get(Some(set.val))));
                                            }
                                            op::WriteOp::Del(_) => {
                                                resp = Some(Ok(net::Resp::Get(None)));
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
                                match find_leaf(
                                    read.key(),
                                    &mut self.page_cache,
                                    self.page_store.root,
                                ) {
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
                                    Err(pid) => self.page_store.inc_prio(pid, &mut self.page_cache),
                                }
                            }
                            net::RequestOp::Write(write) => {
                                let old_len = txn.writes.len();
                                txn.writes.resize(old_len + write.len(), 0);
                                write.write_to_buf(&mut txn.writes[old_len..]);
                                txn_queue.push_to(Ok(net::Resp::Success));
                                txn_queue.pop_from();
                            }
                            net::RequestOp::Commit => {
                                txn_queue.sealed = true;
                                self.mesh.push(
                                    mesh::Msg::CommitRequest {
                                        txn_id,
                                        start_ts: txn.start_ts,
                                        writes: txn.writes.clone(),
                                    },
                                    CENTRAL_ID,
                                );
                            }
                        }
                    }
                }
            }
        }

        // process reads
        for (_, group) in reads {
            let mut page = self.page_cache[group.page_idx].read();

            for (txn_id, start_ts, read) in group.reads {
                let queue = self
                    .conns
                    .get_mut(&txn_id.conn_id)
                    .unwrap()
                    .txns
                    .get_mut(&txn_id.conn_txn_id)
                    .unwrap();
                queue.push_to(Ok(net::Resp::Get(
                    page.search_leaf(op::ReadOp::from_bytes(&read).unwrap().key(), start_ts),
                )));
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
        if let Some((ts, commits)) = self.pending_commits.pop_first() {
            if {
                // check if we have all the pages in memory to do these writes
                let mut out = true;
                for commit in &commits {
                    for write in format::FormatIter::<op::WriteOp>::from(&commit.writes[..]) {
                        if let Err(pid) =
                            find_leaf(write.key(), &mut self.page_cache, self.page_store.root)
                        {
                            out = false;
                            self.page_store.inc_prio(pid, &mut self.page_cache);
                        }
                    }
                }
                out
            } {
                // process commits
                for commit in commits {
                    for write_op in format::FormatIter::<op::WriteOp>::from(&commit.writes[..]) {
                        let write = format::page::PageWrite {
                            ts: commit.commit_ts,
                            write: write_op,
                        };
                        apply_write(
                            &mut self.page_cache,
                            &mut self.page_store,
                            write,
                            tick_oldest_active_ts,
                            self.config.page_size,
                        );
                    }

                    self.mesh.push(mesh::Msg::WriteResponse(commit), CENTRAL_ID);
                }
            } else {
                self.pending_commits.insert(ts, commits);
            }
        }

        // do any evictions if we need to
        self.page_cache.tick(&mut self.page_store, &mut self.io);

        // queue up any reads that we may want to do
        self.page_store.tick(&mut self.io, &mut self.page_cache);

        // submit io
        self.io.submit();
    }
}

pub struct Txn {
    start_ts: format::Timestamp,
    /// these are in chronological (not reverse chron) order
    /// the oldest write is first
    writes: Vec<u8>,
}

/// a bunch of [`op::ReadOp`]s for keys in the same page
pub struct ReadGroup {
    page_idx: usize,
    reads: Vec<(crate::ShardTxnId, format::Timestamp, Vec<u8>)>,
}

fn find_leaf(
    target: &[u8],
    page_cache: &mut page_cache::PageCache,
    root: format::PageId,
) -> Result<(format::PageId, usize), format::PageId> {
    let (mut current, mut current_id, mut current_idx) = match page_cache.get(root) {
        Some(idx) => (page_cache[idx].read(), root, idx),
        None => return Err(root),
    };

    while current.is_inner() {
        current_id = current.search_inner(target);
        current = match page_cache.get(current_id) {
            Some(idx) => {
                current_idx = idx;
                page_cache[idx].read()
            }
            None => return Err(current_id),
        };
    }

    Ok((current_id, current_idx))
}

pub fn apply_write(
    page_cache: &mut page_cache::PageCache,
    page_store: &mut io::PageStore,
    write: format::page::PageWrite,
    oldest_active_ts: format::Timestamp,
    page_size: usize,
) {
    let mut path = Vec::new();
    let mut page_mut = page::PageMut::new(page_size, format::PageId(0));

    let op = format::page::PageOp::Write(write);
    let key = write.key();

    path.push((page_store.root, page_cache.get(page_store.root).unwrap()));
    loop {
        let (_, current_idx) = path.last().unwrap();
        let mut current = page_cache[*current_idx].read();
        if current.is_inner() {
            let id = current.search_inner(key);
            path.push((id, page_cache.get(id).unwrap()));
        } else {
            break;
        }
    }

    let (leaf_id, leaf_idx) = path.pop().unwrap();

    if !page_cache[leaf_idx].write(&op) {
        let new_page = &mut page_mut;
        new_page.clear();
        new_page.pid = leaf_id;
        new_page.compact(page_cache[leaf_idx].read(), oldest_active_ts);

        if let Err(()) = new_page.write_op(op) {
            let mut middle_key = Vec::new();

            let to_page_id = page_store.new_page_id();
            let mut to_page = new_page.split_leaf(&mut middle_key, to_page_id);

            if key <= &middle_key[..] {
                to_page.write_op(op).unwrap();
            } else {
                new_page.write_op(op).unwrap();
            }

            let to_page_idx = page_cache.pop_free();
            to_page.pack(&mut page_cache[to_page_idx]);
            page_cache.insert(to_page_id, to_page_idx);

            new_page.pack(&mut page_cache[leaf_idx]);

            let mut parent_op = format::page::SplitOp {
                middle_key: &middle_key,
                left_pid: to_page_id,
            };
            let mut right = leaf_id;

            'split: loop {
                match path.pop() {
                    Some((parent_id, parent_idx)) => {
                        if !page_cache[parent_idx].write(&format::page::PageOp::SMO(
                            format::page::SMOp::Split(parent_op),
                        )) {
                            let new_parent = &mut page_mut;
                            new_parent.clear();
                            new_parent.pid = parent_id;
                            new_parent.compact(page_cache[parent_idx].read(), oldest_active_ts);

                            if let Err(()) = new_parent.write_op(format::page::PageOp::SMO(
                                format::page::SMOp::Split(parent_op),
                            )) {
                                let to_parent_id = page_store.new_page_id();
                                let mut temp_key = Vec::new();
                                let mut to_parent =
                                    new_parent.split_inner(&mut temp_key, to_parent_id);

                                if &middle_key <= &temp_key {
                                    to_parent
                                        .write_op(format::page::PageOp::SMO(
                                            format::page::SMOp::Split(parent_op),
                                        ))
                                        .unwrap();
                                } else {
                                    new_parent
                                        .write_op(format::page::PageOp::SMO(
                                            format::page::SMOp::Split(parent_op),
                                        ))
                                        .unwrap();
                                }

                                let to_parent_idx = page_cache.pop_free();
                                to_parent.pack(&mut page_cache[to_parent_idx]);
                                page_cache.insert(to_parent_id, to_parent_idx);

                                new_parent.pack(&mut page_cache[parent_idx]);

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                parent_op = format::page::SplitOp {
                                    middle_key: &middle_key,
                                    left_pid: to_parent_id,
                                };
                                right = parent_id;

                                continue 'split;
                            } else {
                                new_parent.pack(&mut page_cache[parent_idx]);
                                break 'split;
                            }
                        } else {
                            break 'split;
                        }
                    }
                    None => {
                        let new_root = &mut page_mut;
                        new_root.clear();
                        let new_root_id = page_store.new_page_id();
                        new_root.pid = new_root_id;
                        new_root.set_right_pid(right);
                        new_root.set_left_pid(format::PageId(u64::MAX));
                        new_root
                            .write_op(format::page::PageOp::SMO(format::page::SMOp::Split(
                                parent_op,
                            )))
                            .unwrap();

                        let new_root_idx = page_cache.pop_free();
                        page_store.root = new_root_id;

                        new_root.pack(&mut page_cache[new_root_idx]);
                        page_cache.insert(new_root_id, new_root_idx);

                        break 'split;
                    }
                }
            }
        } else {
            new_page.pack(&mut page_cache[leaf_idx]);
        }
    }
}
