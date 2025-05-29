//! # SHARD PIPELINE
//!
//! ## PHASE 0: SETUP
//!
//! ## PHASE 1: INPUT COLLECTION
//!
//! ## PHASE 2: OP PROCESSING
//!
//! ## PHASE 3: OUTPUT SHIPPING

use std::{
    collections::HashMap,
    sync::mpsc::{Receiver, TryRecvError},
};

use crate::{
    Conn, PageId,
    cache::Cache,
    page::{Delta, PageBuffer, PageMut, SetDelta, SplitDelta},
};

const MAX_NEW_CONNS_PER_LOOP: usize = 4;

struct ReadGroup {
    page_idx: usize,
    reads: Vec<(u64, Vec<u8>)>,
}

pub fn shard(
    conn_recv: Receiver<Conn>,
    page_size: usize,
    buf_pool_size: usize,
    block_size: usize,
    free_cap_target: usize,
) {
    // tracking for pages and cache
    let mut buf_pool = vec![PageBuffer::new(page_size); buf_pool_size];
    let mut free_list = Vec::from_iter(0..buf_pool_size);
    let mut cache = Cache::new(buf_pool_size);
    let mut root = 1;
    let mut next_pid = 2;

    // scratch space for writes
    let mut page_mut = PageMut::new(page_size);
    let mut current_level_writes = HashMap::<PageId, WriteGroup>::new();
    let mut next_level_writes = HashMap::<PageId, WriteGroup>::new();

    // tracking for connections
    let mut next_conn_id: u64 = 0;
    let mut conns = HashMap::new();
    let mut disconns = Vec::new();

    let mut reads = HashMap::<PageId, ReadGroup>::new();

    let mut resps = Vec::new();

    // make the root
    {
        let root_idx = free_list.pop().unwrap();
        page_mut.pack(&mut buf_pool[root_idx]);
        cache.insert(root, root_idx);
    }

    // main pipeline
    loop {
        // add any new conns to our list
        for _ in 0..MAX_NEW_CONNS_PER_LOOP {
            match conn_recv.try_recv() {
                Ok(conn) => {
                    conns.insert(next_conn_id, conn);
                    next_conn_id += 1;
                }
                Err(_) => break,
            }
        }

        // load up our work for this loop
        for (conn_id, conn) in conns.iter_mut() {
            match conn.recv.try_recv() {
                Ok(mut req) => match *req.first().unwrap() {
                    1 => {
                        // get
                        // would have key length parsing in here as well
                        req.drain(0..5);
                        match find_leaf(&req, &buf_pool, &mut cache, root) {
                            Ok((pid, idx)) => match reads.get_mut(&pid) {
                                Some(group) => {
                                    group.reads.push((*conn_id, req));
                                }
                                None => {
                                    reads.insert(
                                        pid,
                                        ReadGroup {
                                            page_idx: idx,
                                            reads: vec![(*conn_id, req)],
                                        },
                                    );
                                }
                            },
                            Err(_) => panic!(),
                        }
                    }
                    2 => {
                        // set
                        let mut cursor = 1;
                        let key_len =
                            u32::from_be_bytes(req[cursor..cursor + 4].try_into().unwrap())
                                as usize;
                        cursor += 8;
                        match find_path(&req[cursor..cursor + key_len], &buf_pool, &mut cache, root)
                        {
                            Ok(mut path) => {
                                let (pid, idx) = path.pop().unwrap();
                                match current_level_writes.get_mut(&path.last().unwrap().0) {
                                    Some(group) => {
                                        group.writes.push((Some(*conn_id), req));
                                    }
                                    None => {
                                        current_level_writes.insert(
                                            pid,
                                            WriteGroup {
                                                page_idx: idx,
                                                path,
                                                writes: vec![(Some(*conn_id), req)],
                                            },
                                        );
                                    }
                                }
                            }
                            Err(_) => panic!(),
                        }
                    }
                    _ => panic!(),
                },
                Err(e) => {
                    if let TryRecvError::Disconnected = e {
                        disconns.push(*conn_id);
                    } else {
                        panic!()
                    }
                }
            }
        }

        // remove any disconnections
        while let Some(conn_id) = disconns.pop() {
            conns.remove(&conn_id);
        }

        // process reads
        for (_, group) in reads.drain() {
            let mut page = buf_pool[group.page_idx].read();
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
        while !current_level_writes.is_empty() {
            for (_, mut group) in current_level_writes.drain() {
                let page = &mut buf_pool[group.page_idx];

                let total_len: usize = group
                    .writes
                    .iter()
                    .map(|(_, d)| Delta::from_top(d).len())
                    .sum();

                if page.top < total_len {
                    // don't have enough space, need to compact
                    page_mut.clear();
                    page_mut.compact(page.read());
                    let mut left: Option<PageMut> = None;
                    let mut middle_key = Vec::new();

                    for w in group.writes {
                        let delta = Delta::from_top(&w.1);
                        match left.as_mut() {
                            Some(l) => {
                                if delta.key() <= &middle_key {
                                    assert!(l.apply_delta(&delta));
                                } else {
                                    assert!(page_mut.apply_delta(&delta));
                                }
                            }
                            None => {
                                if !page_mut.apply_delta(&delta) {
                                    match w.0 {
                                        Some(_) => {
                                            left = Some(page_mut.split_leaf(&mut middle_key));
                                            if delta.key() <= &middle_key {
                                                assert!(left.as_mut().unwrap().apply_delta(&delta));
                                            } else {
                                                assert!(page_mut.apply_delta(&delta));
                                            }
                                        }
                                        None => {
                                            left = Some(page_mut.split_inner(&mut middle_key));
                                            if delta.key() <= &middle_key {
                                                assert!(left.as_mut().unwrap().apply_delta(&delta));
                                            } else {
                                                assert!(page_mut.apply_delta(&delta));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    page_mut.pack(&mut buf_pool[group.page_idx]);

                    if let Some(mut l) = left {
                        let left_pid = next_pid;
                        next_pid += 1;
                        let left_idx = free_list.pop().unwrap();
                        cache.insert(left_pid, left_idx);
                        l.pack(&mut buf_pool[left_idx]);

                        let split_delta = Delta::Split(SplitDelta {
                            middle_key: &middle_key,
                            left_pid,
                        });
                        let mut buf = vec![0; split_delta.len()];
                        split_delta.write_to_buf(&mut buf);

                        match group.path.pop() {
                            Some((parent_id, parent_idx)) => {
                                match next_level_writes.get_mut(&parent_id) {
                                    Some(g) => {
                                        g.writes.push((None, buf));
                                    }
                                    None => {
                                        next_level_writes.insert(
                                            parent_id,
                                            WriteGroup {
                                                page_idx: parent_idx,
                                                path: group.path,
                                                writes: vec![(None, buf)],
                                            },
                                        );
                                    }
                                }
                            }
                            None => {
                                // we split the root, which means we're at the last level, and
                                // there's only one page (the old root), so we should just make the
                                // new root, and then we're done
                                todo!()
                            }
                        }
                    }
                } else {
                    // can just write the deltas in
                    for w in group.writes {
                        page.write_delta(&Delta::from_top(&w.1));

                        // PERF: this option for the conn id is us making a decision too far down,
                        // we have this information much earlier than we're taking a branch on it
                        if let Some(conn_id) = w.0 {
                            resps.push((conn_id, vec![0]));
                        }
                    }
                }
            }

            std::mem::swap(&mut current_level_writes, &mut next_level_writes);
        }

        // send responses
        while let Some((conn_id, resp)) = resps.pop() {
            conns.get_mut(&conn_id).unwrap().send.send(resp).unwrap();
        }
    }
}

// op prep ========================================================================================
fn find_leaf(
    target: &[u8],
    buf_pool: &Vec<PageBuffer>,
    cache: &mut Cache,
    root: PageId,
) -> Result<(PageId, usize), PageId> {
    let (mut current, mut current_id, mut current_idx) = match cache.get(root) {
        Some(idx) => (buf_pool[idx].read(), root, idx),
        None => return Err(root),
    };

    while current.is_inner() {
        current_id = current.search_inner(target);
        current = match cache.get(current_id) {
            Some(idx) => {
                current_idx = idx;
                buf_pool[idx].read()
            }
            None => return Err(current_id),
        };
    }

    Ok((current_id, current_idx))
}
fn find_path(
    target: &[u8],
    buf_pool: &Vec<PageBuffer>,
    cache: &mut Cache,
    root: PageId,
) -> Result<Vec<(PageId, usize)>, PageId> {
    let mut path = Vec::new();

    match cache.get(root) {
        Some(idx) => path.push((root, idx)),
        None => return Err(root),
    }

    loop {
        let mut current = buf_pool[path.last().unwrap().1].read();
        if current.is_inner() {
            let id = current.search_inner(target);
            match cache.get(id) {
                Some(idx) => path.push((id, idx)),
                None => return Err(id),
            }
        } else {
            break;
        }
    }

    Ok(path)
}

// writes =========================================================================================
struct LeafWriteGroup {
    page_idx: usize,
    path: Vec<(PageId, usize)>,
    writes: Vec<(u64, Vec<u8>)>,
}
struct InnerWriteGroup {
    page_idx: usize,
    path: Vec<(PageId, usize)>,
    writes: Vec<Vec<u8>>,
}

fn apply_leaf_writes(
    write_batch: &mut HashMap<PageId, LeafWriteGroup>,
    next_batch: &mut HashMap<PageId, InnerWriteGroup>,
    buf_pool: &mut Vec<PageBuffer>,
    page_mut: &mut PageMut,
    resps: &mut Vec<(u64, Vec<u8>)>,
    next_pid: &mut PageId,
    free_list: &mut Vec<usize>,
    cache: &mut Cache,
) {
    for (_, group) in write_batch.drain() {
        let page = &mut buf_pool[group.page_idx];

        let total_len: usize = group
            .writes
            .iter()
            .map(|(_, d)| Delta::from_top(d).len())
            .sum();

        if page.top >= total_len {
            // fast path, enough room to just dump the deltas
            for (conn_id, buf) in group.writes {
                page.write_delta(&Delta::from_top(&buf));
                resps.push((conn_id, vec![0]));
            }
        } else {
            // need to compact, then maybe split
            page_mut.clear();
            page_mut.compact(page.read());

            let mut left: Option<PageMut> = None;
            let mut middle_key = Vec::new();

            for (conn_id, buf) in group.writes {
                let delta = Delta::from_top(&buf);
                match left.as_mut() {
                    Some(l) => {
                        if delta.key() <= &middle_key {
                            assert!(l.apply_delta(&delta));
                        } else {
                            assert!(page_mut.apply_delta(&delta));
                        }
                        resps.push((conn_id, vec![0]));
                    }
                    None => {
                        if !page_mut.apply_delta(&delta) {
                            left = Some(page_mut.split_leaf(&mut middle_key));
                            if delta.key() <= &middle_key {
                                assert!(left.as_mut().unwrap().apply_delta(&delta));
                            } else {
                                assert!(page_mut.apply_delta(&delta));
                            }
                        }
                        resps.push((conn_id, vec![0]));
                    }
                }
            }
            page_mut.pack(&mut buf_pool[group.page_idx]);
            if let Some(mut l) = left {
                let left_id = *next_pid;
                *next_pid += 1;
                let left_idx = free_list.pop().unwrap();
                cache.insert(left_id, left_idx);

                l.pack(&mut buf_pool[left_idx]);
                todo!()
            }
        }
    }
}
