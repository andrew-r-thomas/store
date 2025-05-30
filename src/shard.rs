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
    page::{Delta, PageBuffer, PageMut, SplitDelta},
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
    let mut buf_pool = Vec::with_capacity(buf_pool_size);
    for _ in 0..buf_pool_size {
        buf_pool.push(PageBuffer::new(page_size));
    }
    let mut free_list = Vec::from_iter(0..buf_pool_size);
    let mut cache = Cache::new(buf_pool_size);
    let mut root = 1;
    let mut next_pid = 2;

    // tracking for ops
    let mut page_mut = PageMut::new(page_size);
    let mut path = Vec::new();
    let mut reads = HashMap::<PageId, ReadGroup>::new();
    let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();

    // tracking for connections
    let mut next_conn_id: u64 = 0;
    let mut conns = HashMap::new();
    let mut disconns = Vec::new();
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
                        writes.push((*conn_id, req));
                    }
                    _ => panic!(),
                },
                Err(e) => {
                    if let TryRecvError::Disconnected = e {
                        disconns.push(*conn_id);
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
        for (conn_id, buf) in writes.drain(..) {
            path.clear();
            page_mut.clear();

            apply_write(
                &mut cache,
                &mut buf_pool,
                &mut free_list,
                &mut next_pid,
                &mut root,
                &mut path,
                &mut page_mut,
                Delta::from_top(&buf),
            );

            resps.push((conn_id, vec![0]));
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

fn apply_write(
    cache: &mut Cache,
    buf_pool: &mut Vec<PageBuffer>,
    free_list: &mut Vec<usize>,
    next_pid: &mut PageId,
    root: &mut PageId,

    path: &mut Vec<(PageId, usize)>,
    page_mut: &mut PageMut,

    delta: Delta,
) {
    let key = delta.key();

    path.push((*root, cache.get(*root).unwrap()));
    loop {
        let mut current = buf_pool[path.last().unwrap().1].read();
        if current.is_inner() {
            let id = current.search_inner(key);
            path.push((id, cache.get(id).unwrap()));
        } else {
            break;
        }
    }

    let (leaf_id, leaf_idx) = path.pop().unwrap();
    if !buf_pool[leaf_idx].write_delta(&delta) {
        let new_page = &mut *page_mut;
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

            let to_page_id = *next_pid;
            *next_pid += 1;
            let to_page_idx = free_list.pop().unwrap();
            to_page.pack(&mut buf_pool[to_page_idx]);
            cache.insert(to_page_id, to_page_idx);

            new_page.pack(&mut buf_pool[leaf_idx]);

            let mut parent_delta = SplitDelta {
                middle_key: &middle_key,
                left_pid: to_page_id,
            };
            let mut right = leaf_id;

            'split: loop {
                match path.pop() {
                    Some((parent_id, parent_idx)) => {
                        if !buf_pool[parent_idx].write_delta(&Delta::Split(parent_delta)) {
                            let new_parent = &mut *page_mut;
                            new_parent.clear();
                            new_parent.compact(buf_pool[parent_idx].read());

                            if !new_parent.apply_delta(&Delta::Split(parent_delta)) {
                                let mut temp_key = Vec::new();
                                let mut to_parent = new_parent.split_inner(&mut temp_key);

                                if &middle_key <= &temp_key {
                                    to_parent.apply_delta(&Delta::Split(parent_delta));
                                } else {
                                    new_parent.apply_delta(&Delta::Split(parent_delta));
                                }

                                let to_parent_id = *next_pid;
                                *next_pid += 1;
                                let to_parent_idx = free_list.pop().unwrap();
                                to_parent.pack(&mut buf_pool[to_parent_idx]);
                                cache.insert(to_parent_id, to_parent_idx);

                                new_parent.pack(&mut buf_pool[parent_idx]);

                                middle_key.clear();
                                middle_key.extend(temp_key);
                                parent_delta = SplitDelta {
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
                        let new_root = &mut *page_mut;
                        new_root.clear();
                        new_root.set_right_pid(right);
                        new_root.set_left_pid(u64::MAX);
                        new_root.apply_delta(&Delta::Split(parent_delta));

                        let new_root_id = *next_pid;
                        *next_pid += 1;
                        *root = new_root_id;

                        let new_root_idx = free_list.pop().unwrap();
                        new_root.pack(&mut buf_pool[new_root_idx]);
                        cache.insert(new_root_id, new_root_idx);

                        break 'split;
                    }
                }
            }
        } else {
            new_page.pack(&mut buf_pool[leaf_idx]);
        }
    }
}
