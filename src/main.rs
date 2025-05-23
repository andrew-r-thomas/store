pub mod cache;
pub mod page;

use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    ops::Range,
    path::Path,
    sync::mpsc::{self, Receiver, Sender, TryRecvError},
    thread,
};

use cache::Cache;
use itertools::Itertools;
use page::{Delta, PageBuffer, PageMut, SetDelta, SplitDelta};
use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};
use rand_chacha::ChaCha8Rng;

type PageId = u64;

const PAGE_SIZE: usize = 1024 * 1024;
const BLOCK_SIZE: usize = 4 * 1024 * 1024;

const BUF_POOL_SIZE: usize = 1024;
const FREE_CAP_TARGET: usize = 256;

const NUM_INGEST: usize = 1024;
const NUM_OPS: usize = 2048;

const NUM_CONNS: usize = 128;
const MAX_NEW_CONNS_PER_LOOP: usize = 4;

const KEY_LEN_RANGE: Range<usize> = 128..256;
const VAL_LEN_RANGE: Range<usize> = 512..1024;

fn main() {
    let (conn_send, conn_recv) = mpsc::channel();
    thread::Builder::new()
        .name("shard".into())
        .spawn(|| {
            shard(
                conn_recv,
                PAGE_SIZE,
                BUF_POOL_SIZE,
                BLOCK_SIZE,
                FREE_CAP_TARGET,
            )
        })
        .unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut conns = Vec::new();
    loop {
        if rng.random() {
            let (c_send, s_recv) = mpsc::channel();
            let (s_send, c_recv) = mpsc::channel();
            let seed = conns.len() as u64 ^ 69;
            conns.push(
                thread::Builder::new()
                    .name(format!("conn {}", conns.len()))
                    .spawn(move || {
                        conn(
                            seed,
                            NUM_INGEST,
                            NUM_OPS,
                            KEY_LEN_RANGE,
                            VAL_LEN_RANGE,
                            c_send,
                            c_recv,
                        )
                    })
                    .unwrap(),
            );
            conn_send
                .send(Conn {
                    send: s_send,
                    recv: s_recv,
                    pending_req: None,
                })
                .unwrap();

            if conns.len() >= NUM_CONNS {
                break;
            }
        }
    }

    for c in conns {
        c.join().unwrap();
    }
}

fn conn(
    seed: u64,
    num_ingest: usize,
    num_ops: usize,
    key_len_range: Range<usize>,
    val_len_range: Range<usize>,

    send: Sender<ConnReq>,
    recv: Receiver<ConnResp>,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut entries = Vec::new();

    for i in 0..num_ingest {
        let key_len = rng.random_range(key_len_range.clone());
        let val_len = rng.random_range(val_len_range.clone());
        let mut key = vec![0; key_len];
        let mut val = vec![0; val_len];
        rng.fill(&mut key[..]);
        rng.fill(&mut val[..]);

        entries.push((key.clone(), val.clone()));

        send.send(ConnReq::Set {
            id: i as u64,
            key,
            val,
        })
        .unwrap();

        match recv.recv() {
            Ok(resp) => match resp {
                ConnResp::Set { id } => {
                    assert_eq!(id, i as u64);
                    break;
                }
                _ => panic!(),
            },
            _ => {}
        }
    }

    let ops = ["get", "insert", "update"];
    for i in 0..num_ops {
        if rng.random_bool(0.05) {
            return;
        }
        match *ops.choose(&mut rng).unwrap() {
            "get" => {
                let (k, v) = entries.choose(&mut rng).unwrap();
                send.send(ConnReq::Get {
                    id: i as u64,
                    key: k.clone(),
                })
                .unwrap();

                match recv.recv() {
                    Ok(resp) => match resp {
                        ConnResp::Get { id, val, succ } => {
                            assert_eq!(id, i as u64);
                            assert!(succ);
                            assert_eq!(&val, v);
                            break;
                        }
                        _ => panic!(),
                    },
                    _ => {}
                }
            }
            "insert" => {
                let key_len = rng.random_range(key_len_range.clone());
                let val_len = rng.random_range(val_len_range.clone());
                let mut k: Vec<u8> = vec![0; key_len];
                let mut v: Vec<u8> = vec![0; val_len];
                rng.fill(&mut k[..]);
                rng.fill(&mut v[..]);

                send.send(ConnReq::Set {
                    id: i as u64,
                    key: k.clone(),
                    val: v.clone(),
                })
                .unwrap();

                entries.push((k, v));

                match recv.recv() {
                    Ok(resp) => match resp {
                        ConnResp::Set { id } => {
                            assert_eq!(id, i as u64);
                            break;
                        }
                        _ => panic!(),
                    },
                    _ => {}
                }
            }
            "update" => {
                let (k, v) = entries.choose_mut(&mut rng).unwrap();
                let val_len = rng.random_range(val_len_range.clone());
                let mut new_val = vec![0; val_len];
                rng.fill(&mut new_val[..]);
                *v = new_val;

                send.send(ConnReq::Set {
                    id: i as u64,
                    key: k.clone(),
                    val: v.clone(),
                })
                .unwrap();

                match recv.recv() {
                    Ok(resp) => match resp {
                        ConnResp::Set { id } => {
                            assert_eq!(id, i as u64);
                            break;
                        }
                        _ => panic!(),
                    },
                    _ => {}
                }
            }
            _ => panic!(),
        }
    }
}

enum HitLoc {
    A1(usize),
    Am(usize),
}
struct PageMeta {
    buf_loc: usize,
    hit_loc: HitLoc,
}

// NOTE:
// - for now, we're just gonna have one op per connection at a time, this will keep things simple,
//  but obviously we'll want to adjust this later as it's pretty inefficient
// - WE READ/WRITE ONE BLOCK AT A TIME, blocks are by definition the io granularity of our system,
//   also for now, only reading one page at a time
fn shard(
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
    let mut offset_table = HashMap::<PageId, usize>::new();
    let mut root = 1;
    let mut next_pid = 2;

    // scratch space for writes
    let mut page_mut = PageMut::new(page_size);
    let mut path = Vec::new();

    // tracking for connections
    let mut next_conn_id: u64 = 0;
    let mut conns = HashMap::new();
    let mut disconns = Vec::new();

    // tracking for ios
    let mut pending_ios = HashMap::<PageId, u32>::new();
    let mut current_read = None;
    let mut read_buf = Some(vec![0; block_size]);
    let mut write_buf = vec![0; block_size];

    // make the root
    {
        let root_idx = free_list.pop().unwrap();
        page_mut.pack(&mut buf_pool[root_idx]);
        cache.insert(root, root_idx);
    }

    // start the io thread
    let (io_send, io_recv_) = mpsc::channel();
    let (io_send_, io_recv) = mpsc::channel();
    thread::Builder::new()
        .name("io".into())
        .spawn(move || io_manager(Path::new("temp.store"), block_size, io_send_, io_recv_))
        .unwrap();

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

        // iterate over conns and try to make progress towards their requests
        for (conn_id, conn) in conns.iter_mut() {
            if conn.pending_req.is_none() {
                match conn.recv.try_recv() {
                    Ok(req) => conn.pending_req = Some(req),
                    Err(e) => {
                        if let TryRecvError::Disconnected = e {
                            disconns.push(*conn_id);
                        }
                    }
                }
            }

            if let Some(req) = &conn.pending_req {
                match req {
                    ConnReq::Get { id, key } => {
                        let mut val_buf = Vec::new();
                        match get(&key, &mut val_buf, root, &buf_pool, &mut cache) {
                            OpRes::Ok => {
                                conn.send
                                    .send(ConnResp::Get {
                                        id: *id,
                                        val: val_buf,
                                        succ: true,
                                    })
                                    .unwrap();
                                conn.pending_req = None;
                            }
                            OpRes::Err => {
                                conn.send
                                    .send(ConnResp::Get {
                                        id: *id,
                                        val: val_buf,
                                        succ: false,
                                    })
                                    .unwrap();
                                conn.pending_req = None;
                            }
                            OpRes::NeedsIO(pid) => {
                                *pending_ios.entry(pid).or_insert(0) += 1;
                            }
                        }
                    }
                    ConnReq::Set { id, key, val } => {
                        path.clear();
                        match set(
                            &mut root,
                            &mut next_pid,
                            &mut buf_pool,
                            &mut free_list,
                            &mut cache,
                            &mut path,
                            &mut page_mut,
                            &key,
                            &val,
                        ) {
                            OpRes::Ok => {
                                conn.send.send(ConnResp::Set { id: *id }).unwrap();
                                conn.pending_req = None;
                            }
                            OpRes::NeedsIO(pid) => {
                                *pending_ios.entry(pid).or_insert(0) += 1;
                            }
                            OpRes::Err => panic!(),
                        }
                    }
                }
            }
        }

        // remove any disconnected conns
        while let Some(conn_id) = disconns.pop() {
            conns.remove(&conn_id);
        }

        // issue a read if we can/need to
        if current_read.is_none() && !pending_ios.is_empty() {
            let to_read = pending_ios.iter().max_by(|a, b| a.1.cmp(b.1)).unwrap().0;
            let offset = *offset_table.get(to_read).unwrap();
            let block = offset / block_size;
            current_read = Some((free_list.pop().unwrap(), offset));
            io_send
                .send(IOReq {
                    block: Some(block),
                    buf: read_buf.take().unwrap(),
                })
                .unwrap();
        }

        // figure out if/what we need to evict to keep the free list nice and full
        if free_list.len() < free_cap_target {
            let num_evictions = free_cap_target - free_list.len();
            for ((_, idx), _) in cache_tracker
                .iter()
                .zip(0_usize..)
                .map(|(hits, i)| (hits[0] - hits[1], i))
                .sorted_unstable_by(|a, b| a.0.cmp(&b.0))
                .zip(0..num_evictions)
            {
                todo!()
            }
        }
    }
}

fn io_manager(path: &Path, block_size: usize, send: Sender<IOResp>, recv: Receiver<IOReq>) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
}

struct IOResp {
    buf: Vec<u8>,
    read: bool,
}
struct IOReq {
    buf: Vec<u8>,
    block: Option<usize>,
}

struct Conn {
    send: Sender<ConnResp>,
    recv: Receiver<ConnReq>,
    pending_req: Option<ConnReq>,
}
enum ConnReq {
    Get { id: u64, key: Vec<u8> },
    Set { id: u64, key: Vec<u8>, val: Vec<u8> },
}
enum ConnResp {
    Get { id: u64, val: Vec<u8>, succ: bool },
    Set { id: u64 },
}

enum OpRes {
    NeedsIO(PageId),
    Ok,
    Err,
}

fn get(
    target: &[u8],
    val_buf: &mut Vec<u8>,
    root: PageId,
    buf_pool: &Vec<PageBuffer>,
    cache: &mut Cache,
) -> OpRes {
    let mut current = match cache.get(root) {
        Some(idx) => buf_pool[idx].read(),
        None => return OpRes::NeedsIO(root),
    };

    while current.is_inner() {
        let next = current.search_inner(target);
        current = match cache.get(next) {
            Some(idx) => buf_pool[idx].read(),
            None => return OpRes::NeedsIO(next),
        };
    }

    match current.search_leaf(target) {
        Some(val) => {
            val_buf.extend(val);
            OpRes::Ok
        }
        None => OpRes::Err,
    }
}

// NOTE: we will assume for now that our free list will *always* have enough space to allocate new
// pages for splits etc, and it will be the cache management logic's job to make sure that this is
// true
fn set(
    root: &mut PageId,
    next_pid: &mut PageId,
    buf_pool: &mut Vec<PageBuffer>,
    free_list: &mut Vec<usize>,
    cache: &mut Cache,

    path: &mut Vec<(PageId, usize)>,
    page_mut: &mut PageMut,

    key: &[u8],
    val: &[u8],
) -> OpRes {
    match cache.get(*root) {
        Some(idx) => path.push((*root, idx)),
        None => return OpRes::NeedsIO(*root),
    }
    loop {
        let mut current = buf_pool[path.last().unwrap().1].read();
        if current.is_inner() {
            let id = current.search_inner(key);
            match cache.get(id) {
                Some(idx) => path.push((id, idx)),
                None => return OpRes::NeedsIO(id),
            }
        } else {
            break;
        }
    }

    // ok so everything in our path at this point we know is in memory
    let delta = SetDelta { key, val };
    let (leaf_id, leaf_idx) = path.pop().unwrap();
    if !buf_pool[leaf_idx].write_delta(&Delta::Set(delta)) {
        let old_page = buf_pool[leaf_idx].read();
        let new_page = &mut *page_mut;
        new_page.clear();
        new_page.compact(old_page);

        if !new_page.apply_delta(&Delta::Set(delta)) {
            let mut middle_key = Vec::new();
            let mut to_page = new_page.split_leaf(&mut middle_key);

            if key <= &middle_key {
                to_page.apply_delta(&Delta::Set(delta));
            } else {
                new_page.apply_delta(&Delta::Set(delta));
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
                            let old_parent = buf_pool[parent_idx].read();
                            let new_parent = &mut *page_mut;
                            new_parent.clear();
                            new_parent.compact(old_parent);

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

    OpRes::Ok
}
