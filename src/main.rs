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

const OFFSET_SIZE: usize = 8;
const CHUNK_LEN_SIZE: usize = 8;

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

// NOTE:
// - for now, we're just gonna have one op per connection at a time, this will keep things simple,
//  but obviously we'll want to adjust this later as it's pretty inefficient
// - WE READ/WRITE ONE BLOCK AT A TIME, blocks are by definition the io granularity of our system,
//   also for now, only reading one page at a time
//
// TODO:
// - stuff may temporarily be in the write buffer
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
    let mut evict_list = Vec::new();
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
    let mut current_read: Option<PendingRead> = None;
    let mut read_buf = Some(vec![0; block_size]);
    let mut write_bufs = vec![vec![0; block_size]; 4];
    let mut write_buf_pos = 0;
    let mut current_write_block = 0;

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

        // check for any io results
        while let Ok(mut res) = io_recv.try_recv() {
            if res.read {
                let pending = current_read.as_mut().unwrap();
                let page_buf = buf_pool[pending.buf_pool_idx].raw_buffer_mut();

                let mut block_cursor = pending.block_offset;
                let next = u64::from_be_bytes(
                    res.buf[block_cursor..block_cursor + OFFSET_SIZE]
                        .try_into()
                        .unwrap(),
                ) as usize;
                block_cursor += OFFSET_SIZE;
                let chunk_len = u64::from_be_bytes(
                    res.buf[block_cursor..block_cursor + CHUNK_LEN_SIZE]
                        .try_into()
                        .unwrap(),
                ) as usize;
                block_cursor += CHUNK_LEN_SIZE;

                page_buf[pending.buffer_offset..pending.buffer_offset + chunk_len]
                    .copy_from_slice(&res.buf[block_cursor..block_cursor + chunk_len]);
                pending.buffer_offset += chunk_len;

                if next == 0 {
                    // no more to read for this page
                    page_buf.copy_within(
                        0..pending.buffer_offset,
                        page_buf.len() - pending.buffer_offset,
                    );
                    let top = page_buf.len() - pending.buffer_offset;
                    buf_pool[pending.buf_pool_idx].top = top;
                    buf_pool[pending.buf_pool_idx].flush = top;

                    pending_ios.remove(&pending.page_id).unwrap();
                    cache.insert(pending.page_id, pending.buf_pool_idx);

                    res.buf.fill(0);
                    read_buf = Some(res.buf);
                    current_read = None;
                } else {
                    // need to keep reading
                    let next_block = next / block_size;
                    pending.block_offset = next % block_size;
                    res.buf.fill(0);
                    io_send
                        .send(IOReq {
                            buf: res.buf,
                            block: Some(next_block),
                        })
                        .unwrap();
                }
            } else {
                let current = write_bufs.pop().unwrap();
                res.buf.fill(0);
                write_bufs.push(res.buf);
                write_bufs.push(current);
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
        while let Some(conn_id) = disconns.pop() {
            conns.remove(&conn_id);
        }

        // issue a read if we can/need to
        if current_read.is_none() && !pending_ios.is_empty() {
            let to_read = pending_ios.iter().max_by(|a, b| a.1.cmp(b.1)).unwrap().0;
            let file_offset = *offset_table.get(to_read).unwrap();
            let block = file_offset / block_size;
            let block_offset = file_offset % block_size;
            current_read = Some(PendingRead {
                page_id: *to_read,
                buf_pool_idx: free_list.pop().unwrap(),
                block_offset,
                buffer_offset: 0,
            });
            io_send
                .send(IOReq {
                    block: Some(block),
                    buf: read_buf.take().unwrap(),
                })
                .unwrap();
        }

        // figure out if/what we need to evict to keep the free list nice and full
        {
            let mut cands = cache.evict_cands();
            while free_list.len() < free_cap_target {
                let (pid, idx) = cands.next().unwrap();
                let buf = &mut buf_pool[idx];
                let flush_len = buf.flush_len();
                if flush_len > 0 {
                    // first we check if we have enough space in the current write buf to take this
                    // write
                    if block_size - write_buf_pos < OFFSET_SIZE + CHUNK_LEN_SIZE + flush_len {
                        // we don't have enough room and need to write the buffer out
                        io_send
                            .send(IOReq {
                                block: None,
                                buf: write_bufs.pop().unwrap(),
                            })
                            .unwrap();
                        write_buf_pos = 0;
                        current_write_block += 1;
                    }

                    let write_buf = write_bufs.last_mut().unwrap();

                    // need to write before we evict
                    let new_offset = (current_write_block * block_size) + write_buf_pos;
                    let old_offset = offset_table.insert(pid, new_offset).unwrap();
                    if buf.flush != buf.cap {
                        // we're flushing deltas, not base page, so need to store old offset
                        write_buf[write_buf_pos..write_buf_pos + OFFSET_SIZE]
                            .copy_from_slice(&(old_offset as u64).to_be_bytes());
                    }
                    write_buf_pos += OFFSET_SIZE;
                    write_buf[write_buf_pos..write_buf_pos + CHUNK_LEN_SIZE]
                        .copy_from_slice(&(flush_len as u64).to_be_bytes());
                    write_buf_pos += CHUNK_LEN_SIZE;
                    buf.flush(&mut write_buf[write_buf_pos..write_buf_pos + flush_len]);
                    write_buf_pos += flush_len;
                }
                free_list.push(idx);
                evict_list.push(pid);
            }
        }
        while let Some(pid) = evict_list.pop() {
            cache.remove(pid);
        }
    }
}

struct PendingRead {
    page_id: PageId,
    buf_pool_idx: usize,
    block_offset: usize,
    buffer_offset: usize,
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
