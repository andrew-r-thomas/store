use std::collections::HashMap;

use rtrb::{Consumer, Producer};
use store::{
    PageId,
    page::{Delta, PageBuffer, PageMut, SetDelta, SplitDelta},
};

fn main() {}

// just gonna do stuff in memory first
fn shard(mut conn_recv: Consumer<Conn>, page_size: usize) {
    let mut buf_pool = Vec::<PageBuffer>::new();
    let mut mapping_table = HashMap::<PageId, usize>::new();
    let mut root = 1;
    let mut next_pid = 2;
    let mut page_mut = PageMut::new(page_size);

    // TODO: make root page

    let mut next_conn_id: u64 = 0;
    let mut conns = HashMap::new();
    let mut ops = Vec::new();

    loop {
        // add any new conns to our list
        for _ in 0..conn_recv.slots() {
            conns.insert(next_conn_id, conn_recv.pop().unwrap());
            next_conn_id += 1;
        }

        // load up our ops
        for (conn_id, conn) in conns.iter_mut() {
            if let Ok(req) = conn.recv.pop() {
                ops.push(Op {
                    conn_id: *conn_id,
                    req,
                    resp: None,
                });
            }
        }

        // execute ops
        while let Some(op) = ops.pop() {
            match op.req {
                ConnReq::Get { key } => {
                    let mut val_buf = Vec::new();
                    if get(root, &mapping_table, &buf_pool, &key, &mut val_buf) {
                    } else {
                    }
                }
                ConnReq::Set { .. } => {}
                ConnReq::DisConn => {
                    conns.remove(&op.conn_id);
                }
            }
        }
    }
}

struct Conn {
    send: Producer<ConnResp>,
    recv: Consumer<ConnReq>,
}
enum ConnReq {
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, val: Vec<u8> },
    DisConn,
}
enum ConnResp {
    Get { val: Vec<u8> },
    Set,
}

struct Op {
    conn_id: u64,
    req: ConnReq,
    resp: Option<ConnResp>,
}

fn get(
    root: PageId,
    mapping_table: &HashMap<PageId, usize>,
    buf_pool: &Vec<PageBuffer>,
    key: &[u8],
    val_buf: &mut Vec<u8>,
) -> bool {
    let mut current = buf_pool[*mapping_table.get(&root).unwrap()].read();
    while current.is_inner() {
        current = buf_pool[*mapping_table.get(&current.search_inner(key)).unwrap()].read();
    }
    match current.search_leaf(key) {
        Some(val) => {
            val_buf.extend(val);
            true
        }
        None => false,
    }
}

fn set(
    root: &mut PageId,
    mapping_table: &mut HashMap<PageId, usize>,
    buf_pool: &mut Vec<PageBuffer>,
    next_pid: &mut PageId,
    page_size: usize,

    path: &mut Vec<(PageId, usize)>,
    page_mut: &mut PageMut,

    key: &[u8],
    val: &[u8],
) {
    path.push((*root, *mapping_table.get(root).unwrap()));
    loop {
        let mut current = buf_pool[path.last().unwrap().1].read();
        if current.is_inner() {
            let id = current.search_inner(key);
            path.push((id, *mapping_table.get(&id).unwrap()));
        } else {
            break;
        }
    }

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
            let mut to_page_buf = PageBuffer::new(page_size);
            to_page.pack(&mut to_page_buf);
            let to_page_idx = buf_pool.len();
            buf_pool.push(to_page_buf);
            mapping_table.insert(to_page_id, to_page_idx);

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
                                let mut to_parent_buf = PageBuffer::new(page_size);
                                to_parent.pack(&mut to_parent_buf);
                                let to_parent_idx = buf_pool.len();
                                buf_pool.push(to_parent_buf);
                                mapping_table.insert(to_parent_id, to_parent_idx);

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

                        let mut new_root_buf = PageBuffer::new(page_size);
                        new_root.pack(&mut new_root_buf);
                        let new_root_idx = buf_pool.len();
                        buf_pool.push(new_root_buf);
                        mapping_table.insert(new_root_id, new_root_idx);

                        break 'split;
                    }
                }
            }
        } else {
            new_page.pack(&mut buf_pool[leaf_idx]);
        }
    }
}
