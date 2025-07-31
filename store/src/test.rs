#![cfg(test)]

use crate::{
    central,
    format::{self, Format},
    io::{self, IOFace},
    mesh, shard,
};

use std::{
    io::{Read, Seek, Write},
    sync::{self, atomic},
};

use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};

// TODO: errors (read after delete, etc) should be checked for correct values (i.e. read after
// delete should return None)

#[test]
fn scratch() {
    const SEED: u64 = 420 ^ 69;
    const KEY_LEN_RANGE: std::ops::Range<usize> = 128..256;
    const VAL_LEN_RANGE: std::ops::Range<usize> = 512..1024;

    const NUM_CLIENTS: usize = 256;
    const NUM_INGEST: usize = 1024;
    const NUM_BATCHES: usize = 3072;

    const PAGE_SIZE: usize = 16 * 1024;
    const BUF_POOL_SIZE: usize = 8 * 4096;
    const FREE_CAP_TARGET: usize = BUF_POOL_SIZE / 5;
    const BLOCK_CAP: u64 = BLOCK_SIZE as u64 * 1024;

    const BLOCK_SIZE: usize = 1024 * 1024;
    const NET_BUF_SIZE: usize = 2 * 1024;
    const NUM_BLOCK_BUFS: usize = 4;
    const NUM_NET_BUFS: usize = 1024;

    const QUEUE_SIZE: usize = 128;

    run_sim(
        SEED,
        KEY_LEN_RANGE,
        VAL_LEN_RANGE,
        NUM_CLIENTS,
        NUM_INGEST,
        NUM_BATCHES,
        PAGE_SIZE,
        BUF_POOL_SIZE,
        BLOCK_SIZE,
        NUM_BLOCK_BUFS,
        NUM_NET_BUFS,
        NET_BUF_SIZE,
        FREE_CAP_TARGET,
        BLOCK_CAP,
        QUEUE_SIZE,
    );
}

pub fn run_sim(
    seed: u64,
    key_len_range: std::ops::Range<usize>,
    val_len_range: std::ops::Range<usize>,

    num_clients: usize,
    num_ingest: usize,
    num_ops: usize,

    page_size: usize,
    buf_pool_size: usize,
    block_size: usize,
    num_block_bufs: usize,
    num_net_bufs: usize,
    net_buf_size: usize,
    free_cap_target: usize,
    block_cap: u64,
    queue_size: usize,
) {
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

    let committed_ts = sync::Arc::new(atomic::AtomicU64::new(0));
    let oldest_active_ts = sync::Arc::new(atomic::AtomicU64::new(0));

    let mut meshes = mesh::Mesh::new(queue_size, 1);

    // setup shard
    let io = TestIO {
        file: std::io::Cursor::new(vec![0; block_cap as usize]),
        comps: Vec::new(),
        pending_subs: Vec::new(),
        subs: Vec::new(),
    };
    let shard = shard::Shard::new(
        page_size,
        buf_pool_size,
        block_size,
        num_block_bufs,
        num_net_bufs,
        net_buf_size,
        free_cap_target,
        block_cap,
        io,
        meshes.pop().unwrap(),
        committed_ts.clone(),
        oldest_active_ts.clone(),
    );

    let io = TestIO {
        file: std::io::Cursor::new(vec![0; block_cap as usize]),
        comps: Vec::new(),
        pending_subs: Vec::new(),
        subs: Vec::new(),
    };

    // setup central
    let central = central::Central::new(
        committed_ts.clone(),
        oldest_active_ts.clone(),
        io,
        meshes.pop().unwrap(),
    );

    let mut store_node = Node {
        workers: vec![
            StoreNodeWorker::Central(central),
            StoreNodeWorker::Shard(shard),
        ],
    };

    let mut query_node = Node {
        workers: Vec::from_iter((0..num_clients).map(|_| {
            TestConn::new(
                num_ingest,
                num_ops,
                seed,
                key_len_range.clone(),
                val_len_range.clone(),
            )
        })),
    };

    loop {
        // run one of the nodes
        if rng.random() {
            // run store
            let worker = store_node.workers.choose_mut(&mut rng).unwrap();
            worker.tick();
            match worker {
                StoreNodeWorker::Central(central) => central.io.process_subs(),
                StoreNodeWorker::Shard(shard) => shard.io.process_subs(),
            }
        } else {
            // run query node
            if query_node.workers.len() == 0 {
                break;
            }
            let i = rng.random_range(0..query_node.workers.len());
            let worker = &mut query_node.workers[i];
            worker.tick();
            if worker.is_done() {
                query_node.workers.remove(i);
            }
        }

        // run io (this will get more creative later)
    }
}

struct TestConn {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    to_shard: std::collections::VecDeque<u8>,
    from_shard: std::collections::VecDeque<u8>,
    expected: std::collections::VecDeque<Vec<u8>>,
    ticks: usize,
    num_ingest: usize,
    num_ops: usize,
    key_len_range: std::ops::Range<usize>,
    val_len_range: std::ops::Range<usize>,
    rng: rand_chacha::ChaCha8Rng,
}
impl TestConn {
    fn new(
        num_ingest: usize,
        num_ops: usize,
        seed: u64,
        key_len_range: std::ops::Range<usize>,
        val_len_range: std::ops::Range<usize>,
    ) -> Self {
        let rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        Self {
            entries: Vec::new(),
            to_shard: std::collections::VecDeque::new(),
            from_shard: std::collections::VecDeque::new(),
            expected: std::collections::VecDeque::new(),
            ticks: 0,
            num_ingest,
            num_ops,
            key_len_range,
            val_len_range,
            rng,
        }
    }
    fn is_done(&self) -> bool {
        (self.ticks >= self.num_ingest + self.num_ops) && self.expected.is_empty()
    }
}
impl Tick for TestConn {
    fn tick(&mut self) {
        if self.ticks < self.num_ingest {
            // do an ingest
            let key_len = self.rng.random_range(self.key_len_range.clone());
            let val_len = self.rng.random_range(self.val_len_range.clone());
            let mut key = vec![0; key_len];
            let mut val = vec![0; val_len];
            self.rng.fill(&mut key[..]);
            self.rng.fill(&mut val[..]);

            let req = format::Request {
                txn_id: crate::ConnTxnId(1),
                op: format::RequestOp::Write(format::WriteOp::Set(format::SetOp {
                    key: &key,
                    val: &val,
                })),
            };
            let mut reqbuf = vec![0; req.len()];
            req.write_to_buf(&mut reqbuf);
            self.to_shard.extend(reqbuf);

            let expected_resp = format::Response {
                txn_id: crate::ConnTxnId(1),
                op: Ok(format::Resp::Success),
            };

            self.entries.push((key, val));
            self.expected.push_back(expected_resp.to_vec());
        } else if self.ticks < self.num_ingest + self.num_ops {
            // do an op
            let ops = ["insert", "update", "get", "del"];
            let chosen_op = ops.choose(&mut self.rng).unwrap();
            match *chosen_op {
                "insert" => {
                    let key_len = self.rng.random_range(self.key_len_range.clone());
                    let val_len = self.rng.random_range(self.val_len_range.clone());
                    let mut key = vec![0; key_len];
                    let mut val = vec![0; val_len];
                    self.rng.fill(&mut key[..]);
                    self.rng.fill(&mut val[..]);

                    let req = format::Request {
                        txn_id: crate::ConnTxnId(1),
                        op: format::RequestOp::Write(format::WriteOp::Set(format::SetOp {
                            key: &key,
                            val: &val,
                        })),
                    };
                    let mut reqbuf = vec![0; req.len()];
                    req.write_to_buf(&mut reqbuf);
                    self.to_shard.extend(reqbuf);

                    let expected_resp = format::Response {
                        txn_id: crate::ConnTxnId(1),
                        op: Ok(format::Resp::Success),
                    };

                    self.entries.push((key, val));
                    self.expected.push_back(expected_resp.to_vec());
                }
                "update" => {
                    let (key, val) = self.entries.choose_mut(&mut self.rng).unwrap();
                    let new_val_len = self.rng.random_range(self.val_len_range.clone());
                    let mut new_val = vec![0; new_val_len];
                    self.rng.fill(&mut new_val[..]);
                    *val = new_val;

                    let req = format::Request {
                        txn_id: crate::ConnTxnId(1),
                        op: format::RequestOp::Write(format::WriteOp::Set(format::SetOp {
                            key: &key,
                            val: &val,
                        })),
                    };
                    let mut reqbuf = vec![0; req.len()];
                    req.write_to_buf(&mut reqbuf);
                    self.to_shard.extend(reqbuf);

                    let expected_resp = format::Response {
                        txn_id: crate::ConnTxnId(1),
                        op: Ok(format::Resp::Success),
                    };

                    self.expected.push_back(expected_resp.to_vec());
                }
                "get" => {
                    let (key, val) = self.entries.choose(&mut self.rng).unwrap();

                    let req = format::Request {
                        txn_id: crate::ConnTxnId(1),
                        op: format::RequestOp::Read(format::ReadOp::Get(format::GetOp {
                            key: &key,
                        })),
                    };
                    let mut reqbuf = vec![0; req.len()];
                    req.write_to_buf(&mut reqbuf);
                    self.to_shard.extend(reqbuf);

                    let expected_resp = format::Response {
                        txn_id: crate::ConnTxnId(1),
                        op: Ok(format::Resp::Get(Some(&val))),
                    };

                    self.expected.push_back(expected_resp.to_vec());
                }
                "del" => {
                    let idx = self.rng.random_range(0..self.entries.len());
                    let (key, _) = self.entries.remove(idx);

                    let req = format::Request {
                        txn_id: crate::ConnTxnId(1),
                        op: format::RequestOp::Write(format::WriteOp::Del(format::DelOp {
                            key: &key,
                        })),
                    };
                    let mut reqbuf = vec![0; req.len()];
                    req.write_to_buf(&mut reqbuf);
                    self.to_shard.extend(reqbuf);

                    let expected_resp = format::Response {
                        txn_id: crate::ConnTxnId(1),
                        op: Ok(format::Resp::Success),
                    };

                    self.expected.push_back(expected_resp.to_vec());
                }
                _ => panic!(),
            }
        }
        // try to clear expecteds
        while let Some(e) = self.expected.pop_front() {
            if self.from_shard.len() >= e.len() {
                let mut got = vec![0; e.len()];
                self.from_shard.read_exact(&mut got).unwrap();
                assert_eq!(got, e);
            } else {
                self.expected.push_front(e);
                break;
            }
        }

        self.ticks += 1;
    }
}

struct TestIO {
    file: std::io::Cursor<Vec<u8>>,

    subs: Vec<io::Sub>,
    pending_subs: Vec<io::Sub>,
    comps: Vec<io::Comp>,
}
impl TestIO {
    fn process_subs(&mut self) {
        for sub in self.subs.drain(..) {
            match sub {
                io::Sub::FileRead { mut buf, offset } => {
                    self.file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                    self.file.read_exact(&mut buf).unwrap();
                    self.comps.push(io::Comp::FileRead { buf, offset });
                }
                io::Sub::FileWrite { buf, offset } => {
                    self.file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                    self.file.write_all(&buf).unwrap();
                    self.comps.push(io::Comp::FileWrite { buf });
                }
                io::Sub::TcpRead { mut buf, conn_id } => match self.conns.get_mut(&conn_id) {
                    Some(conn) => {
                        let bytes = conn.to_shard.read(&mut buf).unwrap();
                        self.comps.push(io::Comp::TcpRead {
                            buf,
                            conn_id,
                            res: 1,
                            bytes,
                        });
                    }
                    None => self.comps.push(io::Comp::TcpRead {
                        buf,
                        conn_id,
                        res: -1,
                        bytes: 0,
                    }),
                },
                io::Sub::TcpWrite { buf, conn_id } => match self.conns.get_mut(&conn_id) {
                    Some(conn) => {
                        conn.from_shard.write_all(&buf).unwrap();
                        self.comps.push(io::Comp::TcpWrite {
                            buf,
                            conn_id,
                            res: 1,
                        });
                    }
                    None => self.comps.push(io::Comp::TcpWrite {
                        buf,
                        conn_id,
                        res: -1,
                    }),
                },
                io::Sub::Accept { .. } => {}
            }
        }
    }
}
impl io::IOFace for TestIO {
    fn poll(&mut self) -> Vec<io::Comp> {
        let out = self.comps.clone();
        self.comps.clear();
        out
    }
    fn register_sub(&mut self, sub: io::Sub) {
        self.pending_subs.push(sub);
    }
    fn submit(&mut self) {
        self.subs.extend(self.pending_subs.drain(..));
    }
}

/// a logical "node" in a network
pub struct Node<T: Tick> {
    workers: Vec<T>,
}

pub trait Tick {
    fn tick(&mut self);
}

pub enum StoreNodeWorker<IO: IOFace> {
    Central(central::Central<IO>),
    Shard(shard::Shard<IO>),
}

impl<IO: IOFace> Tick for StoreNodeWorker<IO> {
    fn tick(&mut self) {
        match self {
            Self::Central(central) => central.tick(),
            Self::Shard(shard) => shard.tick(),
        }
    }
}
