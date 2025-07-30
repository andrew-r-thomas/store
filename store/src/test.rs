#![cfg(test)]

use crate::{
    format::{self, Format},
    io, mesh, shard,
};

use std::io::{Read, Seek, Write};

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
) {
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

    let io = TestIO {
        file: std::io::Cursor::new(vec![0; block_cap as usize]),
        comps: Vec::new(),
        pending_subs: Vec::new(),
        subs: Vec::new(),
        conns: std::collections::BTreeMap::new(),
    };
    let mesh = TestMesh::new(1);

    // setup shard
    let mut shard = shard::Shard::new(
        page_size,
        buf_pool_size,
        block_size,
        num_block_bufs,
        num_net_bufs,
        net_buf_size,
        free_cap_target,
        block_cap,
        io,
        mesh,
    );

    // for now we'll just set up all the clients up front
    for conn_id in 0..num_clients as u32 {
        let conn = TestConn::new();
        shard.io.conns.insert(crate::ConnId(conn_id), conn);
        shard
            .mesh
            .push_from(mesh::Msg::NewConnection(crate::ConnId(conn_id)), 0);
    }

    while shard.io.conns.len() > 0 {
        let id = rng.random_range(0..num_clients as u32 + 1);
        if id >= num_clients as u32 {
            shard.tick();
            shard.io.process_subs();
        } else {
            if let Some(conn) = shard.io.conns.get_mut(&crate::ConnId(id)) {
                conn.tick(
                    num_ingest,
                    num_ops,
                    &mut rng,
                    key_len_range.clone(),
                    val_len_range.clone(),
                );
                if conn.is_done(num_ingest, num_ops) {
                    println!("conn {id} done!");
                    shard.io.conns.remove(&crate::ConnId(id)).unwrap();
                }
            }
        }
    }
}

struct TestConn {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    to_shard: std::collections::VecDeque<u8>,
    from_shard: std::collections::VecDeque<u8>,
    expected: std::collections::VecDeque<Vec<u8>>,
    ticks: usize,
}
impl TestConn {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            to_shard: std::collections::VecDeque::new(),
            from_shard: std::collections::VecDeque::new(),
            expected: std::collections::VecDeque::new(),
            ticks: 0,
        }
    }
    fn tick(
        &mut self,
        num_ingest: usize,
        num_ops: usize,
        rng: &mut rand_chacha::ChaCha8Rng,
        key_len_range: std::ops::Range<usize>,
        val_len_range: std::ops::Range<usize>,
    ) {
        if self.ticks < num_ingest {
            // do an ingest
            let key_len = rng.random_range(key_len_range);
            let val_len = rng.random_range(val_len_range);
            let mut key = vec![0; key_len];
            let mut val = vec![0; val_len];
            rng.fill(&mut key[..]);
            rng.fill(&mut val[..]);

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
        } else if self.ticks < num_ingest + num_ops {
            // do an op
            let ops = ["insert", "update", "get", "del"];
            let chosen_op = ops.choose(rng).unwrap();
            match *chosen_op {
                "insert" => {
                    let key_len = rng.random_range(key_len_range.clone());
                    let val_len = rng.random_range(val_len_range.clone());
                    let mut key = vec![0; key_len];
                    let mut val = vec![0; val_len];
                    rng.fill(&mut key[..]);
                    rng.fill(&mut val[..]);

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
                    let (key, val) = self.entries.choose_mut(rng).unwrap();
                    let new_val_len = rng.random_range(val_len_range.clone());
                    let mut new_val = vec![0; new_val_len];
                    rng.fill(&mut new_val[..]);
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
                    let (key, val) = self.entries.choose(rng).unwrap();

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
                    let idx = rng.random_range(0..self.entries.len());
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
    fn is_done(&self, num_ingest: usize, num_ops: usize) -> bool {
        (self.ticks >= num_ingest + num_ops) && self.expected.is_empty()
    }
}

struct TestIO {
    file: std::io::Cursor<Vec<u8>>,
    conns: std::collections::BTreeMap<crate::ConnId, TestConn>,

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

struct TestMesh {
    to: Vec<Vec<mesh::Msg>>,
    from: Vec<Vec<mesh::Msg>>,
}
impl TestMesh {
    pub fn new(num_shards: usize) -> Self {
        Self {
            to: Vec::from_iter((0..num_shards + 1).map(|_| Vec::new())),
            from: Vec::from_iter((0..num_shards + 1).map(|_| Vec::new())),
        }
    }
    pub fn push_from(&mut self, msg: mesh::Msg, from: usize) {
        self.from[from].push(msg);
    }
}
impl mesh::Mesh for TestMesh {
    fn poll(&mut self) -> Vec<Vec<mesh::Msg>> {
        let out = self.from.clone();
        self.from.clear();
        out
    }
    fn push(&mut self, msg: mesh::Msg, to: usize) {
        self.to[to].push(msg);
    }
}
