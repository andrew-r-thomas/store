#![cfg(test)]

use crate::shard::{Comp, IO, Mesh, Msg, Shard, Sub};

use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::Path,
};

use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};
use rand_chacha::ChaCha8Rng;

#[test]
fn scratch() {
    const SEED: u64 = 420 ^ 69;
    const KEY_LEN_RANGE: Range<usize> = 128..256;
    const VAL_LEN_RANGE: Range<usize> = 512..1024;

    const NUM_CLIENTS: usize = 128;
    const NUM_INGEST: usize = 1024;
    const NUM_BATCHES: usize = 2048;

    const PAGE_SIZE: usize = 16 * 1024;
    const BUF_POOL_SIZE: usize = 128 * 4096;
    const FREE_CAP_TARGET: usize = BUF_POOL_SIZE / 5;

    const BLOCK_SIZE: usize = 1024 * 1024;
    const NET_BUF_SIZE: usize = 1024;
    const NUM_BLOCK_BUFS: usize = 4;
    const NUM_NET_BUFS: usize = 1024;

    run_sim(
        Path::new("test.store"),
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
    );

    fs::remove_file("test.store").unwrap();
}

pub fn run_sim(
    file_path: &Path,
    seed: u64,
    key_len_range: Range<usize>,
    val_len_range: Range<usize>,

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
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let io = TestIO {
        file: OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_path)
            .unwrap(),
        comps: Vec::new(),
        pending_subs: Vec::new(),
        subs: Vec::new(),
        conns: HashMap::new(),
    };
    let mesh = TestMesh { msgs: Vec::new() };

    // setup shard
    let mut shard = Shard::new(
        page_size,
        buf_pool_size,
        block_size,
        num_block_bufs,
        num_net_bufs,
        net_buf_size,
        free_cap_target,
        io,
        mesh,
    );

    // for now we'll just set up all the clients up front
    for conn_id in 0..num_clients as u32 {
        let conn = TestConn::new();
        shard.io.conns.insert(conn_id, conn);
        shard.mesh.msgs.push(Msg::NewConn(conn_id));
    }

    while shard.io.conns.len() > 0 {
        let id = rng.random_range(0..num_clients as u32 + 1);
        if id >= num_clients as u32 {
            shard.run_pipeline();
            shard.io.process_subs();
        } else {
            if let Some(conn) = shard.io.conns.get_mut(&id) {
                conn.tick(
                    num_ingest,
                    num_ops,
                    &mut rng,
                    key_len_range.clone(),
                    val_len_range.clone(),
                );
                if conn.is_done(num_ingest, num_ops) {
                    shard.io.conns.remove(&id).unwrap();
                }
            }
        }
    }
}

struct TestConn {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    to_shard: VecDeque<u8>,
    from_shard: VecDeque<u8>,
    expected: VecDeque<Vec<u8>>,
    ticks: usize,
}
impl TestConn {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            to_shard: VecDeque::new(),
            from_shard: VecDeque::new(),
            expected: VecDeque::new(),
            ticks: 0,
        }
    }
    fn tick(
        &mut self,
        num_ingest: usize,
        num_ops: usize,
        rng: &mut ChaCha8Rng,
        key_len_range: Range<usize>,
        val_len_range: Range<usize>,
    ) {
        if self.ticks < num_ingest {
            // do an ingest
            let key_len = rng.random_range(key_len_range);
            let val_len = rng.random_range(val_len_range);
            let mut key = vec![0; key_len];
            let mut val = vec![0; val_len];
            rng.fill(&mut key[..]);
            rng.fill(&mut val[..]);

            self.to_shard.write_all(&[2]).unwrap();
            self.to_shard
                .write_all(&(key_len as u32).to_be_bytes())
                .unwrap();
            self.to_shard
                .write_all(&(val_len as u32).to_be_bytes())
                .unwrap();
            self.to_shard.write_all(&key).unwrap();
            self.to_shard.write_all(&val).unwrap();

            self.entries.push((key, val));
            self.expected.push_back(vec![0]);
        } else if self.ticks < num_ingest + num_ops {
            // do an op
            let ops = ["insert", "update", "get"];
            let chosen_op = ops.choose(rng).unwrap();
            match *chosen_op {
                "insert" => {
                    let key_len = rng.random_range(key_len_range.clone());
                    let val_len = rng.random_range(val_len_range.clone());
                    let mut key = vec![0; key_len];
                    let mut val = vec![0; val_len];
                    rng.fill(&mut key[..]);
                    rng.fill(&mut val[..]);

                    self.to_shard.write_all(&[2]).unwrap();
                    self.to_shard
                        .write_all(&(key_len as u32).to_be_bytes())
                        .unwrap();
                    self.to_shard
                        .write_all(&(val_len as u32).to_be_bytes())
                        .unwrap();
                    self.to_shard.write_all(&key).unwrap();
                    self.to_shard.write_all(&val).unwrap();

                    self.entries.push((key, val));
                    self.expected.push_back(vec![0]);
                }
                "update" => {
                    let (key, val) = self.entries.choose_mut(rng).unwrap();
                    let new_val_len = rng.random_range(val_len_range.clone());
                    let mut new_val = vec![0; new_val_len];
                    rng.fill(&mut new_val[..]);
                    *val = new_val;

                    self.to_shard.write_all(&[2]).unwrap();
                    self.to_shard
                        .write_all(&(key.len() as u32).to_be_bytes())
                        .unwrap();
                    self.to_shard
                        .write_all(&(val.len() as u32).to_be_bytes())
                        .unwrap();
                    self.to_shard.write_all(&key[..]).unwrap();
                    self.to_shard.write_all(&val[..]).unwrap();

                    self.expected.push_back(vec![0]);
                }
                "get" => {
                    let (key, val) = self.entries.choose(rng).unwrap();

                    self.to_shard.write_all(&[1]).unwrap();
                    self.to_shard
                        .write_all(&(key.len() as u32).to_be_bytes())
                        .unwrap();
                    self.to_shard.write_all(&key[..]).unwrap();

                    let mut e = Vec::new();
                    e.push(0);
                    e.extend(&(val.len() as u32).to_be_bytes());
                    e.extend(&val[..]);
                    self.expected.push_back(e);
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
    }
    fn is_done(&self, num_ingest: usize, num_ops: usize) -> bool {
        self.ticks >= num_ingest + num_ops && self.expected.is_empty()
    }
}

struct TestIO {
    file: File,
    conns: HashMap<u32, TestConn>,

    subs: Vec<Sub>,
    pending_subs: Vec<Sub>,
    comps: Vec<Comp>,
}
impl TestIO {
    fn process_subs(&mut self) {
        for sub in self.subs.drain(..) {
            match sub {
                Sub::FileRead { mut buf, offset } => {
                    self.file.seek(SeekFrom::Start(offset)).unwrap();
                    self.file.read_exact(&mut buf).unwrap();
                    self.comps.push(Comp::FileRead { buf });
                }
                Sub::FileWrite { buf, offset } => {
                    self.file.seek(SeekFrom::Start(offset)).unwrap();
                    self.file.write_all(&buf).unwrap();
                    self.comps.push(Comp::FileWrite { buf });
                }
                Sub::TcpRead { mut buf, conn_id } => match self.conns.get_mut(&conn_id) {
                    Some(conn) => {
                        let bytes = conn.to_shard.read(&mut buf).unwrap();
                        self.comps.push(Comp::TcpRead {
                            buf,
                            conn_id,
                            res: 1,
                            bytes,
                        });
                    }
                    None => self.comps.push(Comp::TcpRead {
                        buf,
                        conn_id,
                        res: -1,
                        bytes: 0,
                    }),
                },
                Sub::TcpWrite { buf, conn_id } => match self.conns.get_mut(&conn_id) {
                    Some(conn) => {
                        conn.from_shard.write_all(&buf).unwrap();
                        self.comps.push(Comp::TcpWrite {
                            buf,
                            conn_id,
                            res: 1,
                        });
                    }
                    None => self.comps.push(Comp::TcpWrite {
                        buf,
                        conn_id,
                        res: -1,
                    }),
                },
            }
        }
    }
}
impl IO for TestIO {
    fn poll(&mut self) -> Vec<Comp> {
        let out = self.comps.clone();
        self.comps.clear();
        out
    }
    fn register_sub(&mut self, sub: Sub) {
        self.pending_subs.push(sub);
    }
    fn submit(&mut self) {
        self.subs.extend(self.pending_subs.drain(..));
    }
}

struct TestMesh {
    msgs: Vec<Msg>,
}
impl Mesh for TestMesh {
    fn poll(&mut self) -> Vec<Msg> {
        let out = self.msgs.clone();
        self.msgs.clear();
        out
    }
}
