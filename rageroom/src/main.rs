use std::{
    cell, collections, fs,
    io::{Read, Seek, Write},
    path, rc,
    sync::{self, atomic},
};

use clap::Parser;
use format::{Format, net, op};
use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};
use serde::Deserialize;
use store::{central, io, mesh, shard};

#[derive(Parser)]
struct Args {
    sim_config: path::PathBuf,
    db_config: path::PathBuf,
}

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::new()).unwrap();
    let args = Args::parse();
    let sim_config = SimConfig::load(&args.sim_config);
    let db_config = store::config::Config::load(&args.db_config).unwrap();
    run_sim(sim_config, db_config);
}

#[derive(Deserialize)]
pub struct SimConfig {
    pub seed: u64,
    pub key_len_range: std::ops::Range<usize>,
    pub val_len_range: std::ops::Range<usize>,
    pub num_clients: usize,
    pub num_ingest: usize,
    pub num_batches: usize,
}
impl SimConfig {
    pub fn load(path: &path::Path) -> Self {
        toml::from_slice(&fs::read(path).unwrap()).unwrap()
    }
}

pub fn run_sim(sim_config: SimConfig, db_config: store::config::Config) {
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(sim_config.seed);

    let committed_ts = sync::Arc::new(atomic::AtomicU64::new(0));
    let oldest_active_ts = sync::Arc::new(atomic::AtomicU64::new(0));

    let mut meshes = mesh::Mesh::new(db_config.queue_size, 1);

    let network_manager = rc::Rc::new(cell::RefCell::new(TestNetworkManager {
        pending_conns: Vec::new(),
        conn_bufs: collections::BTreeMap::new(),
    }));
    let storage_manager = rc::Rc::new(cell::RefCell::new(TestStorageManager {
        shard_files: collections::BTreeMap::from([(1, std::io::Cursor::new(Vec::new()))]),
    }));

    // setup shard
    let shard_io = TestIO {
        id: 1,
        storage_manager: storage_manager.clone(),
        network_manager: network_manager.clone(),
        comps: Vec::new(),
        pending_subs: Vec::new(),
        subs: Vec::new(),
    };
    let shard = shard::Shard::new(
        db_config,
        shard_io,
        meshes.pop().unwrap(),
        committed_ts.clone(),
        oldest_active_ts.clone(),
    );

    let central_io = TestIO {
        id: 0,
        storage_manager: storage_manager.clone(),
        network_manager: network_manager.clone(),
        comps: Vec::new(),
        pending_subs: Vec::new(),
        subs: Vec::new(),
    };

    // setup central
    let central = central::Central::new(
        committed_ts.clone(),
        oldest_active_ts.clone(),
        central_io,
        meshes.pop().unwrap(),
    );

    let mut store_node = Node {
        workers: vec![
            StoreNodeWorker::Central(central),
            StoreNodeWorker::Shard(shard),
        ],
    };

    let mut query_node = Node {
        workers: Vec::from_iter((0..sim_config.num_clients as i32).map(|i| {
            TestConn::new(
                sim_config.num_ingest,
                sim_config.num_batches,
                rng.random(),
                sim_config.key_len_range.clone(),
                sim_config.val_len_range.clone(),
                store::ConnId(i),
                network_manager.clone(),
            )
        })),
    };

    loop {
        if query_node.workers.len() == 0 {
            break;
        }
        // run one of the nodes
        let worker_i = rng.random_range(0..query_node.workers.len() + store_node.workers.len());
        if worker_i < query_node.workers.len() {
            let worker = &mut query_node.workers[worker_i];
            worker.tick();
            if worker.is_done() {
                println!("worker {} done!", worker.id.0);
                let mut network_manager = network_manager.borrow_mut();
                network_manager.conn_bufs.remove(&worker.id);
                query_node.workers.remove(worker_i);
            }
        } else {
            let worker = &mut store_node.workers[worker_i - query_node.workers.len()];
            worker.tick();
            match worker {
                StoreNodeWorker::Central(central) => central.io.process_subs(),
                StoreNodeWorker::Shard(shard) => shard.io.process_subs(),
            }
        }

        // let nm = network_manager.borrow();
        // let mut total_from_conns = 0;
        // let mut total_to_conns = 0;
        // for (_, cb) in &nm.conn_bufs {
        //     total_from_conns += cb.from_conn.len();
        //     total_to_conns += cb.to_conn.len();
        // }
        // if total_from_conns > 0 || total_to_conns > 0 {
        //     println!("total from: {total_from_conns}, total to: {total_to_conns}");
        // }
    }

    println!("test completed successfully!");
}

struct TestConn {
    id: store::ConnId,
    network_manager: rc::Rc<cell::RefCell<TestNetworkManager>>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
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
        id: store::ConnId,
        network_manager: rc::Rc<cell::RefCell<TestNetworkManager>>,
    ) -> Self {
        let rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        {
            let mut nm = network_manager.borrow_mut();
            nm.pending_conns.push(id);
        }
        Self {
            id,
            network_manager,
            entries: Vec::new(),
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
        let mut network_manager = self.network_manager.borrow_mut();
        if let Some(conn_bufs) = network_manager.conn_bufs.get_mut(&self.id) {
            if self.ticks < self.num_ingest {
                // do an ingest
                let key_len = self.rng.random_range(self.key_len_range.clone());
                let val_len = self.rng.random_range(self.val_len_range.clone());
                let mut key = vec![0; key_len];
                let mut val = vec![0; val_len];
                self.rng.fill(&mut key[..]);
                self.rng.fill(&mut val[..]);

                let req = net::Request {
                    txn_id: format::ConnTxnId(1),
                    op: net::RequestOp::Write(op::WriteOp::Set(op::SetOp {
                        key: &key,
                        val: &val,
                    })),
                };
                let mut reqbuf = vec![0; req.len()];
                req.write_to_buf(&mut reqbuf);
                conn_bufs.from_conn.extend(reqbuf);

                let expected_resp = net::Response {
                    txn_id: format::ConnTxnId(1),
                    op: Ok(net::Resp::Success),
                };

                self.entries.push((key, val));
                self.expected.push_back(expected_resp.to_vec());

                let commit = net::Request {
                    txn_id: format::ConnTxnId(1),
                    op: net::RequestOp::Commit,
                };
                let mut commit_buf = vec![0; commit.len()];
                commit.write_to_buf(&mut commit_buf);
                conn_bufs.from_conn.extend(commit_buf);

                let e_r = net::Response {
                    txn_id: format::ConnTxnId(1),
                    op: Ok(net::Resp::Success),
                };
                self.expected.push_back(e_r.to_vec());
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

                        let req = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Write(op::WriteOp::Set(op::SetOp {
                                key: &key,
                                val: &val,
                            })),
                        };
                        let mut reqbuf = vec![0; req.len()];
                        req.write_to_buf(&mut reqbuf);
                        conn_bufs.from_conn.extend(reqbuf);

                        let expected_resp = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Success),
                        };

                        self.entries.push((key, val));
                        self.expected.push_back(expected_resp.to_vec());
                        let commit = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Commit,
                        };
                        let mut commit_buf = vec![0; commit.len()];
                        commit.write_to_buf(&mut commit_buf);
                        conn_bufs.from_conn.extend(commit_buf);

                        let e_r = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Success),
                        };
                        self.expected.push_back(e_r.to_vec());
                    }
                    "update" => {
                        let (key, val) = self.entries.choose_mut(&mut self.rng).unwrap();
                        let new_val_len = self.rng.random_range(self.val_len_range.clone());
                        let mut new_val = vec![0; new_val_len];
                        self.rng.fill(&mut new_val[..]);
                        *val = new_val;

                        let req = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Write(op::WriteOp::Set(op::SetOp {
                                key: &key,
                                val: &val,
                            })),
                        };
                        let mut reqbuf = vec![0; req.len()];
                        req.write_to_buf(&mut reqbuf);
                        conn_bufs.from_conn.extend(reqbuf);

                        let expected_resp = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Success),
                        };

                        self.expected.push_back(expected_resp.to_vec());
                        let commit = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Commit,
                        };
                        let mut commit_buf = vec![0; commit.len()];
                        commit.write_to_buf(&mut commit_buf);
                        conn_bufs.from_conn.extend(commit_buf);

                        let e_r = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Success),
                        };
                        self.expected.push_back(e_r.to_vec());
                    }
                    "get" => {
                        let (key, val) = self.entries.choose(&mut self.rng).unwrap();

                        let req = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Read(op::ReadOp::Get(op::GetOp { key: &key })),
                        };
                        let mut reqbuf = vec![0; req.len()];
                        req.write_to_buf(&mut reqbuf);
                        conn_bufs.from_conn.extend(reqbuf);

                        let expected_resp = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Get(Some(&val))),
                        };

                        self.expected.push_back(expected_resp.to_vec());
                        let commit = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Commit,
                        };
                        let mut commit_buf = vec![0; commit.len()];
                        commit.write_to_buf(&mut commit_buf);
                        conn_bufs.from_conn.extend(commit_buf);

                        let e_r = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Success),
                        };
                        self.expected.push_back(e_r.to_vec());
                    }
                    "del" => {
                        let idx = self.rng.random_range(0..self.entries.len());
                        let (key, _) = self.entries.remove(idx);

                        let req = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Write(op::WriteOp::Del(op::DelOp { key: &key })),
                        };
                        let mut reqbuf = vec![0; req.len()];
                        req.write_to_buf(&mut reqbuf);
                        conn_bufs.from_conn.extend(reqbuf);

                        let expected_resp = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Success),
                        };

                        self.expected.push_back(expected_resp.to_vec());
                        let commit = net::Request {
                            txn_id: format::ConnTxnId(1),
                            op: net::RequestOp::Commit,
                        };
                        let mut commit_buf = vec![0; commit.len()];
                        commit.write_to_buf(&mut commit_buf);
                        conn_bufs.from_conn.extend(commit_buf);

                        let e_r = net::Response {
                            txn_id: format::ConnTxnId(1),
                            op: Ok(net::Resp::Success),
                        };
                        self.expected.push_back(e_r.to_vec());
                    }
                    _ => panic!(),
                }
            }

            // try to clear expecteds
            while let Some(e) = self.expected.pop_front() {
                if conn_bufs.to_conn.len() >= e.len() {
                    let mut got = vec![0; e.len()];
                    conn_bufs.to_conn.read_exact(&mut got).unwrap();
                    assert_eq!(got, e);
                } else {
                    self.expected.push_front(e);
                    break;
                }
            }
            self.ticks += 1;
        }
    }
}

struct TestIO {
    id: usize,
    storage_manager: rc::Rc<cell::RefCell<TestStorageManager>>,
    network_manager: rc::Rc<cell::RefCell<TestNetworkManager>>,

    subs: Vec<io::Sub>,
    pending_subs: Vec<io::Sub>,
    comps: Vec<io::Comp>,
}
impl TestIO {
    fn process_subs(&mut self) {
        for sub in self.subs.drain(..) {
            match sub {
                io::Sub::FileRead { mut buf, offset } => {
                    println!("reading from file!");
                    let mut storage_manager = self.storage_manager.borrow_mut();
                    let file = storage_manager.shard_files.get_mut(&self.id).unwrap();
                    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                    file.read_exact(&mut buf).unwrap();
                    self.comps.push(io::Comp::FileRead { buf, offset });
                }
                io::Sub::FileWrite { buf, offset } => {
                    println!("writing to file!");
                    let mut storage_manager = self.storage_manager.borrow_mut();
                    let file = storage_manager.shard_files.get_mut(&self.id).unwrap();
                    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                    file.write_all(&buf).unwrap();
                    self.comps.push(io::Comp::FileWrite { buf });
                }
                io::Sub::TcpRead { mut buf, conn_id } => {
                    match self
                        .network_manager
                        .borrow_mut()
                        .conn_bufs
                        .get_mut(&conn_id)
                    {
                        Some(conn) => {
                            let bytes = conn.from_conn.read(&mut buf).unwrap();
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
                    }
                }
                io::Sub::TcpWrite { buf, conn_id } => match self
                    .network_manager
                    .borrow_mut()
                    .conn_bufs
                    .get_mut(&conn_id)
                {
                    Some(conn) => {
                        conn.to_conn.write_all(&buf).unwrap();
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
                io::Sub::Accept {} => {
                    let mut nm = self.network_manager.borrow_mut();
                    if let Some(conn_id) = nm.pending_conns.pop() {
                        nm.conn_bufs.insert(
                            conn_id,
                            ConnBufs {
                                from_conn: collections::VecDeque::new(),
                                to_conn: collections::VecDeque::new(),
                            },
                        );
                        self.comps.push(io::Comp::Accept { conn_id });
                    }
                }
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

pub struct ConnBufs {
    from_conn: collections::VecDeque<u8>,
    to_conn: collections::VecDeque<u8>,
}

pub struct TestStorageManager {
    shard_files: collections::BTreeMap<usize, std::io::Cursor<Vec<u8>>>,
}
pub struct TestNetworkManager {
    pending_conns: Vec<store::ConnId>,
    conn_bufs: collections::BTreeMap<store::ConnId, ConnBufs>,
}

/// a logical "node" in a network
pub struct Node<T: Tick> {
    workers: Vec<T>,
}

pub trait Tick {
    fn tick(&mut self);
}

pub enum StoreNodeWorker<IO: io::IOFace> {
    Central(central::Central<IO>),
    Shard(shard::Shard<IO>),
}

impl<IO: io::IOFace> Tick for StoreNodeWorker<IO> {
    fn tick(&mut self) {
        match self {
            Self::Central(central) => central.tick(),
            Self::Shard(shard) => shard.tick(),
        }
    }
}
