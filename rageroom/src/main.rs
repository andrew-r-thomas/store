use std::{
    cell, collections, fs,
    io::{Read, Seek, Write},
    mem, path, rc,
    sync::{self, atomic},
};

use clap::Parser;
use format::Format;
use rand::{Rng, SeedableRng, distr::Distribution, seq::IteratorRandom};
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
    pub num_conns: usize,
    pub num_txns: usize,
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

    let data_oracle = rc::Rc::new(cell::RefCell::new(collections::BTreeMap::new()));
    let mut query_node = Node {
        workers: Vec::from_iter((0..sim_config.num_conns as i32).map(|i| {
            TestConn::new(
                sim_config.num_txns,
                rng.random(),
                sim_config.key_len_range.clone(),
                sim_config.val_len_range.clone(),
                store::ConnId(i),
                network_manager.clone(),
                data_oracle.clone(),
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
    }

    println!("test completed successfully!");
}

struct TestConn {
    id: store::ConnId,
    network_manager: rc::Rc<cell::RefCell<TestNetworkManager>>,
    data_oracle: rc::Rc<cell::RefCell<collections::BTreeMap<Vec<u8>, Vec<u8>>>>,

    num_txns: usize,
    completed_txns: usize,
    active_txns: collections::BTreeMap<format::ConnTxnId, ActiveTxn>,
    next_txn_id: u64,

    key_len_range: std::ops::Range<usize>,
    val_len_range: std::ops::Range<usize>,
    rng: rand_chacha::ChaCha8Rng,
}
impl TestConn {
    fn new(
        num_txns: usize,
        seed: u64,
        key_len_range: std::ops::Range<usize>,
        val_len_range: std::ops::Range<usize>,
        id: store::ConnId,
        network_manager: rc::Rc<cell::RefCell<TestNetworkManager>>,
        data_oracle: rc::Rc<cell::RefCell<collections::BTreeMap<Vec<u8>, Vec<u8>>>>,
    ) -> Self {
        let rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

        {
            let mut nm = network_manager.borrow_mut();
            nm.pending_conns.push(id);
        }

        Self {
            id,
            network_manager,
            data_oracle,

            num_txns,
            completed_txns: 0,
            next_txn_id: 1,
            active_txns: collections::BTreeMap::new(),

            key_len_range,
            val_len_range,
            rng,
        }
    }
    fn is_done(&self) -> bool {
        self.completed_txns >= self.num_txns
    }
}
impl Tick for TestConn {
    fn tick(&mut self) {
        let mut nm = self.network_manager.borrow_mut();

        let mut data = self.data_oracle.borrow_mut();

        if let Some(buf) = nm.conn_bufs.get_mut(&self.id) {
            if let Some(txn) = {
                if self.rng.random() && !self.active_txns.is_empty() {
                    // make progress on an existing txn
                    let (_, txn) = self.active_txns.iter_mut().choose(&mut self.rng).unwrap();
                    if txn.committed { None } else { Some(txn) }
                } else if self.completed_txns + self.active_txns.len() < self.num_txns {
                    // start a new txn
                    let id = format::ConnTxnId(self.next_txn_id);
                    let txn = ActiveTxn {
                        id,
                        local_writes: Vec::new(),
                        num_sent: 0,
                        num_recv: 0,
                        committed: false,
                    };
                    self.active_txns.insert(txn.id, txn);
                    self.next_txn_id += 1;

                    Some(self.active_txns.get_mut(&id).unwrap())
                } else {
                    // done producing work
                    None
                }
            } {
                if let Some(request) = match self.rng.random::<ConnOp>() {
                    ConnOp::Get => {
                        if self.rng.random() && !txn.local_writes.is_empty() {
                            // read from local data
                            let write = format::FormatIter::<format::op::WriteOp>::from(
                                &txn.local_writes[..],
                            )
                            .choose(&mut self.rng)
                            .unwrap();
                            Some(
                                format::net::Request {
                                    txn_id: txn.id,
                                    op: format::net::RequestOp::Read(format::op::ReadOp::Get(
                                        format::op::GetOp { key: write.key() },
                                    )),
                                }
                                .to_vec(),
                            )
                        } else if let Some((key, _)) = data.iter().choose(&mut self.rng) {
                            // read from oracle (note that sometimes this will be for local keys)
                            Some(
                                format::net::Request {
                                    txn_id: txn.id,
                                    op: format::net::RequestOp::Read(format::op::ReadOp::Get(
                                        format::op::GetOp { key },
                                    )),
                                }
                                .to_vec(),
                            )
                        } else {
                            None
                        }
                    }
                    ConnOp::Insert => {
                        let key_len = self.rng.random_range(self.key_len_range.clone());
                        let val_len = self.rng.random_range(self.val_len_range.clone());
                        let mut key = vec![0; key_len];
                        let mut val = vec![0; val_len];
                        self.rng.fill(&mut key[..]);
                        self.rng.fill(&mut val[..]);

                        let op = format::op::WriteOp::Set(format::op::SetOp {
                            key: &key[..],
                            val: &val[..],
                        });

                        txn.local_writes.extend_from_slice(&op.to_vec());

                        Some(
                            format::net::Request {
                                txn_id: txn.id,
                                op: format::net::RequestOp::Write(op),
                            }
                            .to_vec(),
                        )
                    }
                    ConnOp::Update => {
                        if self.rng.random() && !txn.local_writes.is_empty() {
                            let write = format::FormatIter::<format::op::WriteOp>::from(
                                &txn.local_writes[..],
                            )
                            .choose(&mut self.rng)
                            .unwrap();

                            let val_len = self.rng.random_range(self.val_len_range.clone());
                            let mut val = vec![0; val_len];
                            self.rng.fill(&mut val[..]);

                            let op = format::op::WriteOp::Set(format::op::SetOp {
                                key: write.key(),
                                val: &val,
                            });

                            let out = format::net::Request {
                                txn_id: txn.id,
                                op: format::net::RequestOp::Write(op),
                            }
                            .to_vec();
                            txn.local_writes.extend_from_slice(&op.to_vec());

                            Some(out)
                        } else if let Some((key, _)) = data.iter().choose(&mut self.rng) {
                            let val_len = self.rng.random_range(self.val_len_range.clone());
                            let mut val = vec![0; val_len];
                            self.rng.fill(&mut val[..]);

                            let op = format::op::WriteOp::Set(format::op::SetOp { key, val: &val });

                            let out = format::net::Request {
                                txn_id: txn.id,
                                op: format::net::RequestOp::Write(op),
                            }
                            .to_vec();
                            txn.local_writes.extend_from_slice(&op.to_vec());

                            Some(out)
                        } else {
                            None
                        }
                    }
                    ConnOp::Delete => {
                        if self.rng.random() && !txn.local_writes.is_empty() {
                            let write = format::FormatIter::<format::op::WriteOp>::from(
                                &txn.local_writes[..],
                            )
                            .choose(&mut self.rng)
                            .unwrap();

                            let op =
                                format::op::WriteOp::Del(format::op::DelOp { key: write.key() });

                            let out = format::net::Request {
                                txn_id: txn.id,
                                op: format::net::RequestOp::Write(op),
                            }
                            .to_vec();
                            txn.local_writes.extend_from_slice(&op.to_vec());

                            Some(out)
                        } else if let Some((key, _)) = data.iter().choose(&mut self.rng) {
                            let op = format::op::WriteOp::Del(format::op::DelOp { key });

                            let out = format::net::Request {
                                txn_id: txn.id,
                                op: format::net::RequestOp::Write(op),
                            }
                            .to_vec();
                            txn.local_writes.extend_from_slice(&op.to_vec());

                            Some(out)
                        } else {
                            None
                        }
                    }
                    ConnOp::Commit => {
                        txn.committed = true;
                        Some(
                            format::net::Request {
                                txn_id: txn.id,
                                op: format::net::RequestOp::Commit,
                            }
                            .to_vec(),
                        )
                    }
                } {
                    tracing::info!(
                        conn_id = self.id.0,
                        txn_id = txn.id.0,
                        committed = txn.committed,
                        "issued request",
                        // request = ?format::net::Request::from_bytes(&request).unwrap(),
                    );
                    buf.from_conn.extend(request);
                    txn.num_sent += 1;
                }
            }

            let raw_buffer = buf.to_conn.drain(..).collect::<Vec<_>>();
            // deal with any responses we may have
            for response in format::FormatIter::<format::net::Response>::from(&raw_buffer[..]) {
                let txn = self.active_txns.get_mut(&response.txn_id).unwrap();
                txn.num_recv += 1;
                tracing::info!(
                    conn_id = self.id.0,
                    ?response,
                    num_recv = txn.num_recv,
                    num_sent = txn.num_sent
                );
                if txn.committed && txn.num_recv == txn.num_sent {
                    for write in
                        format::FormatIter::<format::op::WriteOp>::from(&txn.local_writes[..])
                    {
                        match write {
                            format::op::WriteOp::Set(set) => {
                                data.insert(Vec::from(set.key), Vec::from(set.val));
                            }
                            format::op::WriteOp::Del(del) => {
                                data.remove(del.key);
                            }
                        }
                    }
                    self.active_txns.remove(&response.txn_id);
                    self.completed_txns += 1;
                }
            }
        }
    }
}

struct ActiveTxn {
    id: format::ConnTxnId,
    local_writes: Vec<u8>,
    num_sent: usize,
    num_recv: usize,
    committed: bool,
}

enum ConnOp {
    Get,
    Insert,
    Update,
    Delete,
    Commit,
}
impl Distribution<ConnOp> for rand::distr::StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ConnOp {
        let i = rng.random_range(0..5);
        match i {
            0 => ConnOp::Get,
            1 => ConnOp::Insert,
            2 => ConnOp::Update,
            3 => ConnOp::Delete,
            4 => ConnOp::Commit,
            _ => panic!(),
        }
    }
}

#[derive(Debug)]
struct TestIO {
    id: usize,
    storage_manager: rc::Rc<cell::RefCell<TestStorageManager>>,
    network_manager: rc::Rc<cell::RefCell<TestNetworkManager>>,

    subs: Vec<io::Sub>,
    pending_subs: Vec<io::Sub>,
    comps: Vec<io::Comp>,
}
impl TestIO {
    #[tracing::instrument(skip(self))]
    fn process_subs(&mut self) {
        for sub in self.subs.drain(..) {
            match sub {
                io::Sub::FileRead { mut buf, offset } => {
                    let mut storage_manager = self.storage_manager.borrow_mut();
                    let file = storage_manager.shard_files.get_mut(&self.id).unwrap();
                    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                    file.read_exact(&mut buf).unwrap();
                    self.comps.push(io::Comp::FileRead { buf, offset });
                }
                io::Sub::FileWrite { buf, offset } => {
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
        mem::take(&mut self.comps)
    }
    fn register_sub(&mut self, sub: io::Sub) {
        self.pending_subs.push(sub);
    }
    fn submit(&mut self) {
        self.subs.extend(self.pending_subs.drain(..));
    }
}

#[derive(Debug)]
pub struct ConnBufs {
    from_conn: collections::VecDeque<u8>,
    to_conn: collections::VecDeque<u8>,
}

#[derive(Debug)]
pub struct TestStorageManager {
    shard_files: collections::BTreeMap<usize, std::io::Cursor<Vec<u8>>>,
}
#[derive(Debug)]
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
