use crate::{
    format::{Commit, Serialize, Write},
    root::Root,
};

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

pub struct TxnCoordinator {
    txn_chan: mpsc::Receiver<TxnReq>,

    current_ts: u64,
    committed_ts: u64,
    oldest_active_ts: Arc<AtomicU64>,

    root: Arc<Root>,

    active_writes: HashMap<Bytes, u64>,
    active_timestamps: BTreeMap<u64, u64>,
    pending_commits: BTreeMap<u64, Vec<oneshot::Sender<Result<(), ()>>>>,

    wal: Wal,
}
impl TxnCoordinator {
    pub fn new() -> Self {
        todo!()
    }

    pub async fn run(&mut self) {
        while let Some(first) = self.txn_chan.recv().await {
            self.current_ts += 1;

            let mut write_batch = Vec::new();
            let mut resp_sends = Vec::new();

            // process messages
            self.process_req(first, &mut write_batch, &mut resp_sends);
            while let Ok(req) = self.txn_chan.try_recv() {
                self.process_req(req, &mut write_batch, &mut resp_sends);
            }

            // commit batch
            if let Some(committed_ts) = self.wal.tick(self.current_ts, write_batch.clone()) {
                // wal fsync finished
                self.committed_ts = committed_ts;
                let mut committed = self.pending_commits.split_off(&(committed_ts + 1));
                mem::swap(&mut committed, &mut self.pending_commits);
                for (_, resp_sends) in committed {
                    for resp_send in resp_sends {
                        resp_send.send(Ok(())).unwrap();
                    }
                }
            }
            if !resp_sends.is_empty() {
                self.pending_commits.insert(self.current_ts, resp_sends);
            }
            self.root.install(self.current_ts, write_batch);

            // update oldest active ts
            let oldest_active = match self.active_timestamps.first_key_value() {
                Some((&oldest_active, _)) => oldest_active,
                None => self.committed_ts,
            };
            self.oldest_active_ts
                .store(oldest_active, Ordering::Release);

            // clean active writes
            let mut to_remove = Vec::new();
            for (key, ts) in &self.active_writes {
                if *ts < oldest_active {
                    to_remove.push(key.clone());
                }
            }
            for key in to_remove {
                self.active_writes.remove(&key);
            }
        }
    }

    fn process_req(
        &mut self,
        req: TxnReq,
        write_batch: &mut Vec<Write>,
        resp_sends: &mut Vec<oneshot::Sender<Result<(), ()>>>,
    ) {
        match req {
            TxnReq::Begin(begin_req) => {
                begin_req.resp_send.send(self.committed_ts).unwrap();
                self.active_timestamps
                    .entry(self.committed_ts)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }

            TxnReq::Commit(commit_req) => {
                self.dec_ts_count(commit_req.start_ts);
                for write in &commit_req.writes {
                    if let Some(&ts) = self.active_writes.get(&write.key()) {
                        if ts >= commit_req.start_ts {
                            commit_req.resp_send.send(Err(())).unwrap();
                            return;
                        }
                    }
                }
                for write in commit_req.writes {
                    self.active_writes.insert(write.key(), self.current_ts);
                    write_batch.push(write);
                }
                resp_sends.push(commit_req.resp_send);
            }

            TxnReq::Abort(ts) => self.dec_ts_count(ts),
        }
    }

    fn dec_ts_count(&mut self, ts: u64) {
        let count = self.active_timestamps.get_mut(&ts).unwrap();
        *count -= 1;
        if *count == 0 {
            self.active_timestamps.remove(&ts);
        }
    }
}

pub enum TxnReq {
    Begin(BeginReq),
    Commit(CommitReq),
    Abort(u64),
}
pub struct BeginReq {
    pub resp_send: oneshot::Sender<u64>,
}
pub struct CommitReq {
    pub writes: Vec<Write>,
    pub start_ts: u64,
    pub resp_send: oneshot::Sender<Result<(), ()>>,
}

struct Wal {
    io_bufs: Vec<Vec<u8>>,
    current_buf: usize,
    current_buf_head: usize,
    free_list: Vec<usize>,

    file: File,

    active_fsync: Option<()>,
}
impl Wal {
    fn tick(&mut self, ts: u64, writes: Vec<Write>) -> Option<u64> {
        let mut committed_ts = None;
        for entry in self.uring.completion() {
            todo!()
        }

        for write in writes {
            self.write_commit(Commit { ts, write });
        }

        if self.active_fsync.is_none() {
            // flush any buffered writes, and queue up another fsync
        }

        if !self.uring.submission().is_empty() {
            self.uring.submit();
        }

        committed_ts
    }

    fn write_commit(&mut self, commit: Commit) {
        let mut buf = &mut self.io_bufs[self.current_buf][self.current_buf_head..];
        let size = commit.size();
        if buf.len() < size {
            // queue write and swap for new buffer
            let entry = opcode::WriteFixed::new(
                self.fd,
                self.io_bufs[self.current_buf].as_ptr(),
                self.io_bufs[0].len() as u32,
                self.current_buf as u16,
            )
            .offset(self.file_head)
            .build()
            .flags(Flags::IO_LINK);

            unsafe {
                self.uring.submission().push(&entry);
            }

            self.file_head += self.io_bufs[0].len() as u64;
            self.current_buf = self.free_list.pop().unwrap();
            self.current_buf_head = 0;
            buf = &mut self.io_bufs[self.current_buf];
        }
        commit.write_to_buf(&mut buf[..size]);
        self.current_buf_head += size;
    }
}
