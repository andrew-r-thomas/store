pub mod format;
pub mod io;
pub mod root;
pub mod txn;
pub mod zipper;

use crate::{
    format::{Del, Entries, InnerEntries, LeafEntries, Put, Write},
    io::{IOHandle, PageId},
    root::{GetRes, Root},
    txn::{BeginReq, CommitReq, TxnReq},
};

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct Store {
    root: Arc<Root>,
    io_handle: IOHandle,
    txn_chan: mpsc::Sender<TxnReq>,
}
impl Store {
    pub async fn open() -> Self {
        todo!()
    }
    pub async fn create() -> Self {
        todo!()
    }
    pub async fn begin_txn(&self) -> Txn {
        let (resp_send, resp_recv) = oneshot::channel();
        self.txn_chan
            .send(TxnReq::Begin(BeginReq { resp_send }))
            .await
            .unwrap();
        let ts = resp_recv.await.unwrap();
        Txn {
            writes: Vec::new(),
            ts,

            root: self.root.clone(),
            io_handle: self.io_handle.clone(),
            txn_chan: self.txn_chan.clone(),
        }
    }
}
pub struct Config {}

pub struct Txn {
    writes: Vec<Write>,
    ts: u64,

    root: Arc<Root>,
    io_handle: IOHandle,
    txn_chan: mpsc::Sender<TxnReq>,
}
impl Txn {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ()> {
        // check local writes
        for write in self.writes.iter().rev() {
            match write {
                Write::Put(put) => {
                    if put.key == key {
                        return Ok(Some(put.val.clone().into()));
                    }
                }
                Write::Del(del) => {
                    if del.key == key {
                        return Ok(None);
                    }
                }
            }
        }

        // check root
        let mut pid = match self.root.get(key, self.ts) {
            GetRes::Val(val) => return Ok(val),
            GetRes::Pid(pid) => pid,
        };

        // search inner pages
        let mut level = self.io_handle.num_levels() as u8;
        'traverse: while level > 0 {
            let page_id = PageId { pid, level };
            level -= 1;

            // check commits
            let mut commit_chain = self.io_handle.get_commits(page_id);
            while let Some(commits) = commit_chain.next().await {
                for commit in commits {
                    if commit.ts <= self.ts {
                        match commit.write {
                            Write::Put(put) => {
                                if put.key == key {
                                    return Ok(Some(put.val.into()));
                                }
                            }
                            Write::Del(del) => {
                                if del.key == key {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
            }

            // check smops
            let mut smop_chain = self.io_handle.get_smops(page_id);
            while let Some(smops) = smop_chain.next().await {
                for smop in smops {
                    if key > smop.gt_key && key <= smop.lte_key {
                        pid = smop.pid;
                        continue 'traverse;
                    }
                }
            }

            // search entries
            pid = self
                .io_handle
                .get_entries::<InnerEntries>(page_id)
                .await
                .search(key);
        }

        // search leaf page
        let page_id = PageId { pid, level: 0 };

        // check commits
        let mut commit_chain = self.io_handle.get_commits(page_id);
        while let Some(commits) = commit_chain.next().await {
            for commit in commits {
                if commit.ts <= self.ts {
                    match commit.write {
                        Write::Put(put) => {
                            if put.key == key {
                                return Ok(Some(put.val.into()));
                            }
                        }
                        Write::Del(del) => {
                            if del.key == key {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }
        // search entries
        if let Some(val) = self
            .io_handle
            .get_entries::<LeafEntries>(page_id)
            .await
            .search(key)
        {
            return Ok(Some(val.into()));
        }

        Ok(None)
    }

    pub fn put(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.write(Write::Put(Put {
            key: Bytes::from_owner(key),
            val: Bytes::from_owner(val),
        }));
    }
    pub fn delete(&mut self, key: Vec<u8>) {
        self.write(Write::Del(Del {
            key: Bytes::from_owner(key),
        }));
    }
    pub fn write(&mut self, write: Write) {
        self.writes.push(write);
    }

    pub async fn commit(self) -> Result<(), ()> {
        let (resp_send, resp_recv) = oneshot::channel();
        self.txn_chan
            .send(TxnReq::Commit(CommitReq {
                writes: self.writes,
                start_ts: self.ts,
                resp_send,
            }))
            .await
            .unwrap();
        resp_recv.await.unwrap()
    }
}
