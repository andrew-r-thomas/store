pub mod background;
pub mod block_server;
pub mod config;
pub mod format;
pub mod root;

use crate::{
    block_server::PageServer,
    format::{Del, Entries, Format, InnerEntries, LeafEntries, PageChunkData, Put, Write},
    root::{GetRes, Root},
};

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub struct Store {
    root: Arc<Root>,
    page_server: PageServer,
    current_ts: Arc<AtomicU64>,
}
impl Store {
    pub fn begin_txn(&self) -> Txn {
        Txn {
            writes: Vec::new(),
            ts: self.current_ts.load(Ordering::Acquire),

            root: self.root.clone(),
            page_server: self.page_server.clone(),

            committed: false,
        }
    }
}

pub struct Txn {
    writes: Vec<Vec<u8>>,
    ts: u64,

    root: Arc<Root>,
    page_server: PageServer,

    committed: bool,
}
impl Txn {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ()> {
        for bytes in self.writes.iter().rev() {
            match Write::from_bytes(bytes) {
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

        let mut pid = match self.root.get(key, self.ts) {
            GetRes::Val(val) => return Ok(val),
            GetRes::Pid(pid) => pid,
        };

        // search inner pages
        let mut level = self.page_server.levels.len();
        while level > 0 {
            let mut page = self.page_server.get_page(pid, level);
            while let Some(chunk) = page.next().await? {
                match chunk.data::<InnerEntries>() {
                    PageChunkData::Commits(commits) => {
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
                    PageChunkData::Smops(smops) => {
                        for smop in smops {
                            if key > smop.gt_key && key <= smop.lte_key {
                                pid = smop.pid;
                            }
                        }
                    }
                    PageChunkData::Entries(entries) => {
                        pid = entries.search(key);
                    }
                }
            }
            level -= 1;
        }

        // search leaf page
        let mut page = self.page_server.get_page(pid, level);
        while let Some(chunk) = page.next().await? {
            match chunk.data::<LeafEntries>() {
                PageChunkData::Commits(commits) => {
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
                PageChunkData::Entries(entries) => {
                    if let Some(val) = entries.search(key) {
                        return Ok(Some(val.into()));
                    }
                }
                _ => panic!(),
            }
        }

        Ok(None)
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.write(Write::Put(Put { key, val }));
    }
    pub fn delete(&mut self, key: &[u8]) {
        self.write(Write::Del(Del { key }));
    }
    pub fn write(&mut self, write: Write) {
        self.writes.push(write.to_vec());
    }

    pub async fn commit(self) -> Result<(), ()> {
        todo!()
    }
}
impl Drop for Txn {
    fn drop(&mut self) {
        if !self.committed {
            todo!()
        }
    }
}
