use std::{
    collections::{self},
    sync::{self, atomic},
};

use format::{Format, op};

use crate::{io, mesh};

/// this is the central coordinator for the entire system, it runs on the main thread, literally
/// like this:
/// ```rust
/// // main.rs
/// fn main() {
///
///     //
///     // a bunch of setup code
///     //
///
///     loop {
///         central.tick();
///     }
/// }
/// ```
pub struct Central<IO: io::IOFace> {
    pub committed_ts: sync::Arc<atomic::AtomicU64>,
    pub oldest_active_ts: sync::Arc<atomic::AtomicU64>,
    pub active_timestamps: collections::BTreeMap<format::Timestamp, u64>,
    pub recent_commits: collections::BTreeMap<format::Timestamp, collections::BTreeSet<Vec<u8>>>,
    pub txn_processor: TxnProcessor,
    /// ok so we need some incremental state for successful txns because they will involve waiting
    /// for io potentially, and waiting on responses from shards
    pub mesh: mesh::Mesh,
    pub io: IO,
}
impl<IO: io::IOFace> Central<IO> {
    pub fn new(
        committed_ts: sync::Arc<atomic::AtomicU64>,
        oldest_active_ts: sync::Arc<atomic::AtomicU64>,
        io: IO,
        mesh: mesh::Mesh,
    ) -> Self {
        Self {
            committed_ts,
            oldest_active_ts,
            io,
            mesh,
            txn_processor: TxnProcessor {
                queued_successful: collections::BTreeMap::new(),
            },
            active_timestamps: collections::BTreeMap::new(),
            recent_commits: collections::BTreeMap::new(),
        }
    }
    pub fn tick(&mut self) {
        // TODO: should probably move the initial accept logic to a setup function or something
        self.io.register_sub(io::Sub::Accept {});

        // handle any io results
        for comp in self.io.poll() {
            match comp {
                io::Comp::Accept { conn_id } => {
                    // just one shard for now, this will get swapped with load balancing logic
                    // eventually
                    self.mesh.push(mesh::Msg::NewConnection(conn_id), 1);
                }
                io::Comp::FileRead { .. } | io::Comp::FileWrite { .. } => todo!("wal stuff"),
                c => panic!("invalid io completion in central {:?}", c),
            }
        }

        let commit_timestamp =
            format::Timestamp(self.committed_ts.fetch_add(1, atomic::Ordering::Release));

        // read messages from shards
        let mut commits = Vec::new();
        let mut finished_writes = Vec::new();
        for (msgs, from) in self.mesh.poll().into_iter().zip(0_usize..) {
            for msg in msgs {
                match msg {
                    mesh::Msg::CommitRequest {
                        txn_id,
                        start_ts,
                        writes,
                    } => {
                        let mut keys_iter = format::FormatIter::<op::WriteOp>::from(&writes[..]);
                        if writes.len() == 0 {
                            self.mesh.push(
                                mesh::Msg::CommitResponse {
                                    txn_id,
                                    res: Ok(()),
                                },
                                from,
                            );
                        } else if keys_iter.any(|w| {
                            // check for conflicts
                            //
                            // if anything has been committed for any of the keys in this txn since
                            // it started, fail the commit (write-write conflict)
                            self.recent_commits
                                .range(format::Timestamp(start_ts.0 + 1)..)
                                .any(|(_, keys)| keys.contains(w.key()))
                        }) {
                            self.mesh.push(
                                mesh::Msg::CommitResponse {
                                    txn_id,
                                    res: Err(()),
                                },
                                from,
                            );
                        } else {
                            let commit_ts_writes = self
                                .recent_commits
                                .entry(commit_timestamp)
                                .or_insert(collections::BTreeSet::new());
                            for key in keys_iter {
                                commit_ts_writes.insert(key.to_vec());
                            }
                            commits.push(Commit {
                                txn_id: crate::GlobalTxnId {
                                    shard_txn_id: txn_id,
                                    shard_id: from,
                                },
                                commit_ts: commit_timestamp,
                                writes,
                            });
                        }

                        if let Some(count) = self.active_timestamps.get_mut(&start_ts) {
                            *count -= 1;
                            if *count == 0 {
                                self.active_timestamps.remove(&start_ts);
                            }
                        }
                    }
                    mesh::Msg::TxnStart(timestamp) => {
                        self.active_timestamps
                            .entry(timestamp)
                            .and_modify(|count| *count += 1)
                            .or_insert(1);
                    }
                    mesh::Msg::WriteResponse(commit) => {
                        finished_writes.push((from, commit));
                    }
                    m => panic!("invalid mesh message in central {:?}", m),
                }
            }
        }

        // update oldest active timestamp
        if let Some(oldest_active) = self.active_timestamps.first_key_value() {
            self.oldest_active_ts
                .store(oldest_active.0.0, atomic::Ordering::Release);
            self.recent_commits = self.recent_commits.split_off(oldest_active.0);
        }

        // process batch of commits and writes
        self.txn_processor
            .process_commits(commits, &mut self.mesh, 1);
        self.txn_processor
            .process_writes(finished_writes, &mut self.mesh);

        // submit any queued io
        self.io.submit();
    }
}

#[derive(Debug)]
pub struct Commit {
    pub txn_id: crate::GlobalTxnId,
    pub commit_ts: format::Timestamp,
    pub writes: Vec<u8>,
}

pub struct TxnProcessor {
    queued_successful: collections::BTreeMap<crate::GlobalTxnId, collections::BTreeSet<usize>>,
}
impl TxnProcessor {
    pub fn process_commits(
        &mut self,
        commits: Vec<Commit>,
        mesh: &mut mesh::Mesh,
        num_shards: usize,
    ) {
        // process successful batch
        for commit in commits {
            // TODO:
            // here we would be first writing to the wal, but we're not going to worry about
            // durability yet. we will also need checkpointing (which will probably interact with
            // shard mapping tables).

            // we need to iterate through all the writes, find the appropriate shard, send a write
            // message, and queue up waiting for the response
            let mut waiting_on = collections::BTreeSet::new();
            let mut shard_writes = collections::BTreeMap::<usize, Vec<u8>>::new();
            for write in format::FormatIter::<op::WriteOp>::from(&commit.writes[..]) {
                let shard = crate::find_shard(write.key(), num_shards);
                waiting_on.insert(shard);

                match shard_writes.get_mut(&shard) {
                    Some(writes) => {
                        let old_len = writes.len();
                        writes.resize(old_len + write.len(), 0);
                        write.write_to_buf(&mut writes[old_len..]);
                    }
                    None => {
                        shard_writes.insert(shard, write.to_vec());
                    }
                }
            }
            for (shard, writes) in shard_writes {
                mesh.push(
                    mesh::Msg::WriteRequest(Commit {
                        txn_id: commit.txn_id,
                        commit_ts: commit.commit_ts,
                        writes,
                    }),
                    shard,
                );
            }

            self.queued_successful.insert(commit.txn_id, waiting_on);

            // TODO:
            // when we go to implement the wal, we will also be handling fsyncs for the wal buffer,
            // specifically sending them at this point if we've decided to, this way most of the
            // waiting on the shards will be hidden by waiting on the fsync, which we have to do in
            // any situation
        }
    }

    pub fn process_writes(&mut self, writes: Vec<(usize, Commit)>, mesh: &mut mesh::Mesh) {
        for (shard, commit) in writes {
            let waiting_on = self.queued_successful.get_mut(&commit.txn_id).unwrap();
            waiting_on.remove(&shard);
            if waiting_on.is_empty() {
                self.queued_successful.remove(&commit.txn_id).unwrap();
                // TODO: we also will need to wait on WAL fsync
                mesh.push(
                    mesh::Msg::CommitResponse {
                        txn_id: commit.txn_id.shard_txn_id,
                        res: Ok(()),
                    },
                    commit.txn_id.shard_id,
                );
            }
        }
    }
}
