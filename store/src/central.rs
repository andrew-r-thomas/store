// use std::{
//     collections::{self, BTreeSet},
//     hash::{Hash, Hasher},
//     sync::{self, atomic},
// };
//
// use crate::{
//     io::{self, IOFace},
//     mesh, page, ticker, txn,
// };
//
// /// this is the central coordinator for the entire system, it runs on the main thread, literally
// /// like this:
// /// ```rust
// /// // main.rs
// /// fn main() {
// ///
// ///     //
// ///     // a bunch of setup code
// ///     //
// ///
// ///     loop {
// ///         central.tick();
// ///     }
// /// }
// /// ```
// pub struct Central<M: mesh::Mesh, IO: io::IOFace> {
//     timestamp: sync::Arc<atomic::AtomicU64>,
//     /// map from timestamp to (count, writes)
//     /// when count == 0, we remove (no more active txns which started at this ts)
//     /// writes are used for conflict detection when processing commit requests
//     active_txns: collections::BTreeMap<u64, (u64, collections::BTreeSet<Vec<u8>>)>,
//     txn_processor: TxnProcessor,
//     /// ok so we need some incremental state for successful txns because they will involve waiting
//     /// for io potentially, and waiting on responses from shards
//     mesh: M,
//     io: IO,
// }
// impl<M: mesh::Mesh, IO: io::IOFace> Central<M, IO> {
//     pub fn tick(&mut self) {
//         // handle any io results
//         for comp in self.io.poll() {
//             match comp {
//                 io::Comp::Accept { conn_id } => {
//                     // just one shard for now, this will get swapped with load balancing logic
//                     // eventually
//                     self.mesh.push(mesh::Msg::NewConnection(conn_id), 1);
//
//                     self.io.register_sub(io::Sub::Accept {});
//                 }
//                 io::Comp::FileRead { .. } => {}
//                 io::Comp::FileWrite { .. } => {}
//                 c => panic!("invalid io completion in central {:?}", c),
//             }
//         }
//
//         // read messages from shards
//         let mut commits = Vec::new();
//         let mut writes = Vec::new();
//         for (msgs, from) in self.mesh.poll().into_iter().zip(0_usize..) {
//             for msg in msgs {
//                 match msg {
//                     mesh::Msg::CommitRequest(txn) => {
//                         commits.push((from, txn));
//                     }
//                     mesh::Msg::TxnStart(timestamp) => {
//                         self.active_txns
//                             .entry(timestamp)
//                             .and_modify(|(count, _)| *count += 1)
//                             .or_insert((1, collections::BTreeSet::new()));
//                     }
//                     mesh::Msg::WriteResponse(write_response) => {
//                         writes.push((from, write_response));
//                     }
//                     m => panic!("invalid mesh message in central {:?}", m),
//                 }
//             }
//         }
//
//         // process batch of commits
//         let commit_timestamp = self.timestamp.fetch_add(1, atomic::Ordering::Release);
//         self.txn_processor.process_commits(
//             commits,
//             commit_timestamp,
//             &mut self.active_txns,
//             &mut self.mesh,
//             1,
//         );
//         self.txn_processor.process_writes(writes, &mut self.mesh);
//
//         // submit any queued io
//         self.io.submit();
//     }
// }
//
// pub struct TxnProcessor {
//     queued_successful: collections::BTreeMap<u64, Commit>,
// }
// impl TxnProcessor {
//     pub fn process_commits<M: mesh::Mesh>(
//         &mut self,
//         commits: Vec<(usize, txn::Transaction)>,
//         commit_timestamp: u64,
//         active_txns: &mut collections::BTreeMap<u64, (u64, collections::BTreeSet<Vec<u8>>)>,
//         mesh: &mut M,
//         num_shards: usize,
//     ) {
//         let mut successful = Vec::new();
//         let mut failed = Vec::new();
//         for (from, commit) in commits {
//             let commit_start_ts = commit.start_ts;
//             let commit_keys = commit
//                 .writes
//                 .iter()
//                 .map(|w| Vec::from(page::Delta::from_top(w).key()))
//                 .collect::<Vec<Vec<u8>>>();
//
//             if active_txns
//                 .range(commit.start_ts + 1..)
//                 .any(|(_, (_, writes))| commit_keys.iter().any(|key| writes.contains(key)))
//             {
//                 // TODO: maybe move this to above this function, and just pass in vec of
//                 // [`Commit`]s
//                 failed.push((from, commit));
//             } else {
//                 let (_, commit_ts_writes) = active_txns
//                     .entry(commit_timestamp)
//                     .or_insert((0, collections::BTreeSet::new()));
//                 for key in commit_keys {
//                     commit_ts_writes.insert(key);
//                 }
//                 successful.push((from, commit));
//             }
//
//             if let Some((count, _)) = active_txns.get_mut(&commit_start_ts) {
//                 *count -= 1;
//                 if *count == 0 {
//                     active_txns.remove(&commit_start_ts);
//                 }
//             }
//         }
//
//         // tell owning shards about failed txns
//         for (from, commit) in failed {
//             mesh.push(
//                 mesh::Msg::CommitResponse {
//                     txn: commit,
//                     success: false,
//                 },
//                 from,
//             );
//         }
//
//         // process successful batch
//         for (from, mut commit) in successful {
//             // TODO:
//             // here we would be first writing to the wal, but we're not going to worry about
//             // durability yet. we will also need checkpointing (which will probably interact with
//             // shard mapping tables).
//
//             // we need to iterate through all the writes, find the appropriate shard, send a write
//             // message, and queue up waiting for the response
//             let mut waiting_on = collections::BTreeSet::new();
//             let mut shard_writes = collections::BTreeMap::<usize, Vec<Vec<u8>>>::new();
//             for write in commit.writes.drain(..) {
//                 let shard = find_shard(page::Delta::from_top(&write).key(), num_shards);
//                 waiting_on.insert(shard);
//
//                 match shard_writes.get_mut(&shard) {
//                     Some(writes) => writes.push(write),
//                     None => {
//                         shard_writes.insert(shard, vec![write]);
//                     }
//                 }
//             }
//             for (shard, writes) in shard_writes {
//                 mesh.push(
//                     mesh::Msg::WriteRequest {
//                         txn_id: commit.id,
//                         deltas: writes,
//                     },
//                     shard,
//                 );
//             }
//             self.queued_successful.insert(
//                 commit.id,
//                 Commit {
//                     from,
//                     waiting_on,
//                     txn: commit,
//                 },
//             );
//
//             // TODO:
//             // when we go to implement the wal, we will also be handling fsyncs for the wal buffer,
//             // specifically sending them at this point if we've decided to, this way most of the
//             // waiting on the shards will be hidden by waiting on the fsync, which we have to do in
//             // any situation
//         }
//     }
//
//     pub fn process_writes<M: mesh::Mesh>(
//         &mut self,
//         writes: Vec<(usize, Result<(u64, Vec<Vec<u8>>), ()>)>,
//         mesh: &mut M,
//     ) {
//         for (shard, write) in writes {
//             match write {
//                 Ok((txn_id, mut deltas)) => {
//                     let commit = self.queued_successful.get_mut(&txn_id).unwrap();
//                     commit.txn.writes.extend(deltas.drain(..));
//                     commit.waiting_on.remove(&shard);
//                     if commit.waiting_on.is_empty() {
//                         let commit = self.queued_successful.remove(&txn_id).unwrap();
//                         // TODO: we also will need to wait on WAL fsync
//                         mesh.push(
//                             mesh::Msg::CommitResponse {
//                                 txn: commit.txn,
//                                 success: true,
//                             },
//                             commit.from,
//                         );
//                     }
//                 }
//                 Err(_) => todo!("implement central error handling for txn write failures"),
//             }
//         }
//     }
// }
// pub struct Commit {
//     from: usize,
//     waiting_on: collections::BTreeSet<usize>,
//     txn: txn::Transaction,
// }
//
// /// NOTE this function adds 1 to the final output, since the 0 index is used for [`Central`]
// pub fn find_shard(key: &[u8], num_shards: usize) -> usize {
//     let mut hasher = twox_hash::XxHash64::with_seed(0);
//     key.hash(&mut hasher);
//     let hash = hasher.finish();
//     ((hash as usize) % num_shards) + 1
// }
