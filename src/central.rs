use std::{
    collections,
    sync::{self, atomic},
};

use crate::{io, mesh, page};

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
pub struct Central<M: mesh::Mesh, IO: io::IOFace> {
    timestamp: sync::Arc<atomic::AtomicU64>,
    active_txns: collections::BTreeMap<u64, (u64, collections::BTreeSet<Vec<u8>>)>,
    mesh: M,
    io: IO,
}
impl<M: mesh::Mesh, IO: io::IOFace> Central<M, IO> {
    pub fn tick(&mut self) {
        // handle any io results
        for comp in self.io.poll() {
            match comp {
                io::Comp::Accept { conn_id } => {
                    // just one shard for now, this will get swapped with load balancing logic
                    // eventually
                    self.mesh.push(mesh::Msg::NewConnection(conn_id), 1);

                    self.io.register_sub(io::Sub::Accept {});
                }
                io::Comp::FileRead { .. } => {}
                io::Comp::FileWrite { .. } => {}
                c => panic!("invalid io completion in central {:?}", c),
            }
        }

        // read messages from shards
        let mut commits = Vec::new();
        for (msgs, from) in self.mesh.poll().into_iter().zip(0_usize..) {
            for msg in msgs {
                match msg {
                    mesh::Msg::CommitRequest(txn) => {
                        commits.push((from, txn));
                    }
                    mesh::Msg::TxnStart(timestamp) => {
                        self.active_txns
                            .entry(timestamp)
                            .and_modify(|(count, _)| *count += 1)
                            .or_insert((1, collections::BTreeSet::new()));
                    }
                    m => panic!("invalid mesh message in central {:?}", m),
                }
            }
        }

        // process batch of commits
        for (from, txn) in commits {
            let mut ts_range = self.active_txns.range_mut(txn.start_ts..);
            let (_, (first_count, first_committed_keys)) = ts_range.next().unwrap();
            for write in txn.writes {
                let delta = page::Delta::from_top(&write);
                if first_committed_keys.contains(delta.key()) {
                    self.mesh.push(
                        mesh::Msg::CommitResponse {
                            txn_id: txn.id,
                            success: false,
                        },
                        from,
                    );
                }
            }

            *first_count -= 1;
            if *first_count == 0 {
                self.active_txns.remove(&txn.start_ts);
            }
        }

        // submit any queued io
        self.io.submit();
    }
}
