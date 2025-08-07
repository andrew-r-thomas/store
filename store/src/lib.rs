use std::hash::{Hash, Hasher};

pub mod central;
pub mod config;
pub mod io;
pub mod mesh;
pub mod page;
pub mod page_cache;
pub mod shard;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnId(pub i32);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardTxnId {
    pub conn_txn_id: format::ConnTxnId,
    pub conn_id: ConnId,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalTxnId {
    pub shard_txn_id: ShardTxnId,
    pub shard_id: usize,
}

/// ## NOTE
/// this function adds 1 to the final output,
/// since the 0 index is used for [`central::Central`]
#[inline]
pub fn find_shard(key: &[u8], num_shards: usize) -> usize {
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    key.hash(&mut hasher);
    let hash = hasher.finish();
    ((hash as usize) % num_shards) + 1
}
