pub mod central;
pub mod format;
pub mod io;
pub mod mesh;
pub mod page;
pub mod shard;
pub mod txn;

#[cfg(test)]
pub mod test;

#[derive(Copy, Clone, Debug, Ord, Eq, PartialEq, PartialOrd)]
pub struct PageId(u64);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnId(u32);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardId(usize);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnTxnId(u64);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardTxnId {
    conn_txn_id: ConnTxnId,
    conn_id: ConnId,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalTxnId {
    shard_txn_id: ShardTxnId,
    shard_id: ShardId,
}
