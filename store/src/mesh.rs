pub trait Mesh {
    fn poll(&mut self) -> Vec<Vec<Msg>>;
    fn push(&mut self, msg: Msg, to: usize);
}

/// ## PERF
/// these are all messages dealing with single units of data, they should be batched, this will
/// mean we need to slightly modify some existing code
#[derive(Debug)]
pub enum Msg {
    NewConnection(crate::ConnId),

    TxnStart(crate::Timestamp),
    CommitRequest {
        txn_id: crate::ShardTxnId,
        writes: Vec<u8>,
    },
    CommitResponse {
        txn_id: crate::ShardTxnId,
        writes: Vec<u8>,
        res: Result<(), ()>,
    },
    WriteRequest(Commit),
    WriteResponse {
        txn_id: crate::ShardTxnId,
        writes: Vec<u8>,
    },
}

#[derive(Debug)]
pub struct Commit {
    pub txn_id: crate::ShardTxnId,
    pub ts: crate::Timestamp,
    pub writes: Vec<u8>,
}
