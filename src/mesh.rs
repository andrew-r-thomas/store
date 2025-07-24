use crate::txn;

pub trait Mesh {
    fn poll(&mut self) -> Vec<Vec<Msg>>;
    fn push(&mut self, msg: Msg, to: usize);
}

/// ## PERF
/// these are all messages dealing with single units of data, they should be batched, this will
/// mean we need to slightly modify some existing code
#[derive(Clone, Debug)]
pub enum Msg {
    NewConnection(u32),

    TxnStart(u64),
    CommitRequest(txn::Transaction),
    CommitResponse {
        txn: txn::Transaction,
        success: bool,
    },
    WriteRequest {
        txn_id: u64,
        deltas: Vec<Vec<u8>>,
    },
    WriteResponse(Result<(u64, Vec<Vec<u8>>), ()>),
}
