use crate::txn;

pub trait Mesh {
    fn poll(&mut self) -> Vec<Vec<Msg>>;
    fn push(&mut self, msg: Msg, to: usize);
}
#[derive(Clone, Debug)]
pub enum Msg {
    NewConnection(u32),
    CommitRequest(txn::Transaction),
    CommitResponse { txn_id: u64, success: bool },
    TxnStart(u64),
}
