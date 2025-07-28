#[derive(Clone, Debug)]
pub struct Transaction {
    /// this id is given by the client, we can assume it safely differentiates from other
    /// transactons
    pub id: u64,
    pub start_ts: u64,
    pub writes: Vec<Vec<u8>>, // these are just deltas
}
