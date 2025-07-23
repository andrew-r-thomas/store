#[derive(Clone, Debug)]
pub struct Transaction {
    pub id: u64,
    pub start_ts: u64,
    pub writes: Vec<Vec<u8>>, // these are just deltas
}
