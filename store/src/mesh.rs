use crate::central;

/// ## PERF
/// these are all messages dealing with single units of data, they should be batched, this will
/// mean we need to slightly modify some existing code
#[derive(Debug)]
pub enum Msg {
    NewConnection(crate::ConnId),

    TxnStart(crate::Timestamp),
    CommitRequest {
        txn_id: crate::ShardTxnId,
        start_ts: crate::Timestamp,
        writes: Vec<u8>,
    },
    CommitResponse {
        txn_id: crate::ShardTxnId,
        res: Result<(), ()>,
    },
    WriteRequest(central::Commit),
    WriteResponse(central::Commit),
}

pub struct Mesh {
    to: Vec<rtrb::Producer<Msg>>,
    from: Vec<rtrb::Consumer<Msg>>,
}
impl Mesh {
    pub fn new(queue_size: usize, num_shards: usize) -> Vec<Self> {
        let total_participants = num_shards + 1;

        let mut selves: Vec<Self> = (0..total_participants)
            .map(|_| Self {
                to: Vec::with_capacity(total_participants),
                from: Vec::with_capacity(total_participants),
            })
            .collect();

        for y in 0..total_participants {
            for x in 0..total_participants {
                let size = if x == y { 0 } else { queue_size };
                let (to_x, from_y) = rtrb::RingBuffer::<Msg>::new(size);

                selves[y].to.push(to_x);
                selves[x].from.push(from_y);
            }
        }

        selves
    }
    pub fn poll(&mut self) -> Vec<Vec<Msg>> {
        Vec::from_iter((0..self.from.len()).map(|i| {
            Vec::from_iter((0..self.from[i].slots()).map(|_| self.from[i].pop().unwrap()))
        }))
    }
    pub fn push(&mut self, msg: Msg, to: usize) {
        self.to[to].push(msg).unwrap();
    }
}
