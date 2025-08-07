use std::{
    collections,
    sync::{self, atomic},
    u64,
};

use format::Format;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{self, ToSocketAddrs},
    sync::{mpsc, oneshot},
    task,
};

pub struct Client {
    conns: Vec<Conn>,
}
impl Client {
    pub async fn new<A: ToSocketAddrs>(addr: A, num_conns: usize) -> Self {
        let mut conns = Vec::new();
        for _ in 0..num_conns {
            conns.push(Conn::new(&addr).await);
        }
        Self { conns }
    }
    pub fn begin_txn(&self) -> Txn {
        let mut min = u64::MAX;
        let mut min_idx = 0;
        for (conn, i) in self.conns.iter().zip(0..) {
            let next_id = conn.next_txn_id.load(atomic::Ordering::Acquire);
            if next_id < min {
                min = next_id;
                min_idx = i;
            }
        }
        let id = format::ConnTxnId(
            self.conns[min_idx]
                .next_txn_id
                .fetch_add(1, atomic::Ordering::AcqRel),
        );
        Txn {
            id,
            send: self.conns[min_idx].send.clone(),
        }
    }
}

pub struct Conn {
    write_handle: task::JoinHandle<()>,
    read_handle: task::JoinHandle<()>,
    send: mpsc::UnboundedSender<TxnRequest>,
    next_txn_id: sync::Arc<atomic::AtomicU64>,
}
impl Conn {
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let (r, w) = net::TcpStream::connect(addr).await.unwrap().into_split();
        let (txn_send, mut txn_recv) = mpsc::unbounded_channel::<TxnRequest>();
        let (write_send, mut read_recv) = mpsc::unbounded_channel::<TxnRequest>();
        let read_handle = tokio::spawn(async move {
            let mut txns = collections::HashMap::<format::ConnTxnId, TxnRequest>::new();
            let mut reader = io::BufReader::new(r);
            let mut buf = vec![0; 1024];
            loop {
                for _ in 0..read_recv.len() {
                    let txn_req = read_recv.recv().await.unwrap();
                    txns.insert(txn_req.id, txn_req);
                }

                buf.fill(0);
                let n = reader.read(&mut buf).await.unwrap();
                for response in format::FormatIter::<format::net::Response>::from(&buf[..n]) {
                    let mut txn_req = txns.remove(&response.txn_id).unwrap();
                    txn_req.buf.resize(response.op.len(), 0);
                    response.op.write_to_buf(&mut txn_req.buf);
                    txn_req.send.send(txn_req.buf).unwrap();
                }
            }
        });
        let write_handle = tokio::spawn(async move {
            let mut writer = io::BufWriter::new(w);
            while let Some(txn_req) = txn_recv.recv().await {
                writer.write_all(&txn_req.buf).await.unwrap();
                write_send.send(txn_req).unwrap();
            }
        });

        Self {
            write_handle,
            read_handle,
            send: txn_send,
            next_txn_id: sync::Arc::new(atomic::AtomicU64::new(1)),
        }
    }
}
impl Drop for Conn {
    fn drop(&mut self) {
        self.write_handle.abort();
        self.read_handle.abort();
    }
}

pub struct TxnRequest {
    id: format::ConnTxnId,
    buf: Vec<u8>,
    send: oneshot::Sender<Vec<u8>>,
}

pub struct Txn {
    id: format::ConnTxnId,
    send: mpsc::UnboundedSender<TxnRequest>,
}
impl Txn {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, format::Error> {
        let (send, recv) = oneshot::channel();
        self.send
            .send(TxnRequest {
                id: self.id,
                buf: format::net::Request {
                    txn_id: self.id,
                    op: format::net::RequestOp::Read(format::op::ReadOp::Get(format::op::GetOp {
                        key,
                    })),
                }
                .to_vec(),
                send,
            })
            .unwrap();
        let resp = recv.await.unwrap();
        let res = Result::<format::net::Resp, format::Error>::from_bytes(&resp).unwrap();
        match res {
            Ok(r) => match r {
                format::net::Resp::Get(g) => Ok(g.map(|s| Vec::from(s))),
                _ => panic!(),
            },
            Err(e) => Err(e),
        }
    }
    pub async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), format::Error> {
        let (send, recv) = oneshot::channel();
        self.send
            .send(TxnRequest {
                id: self.id,
                buf: format::net::Request {
                    txn_id: self.id,
                    op: format::net::RequestOp::Write(format::op::WriteOp::Set(
                        format::op::SetOp { key, val },
                    )),
                }
                .to_vec(),
                send,
            })
            .unwrap();
        let resp = recv.await.unwrap();
        let res = Result::<format::net::Resp, format::Error>::from_bytes(&resp).unwrap();
        match res {
            Ok(r) => match r {
                format::net::Resp::Success => Ok(()),
                _ => panic!(),
            },
            Err(e) => Err(e),
        }
    }
    pub async fn del(&self, key: &[u8]) -> Result<(), format::Error> {
        let (send, recv) = oneshot::channel();
        self.send
            .send(TxnRequest {
                id: self.id,
                buf: format::net::Request {
                    txn_id: self.id,
                    op: format::net::RequestOp::Write(format::op::WriteOp::Del(
                        format::op::DelOp { key },
                    )),
                }
                .to_vec(),
                send,
            })
            .unwrap();
        let resp = recv.await.unwrap();
        let res = Result::<format::net::Resp, format::Error>::from_bytes(&resp).unwrap();
        match res {
            Ok(r) => match r {
                format::net::Resp::Success => Ok(()),
                _ => panic!(),
            },
            Err(e) => Err(e),
        }
    }
    pub async fn commit(self) -> Result<(), format::Error> {
        let (send, recv) = oneshot::channel();
        self.send
            .send(TxnRequest {
                id: self.id,
                buf: format::net::Request {
                    txn_id: self.id,
                    op: format::net::RequestOp::Commit,
                }
                .to_vec(),
                send,
            })
            .unwrap();
        let resp = recv.await.unwrap();
        let res = Result::<format::net::Resp, format::Error>::from_bytes(&resp).unwrap();
        match res {
            Ok(r) => match r {
                format::net::Resp::Success => Ok(()),
                _ => panic!(),
            },
            Err(e) => Err(e),
        }
    }
}
