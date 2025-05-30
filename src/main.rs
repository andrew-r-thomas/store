pub mod cache;
pub mod page;
pub mod shard;

use std::{
    ops::Range,
    sync::mpsc::{self, Receiver, Sender},
    thread,
};

use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};
use rand_chacha::ChaCha8Rng;

type PageId = u64;

const PAGE_SIZE: usize = 1024 * 1024;
const BLOCK_SIZE: usize = 4 * 1024 * 1024;

const BUF_POOL_SIZE: usize = 1024;
const FREE_CAP_TARGET: usize = 256;

const NUM_INGEST: usize = 1024;
const NUM_OPS: usize = 2048;

const NUM_CONNS: usize = 128;

const KEY_LEN_RANGE: Range<usize> = 128..256;
const VAL_LEN_RANGE: Range<usize> = 512..1024;

const OFFSET_SIZE: usize = 8;
const CHUNK_LEN_SIZE: usize = 8;

fn main() {
    let (conn_send, conn_recv) = mpsc::channel();
    thread::Builder::new()
        .name("shard".into())
        .spawn(|| {
            shard::shard(
                conn_recv,
                PAGE_SIZE,
                BUF_POOL_SIZE,
                BLOCK_SIZE,
                FREE_CAP_TARGET,
            )
        })
        .unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut conns = Vec::new();
    loop {
        if rng.random() {
            let (c_send, s_recv) = mpsc::channel();
            let (s_send, c_recv) = mpsc::channel();
            let seed = conns.len() as u64 ^ 69;
            conns.push(
                thread::Builder::new()
                    .name(format!("conn {}", conns.len()))
                    .spawn(move || {
                        conn(
                            seed,
                            NUM_INGEST,
                            NUM_OPS,
                            KEY_LEN_RANGE,
                            VAL_LEN_RANGE,
                            c_send,
                            c_recv,
                        )
                    })
                    .unwrap(),
            );
            conn_send
                .send(Conn {
                    send: s_send,
                    recv: s_recv,
                })
                .unwrap();

            if conns.len() >= NUM_CONNS {
                break;
            }
        }
    }

    for c in conns {
        if let Err(e) = c.join() {
            println!("error on join: {:?}", e);
        }
    }
}

fn conn(
    seed: u64,
    num_ingest: usize,
    num_ops: usize,
    key_len_range: Range<usize>,
    val_len_range: Range<usize>,

    send: Sender<Vec<u8>>,
    recv: Receiver<Vec<u8>>,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut entries = Vec::new();

    for _ in 0..num_ingest {
        let key_len = rng.random_range(key_len_range.clone());
        let val_len = rng.random_range(val_len_range.clone());
        let mut key = vec![0; key_len];
        let mut val = vec![0; val_len];
        rng.fill(&mut key[..]);
        rng.fill(&mut val[..]);

        entries.push((key.clone(), val.clone()));

        let mut req_buf = Vec::new();
        req_buf.push(2);
        req_buf.extend(&(key_len as u32).to_be_bytes());
        req_buf.extend(&(val_len as u32).to_be_bytes());
        req_buf.extend(&key);
        req_buf.extend(&val);
        send.send(req_buf).unwrap();

        match recv.recv() {
            Ok(resp) => {
                assert_eq!(resp[0], 0);
            }
            _ => panic!(),
        }
    }

    let ops = ["get", "insert", "update"];
    for _ in 0..num_ops {
        if rng.random_bool(0.05) {
            return;
        }
        match *ops.choose(&mut rng).unwrap() {
            "get" => {
                let (k, v) = entries.choose(&mut rng).unwrap();
                let mut req_buf = Vec::new();
                req_buf.push(1);
                req_buf.extend(&(k.len() as u32).to_be_bytes());
                req_buf.extend(k);
                send.send(req_buf).unwrap();

                match recv.recv() {
                    Ok(resp) => {
                        assert_eq!(resp[0], 0);
                        assert_eq!(&resp[5..], v);
                    }
                    _ => panic!(),
                }
            }
            "insert" => {
                let key_len = rng.random_range(key_len_range.clone());
                let val_len = rng.random_range(val_len_range.clone());
                let mut k: Vec<u8> = vec![0; key_len];
                let mut v: Vec<u8> = vec![0; val_len];
                rng.fill(&mut k[..]);
                rng.fill(&mut v[..]);

                let mut req_buf = Vec::new();
                req_buf.push(2);
                req_buf.extend(&(key_len as u32).to_be_bytes());
                req_buf.extend(&(val_len as u32).to_be_bytes());
                req_buf.extend(&k);
                req_buf.extend(&v);
                send.send(req_buf).unwrap();

                entries.push((k, v));

                match recv.recv() {
                    Ok(resp) => {
                        assert_eq!(resp[0], 0);
                    }
                    _ => panic!(),
                }
            }
            "update" => {
                let (k, v) = entries.choose_mut(&mut rng).unwrap();
                let val_len = rng.random_range(val_len_range.clone());
                let mut new_val = vec![0; val_len];
                rng.fill(&mut new_val[..]);
                *v = new_val;

                let mut req_buf = Vec::new();
                req_buf.push(2);
                req_buf.extend(&(k.len() as u32).to_be_bytes());
                req_buf.extend(&(val_len as u32).to_be_bytes());
                req_buf.extend(&*k);
                req_buf.extend(&*v);
                send.send(req_buf).unwrap();

                match recv.recv() {
                    Ok(resp) => {
                        assert_eq!(resp[0], 0);
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    }
}

pub struct Conn {
    send: Sender<Vec<u8>>,
    recv: Receiver<Vec<u8>>,
}
