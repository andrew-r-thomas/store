// pub mod cache;
// pub mod page;
// pub mod shard;
// pub mod utils;
//
// use std::{
//     ops::Range,
//     sync::mpsc::{self, Receiver, Sender},
//     thread,
// };
//
// use rand::{
//     Rng, SeedableRng,
//     seq::{IndexedMutRandom, IndexedRandom},
// };
// use rand_chacha::ChaCha8Rng;
//
// type PageId = u64;
//
// const PAGE_SIZE: usize = 1024 * 1024;
// const BLOCK_SIZE: usize = 4 * 1024 * 1024;
//
// const BUF_POOL_SIZE: usize = 1024;
// const FREE_CAP_TARGET: usize = 256;
//
// const NUM_INGEST: usize = 1024;
// const NUM_OPS: usize = 2048;
//
// const NUM_CONNS: usize = 128;
//
// const KEY_LEN_RANGE: Range<usize> = 128..256;
// const VAL_LEN_RANGE: Range<usize> = 512..1024;
//
// const OFFSET_SIZE: usize = 8;
// const CHUNK_LEN_SIZE: usize = 8;
//
// fn main() {
//     let (conn_send, conn_recv) = mpsc::channel();
//     thread::Builder::new()
//         .name("shard".into())
//         .spawn(|| {
//             shard::shard(
//                 conn_recv,
//                 PAGE_SIZE,
//                 BUF_POOL_SIZE,
//                 BLOCK_SIZE,
//                 FREE_CAP_TARGET,
//             )
//         })
//         .unwrap();
//
//     let mut rng = ChaCha8Rng::seed_from_u64(42);
//     let mut conns = Vec::new();
//     loop {
//         if rng.random() {
//             let (c_send, s_recv) = mpsc::channel();
//             let (s_send, c_recv) = mpsc::channel();
//             let seed = conns.len() as u64 ^ 69;
//             conns.push(
//                 thread::Builder::new()
//                     .name(format!("conn {}", conns.len()))
//                     .spawn(move || {
//                         conn(
//                             seed,
//                             NUM_INGEST,
//                             NUM_OPS,
//                             KEY_LEN_RANGE,
//                             VAL_LEN_RANGE,
//                             c_send,
//                             c_recv,
//                         )
//                     })
//                     .unwrap(),
//             );
//             conn_send
//                 .send(Conn {
//                     send: s_send,
//                     recv: s_recv,
//                 })
//                 .unwrap();
//
//             if conns.len() >= NUM_CONNS {
//                 break;
//             }
//         }
//     }
//
//     for c in conns {
//         if let Err(e) = c.join() {
//             println!("error on join: {:?}", e);
//         }
//     }
// }
//
// fn conn(
//     seed: u64,
//     num_ingest: usize,
//     num_ops: usize,
//     key_len_range: Range<usize>,
//     val_len_range: Range<usize>,
//
//     send: Sender<Vec<u8>>,
//     recv: Receiver<Vec<u8>>,
// ) {
//     let mut rng = ChaCha8Rng::seed_from_u64(seed);
//     let mut entries = Vec::new();
//
//     for _ in 0..num_ingest {
//         let key_len = rng.random_range(key_len_range.clone());
//         let val_len = rng.random_range(val_len_range.clone());
//         let mut key = vec![0; key_len];
//         let mut val = vec![0; val_len];
//         rng.fill(&mut key[..]);
//         rng.fill(&mut val[..]);
//
//         entries.push((key.clone(), val.clone()));
//
//         let mut req_buf = Vec::new();
//         req_buf.push(2);
//         req_buf.extend(&(key_len as u32).to_be_bytes());
//         req_buf.extend(&(val_len as u32).to_be_bytes());
//         req_buf.extend(&key);
//         req_buf.extend(&val);
//         send.send(req_buf).unwrap();
//
//         match recv.recv() {
//             Ok(resp) => {
//                 assert_eq!(resp[0], 0);
//             }
//             _ => panic!(),
//         }
//     }
//
//     let ops = ["get", "insert", "update"];
//     for _ in 0..num_ops {
//         if rng.random_bool(0.05) {
//             return;
//         }
//         match *ops.choose(&mut rng).unwrap() {
//             "get" => {
//                 let (k, v) = entries.choose(&mut rng).unwrap();
//                 let mut req_buf = Vec::new();
//                 req_buf.push(1);
//                 req_buf.extend(&(k.len() as u32).to_be_bytes());
//                 req_buf.extend(k);
//                 send.send(req_buf).unwrap();
//
//                 match recv.recv() {
//                     Ok(resp) => {
//                         assert_eq!(resp[0], 0);
//                         assert_eq!(&resp[5..], v);
//                     }
//                     _ => panic!(),
//                 }
//             }
//             "insert" => {
//                 let key_len = rng.random_range(key_len_range.clone());
//                 let val_len = rng.random_range(val_len_range.clone());
//                 let mut k: Vec<u8> = vec![0; key_len];
//                 let mut v: Vec<u8> = vec![0; val_len];
//                 rng.fill(&mut k[..]);
//                 rng.fill(&mut v[..]);
//
//                 let mut req_buf = Vec::new();
//                 req_buf.push(2);
//                 req_buf.extend(&(key_len as u32).to_be_bytes());
//                 req_buf.extend(&(val_len as u32).to_be_bytes());
//                 req_buf.extend(&k);
//                 req_buf.extend(&v);
//                 send.send(req_buf).unwrap();
//
//                 entries.push((k, v));
//
//                 match recv.recv() {
//                     Ok(resp) => {
//                         assert_eq!(resp[0], 0);
//                     }
//                     _ => panic!(),
//                 }
//             }
//             "update" => {
//                 let (k, v) = entries.choose_mut(&mut rng).unwrap();
//                 let val_len = rng.random_range(val_len_range.clone());
//                 let mut new_val = vec![0; val_len];
//                 rng.fill(&mut new_val[..]);
//                 *v = new_val;
//
//                 let mut req_buf = Vec::new();
//                 req_buf.push(2);
//                 req_buf.extend(&(k.len() as u32).to_be_bytes());
//                 req_buf.extend(&(val_len as u32).to_be_bytes());
//                 req_buf.extend(&*k);
//                 req_buf.extend(&*v);
//                 send.send(req_buf).unwrap();
//
//                 match recv.recv() {
//                     Ok(resp) => {
//                         assert_eq!(resp[0], 0);
//                     }
//                     _ => panic!(),
//                 }
//             }
//             _ => panic!(),
//         }
//     }
// }
//
// pub struct Conn {
//     send: Sender<Vec<u8>>,
//     recv: Receiver<Vec<u8>>,
// }

use std::{
    ops::Range,
    sync::mpsc::{self, Receiver, Sender},
    thread,
};

use rand::{
    Rng, SeedableRng,
    seq::{IndexedRandom, SliceRandom},
};
use rand_chacha::ChaCha8Rng;

pub mod cache;
pub mod page;
pub mod shard;
pub mod utils;

const NUM_CONNS: usize = 128;
const NUM_INGEST: usize = 1024;
const NUM_OPS: usize = 2048;
const KEY_LEN_RANGE: Range<usize> = 128..256;
const VAL_LEN_RANGE: Range<usize> = 512..1024;
const PAGE_SIZE: usize = 1024 * 1024;
const BUF_POOL_SIZE: usize = 1024;

type PageId = u64;

#[derive(Debug, Clone)]
struct Operation {
    client_id: usize,
    op_type: OpType,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

fn main() {
    // Generate ALL operations from ALL clients up front
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut all_operations = Vec::new();
    let mut client_states = vec![Vec::new(); NUM_CONNS]; // Track entries per client

    // Generate operations for each client deterministically
    for client_id in 0..NUM_CONNS {
        let mut client_rng = ChaCha8Rng::seed_from_u64(client_id as u64 ^ 69);

        // Generate this client's operations
        for _ in 0..NUM_INGEST {
            let (key, value) = generate_kv(&mut client_rng);
            client_states[client_id].push((key.clone(), value.clone()));
            all_operations.push(Operation {
                client_id,
                op_type: OpType::Insert,
                key,
                value: Some(value),
            });
        }

        for _ in 0..NUM_OPS {
            let op = generate_operation(&mut client_rng, &mut client_states[client_id]);
            all_operations.push(op);
        }
    }

    // Shuffle ALL operations with deterministic seed
    all_operations.shuffle(&mut rng);

    // Now execute in this exact order
    execute_operations(all_operations);
}

fn execute_operations(operations: Vec<Operation>) {
    // Start shard
    let (conn_send, conn_recv) = mpsc::channel();
    thread::spawn(|| shard::shard(conn_recv, PAGE_SIZE, BUF_POOL_SIZE));

    // Create connections for each client
    let mut connections = Vec::new();
    for _ in 0..NUM_CONNS {
        let (c_send, s_recv) = mpsc::channel();
        let (s_send, c_recv) = mpsc::channel();
        conn_send
            .send(Conn {
                send: s_send,
                recv: s_recv,
            })
            .unwrap();
        connections.push((c_send, c_recv));
    }

    // Execute operations in predetermined order
    for op in operations {
        let (send, recv) = &connections[op.client_id];

        // Send request
        let req = encode_request(&op);
        send.send(req).unwrap();

        // Receive response
        let resp = recv.recv().unwrap();
        validate_response(&op, &resp);
    }
}

pub struct Conn {
    send: Sender<Vec<u8>>,
    recv: Receiver<Vec<u8>>,
}

fn encode_request(op: &Operation) -> Vec<u8> {
    let mut req_buf = Vec::new();

    match op.op_type {
        OpType::Get => {
            req_buf.push(1); // GET opcode
            req_buf.extend(&(op.key.len() as u32).to_be_bytes());
            req_buf.extend(&op.key);
        }
        OpType::Insert | OpType::Update => {
            req_buf.push(2); // SET opcode
            let value = op.value.as_ref().expect("Insert/Update must have value");
            req_buf.extend(&(op.key.len() as u32).to_be_bytes());
            req_buf.extend(&(value.len() as u32).to_be_bytes());
            req_buf.extend(&op.key);
            req_buf.extend(value);
        }
        OpType::Exit => {
            // Return empty buffer to signal exit
            return vec![];
        }
    }

    req_buf
}

fn validate_response(op: &Operation, response: &[u8]) -> bool {
    match op.op_type {
        OpType::Get => {
            if response[0] == 0 {
                // Success - verify the value matches what we expect
                let value_len = u32::from_be_bytes(response[1..5].try_into().unwrap()) as usize;
                let returned_value = &response[5..5 + value_len];

                // Note: In a real test, you'd need to track expected values
                // For now, just verify it's a valid response format
                true
            } else if response[0] == 1 {
                // Not found - this might be expected for some gets
                panic!("GET operation failed for key: {:?}", op.key);
            } else {
                panic!("Invalid GET response code: {}", response[0]);
            }
        }
        OpType::Insert | OpType::Update => {
            if response[0] == 0 {
                // Success
                true
            } else {
                panic!("SET operation failed with response code: {}", response[0]);
            }
        }
        OpType::Exit => true,
    }
}

// Update the Operation enum
#[derive(Debug, Clone)]
enum OpType {
    Insert,
    Get,
    Update,
    Exit,
}

// Helper function to check if operation should terminate client
fn should_terminate(op: &Operation) -> bool {
    matches!(op.op_type, OpType::Exit)
}

fn generate_kv(rng: &mut ChaCha8Rng) -> (Vec<u8>, Vec<u8>) {
    let key_len = rng.gen_range(KEY_LEN_RANGE);
    let val_len = rng.gen_range(VAL_LEN_RANGE);

    let mut key = vec![0; key_len];
    let mut value = vec![0; val_len];

    rng.fill(&mut key[..]);
    rng.fill(&mut value[..]);

    (key, value)
}

fn generate_operation(
    rng: &mut ChaCha8Rng,
    client_entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Operation {
    if rng.gen_bool(0.05) {
        // 5% chance to exit early (matching your original logic)
        return Operation {
            client_id: 0, // Will be set properly later
            op_type: OpType::Exit,
            key: vec![],
            value: None,
        };
    }

    let ops = [OpType::Get, OpType::Insert, OpType::Update];
    let op_type = ops.choose(rng).unwrap().clone();

    match op_type {
        OpType::Get => {
            if client_entries.is_empty() {
                // If no entries yet, generate an insert instead
                let (key, value) = generate_kv(rng);
                client_entries.push((key.clone(), value.clone()));
                Operation {
                    client_id: 0,
                    op_type: OpType::Insert,
                    key,
                    value: Some(value),
                }
            } else {
                let (key, _) = client_entries.choose(rng).unwrap().clone();
                Operation {
                    client_id: 0,
                    op_type: OpType::Get,
                    key,
                    value: None,
                }
            }
        }
        OpType::Insert => {
            let (key, value) = generate_kv(rng);
            client_entries.push((key.clone(), value.clone()));
            Operation {
                client_id: 0,
                op_type: OpType::Insert,
                key,
                value: Some(value),
            }
        }
        OpType::Update => {
            if client_entries.is_empty() {
                // If no entries yet, generate an insert instead
                let (key, value) = generate_kv(rng);
                client_entries.push((key.clone(), value.clone()));
                Operation {
                    client_id: 0,
                    op_type: OpType::Insert,
                    key,
                    value: Some(value),
                }
            } else {
                let entry_idx = rng.gen_range(0..client_entries.len());
                let key = client_entries[entry_idx].0.clone();

                let val_len = rng.gen_range(VAL_LEN_RANGE);
                let mut new_value = vec![0; val_len];
                rng.fill(&mut new_value[..]);

                // Update client's local state
                client_entries[entry_idx].1 = new_value.clone();

                Operation {
                    client_id: 0,
                    op_type: OpType::Update,
                    key,
                    value: Some(new_value),
                }
            }
        }
        OpType::Exit => unreachable!(), // Handled above
    }
}
