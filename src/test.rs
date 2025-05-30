#![cfg(test)]

use std::{collections::HashMap, ops::Range};

use crate::shard::{self, PageDir, Shard, WriteScratch};

use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};
use rand_chacha::ChaCha8Rng;

#[test]
fn scratch() {
    const SEED: u64 = 420 ^ 69;
    const KEY_LEN_RANGE: Range<usize> = 128..256;
    const VAL_LEN_RANGE: Range<usize> = 512..1024;

    const NUM_CLIENTS: usize = 128;
    const NUM_INGEST: usize = 1024;
    const NUM_BATCHES: usize = 2048;

    const PAGE_SIZE: usize = 1024 * 1024;
    const BUF_POOL_SIZE: usize = 1024;

    run_sim(
        SEED,
        KEY_LEN_RANGE,
        VAL_LEN_RANGE,
        NUM_CLIENTS,
        NUM_INGEST,
        NUM_BATCHES,
        PAGE_SIZE,
        BUF_POOL_SIZE,
    );
}

pub fn run_sim(
    seed: u64,
    key_len_range: Range<usize>,
    val_len_range: Range<usize>,

    num_clients: usize,
    num_ingest: usize,
    num_batches: usize,

    page_size: usize,
    buf_pool_size: usize,
) {
    // set up "clients"
    let mut clients = HashMap::<u64, Vec<(Vec<u8>, Vec<u8>)>>::from_iter(
        (0..num_clients as u64).map(|id| (id, Vec::new())),
    );
    let mut reqs = Vec::with_capacity(num_clients);
    let mut resps = Vec::with_capacity(num_clients);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // setup shard
    let mut shard = Shard::new(page_size, buf_pool_size);

    // do ingest
    for i in 0..num_ingest {
        for (id, entries) in clients.iter_mut() {
            let key_len = rng.random_range(key_len_range.clone());
            let val_len = rng.random_range(val_len_range.clone());
            let mut key = vec![0; key_len];
            let mut val = vec![0; val_len];
            rng.fill(&mut key[..]);
            rng.fill(&mut val[..]);

            let mut req = Vec::new();
            req.push(2);
            req.extend(&(key_len as u32).to_be_bytes());
            req.extend(&(val_len as u32).to_be_bytes());
            req.extend(&key);
            req.extend(&val);
            reqs.push((*id, req));

            entries.push((key, val));
        }

        shard.run_pipeline(&mut reqs, &mut resps);

        for (_, resp) in resps.drain(..) {
            assert_eq!(&[0][..], &resp);
        }

        println!("Completed ingest {}", i);
    }

    // process batches
    let ops = ["insert", "update", "get"];
    let mut expected = HashMap::new();
    for b in 0..num_batches {
        for (id, entries) in clients.iter_mut() {
            match *ops.choose(&mut rng).unwrap() {
                "insert" => {
                    let key_len = rng.random_range(key_len_range.clone());
                    let val_len = rng.random_range(val_len_range.clone());
                    let mut key = vec![0; key_len];
                    let mut val = vec![0; val_len];
                    rng.fill(&mut key[..]);
                    rng.fill(&mut val[..]);

                    let mut req = Vec::new();
                    req.push(2);
                    req.extend(&(key_len as u32).to_be_bytes());
                    req.extend(&(val_len as u32).to_be_bytes());
                    req.extend(&key);
                    req.extend(&val);
                    reqs.push((*id, req));

                    entries.push((key, val));
                    expected.insert(*id, vec![0]);
                }
                "update" => {
                    let (key, val) = entries.choose_mut(&mut rng).unwrap();
                    let new_val_len = rng.random_range(val_len_range.clone());
                    let mut new_val = vec![0; new_val_len];
                    rng.fill(&mut new_val[..]);
                    *val = new_val;

                    let mut req = Vec::new();
                    req.push(2);
                    req.extend(&(key.len() as u32).to_be_bytes());
                    req.extend(&(val.len() as u32).to_be_bytes());
                    req.extend(&key[..]);
                    req.extend(&val[..]);
                    reqs.push((*id, req));

                    expected.insert(*id, vec![0]);
                }
                "get" => {
                    let (key, val) = entries.choose(&mut rng).unwrap();

                    let mut req = Vec::new();
                    req.push(1);
                    req.extend(&(key.len() as u32).to_be_bytes());
                    req.extend(&key[..]);
                    reqs.push((*id, req));

                    let mut e = Vec::new();
                    e.push(0);
                    e.extend(&(val.len() as u32).to_be_bytes());
                    e.extend(&val[..]);
                    expected.insert(*id, e);
                }
                _ => panic!(),
            }
        }

        shard.run_pipeline(&mut reqs, &mut resps);

        for (id, resp) in resps.drain(..) {
            assert_eq!(expected.remove(&id).unwrap(), resp);
        }

        println!("Completed batch {}", b);
    }
}
