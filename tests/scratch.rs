use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    ops::Range,
    sync::Arc,
    thread::{self, JoinHandle},
};

use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};
use rand_chacha::ChaCha8Rng;
use store::index::Index;

#[test]
fn scratch() {
    let index = Arc::new(Index::<8, { 1024 * 1024 }>::new(64));

    let threads: Vec<JoinHandle<()>> = (0..4)
        .map(|t| {
            let index = index.clone();
            thread::Builder::new()
                .name(format!("{t}"))
                .spawn(move || {
                    let num_ingest = 32;
                    let num_ops = 32;
                    let sim_file_path = format!("sim_{t}");

                    generate_sim(
                        &sim_file_path,
                        69 ^ t,
                        num_ingest,
                        num_ops,
                        128..256,
                        512..1024,
                    );

                    let mut sim_file = File::open(&sim_file_path).unwrap();

                    let mut code_buf = [0];
                    let mut len_buf = [0; 4];
                    let mut key = Vec::new();
                    let mut val = Vec::new();

                    for _ in 0..num_ingest {
                        sim_file.read_exact(&mut len_buf).unwrap();
                        let key_len =
                            u32::from_be_bytes((&len_buf[..]).try_into().unwrap()) as usize;
                        key.resize(key_len, 0);
                        key.fill(0);
                        sim_file.read_exact(&mut key).unwrap();

                        sim_file.read_exact(&mut len_buf).unwrap();
                        let val_len =
                            u32::from_be_bytes((&len_buf[..]).try_into().unwrap()) as usize;
                        val.resize(val_len, 0);
                        val.fill(0);
                        sim_file.read_exact(&mut val).unwrap();

                        index.set(&key, &val).unwrap();
                    }

                    for _ in 0..num_ops {
                        sim_file.read_exact(&mut code_buf).unwrap();
                        match code_buf[0] {
                            0 => {
                                // get
                                sim_file.read_exact(&mut len_buf).unwrap();
                                let key_len = u32::from_be_bytes(len_buf) as usize;
                                key.resize(key_len, 0);
                                key.fill(0);
                                sim_file.read_exact(&mut key).unwrap();

                                sim_file.read_exact(&mut len_buf).unwrap();
                                let val_len = u32::from_be_bytes(len_buf) as usize;
                                val.resize(val_len, 0);
                                val.fill(0);
                                sim_file.read_exact(&mut val).unwrap();

                                assert_eq!(&val, index.get(&key).unwrap());
                            }
                            1 => {
                                // set
                                sim_file.read_exact(&mut len_buf).unwrap();
                                let key_len = u32::from_be_bytes(len_buf) as usize;
                                key.resize(key_len, 0);
                                key.fill(0);
                                sim_file.read_exact(&mut key).unwrap();

                                sim_file.read_exact(&mut len_buf).unwrap();
                                let val_len = u32::from_be_bytes(len_buf) as usize;
                                val.resize(val_len, 0);
                                val.fill(0);
                                sim_file.read_exact(&mut val).unwrap();

                                index.set(&key, &val).unwrap();
                            }
                            _ => panic!(),
                        }
                    }

                    fs::remove_file(sim_file_path).unwrap();
                })
                .unwrap()
        })
        .collect();

    for t in threads {
        t.join().unwrap()
    }
}

fn generate_sim(
    name: &String,
    seed: u64,
    num_ingest: usize,
    num_ops: usize,
    key_len_range: Range<usize>,
    val_len_range: Range<usize>,
) {
    let mut file = File::create(name).unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut table = Vec::new();

    for _ in 0..num_ingest {
        let key_len = rng.random_range(key_len_range.clone());
        let val_len = rng.random_range(val_len_range.clone());
        let mut key: Vec<u8> = vec![0; key_len];
        let mut val: Vec<u8> = vec![0; val_len];
        rng.fill(&mut key[..]);
        rng.fill(&mut val[..]);

        file.write_all(&(key_len as u32).to_be_bytes()).unwrap();
        file.write_all(&key).unwrap();
        file.write_all(&(val_len as u32).to_be_bytes()).unwrap();
        file.write_all(&val).unwrap();

        table.push((key, val));
    }

    let ops = ["get", "insert", "update"];
    for _ in 0..num_ops {
        match *ops.choose(&mut rng).unwrap() {
            "get" => {
                let (key, val) = table.choose(&mut rng).unwrap();

                file.write_all(&[0]).unwrap();
                file.write_all(&(key.len() as u32).to_be_bytes()).unwrap();
                file.write_all(key).unwrap();
                file.write_all(&(val.len() as u32).to_be_bytes()).unwrap();
                file.write_all(val).unwrap();
            }
            "insert" => {
                let key_len = rng.random_range(key_len_range.clone());
                let val_len = rng.random_range(val_len_range.clone());
                let mut key: Vec<u8> = vec![0; key_len];
                let mut val: Vec<u8> = vec![0; val_len];
                rng.fill(&mut key[..]);
                rng.fill(&mut val[..]);

                file.write_all(&[1]).unwrap();
                file.write_all(&(key_len as u32).to_be_bytes()).unwrap();
                file.write_all(&key).unwrap();
                file.write_all(&(val_len as u32).to_be_bytes()).unwrap();
                file.write_all(&val).unwrap();

                table.push((key, val));
            }
            "update" => {
                let (key, val) = table.choose_mut(&mut rng).unwrap();

                let key_len = rng.random_range(key_len_range.clone());
                let val_len = rng.random_range(val_len_range.clone());
                let mut new_key: Vec<u8> = vec![0; key_len];
                let mut new_val: Vec<u8> = vec![0; val_len];
                rng.fill(&mut new_key[..]);
                rng.fill(&mut new_val[..]);

                file.write_all(&[1]).unwrap();
                file.write_all(&(key_len as u32).to_be_bytes()).unwrap();
                file.write_all(&key).unwrap();
                file.write_all(&(val_len as u32).to_be_bytes()).unwrap();
                file.write_all(&val).unwrap();

                *key = new_key;
                *val = new_val;
            }
            _ => panic!(),
        }
    }

    file.flush().unwrap();
}
