use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
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
    fs::create_dir("sim").unwrap();

    let index = Arc::new(Index::<8, { 1024 * 1024 }>::new(64));

    let threads: Vec<JoinHandle<()>> = (0..4)
        .map(|t| {
            let index = index.clone();
            thread::Builder::new()
                .name(format!("{t}"))
                .spawn(move || {
                    let num_ingest = 64;
                    let num_ops = 1024;
                    let sim_file_path = format!("sim/sim_{t}");

                    generate_sim(
                        &sim_file_path,
                        69 * t,
                        num_ingest,
                        num_ops,
                        128..256,
                        512..1024,
                    );

                    let mut sim_reader = BufReader::new(File::open(&sim_file_path).unwrap());

                    for _ in 0..num_ingest {
                        let (key, val): (Vec<u8>, Vec<u8>) =
                            ciborium::from_reader(&mut sim_reader).unwrap();
                        index.set(&key, &val).unwrap();
                    }

                    for n in 0..num_ops {
                        let (op, key, val): (String, Vec<u8>, Vec<u8>) =
                            ciborium::from_reader(&mut sim_reader).unwrap();
                        match op.as_str() {
                            "get" => {
                                assert_eq!(&val, index.get(&key).unwrap());
                                println!("{t} op {n}: successful get");
                            }
                            "set" => {
                                while let Err(()) = index.set(&key, &val) {}
                                println!("{t} op {n}: successful set");
                            }
                            _ => panic!(),
                        }
                    }
                })
                .unwrap()
        })
        .collect();

    for t in threads {
        t.join().unwrap()
    }

    fs::remove_dir_all("sim").unwrap();
}

fn generate_sim(
    name: &String,
    seed: u64,
    num_ingest: usize,
    num_ops: usize,
    key_len_range: Range<usize>,
    val_len_range: Range<usize>,
) {
    let mut writer = BufWriter::new(File::create(name).unwrap());
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut entries = Vec::new();

    for _ in 0..num_ingest {
        let key_len = rng.random_range(key_len_range.clone());
        let val_len = rng.random_range(val_len_range.clone());
        let mut key: Vec<u8> = vec![0; key_len];
        let mut val: Vec<u8> = vec![0; val_len];
        rng.fill(&mut key[..]);
        rng.fill(&mut val[..]);

        ciborium::into_writer(&(&key, &val), &mut writer).unwrap();
        entries.push((key, val));
    }

    let ops = ["get", "insert", "update"];
    for _ in 0..num_ops {
        match *ops.choose(&mut rng).unwrap() {
            "get" => {
                let (key, val) = entries.choose(&mut rng).unwrap();
                ciborium::into_writer(&("get", key, val), &mut writer).unwrap();
            }
            "insert" => {
                let key_len = rng.random_range(key_len_range.clone());
                let val_len = rng.random_range(val_len_range.clone());
                let mut key: Vec<u8> = vec![0; key_len];
                let mut val: Vec<u8> = vec![0; val_len];
                rng.fill(&mut key[..]);
                rng.fill(&mut val[..]);

                ciborium::into_writer(&("set", &key, &val), &mut writer).unwrap();
                entries.push((key, val));
            }
            "update" => {
                let (key, val) = entries.choose_mut(&mut rng).unwrap();

                let val_len = rng.random_range(val_len_range.clone());
                let mut new_val: Vec<u8> = vec![0; val_len];
                rng.fill(&mut new_val[..]);

                ciborium::into_writer(&("set", &key, &new_val), &mut writer).unwrap();
                *val = new_val;
            }
            _ => panic!(),
        }
    }

    writer.flush().unwrap();
}
