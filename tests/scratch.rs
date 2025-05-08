use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
    ops::Range,
};

use rand::{
    Rng, SeedableRng,
    seq::{IndexedMutRandom, IndexedRandom},
};
use rand_chacha::ChaCha8Rng;
use store::{index, page::LeafPageMut};

#[test]
fn scratch() {
    get_set_sim();
}

fn get_set_sim() {
    fs::create_dir("sim").unwrap();

    const PAGE_SIZE: usize = 1024 * 1024;
    let mut buf_pool = vec![LeafPageMut::new(PAGE_SIZE).unpack::<PAGE_SIZE>()];
    let mut root = 1;
    let mut next_pid = 2;
    let mut page_dir = HashMap::from([(root, 0)]);

    let num_ingest = 2048;
    let num_ops = 4096;
    let sim_file_path = "sim/scratch".into();

    generate_sim(&sim_file_path, 42, num_ingest, num_ops, 64..128, 512..1024);

    let mut sim_reader = BufReader::new(File::open(&sim_file_path).unwrap());
    let mut path_buf = Vec::new();
    for _ in 0..num_ingest {
        let (key, val): (Vec<u8>, Vec<u8>) = ciborium::from_reader(&mut sim_reader).unwrap();
        path_buf.clear();
        index::set(
            &mut buf_pool,
            &mut page_dir,
            &mut root,
            &mut next_pid,
            &key,
            &val,
            &mut path_buf,
        );
    }

    let mut get_buf = Vec::new();
    for o in 0..num_ops {
        print!("op {o}:");

        let (op, key, val): (String, Vec<u8>, Vec<u8>) =
            ciborium::from_reader(&mut sim_reader).unwrap();
        match op.as_str() {
            "get" => {
                println!(" get");
                get_buf.clear();
                assert!(index::get(&buf_pool, &page_dir, root, &key, &mut get_buf));
                assert_eq!(&val, &get_buf);
            }
            "set" => {
                println!(" set");
                path_buf.clear();
                index::set(
                    &mut buf_pool,
                    &mut page_dir,
                    &mut root,
                    &mut next_pid,
                    &key,
                    &val,
                    &mut path_buf,
                );
            }
            _ => panic!(),
        }
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

    let ops = ["get", "insert"];
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
