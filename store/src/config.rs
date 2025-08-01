use std::{fs, path};

use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub page_size: usize,
    pub buf_pool_size: usize,
    pub free_cap_target: usize,
    pub block_cap: u64,

    pub block_size: usize,
    pub num_block_bufs: usize,
    pub net_buf_size: usize,
    pub num_net_bufs: usize,

    pub queue_size: usize,
}
impl Config {
    pub fn load(path: &path::Path) -> Result<Self, Error> {
        let bytes = fs::read(path).map_err(|e| Error::IO(e))?;
        match toml::from_slice(&bytes) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::TOML(e)),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    TOML(toml::de::Error),
}
