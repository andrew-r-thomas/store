/*

    TODO:
    - direct io
    - async interface

*/

use std::{fs::File, io, path::Path};

pub struct Store<K, V> {
    file: File,
}

pub enum Error {
    IoError(io::Error),
    TypeError,
}

impl<K, V> Store<K, V> {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let f = File::open(path);
        match f {
            Ok(file) => Ok(Self { file }),
            Err(e) => Err(Error::IoError(e)),
        }
        // TODO: check the types in the file against the generics
    }
    pub fn create(path: &Path) {}

    pub fn get(&self, key: K) -> Option<V> {
        None
    }
    pub fn set(&self, key: K, val: V) -> Result<(), Error> {
        Ok(())
    }
    pub fn delete(&self, key: K) -> Result<(), Error> {
        Ok(())
    }
}

impl<K, V> Drop for Store<K, V> {
    fn drop(&mut self) {
        todo!()
    }
}
