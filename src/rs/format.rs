use std::{marker::PhantomData, mem};

use bytes::Bytes;

pub trait Parse: From<Bytes> {}
impl<T: From<Bytes>> Parse for T {}
pub trait Serialize {
    fn write_to_buf(&self, buf: &mut [u8]);
    fn size(&self) -> usize;

    fn to_vec(&self) -> Vec<u8> {
        let mut vec = vec![0; self.size()];
        self.write_to_buf(&mut vec);
        vec
    }
}
pub trait Format: Parse + Serialize {}
impl<T: Parse + Serialize> Format for T {}

pub struct Iter<F: Format> {
    bytes: Bytes,
    idx: usize,
    _ph: PhantomData<F>,
}
impl<F: Format> Iter<F> {
    pub fn reset(&mut self) {
        self.idx = 0;
    }
}
impl<F: Format> From<Bytes> for Iter<F> {
    fn from(bytes: Bytes) -> Self {
        Self {
            bytes,
            idx: 0,
            _ph: PhantomData,
        }
    }
}
impl<F: Format> Serialize for Iter<F> {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        buf.copy_from_slice(&self.bytes);
    }
    fn size(&self) -> usize {
        self.bytes.len()
    }
}
impl<F: Format> Iterator for Iter<F> {
    type Item = F;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.bytes.len() {
            return None;
        }
        let f = F::from(self.bytes.slice(self.idx..));
        self.idx += f.size();
        Some(f)
    }
}

pub struct ChainChunk {
    pub next: Option<ChunkLoc>,
    pub data: Bytes,
}
#[derive(Clone, Copy)]
pub struct ChunkLoc {
    pub block_num: u32,
    pub block_off: u32,
}
impl From<Bytes> for ChainChunk {
    fn from(bytes: Bytes) -> Self {
        todo!()
    }
}
impl Serialize for ChainChunk {
    fn size(&self) -> usize {
        todo!()
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        todo!()
    }
}
impl From<Bytes> for ChunkLoc {
    fn from(bytes: Bytes) -> Self {
        todo!()
    }
}
impl Serialize for ChunkLoc {
    fn size(&self) -> usize {
        todo!()
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        todo!()
    }
}

pub struct Commit {
    pub ts: u64,
    pub write: Write,
}
impl Commit {
    pub const TS_SIZE: usize = mem::size_of::<u64>();
}
impl From<Bytes> for Commit {
    fn from(bytes: Bytes) -> Self {
        let mut cursor = 0;
        let ts = u64::from_le_bytes(bytes[cursor..cursor + Self::TS_SIZE].try_into().unwrap());
        cursor += Self::TS_SIZE;
        let write = Write::from(bytes.slice(cursor..));
        Self { ts, write }
    }
}
impl Serialize for Commit {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + Self::TS_SIZE].copy_from_slice(&self.ts.to_le_bytes());
        cursor += Self::TS_SIZE;
        self.write.write_to_buf(&mut buf[cursor..]);
    }
    fn size(&self) -> usize {
        Self::TS_SIZE + self.write.size()
    }
}

#[derive(Clone)]
pub enum Write {
    Put(Put),
    Del(Del),
}
impl Write {
    pub const FLAG_SIZE: usize = mem::size_of::<u8>();
    pub const PUT_FLAG: u8 = 1;
    pub const DEL_FLAG: u8 = 2;

    pub fn key(&self) -> Bytes {
        match self {
            Self::Put(put) => put.key.clone(),
            Self::Del(del) => del.key.clone(),
        }
    }
}
impl From<Bytes> for Write {
    fn from(bytes: Bytes) -> Self {
        match bytes[0] {
            Self::PUT_FLAG => Self::Put(Put::from(bytes.slice(1..))),
            Self::DEL_FLAG => Self::Del(Del::from(bytes.slice(1..))),
            _ => panic!(),
        }
    }
}
impl Serialize for Write {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        match self {
            Self::Put(put) => {
                buf[0] = Self::PUT_FLAG;
                put.write_to_buf(&mut buf[1..]);
            }
            Self::Del(del) => {
                buf[0] = Self::DEL_FLAG;
                del.write_to_buf(&mut buf[1..]);
            }
        }
    }
    fn size(&self) -> usize {
        Self::FLAG_SIZE
            + match self {
                Self::Put(put) => put.size(),
                Self::Del(del) => del.size(),
            }
    }
}

pub const KEY_LEN_SIZE: usize = mem::size_of::<u16>();
pub const VAL_LEN_SIZE: usize = mem::size_of::<u32>();

#[derive(Clone)]
pub struct Put {
    pub key: Bytes,
    pub val: Bytes,
}
impl From<Bytes> for Put {
    fn from(bytes: Bytes) -> Self {
        let mut cursor = 0;
        let key_len =
            u16::from_le_bytes(bytes[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let val_len =
            u32::from_le_bytes(bytes[cursor..cursor + VAL_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += VAL_LEN_SIZE;
        let key = bytes.slice(cursor..cursor + key_len);
        cursor += key_len;
        let val = bytes.slice(cursor..cursor + val_len);
        Self { key, val }
    }
}
impl Serialize for Put {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + KEY_LEN_SIZE].copy_from_slice(&(self.key.len() as u16).to_le_bytes());
        cursor += KEY_LEN_SIZE;
        buf[cursor..cursor + VAL_LEN_SIZE].copy_from_slice(&(self.val.len() as u16).to_le_bytes());
        cursor += VAL_LEN_SIZE;
        buf[cursor..cursor + self.key.len()].copy_from_slice(&self.key);
        cursor += self.key.len();
        buf[cursor..cursor + self.val.len()].copy_from_slice(&self.val);
    }
    fn size(&self) -> usize {
        KEY_LEN_SIZE + VAL_LEN_SIZE + self.key.len() + self.val.len()
    }
}
#[derive(Clone)]
pub struct Del {
    pub key: Bytes,
}
impl From<Bytes> for Del {
    fn from(bytes: Bytes) -> Self {
        let mut cursor = 0;
        let key_len =
            u16::from_le_bytes(bytes[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let key = bytes.slice(cursor..cursor + key_len);
        Self { key }
    }
}
impl Serialize for Del {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + KEY_LEN_SIZE].copy_from_slice(&(self.key.len() as u16).to_le_bytes());
        cursor += KEY_LEN_SIZE;
        buf[cursor..cursor + self.key.len()].copy_from_slice(&self.key);
    }
    fn size(&self) -> usize {
        KEY_LEN_SIZE + self.key.len()
    }
}

pub const PID_SIZE: usize = mem::size_of::<u64>();

pub struct Smop {
    pub pid: u64,
    pub gt_key: Bytes,
    pub lte_key: Bytes,
}
impl From<Bytes> for Smop {
    fn from(bytes: Bytes) -> Self {
        let mut cursor = 0;
        let pid = u64::from_le_bytes(bytes[cursor..cursor + PID_SIZE].try_into().unwrap());
        cursor += PID_SIZE;
        let gt_key_len =
            u16::from_le_bytes(bytes[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let lte_key_len =
            u16::from_le_bytes(bytes[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let gt_key = bytes.slice(cursor..cursor + gt_key_len);
        cursor += gt_key_len;
        let lte_key = bytes.slice(cursor..cursor + lte_key_len);
        Self {
            pid,
            gt_key,
            lte_key,
        }
    }
}
impl Serialize for Smop {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + PID_SIZE].copy_from_slice(&self.pid.to_le_bytes());
        cursor += PID_SIZE;
        buf[cursor..cursor + KEY_LEN_SIZE]
            .copy_from_slice(&(self.gt_key.len() as u16).to_le_bytes());
        cursor += KEY_LEN_SIZE;
        buf[cursor..cursor + KEY_LEN_SIZE]
            .copy_from_slice(&(self.lte_key.len() as u16).to_le_bytes());
        cursor += KEY_LEN_SIZE;
        buf[cursor..cursor + self.gt_key.len()].copy_from_slice(&self.gt_key);
        cursor += self.gt_key.len();
        buf[cursor..cursor + self.lte_key.len()].copy_from_slice(&self.lte_key);
    }
    fn size(&self) -> usize {
        PID_SIZE + (2 * KEY_LEN_SIZE) + self.gt_key.len() + self.lte_key.len()
    }
}

pub trait Entries: Format {
    type Val;
    fn search(&self, target: &[u8]) -> Self::Val;
}

pub const NUM_ENTRIES_SIZE: usize = mem::size_of::<u16>();
pub const OFF_SIZE: usize = mem::size_of::<u32>();

/// ## format
/// [ num_entries (u16) ]
/// [ key_offs ...(u32) ]
/// [ key_lens ...(u16) ]
/// [ pids     ...(u64) ] <- one extra for the rightmost pointer
/// [ keys   ...(bytes) ]
pub struct InnerEntries {
    pub key_offs: Bytes,
    pub key_lens: Bytes,
    pub pids: Bytes,
    pub keys: Bytes,
}
impl Entries for InnerEntries {
    type Val = u64;
    fn search(&self, _target: &[u8]) -> Self::Val {
        todo!()
    }
}
impl From<Bytes> for InnerEntries {
    /// expects exact size buf
    fn from(bytes: Bytes) -> Self {
        let mut cursor = 0;
        let num_entries =
            u16::from_le_bytes(bytes[cursor..cursor + NUM_ENTRIES_SIZE].try_into().unwrap())
                as usize;
        cursor += NUM_ENTRIES_SIZE;
        let key_offs = bytes.slice(cursor..cursor + (OFF_SIZE * num_entries));
        cursor += key_offs.len();
        let key_lens = bytes.slice(cursor..cursor + (KEY_LEN_SIZE * num_entries));
        cursor += key_lens.len();
        let pids = bytes.slice(cursor..cursor + (PID_SIZE * (num_entries + 1)));
        cursor += pids.len();
        let keys = bytes.slice(cursor..);
        Self {
            key_lens,
            key_offs,
            pids,
            keys,
        }
    }
}
impl Serialize for InnerEntries {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + NUM_ENTRIES_SIZE]
            .copy_from_slice(&(self.key_lens.len() as u16).to_le_bytes());
        cursor += NUM_ENTRIES_SIZE;
        buf[cursor..cursor + self.key_offs.len()].copy_from_slice(&self.key_offs);
        cursor += self.key_offs.len();
        buf[cursor..cursor + self.key_lens.len()].copy_from_slice(&self.key_lens);
        cursor += self.key_lens.len();
        buf[cursor..cursor + self.pids.len()].copy_from_slice(&self.pids);
        cursor += self.pids.len();
        buf[cursor..cursor + self.keys.len()].copy_from_slice(&self.keys);
    }
    fn size(&self) -> usize {
        NUM_ENTRIES_SIZE
            + self.key_offs.len()
            + self.key_lens.len()
            + self.pids.len()
            + self.keys.len()
    }
}

/// ## format
/// [ num_entries  (u16) ]
/// [ offs      ...(u32) ]
/// [ key_lens  ...(u16) ]
/// [ val_lens  ...(u32) ]
/// [ entries ...(bytes) ]
pub struct LeafEntries {
    offs: Bytes,
    key_lens: Bytes,
    val_lens: Bytes,
    entries: Bytes,
}
impl Entries for LeafEntries {
    type Val = Option<Bytes>;
    fn search(&self, _target: &[u8]) -> Self::Val {
        todo!()
    }
}
impl From<Bytes> for LeafEntries {
    fn from(bytes: Bytes) -> Self {
        let mut cursor = 0;
        let num_entries =
            u16::from_le_bytes(bytes[cursor..cursor + NUM_ENTRIES_SIZE].try_into().unwrap())
                as usize;
        cursor += NUM_ENTRIES_SIZE;
        let offs = bytes.slice(cursor..cursor + (OFF_SIZE * num_entries));
        cursor += offs.len();
        let key_lens = bytes.slice(cursor..cursor + (KEY_LEN_SIZE * num_entries));
        cursor += key_lens.len();
        let val_lens = bytes.slice(cursor..cursor + (VAL_LEN_SIZE * num_entries));
        cursor += val_lens.len();
        let entries = bytes.slice(cursor..);
        Self {
            offs,
            key_lens,
            val_lens,
            entries,
        }
    }
}
impl Serialize for LeafEntries {
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + NUM_ENTRIES_SIZE]
            .copy_from_slice(&(self.offs.len() as u16).to_le_bytes());
        cursor += NUM_ENTRIES_SIZE;
        buf[cursor..cursor + self.offs.len()].copy_from_slice(&self.offs);
        cursor += self.offs.len();
        buf[cursor..cursor + self.key_lens.len()].copy_from_slice(&self.key_lens);
        cursor += self.key_lens.len();
        buf[cursor..cursor + self.val_lens.len()].copy_from_slice(&self.val_lens);
        cursor += self.val_lens.len();
        buf[cursor..cursor + self.entries.len()].copy_from_slice(&self.entries);
    }
    fn size(&self) -> usize {
        NUM_ENTRIES_SIZE
            + self.offs.len()
            + self.key_lens.len()
            + self.val_lens.len()
            + self.entries.len()
    }
}
