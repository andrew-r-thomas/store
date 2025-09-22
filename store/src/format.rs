use std::{marker::PhantomData, mem};

pub trait Format<'f> {
    fn from_bytes(buf: &'f [u8]) -> Self;
    fn write_to_buf(&self, buf: &mut [u8]);
    fn size(&self) -> usize;

    fn to_vec(&self) -> Vec<u8> {
        let mut vec = vec![0; self.size()];
        self.write_to_buf(&mut vec);
        vec
    }
}
pub trait Fixed<'f>: Format<'f> {
    const SIZE: usize;
}

pub struct PageChunk<'p, E: Entries<'p>> {
    pub header: PageChunkHeader,
    pub data: PageChunkData<'p, E>,
}
pub struct PageChunkHeader {
    pub len: usize,
    pub next: Option<u64>,
}
impl PageChunkHeader {
    pub const LEN_SIZE: usize = mem::size_of::<u64>();
    pub const NEXT_SIZE: usize = mem::size_of::<u64>();
}
impl Format<'_> for PageChunkHeader {
    fn from_bytes(buf: &[u8]) -> Self {
        let mut cursor = 0;
        let len =
            u64::from_le_bytes(buf[cursor..cursor + Self::LEN_SIZE].try_into().unwrap()) as usize;
        cursor += Self::LEN_SIZE;
        let next = {
            let off = u64::from_le_bytes(buf[cursor..cursor + Self::NEXT_SIZE].try_into().unwrap());
            if off == 0 { None } else { Some(off) }
        };
        Self { len, next }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(&(self.len as u64).to_le_bytes());
        cursor += Self::LEN_SIZE;
        let n = match self.next {
            Some(off) => off,
            None => 0,
        };
        buf[cursor..cursor + Self::NEXT_SIZE].copy_from_slice(&n.to_le_bytes());
    }
    fn size(&self) -> usize {
        Self::LEN_SIZE + Self::NEXT_SIZE
    }
}
pub enum PageChunkData<'p, E: Entries<'p>> {
    Commits(Iter<'p, Commit<'p>>),
    Smops(Iter<'p, Smop<'p>>),
    Entries(E),
}
impl<'p, E: Entries<'p>> PageChunkData<'p, E> {
    pub const FLAG_SIZE: usize = mem::size_of::<u8>();
    pub const COMMITS_FLAG: u8 = 1;
    pub const SMOPS_FLAG: u8 = 2;
    pub const ENTRIES_FLAG: u8 = 3;
}
impl<'p, E: Entries<'p>> Format<'p> for PageChunkData<'p, E> {
    /// expects exact size buffer
    fn from_bytes(buf: &'p [u8]) -> Self {
        match buf[0] {
            Self::COMMITS_FLAG => Self::Commits(Iter::<'p, Commit<'p>>::from_bytes(&buf[1..])),
            Self::SMOPS_FLAG => Self::Smops(Iter::<'p, Smop<'p>>::from_bytes(&buf[1..])),
            Self::ENTRIES_FLAG => Self::Entries(E::from_bytes(&buf[1..])),
            _ => panic!(),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        match self {
            Self::Commits(commits) => {
                buf[0] = Self::COMMITS_FLAG;
                commits.write_to_buf(&mut buf[1..]);
            }
            Self::Smops(smops) => {
                buf[0] = Self::SMOPS_FLAG;
                smops.write_to_buf(&mut buf[1..]);
            }
            Self::Entries(entries) => {
                buf[0] = Self::ENTRIES_FLAG;
                entries.write_to_buf(&mut buf[1..]);
            }
        }
    }
    fn size(&self) -> usize {
        Self::FLAG_SIZE
            + match self {
                Self::Commits(commits) => commits.size(),
                Self::Smops(smops) => smops.size(),
                Self::Entries(entries) => entries.size(),
            }
    }
}

pub struct Iter<'i, F: Format<'i>> {
    buf: &'i [u8],
    idx: usize,
    _ph: PhantomData<F>,
}
impl<'i, F: Format<'i>> Iter<'i, F> {
    pub fn reset(&mut self) {
        self.idx = 0;
    }
}
impl<'i, F: Format<'i>> Format<'i> for Iter<'i, F> {
    fn from_bytes(buf: &'i [u8]) -> Self {
        Self {
            buf,
            idx: 0,
            _ph: PhantomData,
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        buf.copy_from_slice(self.buf);
    }
    fn size(&self) -> usize {
        self.buf.len()
    }
}
impl<'i, F: Format<'i>> Iterator for Iter<'i, F> {
    type Item = F;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.buf.len() {
            return None;
        }
        let f = F::from_bytes(&self.buf[self.idx..]);
        self.idx += f.size();
        Some(f)
    }
}

pub struct Commit<'c> {
    pub ts: u64,
    pub write: Write<'c>,
}
impl Commit<'_> {
    pub const TS_SIZE: usize = mem::size_of::<u64>();
}
impl<'c> Format<'c> for Commit<'c> {
    fn from_bytes(buf: &'c [u8]) -> Self {
        let mut cursor = 0;
        let ts = u64::from_le_bytes(buf[cursor..cursor + Self::TS_SIZE].try_into().unwrap());
        cursor += Self::TS_SIZE;
        let write = Write::from_bytes(&buf[cursor..]);
        Self { ts, write }
    }
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
pub enum Write<'w> {
    Put(Put<'w>),
    Del(Del<'w>),
}
impl Write<'_> {
    pub const FLAG_SIZE: usize = mem::size_of::<u8>();
    pub const PUT_FLAG: u8 = 1;
    pub const DEL_FLAG: u8 = 2;

    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put(put) => put.key,
            Self::Del(del) => del.key,
        }
    }
}
impl<'w> Format<'w> for Write<'w> {
    fn from_bytes(buf: &'w [u8]) -> Self {
        match buf[0] {
            Self::PUT_FLAG => Self::Put(Put::from_bytes(&buf[1..])),
            Self::DEL_FLAG => Self::Del(Del::from_bytes(&buf[1..])),
            _ => panic!(),
        }
    }
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

pub struct Put<'p> {
    pub key: &'p [u8],
    pub val: &'p [u8],
}
impl<'p> Format<'p> for Put<'p> {
    fn from_bytes(buf: &'p [u8]) -> Self {
        let mut cursor = 0;
        let key_len =
            u16::from_le_bytes(buf[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let val_len =
            u32::from_le_bytes(buf[cursor..cursor + VAL_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += VAL_LEN_SIZE;
        let key = &buf[cursor..cursor + key_len];
        cursor += key_len;
        let val = &buf[cursor..cursor + val_len];
        Self { key, val }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + KEY_LEN_SIZE].copy_from_slice(&(self.key.len() as u16).to_le_bytes());
        cursor += KEY_LEN_SIZE;
        buf[cursor..cursor + VAL_LEN_SIZE].copy_from_slice(&(self.val.len() as u16).to_le_bytes());
        cursor += VAL_LEN_SIZE;
        buf[cursor..cursor + self.key.len()].copy_from_slice(self.key);
        cursor += self.key.len();
        buf[cursor..cursor + self.val.len()].copy_from_slice(self.val);
    }
    fn size(&self) -> usize {
        KEY_LEN_SIZE + VAL_LEN_SIZE + self.key.len() + self.val.len()
    }
}
pub struct Del<'d> {
    pub key: &'d [u8],
}
impl<'d> Format<'d> for Del<'d> {
    fn from_bytes(buf: &'d [u8]) -> Self {
        let mut cursor = 0;
        let key_len =
            u16::from_le_bytes(buf[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let key = &buf[cursor..cursor + key_len];
        Self { key }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + KEY_LEN_SIZE].copy_from_slice(&(self.key.len() as u16).to_le_bytes());
        cursor += KEY_LEN_SIZE;
        buf[cursor..cursor + self.key.len()].copy_from_slice(self.key);
    }
    fn size(&self) -> usize {
        KEY_LEN_SIZE + self.key.len()
    }
}

pub const PID_SIZE: usize = mem::size_of::<u64>();

pub struct Smop<'s> {
    pub pid: u64,
    pub gt_key: &'s [u8],
    pub lte_key: &'s [u8],
}
impl<'s> Format<'s> for Smop<'s> {
    fn from_bytes(buf: &'s [u8]) -> Self {
        let mut cursor = 0;
        let pid = u64::from_le_bytes(buf[cursor..cursor + PID_SIZE].try_into().unwrap());
        cursor += PID_SIZE;
        let gt_key_len =
            u16::from_le_bytes(buf[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let lte_key_len =
            u16::from_le_bytes(buf[cursor..cursor + KEY_LEN_SIZE].try_into().unwrap()) as usize;
        cursor += KEY_LEN_SIZE;
        let gt_key = &buf[cursor..cursor + gt_key_len];
        cursor += gt_key_len;
        let lte_key = &buf[cursor..cursor + lte_key_len];
        Self {
            pid,
            gt_key,
            lte_key,
        }
    }
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
        buf[cursor..cursor + self.gt_key.len()].copy_from_slice(self.gt_key);
        cursor += self.gt_key.len();
        buf[cursor..cursor + self.lte_key.len()].copy_from_slice(self.lte_key);
    }
    fn size(&self) -> usize {
        PID_SIZE + (2 * KEY_LEN_SIZE) + self.gt_key.len() + self.lte_key.len()
    }
}

pub trait Entries<'e>: Format<'e> {
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
pub struct InnerEntries<'i> {
    pub key_offs: &'i [u8],
    pub key_lens: &'i [u8],
    pub pids: &'i [u8],
    pub keys: &'i [u8],
}
impl<'i> Entries<'i> for InnerEntries<'i> {
    type Val = u64;
    fn search(&self, _target: &[u8]) -> Self::Val {
        todo!()
    }
}
impl<'i> Format<'i> for InnerEntries<'i> {
    /// expects exact size buf
    fn from_bytes(buf: &'i [u8]) -> Self {
        let mut cursor = 0;
        let num_entries =
            u16::from_le_bytes(buf[cursor..cursor + NUM_ENTRIES_SIZE].try_into().unwrap()) as usize;
        cursor += NUM_ENTRIES_SIZE;
        let key_offs = &buf[cursor..cursor + (OFF_SIZE * num_entries)];
        cursor += key_offs.len();
        let key_lens = &buf[cursor..cursor + (KEY_LEN_SIZE * num_entries)];
        cursor += key_lens.len();
        let pids = &buf[cursor..cursor + (PID_SIZE * (num_entries + 1))];
        cursor += pids.len();
        let keys = &buf[cursor..];
        Self {
            key_lens,
            key_offs,
            pids,
            keys,
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + NUM_ENTRIES_SIZE]
            .copy_from_slice(&(self.key_lens.len() as u16).to_le_bytes());
        cursor += NUM_ENTRIES_SIZE;
        buf[cursor..cursor + self.key_offs.len()].copy_from_slice(self.key_offs);
        cursor += self.key_offs.len();
        buf[cursor..cursor + self.key_lens.len()].copy_from_slice(self.key_lens);
        cursor += self.key_lens.len();
        buf[cursor..cursor + self.pids.len()].copy_from_slice(self.pids);
        cursor += self.pids.len();
        buf[cursor..cursor + self.keys.len()].copy_from_slice(self.keys);
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
pub struct LeafEntries<'l> {
    offs: &'l [u8],
    key_lens: &'l [u8],
    val_lens: &'l [u8],
    entries: &'l [u8],
}
impl<'l> Entries<'l> for LeafEntries<'l> {
    type Val = Option<&'l [u8]>;
    fn search(&self, _target: &[u8]) -> Self::Val {
        todo!()
    }
}
impl<'l> Format<'l> for LeafEntries<'l> {
    fn from_bytes(buf: &'l [u8]) -> Self {
        let mut cursor = 0;
        let num_entries =
            u16::from_le_bytes(buf[cursor..cursor + NUM_ENTRIES_SIZE].try_into().unwrap()) as usize;
        cursor += NUM_ENTRIES_SIZE;
        let offs = &buf[cursor..cursor + (OFF_SIZE * num_entries)];
        cursor += offs.len();
        let key_lens = &buf[cursor..cursor + (KEY_LEN_SIZE * num_entries)];
        cursor += key_lens.len();
        let val_lens = &buf[cursor..cursor + (VAL_LEN_SIZE * num_entries)];
        cursor += val_lens.len();
        let entries = &buf[cursor..];
        Self {
            offs,
            key_lens,
            val_lens,
            entries,
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.size());
        let mut cursor = 0;
        buf[cursor..cursor + NUM_ENTRIES_SIZE]
            .copy_from_slice(&(self.offs.len() as u16).to_le_bytes());
        cursor += NUM_ENTRIES_SIZE;
        buf[cursor..cursor + self.offs.len()].copy_from_slice(self.offs);
        cursor += self.offs.len();
        buf[cursor..cursor + self.key_lens.len()].copy_from_slice(self.key_lens);
        cursor += self.key_lens.len();
        buf[cursor..cursor + self.val_lens.len()].copy_from_slice(self.val_lens);
        cursor += self.val_lens.len();
        buf[cursor..cursor + self.entries.len()].copy_from_slice(self.entries);
    }
    fn size(&self) -> usize {
        NUM_ENTRIES_SIZE
            + self.offs.len()
            + self.key_lens.len()
            + self.val_lens.len()
            + self.entries.len()
    }
}
