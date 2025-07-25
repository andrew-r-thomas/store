use std::{iter, mem};

pub const CODE_SIZE: usize = mem::size_of::<u8>();
pub const LEN_SIZE: usize = mem::size_of::<u32>();
pub const PID_SIZE: usize = mem::size_of::<u64>();
pub const TXN_ID_SIZE: usize = mem::size_of::<u64>();

/// an operation
///
/// layout:
/// `[ code (u8) ][ variant ... ]`
#[derive(Clone, Copy, Debug)]
pub enum Op<'o> {
    Get(Get<'o>),
    Set(Set<'o>),
    Del(Del<'o>),
    Split(Split<'o>),
}
impl Op<'_> {
    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        match self {
            Self::Get(get) => {
                buf[0] = Get::CODE;
                get.write_to_buf(&mut buf[1..]);
            }
            Self::Set(set) => {
                buf[0] = Set::CODE;
                set.write_to_buf(&mut buf[1..]);
            }
            Self::Del(del) => {
                buf[0] = Del::CODE;
                del.write_to_buf(&mut buf[1..]);
            }
            Self::Split(split) => {
                buf[0] = Split::CODE;
                split.write_to_buf(&mut buf[1..]);
            }
        }
    }
    pub fn len(&self) -> usize {
        CODE_SIZE
            + match self {
                Self::Get(get) => get.len(),
                Self::Set(set) => set.len(),
                Self::Del(del) => del.len(),
                Self::Split(split) => split.len(),
            }
    }
}
impl<'o> TryFrom<&'o [u8]> for Op<'o> {
    type Error = ();
    fn try_from(buf: &'o [u8]) -> Result<Self, Self::Error> {
        let code = *buf.first().ok_or(())?;
        match code {
            Get::CODE => Ok(Self::Get(Get::try_from(buf.get(1..).ok_or(())?)?)),
            Set::CODE => Ok(Self::Set(Set::try_from(buf.get(1..).ok_or(())?)?)),
            Del::CODE => Ok(Self::Del(Del::try_from(buf.get(1..).ok_or(())?)?)),
            Split::CODE => Ok(Self::Split(Split::try_from(buf.get(1..).ok_or(())?)?)),
            c => panic!("{c}"),
        }
    }
}

/// a get operation
///
/// layout:
/// `[ key_len (u32) ][ key ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct Get<'g> {
    key: &'g [u8],
}
impl Get<'_> {
    pub const CODE: u8 = 1;

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(self.key);
    }
    pub fn len(&self) -> usize {
        LEN_SIZE + self.key.len()
    }
}
impl<'g> TryFrom<&'g [u8]> for Get<'g> {
    type Error = ();
    fn try_from(buf: &'g [u8]) -> Result<Self, Self::Error> {
        let mut cursor = 0;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        ) as usize;
        cursor += LEN_SIZE;
        let key = buf.get(cursor..cursor + key_len).ok_or(())?;

        Ok(Self { key })
    }
}

/// a set operation
///
/// layout:
/// `[ key_len (u32) ][ val_len (u32) ][ key ([u8]) ... ][ val ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct Set<'s> {
    pub key: &'s [u8],
    pub val: &'s [u8],
}
impl Set<'_> {
    pub const CODE: u8 = 2;

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;
        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.val.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(self.key);
        cursor += self.key.len();
        buf[cursor..cursor + self.val.len()].copy_from_slice(self.val);
    }
    pub fn len(&self) -> usize {
        (LEN_SIZE * 2) + self.key.len() + self.val.len()
    }
}
impl<'s> TryFrom<&'s [u8]> for Set<'s> {
    type Error = ();
    fn try_from(buf: &'s [u8]) -> Result<Self, Self::Error> {
        let mut cursor = 0;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        ) as usize;
        cursor += LEN_SIZE;
        let val_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        ) as usize;
        cursor += LEN_SIZE;

        let key = buf.get(cursor..cursor + key_len).ok_or(())?;
        cursor += key_len;
        let val = buf.get(cursor..cursor + val_len).ok_or(())?;

        Ok(Self { key, val })
    }
}

/// a delete operation
///
/// layout
/// `[ key_len (u32) ][ key ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct Del<'d> {
    pub key: &'d [u8],
}
impl Del<'_> {
    pub const CODE: u8 = 3;

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(self.key);
    }
    pub fn len(&self) -> usize {
        LEN_SIZE + self.key.len()
    }
}
impl<'d> TryFrom<&'d [u8]> for Del<'d> {
    type Error = ();
    fn try_from(buf: &'d [u8]) -> Result<Self, Self::Error> {
        let mut cursor = 0;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        ) as usize;
        cursor += LEN_SIZE;
        let key = buf.get(cursor..cursor + key_len).ok_or(())?;

        Ok(Self { key })
    }
}

/// a split operation
///
/// layout:
/// `[ left_pid (u64) ][ middle_key_len (u32) ][ middle_key ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct Split<'s> {
    pub left_pid: crate::PageId,
    pub middle_key: &'s [u8],
}
impl Split<'_> {
    pub const CODE: u8 = 10;

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor..cursor + PID_SIZE].copy_from_slice(&self.left_pid.to_be_bytes());
        cursor += PID_SIZE;
        buf[cursor..cursor + LEN_SIZE]
            .copy_from_slice(&(self.middle_key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.middle_key.len()].copy_from_slice(self.middle_key);
    }
    pub fn len(&self) -> usize {
        PID_SIZE + LEN_SIZE + self.middle_key.len()
    }
}
impl<'s> TryFrom<&'s [u8]> for Split<'s> {
    type Error = ();
    fn try_from(buf: &'s [u8]) -> Result<Self, Self::Error> {
        let mut cursor = 0;

        let left_pid = u64::from_be_bytes(
            buf.get(cursor..cursor + PID_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        );
        cursor += PID_SIZE;

        let middle_key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        ) as usize;
        cursor += LEN_SIZE;
        let middle_key = buf.get(cursor..cursor + middle_key_len).ok_or(())?;

        Ok(Self {
            left_pid,
            middle_key,
        })
    }
}

/// a network request
///
/// layout:
/// `[ txn_id (u64) ][ op ... ]`
///
/// TODO error handling, including invalid op types
pub struct Request<'r> {
    txn_id: crate::TxnId,
    op: Op<'r>,
}
impl Request<'_> {
    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        buf[0..TXN_ID_SIZE].copy_from_slice(&self.txn_id.to_be_bytes());
        self.op.write_to_buf(&mut buf[TXN_ID_SIZE..]);
    }
    pub fn len(&self) -> usize {
        TXN_ID_SIZE + self.op.len()
    }
}
impl<'r> TryFrom<&'r [u8]> for Request<'r> {
    type Error = ();
    fn try_from(buf: &'r [u8]) -> Result<Self, Self::Error> {
        let mut cursor = 0;

        let txn_id = u64::from_be_bytes(
            buf.get(cursor..TXN_ID_SIZE)
                .ok_or(())?
                .try_into()
                .map_err(|_| ())?,
        );
        cursor += TXN_ID_SIZE;

        Ok(Self {
            txn_id,
            op: Op::try_from(buf.get(cursor..).ok_or(())?)?,
        })
    }
}

/// a log-structured b-tree page
///
/// layout:
/// `[ ops ... ][ base_page ][ base_len (u64) ][ left_pid (u64) ][ right_pid (u64) ]`
pub struct Page<'p> {
    pub ops: OpIter<'p>,
    pub entries: Entries<'p>,
    pub left_pid: crate::PageId,
    pub right_pid: crate::PageId,
}
impl Page<'_> {
    pub const BASE_LEN_SIZE: usize = mem::size_of::<u64>();

    pub fn search_inner(&mut self, target: &[u8]) -> crate::PageId {
        self.ops.reset();

        let mut best: Option<Split<'_>> = None;
        while let Some(op) = self.ops.next() {
            match op {
                Op::Split(split) => {
                    if target <= split.middle_key {
                        match best {
                            Some(best_split) => {
                                if split.middle_key < best_split.middle_key {
                                    best = Some(split);
                                }
                            }
                            None => best = Some(split),
                        }
                    }
                }
                _ => panic!(),
            }
        }

        match (self.entries.search_inner(target), best) {
            (Some(base_split), Some(best_split)) => {
                if base_split.middle_key < best_split.middle_key {
                    base_split.left_pid
                } else {
                    best_split.left_pid
                }
            }
            (Some(base_split), None) => base_split.left_pid,
            (None, Some(best_split)) => best_split.left_pid,
            (None, None) => self.right_pid,
        }
    }
    pub fn search_leaf(&mut self, target: &[u8]) -> Option<&[u8]> {
        self.ops.reset();

        while let Some(op) = self.ops.next() {
            match op {
                Op::Set(set) => {
                    if set.key == target {
                        return Some(set.val);
                    }
                }
                Op::Del(del) => {
                    if del.key == target {
                        return None;
                    }
                }
                o => panic!("{:?}", o),
            }
        }

        self.entries.search_leaf(target)
    }

    #[inline]
    pub fn is_inner(&self) -> bool {
        self.left_pid == u64::MAX
    }
}
impl<'p> From<&'p [u8]> for Page<'p> {
    fn from(buf: &'p [u8]) -> Self {
        let mut cursor = buf.len();

        let right_pid = u64::from_be_bytes(buf[cursor - PID_SIZE..cursor].try_into().unwrap());
        cursor -= PID_SIZE;
        let left_pid = u64::from_be_bytes(buf[cursor - PID_SIZE..cursor].try_into().unwrap());
        cursor -= PID_SIZE;

        let base_len =
            u64::from_be_bytes(buf[cursor - Self::BASE_LEN_SIZE..].try_into().unwrap()) as usize;
        cursor -= Self::BASE_LEN_SIZE;

        let ops = OpIter::from(&buf[..cursor - base_len]);
        let entries = Entries::from(&buf[cursor - base_len..cursor]);

        Self {
            ops,
            entries,
            left_pid,
            right_pid,
        }
    }
}

/// a stream of ops
///
/// layout:
/// `[ op 0 ][ op 1 ][ op 2 ][ ... ][ op n ]`
///
/// NOTE: right now this is used only in [`Page`], so it is assumed to be infalible
pub struct OpIter<'i> {
    pub buf: &'i [u8],
    pub top: usize,
}
impl OpIter<'_> {
    pub fn reset(&mut self) {
        self.top = 0;
    }
}
impl<'i> From<&'i [u8]> for OpIter<'i> {
    fn from(buf: &'i [u8]) -> Self {
        Self { buf, top: 0 }
    }
}
impl<'i> Iterator for OpIter<'i> {
    type Item = Op<'i>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.buf.len() {
            return None;
        }

        let op = Op::try_from(&self.buf[self.top..]).unwrap();
        self.top += op.len();

        Some(op)
    }
}

/// compacted data in a log-structured b-tree page
///
/// entries are just [`Op`]s without codes, where the type is implicit,
/// they are sorted by key in ascending order
///
/// layout:
/// `[ key 1 entry ][ key 2 entry ][ ... ][ key n entry ]`
pub struct Entries<'e> {
    pub buf: &'e [u8],
}
impl Entries<'_> {
    pub fn search_inner(&self, target: &[u8]) -> Option<Split<'_>> {
        for split in self.iter_inner() {
            if target <= split.middle_key {
                return Some(split);
            }
        }
        None
    }
    pub fn search_leaf(&self, target: &[u8]) -> Option<&[u8]> {
        for set in self.iter_leaf() {
            if target == set.key {
                return Some(set.val);
            }
        }
        None
    }

    pub fn iter_inner(&self) -> impl Iterator<Item = Split<'_>> {
        let mut cursor = 0;
        iter::from_fn(move || {
            if cursor == self.buf.len() {
                return None;
            }
            let split = Split::try_from(&self.buf[cursor..]).unwrap();
            cursor += split.len();

            Some(split)
        })
    }
    pub fn iter_leaf(&self) -> impl Iterator<Item = Set<'_>> {
        let mut cursor = 0;
        iter::from_fn(move || {
            if cursor == self.buf.len() {
                return None;
            }
            let set = Set::try_from(&self.buf[cursor..]).unwrap();
            cursor += set.len();

            Some(set)
        })
    }
}
impl<'b> From<&'b [u8]> for Entries<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}

/// a chunk of a page, these are how pages are represented on disk
///
/// layout:
/// `[ chunk_len (u64) ][ ops &| (entries + footer) ]`
pub struct PageChunk<'c> {
    buf: &'c [u8],
}
impl PageChunk<'_> {
    const CHUNK_LEN_SIZE: usize = mem::size_of::<u64>();

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        buf[0..Self::CHUNK_LEN_SIZE].copy_from_slice(&(self.buf.len()).to_be_bytes());
        buf[Self::CHUNK_LEN_SIZE..].copy_from_slice(self.buf);
    }
    pub fn len(&self) -> usize {
        Self::CHUNK_LEN_SIZE + self.buf.len()
    }
}
impl<'c> From<&'c [u8]> for PageChunk<'c> {
    fn from(buf: &'c [u8]) -> Self {
        Self { buf }
    }
}
