use std::{fmt, iter, marker, mem, ops::Deref};

pub const CODE_SIZE: usize = mem::size_of::<u8>();
pub const LEN_SIZE: usize = mem::size_of::<u32>();
pub const PID_SIZE: usize = mem::size_of::<u64>();
pub const TXN_ID_SIZE: usize = mem::size_of::<u64>();

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key<'k>(pub &'k [u8]);
impl Deref for Key<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Val<'v>(pub &'v [u8]);
impl Deref for Val<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Record<'r> {
    key: Key<'r>,
    val: Val<'r>,
}

pub trait Op<'o>: Copy + fmt::Debug {
    fn code(&self) -> u8;
    fn from_bytes(buf: &'o [u8]) -> Result<Self, ErrOp>;
    fn write_to_buf(&self, buf: &mut [u8]);
    fn len(&self) -> usize;
    fn key(&self) -> Option<&[u8]>;

    /// anytime this is called is bad, we want to minimize this as much as possible
    fn to_vec(&self) -> Vec<u8> {
        let mut buf = vec![0; self.len()];
        self.write_to_buf(&mut buf);
        buf
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ErrOp {
    EOF,
    InvalidCode,
    CorruptData,
}

/// a get operation
///
/// layout:
/// `[ 1 (u8) ][ key_len (u32) ][ key ([u8]) ... ]`
#[derive(Clone, Copy, Debug)]
pub struct GetOp<'g> {
    pub key: Key<'g>,
}
impl GetOp<'_> {
    pub const CODE: u8 = 1;
}
impl<'g> Op<'g> for GetOp<'g> {
    fn code(&self) -> u8 {
        Self::CODE
    }
    fn len(&self) -> usize {
        CODE_SIZE + LEN_SIZE + self.key.len()
    }
    fn key(&self) -> Option<&[u8]> {
        Some(self.key.0)
    }
    fn from_bytes(buf: &'g [u8]) -> Result<Self, ErrOp> {
        let code = *buf.first().ok_or(ErrOp::EOF)?;
        if code != Self::CODE {
            return Err(ErrOp::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(ErrOp::EOF)?
                .try_into()
                .map_err(|_| ErrOp::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let key = Key(buf.get(cursor..cursor + key_len).ok_or(ErrOp::EOF)?);

        Ok(Self { key })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = self.code();
        cursor += CODE_SIZE;

        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(&self.key);
    }
}

/// a set operation
///
/// layout:
/// `[ 2 (u8) ][ key_len (u32) ][ val_len (u32) ][ key ([u8]) ... ][ val ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct SetOp<'s> {
    pub key: Key<'s>,
    pub val: Val<'s>,
}
impl SetOp<'_> {
    pub const CODE: u8 = 2;
}
impl<'s> Op<'s> for SetOp<'s> {
    fn code(&self) -> u8 {
        Self::CODE
    }
    fn len(&self) -> usize {
        CODE_SIZE + (LEN_SIZE * 2) + self.key.len() + self.val.len()
    }
    fn key(&self) -> Option<&[u8]> {
        Some(self.key.0)
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, ErrOp> {
        let code = *buf.first().ok_or(ErrOp::EOF)?;
        if code != Self::CODE {
            return Err(ErrOp::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(ErrOp::EOF)?
                .try_into()
                .map_err(|_| ErrOp::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let val_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(ErrOp::EOF)?
                .try_into()
                .map_err(|_| ErrOp::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;

        let key = Key(buf.get(cursor..cursor + key_len).ok_or(ErrOp::EOF)?);
        cursor += key_len;
        let val = Val(buf.get(cursor..cursor + val_len).ok_or(ErrOp::EOF)?);

        Ok(Self { key, val })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = self.code();
        cursor += CODE_SIZE;

        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;
        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.val.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(&self.key);
        cursor += self.key.len();
        buf[cursor..cursor + self.val.len()].copy_from_slice(&self.val);
    }
}

/// a delete operation
///
/// layout
/// `[ 3 (u8) ][ key_len (u32) ][ key ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct DelOp<'d> {
    pub key: Key<'d>,
}
impl DelOp<'_> {
    pub const CODE: u8 = 3;
}
impl<'d> Op<'d> for DelOp<'d> {
    fn code(&self) -> u8 {
        Self::CODE
    }
    fn len(&self) -> usize {
        CODE_SIZE + LEN_SIZE + self.key.len()
    }
    fn key(&self) -> Option<&[u8]> {
        Some(self.key.0)
    }
    fn from_bytes(buf: &'d [u8]) -> Result<Self, ErrOp> {
        let code = *buf.first().ok_or(ErrOp::EOF)?;
        if code != Self::CODE {
            return Err(ErrOp::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(ErrOp::EOF)?
                .try_into()
                .map_err(|_| ErrOp::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let key = Key(buf.get(cursor..cursor + key_len).ok_or(ErrOp::EOF)?);

        Ok(Self { key })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = self.code();
        cursor += CODE_SIZE;

        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(&self.key);
    }
}

/// a split operation
///
/// layout:
/// `[ 10 (u8) ][ left_pid (u64) ][ middle_key_len (u32) ][ middle_key ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct SplitOp<'s> {
    pub left_pid: crate::PageId,
    pub middle_key: Key<'s>,
}
impl SplitOp<'_> {
    pub const CODE: u8 = 10;
}
impl<'s> Op<'s> for SplitOp<'s> {
    fn code(&self) -> u8 {
        Self::CODE
    }
    fn len(&self) -> usize {
        CODE_SIZE + PID_SIZE + LEN_SIZE + self.middle_key.len()
    }
    fn key(&self) -> Option<&[u8]> {
        Some(self.middle_key.0)
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, ErrOp> {
        let code = *buf.first().ok_or(ErrOp::EOF)?;
        if code != Self::CODE {
            return Err(ErrOp::CorruptData);
        }

        let mut cursor = 1;

        let left_pid = crate::PageId(u64::from_be_bytes(
            buf.get(cursor..cursor + PID_SIZE)
                .ok_or(ErrOp::EOF)?
                .try_into()
                .map_err(|_| ErrOp::CorruptData)?,
        ));
        cursor += PID_SIZE;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(ErrOp::EOF)?
                .try_into()
                .map_err(|_| ErrOp::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let middle_key = Key(buf.get(cursor..cursor + key_len).ok_or(ErrOp::EOF)?);

        Ok(Self {
            left_pid,
            middle_key,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = self.code();
        cursor += CODE_SIZE;

        buf[cursor..cursor + PID_SIZE].copy_from_slice(&self.left_pid.0.to_be_bytes());
        cursor += PID_SIZE;

        buf[cursor..cursor + LEN_SIZE]
            .copy_from_slice(&(self.middle_key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;
        buf[cursor..cursor + self.middle_key.len()].copy_from_slice(&self.middle_key);
    }
}

#[derive(Copy, Clone, Debug)]
pub enum WriteOp<'w> {
    Set(SetOp<'w>),
    Del(DelOp<'w>),
}
impl<'w> Op<'w> for WriteOp<'w> {
    fn code(&self) -> u8 {
        match self {
            Self::Set(set) => set.code(),
            Self::Del(del) => del.code(),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Set(set) => set.len(),
            Self::Del(del) => del.len(),
        }
    }
    fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Set(set) => set.key(),
            Self::Del(del) => del.key(),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Set(set) => set.write_to_buf(buf),
            Self::Del(del) => del.write_to_buf(buf),
        }
    }
    fn from_bytes(buf: &'w [u8]) -> Result<Self, ErrOp> {
        Ok(match *buf.first().ok_or(ErrOp::EOF)? {
            SetOp::CODE => Self::Set(SetOp::from_bytes(buf)?),
            DelOp::CODE => Self::Del(DelOp::from_bytes(buf)?),
            _ => return Err(ErrOp::InvalidCode),
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ReadOp<'r> {
    Get(GetOp<'r>),
}
impl<'r> Op<'r> for ReadOp<'r> {
    fn code(&self) -> u8 {
        match self {
            Self::Get(get) => get.code(),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Get(get) => get.len(),
        }
    }
    fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Get(get) => get.key(),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Get(get) => get.write_to_buf(buf),
        }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, ErrOp> {
        Ok(match *buf.first().ok_or(ErrOp::EOF)? {
            GetOp::CODE => Self::Get(GetOp::from_bytes(buf)?),
            _ => return Err(ErrOp::InvalidCode),
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub enum RequestOp<'r> {
    Read(ReadOp<'r>),
    Write(WriteOp<'r>),
}

impl<'r> Op<'r> for RequestOp<'r> {
    fn code(&self) -> u8 {
        match self {
            Self::Read(read) => read.code(),
            Self::Write(write) => write.code(),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Read(read) => read.len(),
            Self::Write(write) => write.len(),
        }
    }
    fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Read(read) => read.key(),
            Self::Write(write) => write.key(),
        }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, ErrOp> {
        Ok(match *buf.first().ok_or(ErrOp::EOF)? {
            GetOp::CODE => Self::Read(ReadOp::from_bytes(buf)?),
            SetOp::CODE | DelOp::CODE => Self::Write(WriteOp::from_bytes(buf)?),
            _ => return Err(ErrOp::InvalidCode),
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Read(read) => read.write_to_buf(buf),
            Self::Write(write) => write.write_to_buf(buf),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum OkOp<'r> {
    Get(Option<Val<'r>>),
    Success,
}

/// a network request
///
/// transactions are began implicitly, on the first op sent for a given txn_id.
/// txn_ids are scoped to individual connections, and generated client side.
/// see [`crate::ConnTxnId`] for more details
///
/// layout:
/// `[ txn_id (u64) ][ op ... ]`
pub struct Request<'r> {
    pub txn_id: crate::ConnTxnId,
    pub op: RequestOp<'r>,
}
impl<'r> Request<'r> {
    pub fn len(&self) -> usize {
        TXN_ID_SIZE + self.op.len()
    }
    pub fn from_bytes(buf: &'r [u8]) -> Result<Self, ErrOp> {
        let mut cursor = 0;

        let txn_id = crate::ConnTxnId(u64::from_be_bytes(
            buf.get(cursor..TXN_ID_SIZE)
                .ok_or(ErrOp::EOF)?
                .try_into()
                .map_err(|_| ErrOp::CorruptData)?,
        ));
        cursor += TXN_ID_SIZE;

        Ok(Self {
            txn_id,
            op: RequestOp::from_bytes(buf.get(cursor..).ok_or(ErrOp::EOF)?)?,
        })
    }
}

pub struct Response<'r> {
    pub txn_id: crate::ConnTxnId,
    pub op: Result<OkOp<'r>, ErrOp>,
}

#[derive(Copy, Clone, Debug)]
pub enum SMOp<'s> {
    Split(SplitOp<'s>),
}
impl<'s> Op<'s> for SMOp<'s> {
    fn code(&self) -> u8 {
        match self {
            Self::Split(split) => split.code(),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Split(split) => split.len(),
        }
    }
    fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Split(split) => split.key(),
        }
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, ErrOp> {
        Ok(match *buf.first().ok_or(ErrOp::EOF)? {
            SplitOp::CODE => Self::Split(SplitOp::from_bytes(buf)?),
            _ => return Err(ErrOp::InvalidCode),
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Split(split) => split.write_to_buf(buf),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum PageOp<'p> {
    Write(WriteOp<'p>),
    SMO(SMOp<'p>),
}
impl<'p> Op<'p> for PageOp<'p> {
    fn code(&self) -> u8 {
        match self {
            Self::Write(write) => write.code(),
            Self::SMO(smo) => smo.code(),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Write(write) => write.len(),
            Self::SMO(smo) => smo.len(),
        }
    }
    fn key(&self) -> Option<&[u8]> {
        match self {
            Self::SMO(smop) => smop.key(),
            Self::Write(write) => write.key(),
        }
    }
    fn from_bytes(buf: &'p [u8]) -> Result<Self, ErrOp> {
        Ok(match *buf.first().ok_or(ErrOp::EOF)? {
            SetOp::CODE | DelOp::CODE => Self::Write(WriteOp::from_bytes(buf)?),
            SplitOp::CODE => Self::SMO(SMOp::from_bytes(buf)?),
            _ => return Err(ErrOp::InvalidCode),
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Write(write) => write.write_to_buf(buf),
            Self::SMO(smo) => smo.write_to_buf(buf),
        }
    }
}

/// a log-structured b-tree page
///
/// layout:
/// `[ ops ... ][ entries ... ][ entries_len (u64) ][ left_pid (u64) ][ right_pid (u64) ]`
pub struct Page<'p> {
    pub ops: OpIter<'p, PageOp<'p>>,
    pub entries: RecordMap<'p>,
    pub left_pid: crate::PageId,
    pub right_pid: crate::PageId,
}
impl Page<'_> {
    pub const ENTRIES_LEN_SIZE: usize = mem::size_of::<u64>();

    pub fn search_inner(&mut self, target: &[u8]) -> crate::PageId {
        self.ops.reset();

        let mut best_ops_split: Option<SplitOp<'_>> = None;
        while let Some(op) = self.ops.next() {
            match op {
                PageOp::SMO(smop) => match smop {
                    SMOp::Split(split) => {
                        if target <= split.key().unwrap() {
                            match best_ops_split {
                                Some(best_split) => {
                                    if split.middle_key < best_split.middle_key {
                                        best_ops_split = Some(split);
                                    }
                                }
                                None => best_ops_split = Some(split),
                            }
                        }
                    }
                },
                _ => panic!(),
            }
        }

        match (self.entries.search_inner(target), best_ops_split) {
            (Some(entries_split), Some(ops_split)) => {
                if entries_split.middle_key < ops_split.middle_key {
                    entries_split.left_pid
                } else {
                    ops_split.left_pid
                }
            }
            (Some(entries_split), None) => entries_split.left_pid,
            (None, Some(ops_split)) => ops_split.left_pid,
            (None, None) => self.right_pid,
        }
    }
    pub fn search_leaf(&mut self, target: &[u8]) -> Option<Val> {
        self.ops.reset();

        while let Some(op) = self.ops.next() {
            match op {
                PageOp::Write(write) => match write {
                    WriteOp::Set(set) => {
                        if set.key().unwrap() == target {
                            return Some(set.val);
                        }
                    }
                    WriteOp::Del(del) => {
                        if del.key().unwrap() == target {
                            return None;
                        }
                    }
                },
                o => panic!("{:?}", o),
            }
        }

        self.entries.search_leaf(target)
    }

    #[inline]
    pub fn is_inner(&self) -> bool {
        self.left_pid.0 == u64::MAX
    }
}
impl<'p> From<&'p [u8]> for Page<'p> {
    fn from(buf: &'p [u8]) -> Self {
        let mut cursor = buf.len();

        let right_pid = crate::PageId(u64::from_be_bytes(
            buf[cursor - PID_SIZE..cursor].try_into().unwrap(),
        ));
        cursor -= PID_SIZE;
        let left_pid = crate::PageId(u64::from_be_bytes(
            buf[cursor - PID_SIZE..cursor].try_into().unwrap(),
        ));
        cursor -= PID_SIZE;

        let base_len =
            u64::from_be_bytes(buf[cursor - Self::ENTRIES_LEN_SIZE..].try_into().unwrap()) as usize;
        cursor -= Self::ENTRIES_LEN_SIZE;

        let ops = OpIter::from(&buf[..cursor - base_len]);
        let entries = RecordMap::from(&buf[cursor - base_len..cursor]);

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
/// NOTE: this is assumed to be infalible, data in `buf` should be valid
pub struct OpIter<'i, O: Op<'i>> {
    pub buf: &'i [u8],
    pub top: usize,
    _ph: marker::PhantomData<O>,
}
impl<'i, O: Op<'i>> OpIter<'i, O> {
    pub fn reset(&mut self) {
        self.top = 0;
    }
}
impl<'i, O: Op<'i>> From<&'i [u8]> for OpIter<'i, O> {
    fn from(buf: &'i [u8]) -> Self {
        Self {
            buf,
            top: 0,
            _ph: marker::PhantomData,
        }
    }
}
impl<'i, O: Op<'i>> Iterator for OpIter<'i, O> {
    type Item = O;
    fn next(&mut self) -> Option<Self::Item> {
        if self.top >= self.buf.len() {
            return None;
        }

        let op = O::from_bytes(&self.buf[self.top..]).unwrap();
        self.top += op.len();

        Some(op)
    }
}

/// an efficient read-only ordered map of [`Record`]s
pub struct RecordMap<'r> {
    pub buf: &'r [u8],
}
impl<'r> RecordMap<'r> {
    pub fn search_inner(&self, target: &[u8]) -> Option<SplitOp<'_>> {
        for split in self.iter_inner() {
            if target <= split.key().unwrap() {
                return Some(split);
            }
        }
        None
    }
    pub fn search_leaf(&self, target: &[u8]) -> Option<Val> {
        for set in self.iter_leaf() {
            if target == set.key().unwrap() {
                return Some(set.val);
            }
        }
        None
    }

    pub fn iter_inner(&self) -> impl Iterator<Item = SplitOp<'r>> {
        let mut cursor = 0;
        iter::from_fn(move || {
            if cursor == self.buf.len() {
                return None;
            }
            let split = SplitOp::from_bytes(&self.buf[cursor..]).unwrap();
            cursor += split.len();

            Some(split)
        })
    }
    pub fn iter_leaf(&self) -> impl Iterator<Item = SetOp<'r>> {
        let mut cursor = 0;
        iter::from_fn(move || {
            if cursor == self.buf.len() {
                return None;
            }
            let set = SetOp::from_bytes(&self.buf[cursor..]).unwrap();
            cursor += set.len();

            Some(set)
        })
    }
}
impl<'b> From<&'b [u8]> for RecordMap<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}

/// a chunk of a page, these are how pages are represented on disk.
/// they can contain only ops, entries + footer, or both
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
