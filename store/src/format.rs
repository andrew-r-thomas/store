use std::{fmt, marker, mem};

pub const CODE_SIZE: usize = mem::size_of::<u8>();
pub const LEN_SIZE: usize = mem::size_of::<u32>();
pub const PID_SIZE: usize = mem::size_of::<u64>();
pub const TXN_ID_SIZE: usize = mem::size_of::<u64>();

/// a general trait for parsing and writing the various byte formats in the system
pub trait Format<'f>: Copy + fmt::Debug {
    fn len(&self) -> usize;
    fn from_bytes(buf: &'f [u8]) -> Result<Self, Error>;
    fn write_to_buf(&self, buf: &mut [u8]);

    /// anytime this is called is bad, we want to minimize this as much as possible
    fn to_vec(&self) -> Vec<u8> {
        let mut buf = vec![0; self.len()];
        self.write_to_buf(&mut buf);
        buf
    }
}

/// an infalible iterator of formated data of a certain type,
/// error checking must be done before creating this
///
/// format:
/// `[ F 1 ][ F 2 ][ ... ][ F n ]`
pub struct FormatIter<'f, F: Format<'f>> {
    buf: &'f [u8],
    cursor: usize,
    _ph: marker::PhantomData<F>,
}
impl<'f, F: Format<'f>> FormatIter<'f, F> {
    pub fn reset(&mut self) {
        self.cursor = 0;
    }
}
impl<'f, F: Format<'f>> Iterator for FormatIter<'f, F> {
    type Item = F;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.buf.len() {
            return None;
        }

        let f = F::from_bytes(&self.buf[self.cursor..]).unwrap();
        self.cursor += f.len();

        Some(f)
    }
}
impl<'f, F: Format<'f>> From<&'f [u8]> for FormatIter<'f, F> {
    fn from(buf: &'f [u8]) -> Self {
        Self {
            buf,
            cursor: 0,
            _ph: marker::PhantomData,
        }
    }
}

/// a get operation
///
/// layout:
/// `[ 1 (u8) ][ key_len (u32) ][ key ([u8]) ... ]`
#[derive(Clone, Copy, Debug)]
pub struct GetOp<'g> {
    pub key: &'g [u8],
}
impl GetOp<'_> {
    pub const CODE: u8 = 1;
}
impl<'g> Format<'g> for GetOp<'g> {
    fn len(&self) -> usize {
        CODE_SIZE + LEN_SIZE + self.key.len()
    }
    fn from_bytes(buf: &'g [u8]) -> Result<Self, Error> {
        let code = *buf.first().ok_or(Error::EOF)?;
        if code != Self::CODE {
            return Err(Error::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let key = buf.get(cursor..cursor + key_len).ok_or(Error::EOF)?;

        Ok(Self { key })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = Self::CODE;
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
    pub key: &'s [u8],
    pub val: &'s [u8],
}
impl SetOp<'_> {
    pub const CODE: u8 = 2;
}
impl<'s> Format<'s> for SetOp<'s> {
    fn len(&self) -> usize {
        CODE_SIZE + (LEN_SIZE * 2) + self.key.len() + self.val.len()
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, Error> {
        let code = *buf.first().ok_or(Error::EOF)?;
        if code != Self::CODE {
            return Err(Error::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let val_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;

        let key = buf.get(cursor..cursor + key_len).ok_or(Error::EOF)?;
        cursor += key_len;
        let val = buf.get(cursor..cursor + val_len).ok_or(Error::EOF)?;

        Ok(Self { key, val })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = Self::CODE;
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
    pub key: &'d [u8],
}
impl DelOp<'_> {
    pub const CODE: u8 = 3;
}
impl<'d> Format<'d> for DelOp<'d> {
    fn len(&self) -> usize {
        CODE_SIZE + LEN_SIZE + self.key.len()
    }
    fn from_bytes(buf: &'d [u8]) -> Result<Self, Error> {
        let code = *buf.first().ok_or(Error::EOF)?;
        if code != Self::CODE {
            return Err(Error::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let key = buf.get(cursor..cursor + key_len).ok_or(Error::EOF)?;

        Ok(Self { key })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = Self::CODE;
        cursor += CODE_SIZE;

        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(&self.key);
    }
}

#[derive(Copy, Clone, Debug)]
pub enum WriteOp<'w> {
    Set(SetOp<'w>),
    Del(DelOp<'w>),
}
impl WriteOp<'_> {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Set(set) => set.key,
            Self::Del(del) => del.key,
        }
    }
}
impl<'w> Format<'w> for WriteOp<'w> {
    fn len(&self) -> usize {
        match self {
            Self::Set(set) => set.len(),
            Self::Del(del) => del.len(),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Set(set) => set.write_to_buf(buf),
            Self::Del(del) => del.write_to_buf(buf),
        }
    }
    fn from_bytes(buf: &'w [u8]) -> Result<Self, Error> {
        Ok(match *buf.first().ok_or(Error::EOF)? {
            SetOp::CODE => Self::Set(SetOp::from_bytes(buf)?),
            DelOp::CODE => Self::Del(DelOp::from_bytes(buf)?),
            _ => return Err(Error::InvalidCode),
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ReadOp<'r> {
    Get(GetOp<'r>),
}
impl ReadOp<'_> {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get(get) => get.key,
        }
    }
}
impl<'r> Format<'r> for ReadOp<'r> {
    fn len(&self) -> usize {
        match self {
            Self::Get(get) => get.len(),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Get(get) => get.write_to_buf(buf),
        }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        Ok(match *buf.first().ok_or(Error::EOF)? {
            GetOp::CODE => Self::Get(GetOp::from_bytes(buf)?),
            _ => return Err(Error::InvalidCode),
        })
    }
}

/// a network request
///
/// transactions are began implicitly, on the first op sent for a given txn_id.
/// txn_ids are scoped to individual connections, and generated client side.
/// see [`crate::ConnTxnId`] for more details
///
/// layout:
/// `[ txn_id (u64) ][ op ... ]`
#[derive(Clone, Copy, Debug)]
pub struct Request<'r> {
    pub txn_id: crate::ConnTxnId,
    pub op: RequestOp<'r>,
}
impl<'r> Format<'r> for Request<'r> {
    fn len(&self) -> usize {
        TXN_ID_SIZE + self.op.len()
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        let mut cursor = 0;

        let txn_id = crate::ConnTxnId(u64::from_be_bytes(
            buf.get(cursor..TXN_ID_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ));
        cursor += TXN_ID_SIZE;

        Ok(Self {
            txn_id,
            op: RequestOp::from_bytes(buf.get(cursor..).ok_or(Error::EOF)?)?,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        let mut cursor = 0;

        buf[cursor..cursor + TXN_ID_SIZE].copy_from_slice(&self.txn_id.0.to_be_bytes());
        cursor += TXN_ID_SIZE;

        self.op.write_to_buf(&mut buf[cursor..]);
    }
}
#[derive(Copy, Clone, Debug)]
pub enum RequestOp<'r> {
    Read(ReadOp<'r>),
    Write(WriteOp<'r>),
}
impl<'r> Format<'r> for RequestOp<'r> {
    fn len(&self) -> usize {
        match self {
            Self::Read(read) => read.len(),
            Self::Write(write) => write.len(),
        }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        Ok(match *buf.first().ok_or(Error::EOF)? {
            GetOp::CODE => Self::Read(ReadOp::from_bytes(buf)?),
            SetOp::CODE | DelOp::CODE => Self::Write(WriteOp::from_bytes(buf)?),
            _ => return Err(Error::InvalidCode),
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Read(read) => read.write_to_buf(buf),
            Self::Write(write) => write.write_to_buf(buf),
        }
    }
}

/// ## TODO
/// - packing flag bits into single bytes (useful for errors)
#[derive(Clone, Copy, Debug)]
pub struct Response<'r> {
    pub txn_id: crate::ConnTxnId,
    pub op: Result<Resp<'r>, Error>,
}
impl<'r> Format<'r> for Response<'r> {
    fn len(&self) -> usize {
        TXN_ID_SIZE + self.op.len()
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        let mut cursor = 0;

        let txn_id = crate::ConnTxnId(u64::from_be_bytes(
            buf.get(cursor..TXN_ID_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ));
        cursor += TXN_ID_SIZE;

        Ok(Self {
            txn_id,
            op: Result::<Resp, Error>::from_bytes(buf.get(cursor..).ok_or(Error::EOF)?)?,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        let mut cursor = 0;

        buf[cursor..cursor + TXN_ID_SIZE].copy_from_slice(&self.txn_id.0.to_be_bytes());
        cursor += TXN_ID_SIZE;

        self.op.write_to_buf(&mut buf[cursor..]);
    }
}

impl<'r> Format<'r> for Result<Resp<'r>, Error> {
    fn len(&self) -> usize {
        CODE_SIZE
            + match self {
                Ok(resp) => resp.len(),
                Err(e) => e.len(),
            }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        match *buf.first().ok_or(Error::EOF)? {
            0 => Ok(Self::Ok(Resp::from_bytes(buf.get(1..).ok_or(Error::EOF)?)?)),
            1 => Ok(Self::Err(Error::from_bytes(
                buf.get(1..).ok_or(Error::EOF)?,
            )?)),
            _ => Err(Error::CorruptData),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());
        match self {
            Self::Ok(resp) => {
                buf[0] = 0;
                resp.write_to_buf(&mut buf[1..]);
            }
            Self::Err(e) => {
                buf[0] = 1;
                e.write_to_buf(&mut buf[1..]);
            }
        }
    }
}
#[derive(Copy, Clone, Debug)]
pub enum Resp<'r> {
    Get(Option<&'r [u8]>),
    Write,
}
impl<'r> Format<'r> for Resp<'r> {
    fn len(&self) -> usize {
        match self {
            Self::Get(maybe_val) => {
                CODE_SIZE
                    + match maybe_val {
                        Some(val) => LEN_SIZE + val.len(),
                        None => 0,
                    }
            }
            Self::Write => 0,
        }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        if buf.len() == 0 {
            return Ok(Self::Write);
        }

        let mut cursor = 0;
        match buf[cursor] {
            1 => {
                cursor += CODE_SIZE;
                let val_len = u32::from_be_bytes(
                    buf.get(cursor..cursor + LEN_SIZE)
                        .ok_or(Error::EOF)?
                        .try_into()
                        .map_err(|_| Error::CorruptData)?,
                ) as usize;
                cursor += LEN_SIZE;
                let val = buf.get(cursor..cursor + val_len).ok_or(Error::EOF)?;
                Ok(Self::Get(Some(val)))
            }
            0 => Ok(Self::Get(None)),
            _ => Err(Error::InvalidCode),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        match self {
            Self::Get(maybe_val) => {
                let mut cursor = 0;
                match maybe_val {
                    Some(val) => {
                        buf[cursor] = 1;
                        cursor += CODE_SIZE;
                        buf[cursor..cursor + LEN_SIZE]
                            .copy_from_slice(&(val.len() as u32).to_be_bytes());
                        cursor += LEN_SIZE;
                        buf[cursor..cursor + val.len()].copy_from_slice(val);
                    }
                    None => {
                        buf[cursor] = 0;
                    }
                }
            }
            Self::Write => {}
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum Error {
    EOF,
    InvalidCode,
    CorruptData,
}
impl Error {
    const EOF_CODE: u8 = 101;
    const INVALID_CODE_CODE: u8 = 102;
    const CORRUPT_DATA_CODE: u8 = 103;
}
impl<'e> Format<'e> for Error {
    fn len(&self) -> usize {
        CODE_SIZE
    }
    fn from_bytes(buf: &'e [u8]) -> Result<Self, Error> {
        let code = *buf.first().ok_or(Error::EOF)?;
        match code {
            Self::EOF_CODE => Ok(Self::EOF),
            Self::INVALID_CODE_CODE => Ok(Self::InvalidCode),
            Self::CORRUPT_DATA_CODE => Ok(Self::CorruptData),
            _ => Err(Error::InvalidCode),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());
        match self {
            Self::EOF => buf[0] = Self::EOF_CODE,
            Self::InvalidCode => buf[0] = Self::INVALID_CODE_CODE,
            Self::CorruptData => buf[0] = Self::CORRUPT_DATA_CODE,
        }
    }
}

/// a split operation
///
/// layout:
/// `[ 10 (u8) ][ left_pid (u64) ][ middle_key_len (u32) ][ middle_key ([u8]) ... ]`
#[derive(Copy, Clone, Debug)]
pub struct SplitOp<'s> {
    pub left_pid: crate::PageId,
    pub middle_key: &'s [u8],
}
impl SplitOp<'_> {
    pub const CODE: u8 = 10;
}
impl<'s> Format<'s> for SplitOp<'s> {
    fn len(&self) -> usize {
        CODE_SIZE + PID_SIZE + LEN_SIZE + self.middle_key.len()
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, Error> {
        let code = *buf.first().ok_or(Error::EOF)?;
        if code != Self::CODE {
            return Err(Error::CorruptData);
        }

        let mut cursor = 1;

        let left_pid = crate::PageId(u64::from_be_bytes(
            buf.get(cursor..cursor + PID_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ));
        cursor += PID_SIZE;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ) as usize;
        cursor += LEN_SIZE;
        let middle_key = buf.get(cursor..cursor + key_len).ok_or(Error::EOF)?;

        Ok(Self {
            left_pid,
            middle_key,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = Self::CODE;
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
pub enum SMOp<'s> {
    Split(SplitOp<'s>),
}
impl<'s> Format<'s> for SMOp<'s> {
    fn len(&self) -> usize {
        match self {
            Self::Split(split) => split.len(),
        }
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, Error> {
        Ok(match *buf.first().ok_or(Error::EOF)? {
            SplitOp::CODE => Self::Split(SplitOp::from_bytes(buf)?),
            _ => return Err(Error::InvalidCode),
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
impl<'p> Format<'p> for PageOp<'p> {
    fn len(&self) -> usize {
        match self {
            Self::Write(write) => write.len(),
            Self::SMO(smo) => smo.len(),
        }
    }
    fn from_bytes(buf: &'p [u8]) -> Result<Self, Error> {
        Ok(match *buf.first().ok_or(Error::EOF)? {
            SetOp::CODE | DelOp::CODE => Self::Write(WriteOp::from_bytes(buf)?),
            SplitOp::CODE => Self::SMO(SMOp::from_bytes(buf)?),
            _ => return Err(Error::InvalidCode),
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
    pub ops: FormatIter<'p, PageOp<'p>>,
    pub entries: Entries<'p>,
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
                        if target <= split.middle_key {
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
    pub fn search_leaf(&mut self, target: &[u8]) -> Option<&[u8]> {
        self.ops.reset();

        while let Some(op) = self.ops.next() {
            match op {
                PageOp::Write(write) => match write {
                    WriteOp::Set(set) => {
                        if set.key == target {
                            return Some(set.val);
                        }
                    }
                    WriteOp::Del(del) => {
                        if del.key == target {
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

        let ops = FormatIter::from(&buf[..cursor - base_len]);
        let entries = Entries::from(&buf[cursor - base_len..cursor]);

        Self {
            ops,
            entries,
            left_pid,
            right_pid,
        }
    }
}

/// a read-only ordered map of either key-value pairs, or left_pid-key pairs
///
/// ## TODO
/// - binary search (this will change layout)
pub struct Entries<'r> {
    pub buf: &'r [u8],
}
impl<'r> Entries<'r> {
    pub fn search_inner(&self, target: &[u8]) -> Option<InnerEntry<'r>> {
        for entry in self.iter_inner() {
            if target <= entry.middle_key {
                return Some(entry);
            }
        }
        None
    }
    pub fn search_leaf(&self, target: &[u8]) -> Option<&[u8]> {
        for entry in self.iter_leaf() {
            if target == entry.key {
                return Some(entry.val);
            }
        }
        None
    }

    pub fn iter_inner(&self) -> FormatIter<'r, InnerEntry<'r>> {
        FormatIter::from(self.buf)
    }
    pub fn iter_leaf(&self) -> FormatIter<'r, LeafEntry<'r>> {
        FormatIter::from(self.buf)
    }
}
impl<'b> From<&'b [u8]> for Entries<'b> {
    fn from(buf: &'b [u8]) -> Self {
        Self { buf }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct InnerEntry<'i> {
    pub left_pid: crate::PageId,
    pub middle_key: &'i [u8],
}
impl<'i> Format<'i> for InnerEntry<'i> {
    fn len(&self) -> usize {
        PID_SIZE + LEN_SIZE + self.middle_key.len()
    }
    fn from_bytes(buf: &'i [u8]) -> Result<Self, Error> {
        let mut cursor = 0;

        let left_pid = crate::PageId(u64::from_be_bytes(
            buf.get(cursor..cursor + PID_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ));
        cursor += PID_SIZE;

        let middle_key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::EOF)?,
        ) as usize;
        cursor += LEN_SIZE;

        let middle_key = buf.get(cursor..cursor + middle_key_len).ok_or(Error::EOF)?;

        Ok(Self {
            left_pid,
            middle_key,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor..cursor + PID_SIZE].copy_from_slice(&self.left_pid.0.to_be_bytes());
        cursor += PID_SIZE;

        buf[cursor..cursor + LEN_SIZE]
            .copy_from_slice(&(self.middle_key.len() as u32).to_be_bytes());
        cursor += LEN_SIZE;

        buf[cursor..cursor + self.middle_key.len()].copy_from_slice(self.middle_key);
    }
}
#[derive(Clone, Copy, Debug)]
pub struct LeafEntry<'l> {
    pub key: &'l [u8],
    pub val: &'l [u8],
}
impl<'l> Format<'l> for LeafEntry<'l> {
    fn len(&self) -> usize {
        (LEN_SIZE * 2) + self.key.len() + self.val.len()
    }
    fn from_bytes(buf: &'l [u8]) -> Result<Self, Error> {
        let mut cursor = 0;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::EOF)?,
        ) as usize;
        cursor += LEN_SIZE;
        let val_len = u32::from_be_bytes(
            buf.get(cursor..cursor + LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::EOF)?,
        ) as usize;
        cursor += LEN_SIZE;

        let key = buf.get(cursor..cursor + key_len).ok_or(Error::EOF)?;
        cursor += key_len;
        let val = buf.get(cursor..cursor + val_len).ok_or(Error::EOF)?;

        Ok(Self { key, val })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
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
