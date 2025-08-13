pub mod net;
pub mod page;
pub mod storage;

use std::{fmt, marker, mem};

pub const CODE_SIZE: usize = mem::size_of::<u8>();

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

/// an iterator of formated data of a certain type,
///
/// format:
/// `[ F 1 ][ F 2 ][ ... ][ F n ]`
#[derive(Clone, Copy)]
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

        match F::from_bytes(&self.buf[self.cursor..]) {
            Ok(f) => {
                self.cursor += f.len();
                Some(f)
            }
            Err(_) => None,
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageId(pub u64);
impl PageId {
    pub const SIZE: usize = mem::size_of::<u64>();
}
impl Format<'_> for PageId {
    fn len(&self) -> usize {
        Self::SIZE
    }
    fn from_bytes(buf: &[u8]) -> Result<Self, Error> {
        Ok(Self(u64::from_be_bytes(
            buf.get(0..Self::SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        )))
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());
        buf.copy_from_slice(&self.0.to_be_bytes());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnTxnId(pub u64);
impl ConnTxnId {
    pub const SIZE: usize = mem::size_of::<u64>();
}
impl Format<'_> for ConnTxnId {
    fn len(&self) -> usize {
        Self::SIZE
    }
    fn from_bytes(buf: &[u8]) -> Result<Self, Error> {
        Ok(Self(u64::from_be_bytes(
            buf.get(0..Self::SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        )))
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());
        buf.copy_from_slice(&self.0.to_be_bytes());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub u64);
impl Timestamp {
    pub const SIZE: usize = mem::size_of::<u64>();
}
impl Format<'_> for Timestamp {
    fn len(&self) -> usize {
        Self::SIZE
    }
    fn from_bytes(buf: &'_ [u8]) -> Result<Self, Error> {
        Ok(Self(u64::from_be_bytes(
            buf.get(0..Self::SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        )))
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());
        buf.copy_from_slice(&self.0.to_be_bytes());
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Error {
    EOF,
    InvalidCode,
    CorruptData,
    Conflict,
}
impl Error {
    const EOF_CODE: u8 = 101;
    const INVALID_CODE_CODE: u8 = 102;
    const CORRUPT_DATA_CODE: u8 = 103;
    const CONFLICT_CODE: u8 = 104;
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
            Self::CONFLICT_CODE => Ok(Self::Conflict),
            _ => Err(Error::InvalidCode),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());
        match self {
            Self::EOF => buf[0] = Self::EOF_CODE,
            Self::InvalidCode => buf[0] = Self::INVALID_CODE_CODE,
            Self::CorruptData => buf[0] = Self::CORRUPT_DATA_CODE,
            Self::Conflict => buf[0] = Self::CONFLICT_CODE,
        }
    }
}

pub const FLAGS_SIZE: usize = mem::size_of::<u8>();

pub const TYPE_MASK: u8 = 0b_11_000000;
pub const RES_MASK: u8 = 0b_0000000_1;

pub const FLAG_SUCCESS: u8 = 0b_0000000_0;
pub const FLAG_ERROR: u8 = 0b_0000000_1;

/// for now, basically just a "get"
#[derive(Clone, Copy, Debug)]
pub struct Read<'r> {
    key: Key<'r>,
}
impl Read<'_> {
    pub const FLAG_TYPE: u8 = 0b_00_00000;

    pub fn flags(&self) -> u8 {
        Self::FLAG_TYPE
    }
}
impl<'r> Format<'r> for Read<'r> {
    fn len(&self) -> usize {
        FLAGS_SIZE + self.key.len()
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        let mut cursor = 0;

        let flags = *buf.get(cursor).ok_or(Error::EOF)?;
        assert_eq!(flags & TYPE_MASK, Self::FLAG_TYPE);
        // don't care about the other flags for now
        cursor += FLAGS_SIZE;

        Ok(Self {
            key: Key::from_bytes(buf.get(cursor..).ok_or(Error::EOF)?)?,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());
        let mut cursor = 0;
        buf[cursor] = self.flags();
        cursor += FLAGS_SIZE;
        self.key.write_to_buf(&mut buf[cursor..]);
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ReadResp<'r>(Result<Val<'r>, Error>);
impl ReadResp<'_> {
    pub fn flags(&self) -> u8 {
        Read::FLAG_TYPE
            | match self.0 {
                Ok(_) => FLAG_SUCCESS,
                Err(_) => FLAG_ERROR,
            }
    }
}
impl<'r> Format<'r> for ReadResp<'r> {
    fn len(&self) -> usize {
        FLAGS_SIZE
            + match self.0 {
                Ok(val) => val.len(),
                Err(e) => e.len(),
            }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, Error> {
        let mut cursor = 0;
        let flags = *buf.get(cursor).ok_or(Error::EOF)?;
        cursor += FLAGS_SIZE;

        assert_eq!(flags & TYPE_MASK, Read::FLAG_TYPE);

        if flags & RES_MASK == FLAG_SUCCESS {
            // val
            Ok(Self(Result::Ok(Val::from_bytes(
                buf.get(cursor..).ok_or(Error::EOF)?,
            )?)))
        } else {
            // err
            Ok(Self(Result::Err(Error::from_bytes(
                buf.get(cursor..).ok_or(Error::EOF)?,
            )?)))
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());
        let mut cursor = 0;
        buf[cursor] = self.flags();
        cursor += FLAGS_SIZE;
        match self.0 {
            Ok(val) => val.write_to_buf(&mut buf[cursor..]),
            Err(e) => e.write_to_buf(&mut buf[cursor..]),
        }
    }
}

/// ## NOTE
/// `val.0 == None` semantically means "delete `key`"
#[derive(Clone, Copy, Debug)]
pub struct Write<'w> {
    key: Key<'w>,
    val: Val<'w>,
}
impl<'w> Format<'w> for Write<'w> {
    fn len(&self) -> usize {
        todo!()
    }
    fn from_bytes(buf: &'w [u8]) -> Result<Self, Error> {
        todo!()
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        todo!()
    }
}
#[derive(Clone, Copy, Debug)]
pub struct WriteResp(Result<(), Error>);
impl<'f> Format<'f> for WriteResp {
    fn len(&self) -> usize {
        todo!()
    }
    fn from_bytes(buf: &'f [u8]) -> Result<Self, Error> {
        todo!()
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        todo!()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TxnCtrl {
    Commit,
    Abort,
}
impl<'f> Format<'f> for TxnCtrl {
    fn len(&self) -> usize {
        todo!()
    }
    fn from_bytes(buf: &'f [u8]) -> Result<Self, Error> {
        todo!()
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        todo!()
    }
}
#[derive(Clone, Copy, Debug)]
pub struct TxnCtrlResp(Result<(), Error>);
impl<'f> Format<'f> for TxnCtrlResp {
    fn len(&self) -> usize {
        todo!()
    }
    fn from_bytes(buf: &'f [u8]) -> Result<Self, Error> {
        todo!()
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        todo!()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Key<'k>(&'k [u8]);
impl Key<'_> {
    pub const LEN_SIZE: usize = mem::size_of::<u16>();
}
impl<'k> Format<'k> for Key<'k> {
    fn len(&self) -> usize {
        Self::LEN_SIZE + self.0.len()
    }
    fn from_bytes(buf: &'k [u8]) -> Result<Self, Error> {
        let mut cursor = 0;

        let key_len = u16::from_be_bytes(
            buf.get(cursor..cursor + Self::LEN_SIZE)
                .ok_or(Error::EOF)?
                .try_into()
                .map_err(|_| Error::CorruptData)?,
        ) as usize;
        cursor += Self::LEN_SIZE;

        let key = buf.get(cursor..cursor + key_len).ok_or(Error::EOF)?;

        Ok(Self(key))
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor..cursor + Self::LEN_SIZE].copy_from_slice(&(self.0.len() as u16).to_be_bytes());
        cursor += Self::LEN_SIZE;

        buf[cursor..cursor + self.0.len()].copy_from_slice(self.0);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Val<'v>(Option<&'v [u8]>);
impl Val<'_> {
    pub const KIND_SIZE: usize = mem::size_of::<u8>();
    pub const LEN_SIZE: usize = mem::size_of::<u32>();

    pub const SOME_KIND: u8 = 1;
    pub const NONE_KIND: u8 = 0;
}
impl<'v> Format<'v> for Val<'v> {
    fn len(&self) -> usize {
        Self::KIND_SIZE
            + match self.0 {
                Some(v) => Self::LEN_SIZE + v.len(),
                None => 0,
            }
    }
    fn from_bytes(buf: &'v [u8]) -> Result<Self, Error> {
        let mut cursor = 0;
        match *buf.get(cursor).ok_or(Error::EOF)? {
            Self::SOME_KIND => {
                cursor += Self::KIND_SIZE;

                let val_len = u32::from_be_bytes(
                    buf.get(cursor..cursor + Self::LEN_SIZE)
                        .ok_or(Error::EOF)?
                        .try_into()
                        .map_err(|_| Error::CorruptData)?,
                ) as usize;
                cursor += Self::LEN_SIZE;

                let val = buf.get(cursor..cursor + val_len).ok_or(Error::EOF)?;
                Ok(Self(Some(val)))
            }
            Self::NONE_KIND => Ok(Self(None)),
            _ => Err(Error::InvalidCode),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;
        match self.0 {
            Some(v) => {
                buf[cursor] = Self::SOME_KIND;
                cursor += Self::KIND_SIZE;

                buf[cursor..cursor + Self::LEN_SIZE]
                    .copy_from_slice(&(v.len() as u32).to_be_bytes());
                cursor += Self::LEN_SIZE;

                buf[cursor..cursor + v.len()].copy_from_slice(v);
            }
            None => buf[cursor] = Self::NONE_KIND,
        }
    }
}
