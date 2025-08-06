pub mod net;
pub mod op;
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

#[derive(Clone, Copy, Debug)]
pub struct KVLen(u32);
impl KVLen {
    pub const SIZE: usize = mem::size_of::<u32>();
}
impl Format<'_> for KVLen {
    fn len(&self) -> usize {
        Self::SIZE
    }
    fn from_bytes(buf: &[u8]) -> Result<Self, Error> {
        Ok(Self(u32::from_be_bytes(
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
