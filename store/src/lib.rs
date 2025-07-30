#![feature(phantom_variance_markers)]

use std::mem;

pub mod central;
pub mod format;
pub mod io;
pub mod mesh;
pub mod page;
pub mod shard;

#[cfg(test)]
pub mod test;

#[derive(Copy, Clone, Debug, Ord, Eq, PartialEq, PartialOrd)]
pub struct PageId(u64);
impl PageId {
    const SIZE: usize = mem::size_of::<u64>();
}
impl<'f> format::Format<'f> for PageId {
    fn len(&self) -> usize {
        Self::SIZE
    }
    fn from_bytes(buf: &'f [u8]) -> Result<Self, format::Error> {
        Ok(Self(u64::from_be_bytes(
            buf.get(..Self::SIZE)
                .ok_or(format::Error::EOF)?
                .try_into()
                .map_err(|_| format::Error::CorruptData)?,
        )))
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());
        buf[0..Self::SIZE].copy_from_slice(&self.0.to_be_bytes());
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnId(u32);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardId(usize);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnTxnId(u64);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardTxnId {
    conn_txn_id: ConnTxnId,
    conn_id: ConnId,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalTxnId {
    shard_txn_id: ShardTxnId,
    shard_id: ShardId,
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Timestamp(u64);
impl Timestamp {
    const SIZE: usize = mem::size_of::<u64>();
}
impl<'f> format::Format<'f> for Timestamp {
    fn len(&self) -> usize {
        Self::SIZE
    }
    fn from_bytes(buf: &'f [u8]) -> Result<Self, format::Error> {
        Ok(Self(u64::from_be_bytes(
            buf.get(..Self::SIZE)
                .ok_or(format::Error::EOF)?
                .try_into()
                .map_err(|_| format::Error::CorruptData)?,
        )))
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());
        buf[0..Self::SIZE].copy_from_slice(&self.0.to_be_bytes());
    }
}
