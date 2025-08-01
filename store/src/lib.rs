use std::{
    hash::{Hash, Hasher},
    mem,
};

pub mod central;
pub mod config;
pub mod format;
pub mod io;
pub mod mesh;
pub mod page;
pub mod shard;

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
pub struct ConnId(pub u32);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnTxnId(pub u64);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardTxnId {
    pub conn_txn_id: ConnTxnId,
    pub conn_id: ConnId,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalTxnId {
    pub shard_txn_id: ShardTxnId,
    pub shard_id: usize,
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Timestamp(pub u64);
impl Timestamp {
    pub const SIZE: usize = mem::size_of::<u64>();
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

/// NOTE this function adds 1 to the final output, since the 0 index is used for [`Central`]
#[inline]
pub fn find_shard(key: &[u8], num_shards: usize) -> usize {
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    key.hash(&mut hasher);
    let hash = hasher.finish();
    ((hash as usize) % num_shards) + 1
}
