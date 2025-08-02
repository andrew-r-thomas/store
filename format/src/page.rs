use std::mem;

use crate::{Format, op};

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
impl<'s> crate::Format<'s> for SplitOp<'s> {
    fn len(&self) -> usize {
        crate::CODE_SIZE + self.left_pid.len() + crate::KVLen::SIZE + self.middle_key.len()
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, crate::Error> {
        let code = *buf.first().ok_or(crate::Error::EOF)?;
        if code != Self::CODE {
            return Err(crate::Error::CorruptData);
        }

        let mut cursor = 1;

        let left_pid = crate::PageId::from_bytes(buf.get(cursor..).ok_or(crate::Error::EOF)?)?;
        cursor += left_pid.len();

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + crate::KVLen::SIZE)
                .ok_or(crate::Error::EOF)?
                .try_into()
                .map_err(|_| crate::Error::CorruptData)?,
        ) as usize;
        cursor += crate::KVLen::SIZE;
        let middle_key = buf.get(cursor..cursor + key_len).ok_or(crate::Error::EOF)?;

        Ok(Self {
            left_pid,
            middle_key,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = Self::CODE;
        cursor += crate::CODE_SIZE;

        self.left_pid
            .write_to_buf(&mut buf[cursor..cursor + self.left_pid.len()]);
        cursor += self.left_pid.len();

        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.middle_key.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;
        buf[cursor..cursor + self.middle_key.len()].copy_from_slice(&self.middle_key);
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SMOp<'s> {
    Split(SplitOp<'s>),
}
impl<'s> crate::Format<'s> for SMOp<'s> {
    fn len(&self) -> usize {
        match self {
            Self::Split(split) => split.len(),
        }
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, crate::Error> {
        Ok(match *buf.first().ok_or(crate::Error::EOF)? {
            SplitOp::CODE => Self::Split(SplitOp::from_bytes(buf)?),
            _ => return Err(crate::Error::InvalidCode),
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        match self {
            Self::Split(split) => split.write_to_buf(buf),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PageWrite<'p> {
    pub write: op::WriteOp<'p>,
    pub ts: crate::Timestamp,
}
impl PageWrite<'_> {
    pub fn key(&self) -> &[u8] {
        self.write.key()
    }
}
impl<'p> crate::Format<'p> for PageWrite<'p> {
    fn len(&self) -> usize {
        self.ts.len() + self.write.len()
    }
    fn from_bytes(buf: &'p [u8]) -> Result<Self, crate::Error> {
        let mut cursor = 0;
        let write = op::WriteOp::from_bytes(buf.get(cursor..).ok_or(crate::Error::EOF)?)?;
        cursor += write.len();
        let ts = crate::Timestamp::from_bytes(buf.get(cursor..).ok_or(crate::Error::EOF)?)?;
        Ok(Self { ts, write })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;
        self.write.write_to_buf(&mut buf[cursor..self.write.len()]);
        cursor += self.write.len();
        self.ts
            .write_to_buf(&mut buf[cursor..cursor + self.ts.len()]);
    }
}
#[derive(Copy, Clone, Debug)]
pub enum PageOp<'p> {
    Write(PageWrite<'p>),
    SMO(SMOp<'p>),
}
impl PageOp<'_> {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Write(page_write) => match page_write.write {
                op::WriteOp::Set(set) => set.key,
                op::WriteOp::Del(del) => del.key,
            },
            Self::SMO(smo) => match smo {
                SMOp::Split(split) => split.middle_key,
            },
        }
    }
}
impl<'p> crate::Format<'p> for PageOp<'p> {
    fn len(&self) -> usize {
        match self {
            Self::Write(write) => write.len(),
            Self::SMO(smo) => smo.len(),
        }
    }
    fn from_bytes(buf: &'p [u8]) -> Result<Self, crate::Error> {
        Ok(match *buf.first().ok_or(crate::Error::EOF)? {
            op::SetOp::CODE | op::DelOp::CODE => Self::Write(PageWrite::from_bytes(buf)?),
            SplitOp::CODE => Self::SMO(SMOp::from_bytes(buf)?),
            _ => return Err(crate::Error::InvalidCode),
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
    pub ops: crate::FormatIter<'p, PageOp<'p>>,
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
    pub fn search_leaf(&mut self, target: &[u8], ts: crate::Timestamp) -> Option<&[u8]> {
        self.ops.reset();

        while let Some(op) = self.ops.next() {
            match op {
                PageOp::Write(write) => {
                    if write.ts > ts {
                        continue;
                    }
                    match write.write {
                        op::WriteOp::Set(set) => {
                            if set.key == target {
                                return Some(set.val);
                            }
                        }
                        op::WriteOp::Del(del) => {
                            if del.key == target {
                                return None;
                            }
                        }
                    }
                }
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

        let right_pid = crate::PageId::from_bytes(&buf[cursor - crate::PageId::SIZE..]).unwrap();
        cursor -= right_pid.len();
        let left_pid = crate::PageId::from_bytes(&buf[cursor - crate::PageId::SIZE..]).unwrap();
        cursor -= left_pid.len();

        let base_len = u64::from_be_bytes(
            buf[cursor - Self::ENTRIES_LEN_SIZE..cursor]
                .try_into()
                .unwrap(),
        ) as usize;
        cursor -= Self::ENTRIES_LEN_SIZE;

        let mut ops = crate::FormatIter::from(&buf[..cursor - base_len]);
        let entries = Entries::from(&buf[cursor - base_len..cursor]);

        // invariant
        if left_pid.0 != u64::MAX {
            // leaf pages need ops to be in timestamp order
            if let Some(op) = ops.next() {
                let mut prev = match op {
                    PageOp::Write(page_write) => page_write.ts,
                    PageOp::SMO(_) => panic!("smo in leaf page!"),
                };
                for op in ops {
                    match op {
                        PageOp::Write(page_write) => {
                            assert!(page_write.ts <= prev);
                            prev = page_write.ts;
                        }
                        PageOp::SMO(_) => panic!("smo in leaf page!"),
                    }
                }
                ops.reset();
            }
        }

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

    pub fn iter_inner(&self) -> crate::FormatIter<'r, InnerEntry<'r>> {
        crate::FormatIter::from(self.buf)
    }
    pub fn iter_leaf(&self) -> crate::FormatIter<'r, LeafEntry<'r>> {
        crate::FormatIter::from(self.buf)
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
impl<'i> crate::Format<'i> for InnerEntry<'i> {
    fn len(&self) -> usize {
        self.left_pid.len() + crate::KVLen::SIZE + self.middle_key.len()
    }
    fn from_bytes(buf: &'i [u8]) -> Result<Self, crate::Error> {
        let mut cursor = 0;

        let left_pid = crate::PageId::from_bytes(buf.get(cursor..).ok_or(crate::Error::EOF)?)?;
        cursor += left_pid.len();

        let middle_key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + crate::KVLen::SIZE)
                .ok_or(crate::Error::EOF)?
                .try_into()
                .map_err(|_| crate::Error::EOF)?,
        ) as usize;
        cursor += crate::KVLen::SIZE;

        let middle_key = buf
            .get(cursor..cursor + middle_key_len)
            .ok_or(crate::Error::EOF)?;

        Ok(Self {
            left_pid,
            middle_key,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        self.left_pid
            .write_to_buf(&mut buf[cursor..cursor + self.left_pid.len()]);
        cursor += self.left_pid.len();

        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.middle_key.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;

        buf[cursor..cursor + self.middle_key.len()].copy_from_slice(self.middle_key);
    }
}
#[derive(Clone, Copy, Debug)]
pub struct LeafEntry<'l> {
    pub key: &'l [u8],
    pub val: &'l [u8],
}
impl<'l> crate::Format<'l> for LeafEntry<'l> {
    fn len(&self) -> usize {
        (crate::KVLen::SIZE * 2) + self.key.len() + self.val.len()
    }
    fn from_bytes(buf: &'l [u8]) -> Result<Self, crate::Error> {
        let mut cursor = 0;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + crate::KVLen::SIZE)
                .ok_or(crate::Error::EOF)?
                .try_into()
                .map_err(|_| crate::Error::CorruptData)?,
        ) as usize;
        cursor += crate::KVLen::SIZE;
        let val_len = u32::from_be_bytes(
            buf.get(cursor..cursor + crate::KVLen::SIZE)
                .ok_or(crate::Error::EOF)?
                .try_into()
                .map_err(|_| crate::Error::CorruptData)?,
        ) as usize;
        cursor += crate::KVLen::SIZE;

        let key = buf.get(cursor..cursor + key_len).ok_or(crate::Error::EOF)?;
        cursor += key_len;
        let val = buf.get(cursor..cursor + val_len).ok_or(crate::Error::EOF)?;

        Ok(Self { key, val })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;
        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.val.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;

        buf[cursor..cursor + self.key.len()].copy_from_slice(self.key);
        cursor += self.key.len();
        buf[cursor..cursor + self.val.len()].copy_from_slice(self.val);
    }
}
