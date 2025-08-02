use std::mem;

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
