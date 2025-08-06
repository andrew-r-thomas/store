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
impl<'g> crate::Format<'g> for GetOp<'g> {
    fn len(&self) -> usize {
        crate::CODE_SIZE + crate::KVLen::SIZE + self.key.len()
    }
    fn from_bytes(buf: &'g [u8]) -> Result<Self, crate::Error> {
        let code = *buf.first().ok_or(crate::Error::EOF)?;
        if code != Self::CODE {
            return Err(crate::Error::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + crate::KVLen::SIZE)
                .ok_or(crate::Error::EOF)?
                .try_into()
                .map_err(|_| crate::Error::CorruptData)?,
        ) as usize;
        cursor += crate::KVLen::SIZE;
        let key = buf.get(cursor..cursor + key_len).ok_or(crate::Error::EOF)?;

        Ok(Self { key })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = Self::CODE;
        cursor += crate::CODE_SIZE;

        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;

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
impl<'s> crate::Format<'s> for SetOp<'s> {
    fn len(&self) -> usize {
        crate::CODE_SIZE + (crate::KVLen::SIZE * 2) + self.key.len() + self.val.len()
    }
    fn from_bytes(buf: &'s [u8]) -> Result<Self, crate::Error> {
        let code = *buf.first().ok_or(crate::Error::EOF)?;
        if code != Self::CODE {
            return Err(crate::Error::CorruptData);
        }

        let mut cursor = 1;

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

        buf[cursor] = Self::CODE;
        cursor += crate::CODE_SIZE;

        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;
        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.val.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;

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
impl<'d> crate::Format<'d> for DelOp<'d> {
    fn len(&self) -> usize {
        crate::CODE_SIZE + crate::KVLen::SIZE + self.key.len()
    }
    fn from_bytes(buf: &'d [u8]) -> Result<Self, crate::Error> {
        let code = *buf.first().ok_or(crate::Error::EOF)?;
        if code != Self::CODE {
            return Err(crate::Error::CorruptData);
        }

        let mut cursor = 1;

        let key_len = u32::from_be_bytes(
            buf.get(cursor..cursor + crate::KVLen::SIZE)
                .ok_or(crate::Error::EOF)?
                .try_into()
                .map_err(|_| crate::Error::CorruptData)?,
        ) as usize;
        cursor += crate::KVLen::SIZE;
        let key = buf.get(cursor..cursor + key_len).ok_or(crate::Error::EOF)?;

        Ok(Self { key })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());

        let mut cursor = 0;

        buf[cursor] = Self::CODE;
        cursor += crate::CODE_SIZE;

        buf[cursor..cursor + crate::KVLen::SIZE]
            .copy_from_slice(&(self.key.len() as u32).to_be_bytes());
        cursor += crate::KVLen::SIZE;

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
impl<'w> crate::Format<'w> for WriteOp<'w> {
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
    fn from_bytes(buf: &'w [u8]) -> Result<Self, crate::Error> {
        Ok(match *buf.first().ok_or(crate::Error::EOF)? {
            SetOp::CODE => Self::Set(SetOp::from_bytes(buf)?),
            DelOp::CODE => Self::Del(DelOp::from_bytes(buf)?),
            _ => return Err(crate::Error::InvalidCode),
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
impl<'r> crate::Format<'r> for ReadOp<'r> {
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
    fn from_bytes(buf: &'r [u8]) -> Result<Self, crate::Error> {
        Ok(match *buf.first().ok_or(crate::Error::EOF)? {
            GetOp::CODE => Self::Get(GetOp::from_bytes(buf)?),
            _ => return Err(crate::Error::InvalidCode),
        })
    }
}
