use crate::op;

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
impl<'r> crate::Format<'r> for Request<'r> {
    fn len(&self) -> usize {
        self.txn_id.len() + self.op.len()
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, crate::Error> {
        let mut cursor = 0;

        let txn_id = crate::ConnTxnId::from_bytes(&buf[cursor..])?;
        cursor += txn_id.len();

        Ok(Self {
            txn_id,
            op: RequestOp::from_bytes(buf.get(cursor..).ok_or(crate::Error::EOF)?)?,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        let mut cursor = 0;

        self.txn_id
            .write_to_buf(&mut buf[cursor..cursor + self.txn_id.len()]);
        cursor += self.txn_id.len();

        self.op.write_to_buf(&mut buf[cursor..]);
    }
}
#[derive(Copy, Clone, Debug)]
pub enum RequestOp<'r> {
    Read(op::ReadOp<'r>),
    Write(op::WriteOp<'r>),
    Commit,
}
impl RequestOp<'_> {
    pub const COMMIT_CODE: u8 = 9;
}
impl<'r> crate::Format<'r> for RequestOp<'r> {
    fn len(&self) -> usize {
        match self {
            Self::Read(read) => read.len(),
            Self::Write(write) => write.len(),
            Self::Commit => crate::CODE_SIZE,
        }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, crate::Error> {
        Ok(match *buf.first().ok_or(crate::Error::EOF)? {
            op::GetOp::CODE => Self::Read(op::ReadOp::from_bytes(buf)?),
            op::SetOp::CODE | op::DelOp::CODE => Self::Write(op::WriteOp::from_bytes(buf)?),
            Self::COMMIT_CODE => Self::Commit,
            _ => return Err(crate::Error::InvalidCode),
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(self.len(), buf.len());
        match self {
            Self::Read(read) => read.write_to_buf(buf),
            Self::Write(write) => write.write_to_buf(buf),
            Self::Commit => buf[0] = Self::COMMIT_CODE,
        }
    }
}

/// ## TODO
/// - packing flag bits into single bytes (useful for errors)
#[derive(Clone, Copy, Debug)]
pub struct Response<'r> {
    pub txn_id: crate::ConnTxnId,
    pub op: Result<Resp<'r>, crate::Error>,
}
impl<'r> crate::Format<'r> for Response<'r> {
    fn len(&self) -> usize {
        self.txn_id.len() + self.op.len()
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, crate::Error> {
        let mut cursor = 0;

        let txn_id = crate::ConnTxnId::from_bytes(&buf[cursor..])?;
        cursor += txn_id.len();

        Ok(Self {
            txn_id,
            op: Result::<Resp, crate::Error>::from_bytes(
                buf.get(cursor..).ok_or(crate::Error::EOF)?,
            )?,
        })
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        let mut cursor = 0;

        self.txn_id
            .write_to_buf(&mut buf[cursor..cursor + self.txn_id.len()]);
        cursor += self.txn_id.len();

        self.op.write_to_buf(&mut buf[cursor..]);
    }
}

impl<'r> crate::Format<'r> for Result<Resp<'r>, crate::Error> {
    fn len(&self) -> usize {
        crate::CODE_SIZE
            + match self {
                Ok(resp) => resp.len(),
                Err(e) => e.len(),
            }
    }
    fn from_bytes(buf: &'r [u8]) -> Result<Self, crate::Error> {
        match *buf.first().ok_or(crate::Error::EOF)? {
            0 => Ok(Self::Ok(Resp::from_bytes(
                buf.get(1..).ok_or(crate::Error::EOF)?,
            )?)),
            1 => Ok(Self::Err(crate::Error::from_bytes(
                buf.get(1..).ok_or(crate::Error::EOF)?,
            )?)),
            _ => Err(crate::Error::CorruptData),
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
    Success,
}
impl Resp<'_> {
    const GET_CODE: u8 = 1;
    const SUCCESS_CODE: u8 = 2;
}
impl<'r> crate::Format<'r> for Resp<'r> {
    fn len(&self) -> usize {
        crate::CODE_SIZE
            + match self {
                Self::Get(maybe_val) => {
                    crate::CODE_SIZE
                        + match maybe_val {
                            Some(val) => crate::KVLen::SIZE + val.len(),
                            None => 0,
                        }
                }
                Self::Success => 0,
            }
    }
    #[tracing::instrument]
    fn from_bytes(buf: &'r [u8]) -> Result<Self, crate::Error> {
        let mut cursor = 0;
        match *buf.get(cursor).ok_or(crate::Error::EOF)? {
            Self::GET_CODE => {
                cursor += crate::CODE_SIZE;
                match *buf.get(cursor).ok_or(crate::Error::EOF)? {
                    1 => {
                        cursor += crate::CODE_SIZE;
                        let val_len = u32::from_be_bytes(
                            buf.get(cursor..cursor + crate::KVLen::SIZE)
                                .ok_or(crate::Error::EOF)?
                                .try_into()
                                .map_err(|_| crate::Error::CorruptData)?,
                        ) as usize;
                        cursor += crate::KVLen::SIZE;
                        let val = buf.get(cursor..cursor + val_len).ok_or(crate::Error::EOF)?;
                        Ok(Self::Get(Some(val)))
                    }
                    0 => Ok(Self::Get(None)),
                    _ => Err(crate::Error::InvalidCode),
                }
            }
            Self::SUCCESS_CODE => Ok(Self::Success),
            _ => Err(crate::Error::InvalidCode),
        }
    }
    fn write_to_buf(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), self.len());

        let mut cursor = 0;

        match self {
            Self::Get(maybe_val) => {
                buf[cursor] = Self::GET_CODE;
                cursor += crate::CODE_SIZE;
                match maybe_val {
                    Some(val) => {
                        buf[cursor] = 1;
                        cursor += crate::CODE_SIZE;
                        buf[cursor..cursor + crate::KVLen::SIZE]
                            .copy_from_slice(&(val.len() as u32).to_be_bytes());
                        cursor += crate::KVLen::SIZE;
                        buf[cursor..cursor + val.len()].copy_from_slice(val);
                    }
                    None => {
                        buf[cursor] = 0;
                    }
                }
            }
            Self::Success => {
                buf[cursor] = Self::SUCCESS_CODE;
            }
        }
    }
}
