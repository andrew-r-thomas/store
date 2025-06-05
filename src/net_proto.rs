pub enum Req<'r> {
    Get(GetReq<'r>),
    Set(SetReq<'r>),
}
pub struct GetReq<'r> {
    pub key: &'r [u8],
}
pub struct SetReq<'r> {
    pub key: &'r [u8],
    pub val: &'r [u8],
}

const GET_CODE: u8 = 1;
const SET_CODE: u8 = 2;

pub fn parse_req(req: &[u8]) -> Result<Req, ()> {
    match req.first() {
        Some(code) => match *code {
            GET_CODE => {
                if req.len() < 5 {
                    return Err(());
                }
                let key_len = u32::from_be_bytes(req[1..5].try_into().unwrap()) as usize;
                if req.len() < 5 + key_len {
                    return Err(());
                }
                let key = &req[5..5 + key_len];
                Ok(Req::Get(GetReq { key }))
            }
            SET_CODE => {
                if req.len() < 9 {
                    return Err(());
                }
                let key_len = u32::from_be_bytes(req[1..5].try_into().unwrap()) as usize;
                let val_len = u32::from_be_bytes(req[5..9].try_into().unwrap()) as usize;
                if req.len() < 9 + key_len + val_len {
                    return Err(());
                }
                let key = &req[9..9 + key_len];
                let val = &req[9 + key_len..9 + key_len + val_len];
                Ok(Req::Set(SetReq { key, val }))
            }
            n => panic!("{n} is not an op code"),
        },
        None => Err(()),
    }
}
