use crate::{io, shard};

/// this is the central coordinator thread for the entire system
pub struct Central<M: shard::Mesh, IO: io::IOFace> {
    mesh: M,
    io: IO,
}
impl<M: shard::Mesh, IO: io::IOFace> Central<M, IO> {
    pub fn tick(&mut self) {}
}
