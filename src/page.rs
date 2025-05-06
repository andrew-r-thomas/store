pub struct PageChunk {
    buf: Vec<u8>,
    next: Option<Box<PageChunk>>,
}
