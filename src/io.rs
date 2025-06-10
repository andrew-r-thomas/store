pub fn read_page_from_block(
    block: &[u8],
    buf: &mut [u8],
    mut offset: usize,
    block_start: usize,
) -> Result<usize, (usize, usize)> {
    let mut total = 0;
    while offset < block_start + block.len() && offset >= block_start {
        let mut cursor = offset - block_start;
        let next_off = u64::from_be_bytes(block[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let chunk_len = u64::from_be_bytes(block[cursor..cursor + 8].try_into().unwrap()) as usize;
        cursor += 8;
        let chunk = &block[cursor..cursor + chunk_len];

        buf[total..total + chunk.len()].copy_from_slice(chunk);
        total += chunk.len();

        match next_off {
            u64::MAX => return Ok(total),
            n => offset = n as usize,
        }
    }

    Err((total, offset))
}
