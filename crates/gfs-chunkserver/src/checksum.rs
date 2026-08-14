use crc32fast::Hasher;
use thiserror::Error;

pub const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64 KB

#[derive(Debug, Error)]
pub enum ChecksumError {
    #[error("Checksum mismatch for chunk {chunk} at block index {block_index}: expected {expected:#x}, computed {computed:#x}")]
    ChecksumMismatch {
        chunk: u64,
        block_index: usize,
        expected: u32,
        computed: u32,
    },
}

pub fn compute_block_crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub fn compute_all_blocks_crc32(data: &[u8], block_size: usize) -> Vec<u32> {
    data.chunks(block_size).map(compute_block_crc32).collect()
}

pub fn verify_block_crc32(
    chunk: u64,
    block_index: usize,
    data: &[u8],
    expected: u32,
) -> Result<(), ChecksumError> {
    let computed = compute_block_crc32(data);
    if computed != expected {
        Err(ChecksumError::ChecksumMismatch {
            chunk,
            block_index,
            expected,
            computed,
        })
    } else {
        Ok(())
    }
}
