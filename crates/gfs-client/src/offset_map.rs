pub const DEFAULT_CHUNK_SIZE: u32 = 64 * 1024 * 1024; // 64 MB

/// Computes the chunk index and byte offset within that chunk for a given file offset.
#[inline]
pub fn chunk_index_and_offset(file_offset: u64, chunk_size: u32) -> (u32, u32) {
    let chunk_size_u64 = chunk_size as u64;
    let chunk_index = (file_offset / chunk_size_u64) as u32;
    let chunk_offset = (file_offset % chunk_size_u64) as u32;
    (chunk_index, chunk_offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_map_zero() {
        assert_eq!(chunk_index_and_offset(0, DEFAULT_CHUNK_SIZE), (0, 0));
    }

    #[test]
    fn test_offset_map_within_first_chunk() {
        assert_eq!(chunk_index_and_offset(1024, DEFAULT_CHUNK_SIZE), (0, 1024));
        assert_eq!(
            chunk_index_and_offset(DEFAULT_CHUNK_SIZE as u64 - 1, DEFAULT_CHUNK_SIZE),
            (0, DEFAULT_CHUNK_SIZE - 1)
        );
    }

    #[test]
    fn test_offset_map_boundary() {
        assert_eq!(
            chunk_index_and_offset(DEFAULT_CHUNK_SIZE as u64, DEFAULT_CHUNK_SIZE),
            (1, 0)
        );
        assert_eq!(
            chunk_index_and_offset(DEFAULT_CHUNK_SIZE as u64 * 2, DEFAULT_CHUNK_SIZE),
            (2, 0)
        );
    }

    #[test]
    fn test_offset_map_multi_chunk() {
        let offset = DEFAULT_CHUNK_SIZE as u64 * 5 + 42;
        assert_eq!(chunk_index_and_offset(offset, DEFAULT_CHUNK_SIZE), (5, 42));
    }
}
