use crate::checksum::verify_block_crc32;
use crate::store::ChunkStore;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct Scrubber {
    pub store: Arc<ChunkStore>,
}

impl Scrubber {
    pub fn new(store: Arc<ChunkStore>) -> Self {
        Self { store }
    }

    pub async fn run(&self, interval: Duration, token: CancellationToken) {
        let mut ticker = tokio::time::interval(interval);
        while !token.is_cancelled() {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {
                    self.scrub_all();
                }
            }
        }
        info!("Scrubber task stopped");
    }

    pub fn scrub_all(&self) {
        let chunks = match self.store.list_chunks() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to list chunks for scrubbing: {}", e);
                return;
            }
        };

        for (handle, meta) in chunks {
            if let Ok((data, _)) = self.store.read_chunk_data(handle, 0, meta.size) {
                let block_size = meta.block_size as usize;
                for (i, &expected_crc) in meta.block_crc32.iter().enumerate() {
                    let start = i * block_size;
                    let end = std::cmp::min(start + block_size, data.len());
                    if start < data.len() {
                        let block = &data[start..end];
                        if let Err(e) = verify_block_crc32(handle, i, block, expected_crc) {
                            warn!("Scrubber found corruption in chunk {}: {}", handle, e);
                        }
                    }
                }
            }
        }
    }
}
