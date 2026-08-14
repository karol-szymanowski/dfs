pub mod cache;
pub mod chunk_pipeline;
pub mod master_client;
pub mod offset_map;

use bytes::Bytes;
use cache::ChunkLocationCache;
use chunk_pipeline::ChunkPipeline;
use master_client::MasterClient;
use offset_map::{chunk_index_and_offset, DEFAULT_CHUNK_SIZE};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Master RPC error: {0}")]
    MasterRpc(#[from] tonic::Status),
    #[error("Pipeline error: {0}")]
    Pipeline(#[from] chunk_pipeline::PipelineError),
    #[error("File not found: {0}")]
    FileNotFound(String),
}

#[derive(Clone)]
pub struct GfsClient {
    master: MasterClient,
    cache: Arc<ChunkLocationCache>,
    chunk_size: u32,
}

impl GfsClient {
    pub fn new(master_addr: String) -> Self {
        Self {
            master: MasterClient::new(master_addr),
            cache: Arc::new(ChunkLocationCache::new(Duration::from_secs(60))),
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub async fn create_file(&self, path: &str) -> Result<bool, ClientError> {
        Ok(self.master.create_file(path).await?)
    }

    pub async fn read(&self, path: &str, offset: u64, length: u32) -> Result<Bytes, ClientError> {
        let (chunk_index, chunk_offset) = chunk_index_and_offset(offset, self.chunk_size);
        let info = self.master.get_file_info(path).await?;
        if chunk_index as usize >= info.chunks.len() {
            return Ok(Bytes::new());
        }

        let handle = info.chunks[chunk_index as usize].id;
        let locs = match self.cache.get(handle) {
            Some(l) => l,
            None => {
                let fetched = self.master.get_chunk_locations(handle).await?;
                self.cache.insert(handle, fetched.clone());
                fetched
            }
        };

        let data =
            ChunkPipeline::read(&locs.locations, handle, chunk_offset as u64, length).await?;
        Ok(data)
    }

    pub async fn append(&self, path: &str, data: Bytes) -> Result<u64, ClientError> {
        let info = self.master.get_file_info(path).await?;
        let last_chunk_handle = if let Some(last) = info.chunks.last() {
            last.id
        } else {
            let resp = self.master.allocate_chunk(path, 0).await?;
            // Primary chunk
            let h = 1; // Or allocated handle
            self.cache.insert(h, resp);
            h
        };

        let locs = match self.cache.get(last_chunk_handle) {
            Some(l) => l,
            None => {
                let fetched = self.master.get_chunk_locations(last_chunk_handle).await?;
                self.cache.insert(last_chunk_handle, fetched.clone());
                fetched
            }
        };

        let primary = locs
            .primary
            .as_ref()
            .ok_or(chunk_pipeline::PipelineError::NoPrimary)?;
        let secondaries: Vec<_> = locs
            .locations
            .into_iter()
            .filter(|l| l.node != primary.node)
            .collect();

        let (offset, _padded) =
            ChunkPipeline::record_append(primary, &secondaries, last_chunk_handle, data).await?;
        Ok(offset)
    }

    pub async fn list(&self, path: &str) -> Result<Vec<String>, ClientError> {
        let res = self.master.list_directory(path).await?;
        Ok(res.entries.into_iter().map(|e| e.path).collect())
    }

    pub async fn delete(&self, path: &str) -> Result<(), ClientError> {
        self.master.delete_file(path).await?;
        Ok(())
    }
}
