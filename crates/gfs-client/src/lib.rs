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
        let info = self.master.get_file_info(path).await?;
        if info.chunks.is_empty() {
            return Ok(Bytes::new());
        }

        let mut result = Vec::new();
        let mut cur_offset = offset;
        let read_all = length == 0;
        let mut remaining_len = if read_all { u64::MAX } else { length as u64 };

        while remaining_len > 0 {
            let (chunk_index, chunk_offset) = chunk_index_and_offset(cur_offset, self.chunk_size);
            if chunk_index as usize >= info.chunks.len() {
                break;
            }

            let handle = info.chunks[chunk_index as usize].id;
            let bytes_in_this_chunk =
                std::cmp::min(remaining_len, (self.chunk_size - chunk_offset) as u64) as u32;

            let locs = match self.cache.get(handle) {
                Some(l) => l,
                None => {
                    let fetched = self.master.get_chunk_locations(handle).await?;
                    self.cache.insert(handle, fetched.clone());
                    fetched
                }
            };

            let chunk_data = ChunkPipeline::read(
                &locs.locations,
                handle,
                chunk_offset as u64,
                bytes_in_this_chunk,
            )
            .await?;

            let bytes_read = chunk_data.len() as u64;
            if bytes_read == 0 {
                break;
            }
            result.extend_from_slice(&chunk_data);

            cur_offset += bytes_read;
            if !read_all {
                remaining_len -= bytes_read;
            }

            if bytes_read < bytes_in_this_chunk as u64 {
                break;
            }
        }

        Ok(Bytes::from(result))
    }

    pub async fn append(&self, path: &str, data: Bytes) -> Result<u64, ClientError> {
        let mut remaining = data;
        let mut first_offset = None;

        while !remaining.is_empty() {
            let info = self.master.get_file_info(path).await?;
            let mut handle = if let Some(last) = info.chunks.last() {
                last.id
            } else {
                let resp = self.master.allocate_chunk(path, 0).await?;
                let h = resp.handle.map(|h| h.id).unwrap_or(1);
                self.cache.insert(h, resp);
                h
            };

            let chunk_capacity = self.chunk_size as usize;
            let to_write_len = std::cmp::min(remaining.len(), chunk_capacity);
            let chunk_slice = remaining.slice(0..to_write_len);

            let locs = match self.cache.get(handle) {
                Some(l) => l,
                None => {
                    let fetched = self.master.get_chunk_locations(handle).await?;
                    self.cache.insert(handle, fetched.clone());
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

            let (offset, padded) =
                ChunkPipeline::record_append(primary, &secondaries, handle, chunk_slice.clone())
                    .await?;

            if padded {
                // Current chunk is full, allocate a new chunk
                let resp = self
                    .master
                    .allocate_chunk(path, info.chunks.len() as u32)
                    .await?;
                handle = resp.handle.map(|h| h.id).unwrap_or(1);
                self.cache.insert(handle, resp.clone());

                let new_primary = resp
                    .primary
                    .as_ref()
                    .ok_or(chunk_pipeline::PipelineError::NoPrimary)?;
                let new_secondaries: Vec<_> = resp
                    .locations
                    .into_iter()
                    .filter(|l| l.node != new_primary.node)
                    .collect();

                let (new_offset, _) = ChunkPipeline::record_append(
                    new_primary,
                    &new_secondaries,
                    handle,
                    chunk_slice,
                )
                .await?;
                if first_offset.is_none() {
                    first_offset = Some(new_offset);
                }
            } else if first_offset.is_none() {
                first_offset = Some(offset);
            }

            remaining = remaining.slice(to_write_len..);
        }

        Ok(first_offset.unwrap_or(0))
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
