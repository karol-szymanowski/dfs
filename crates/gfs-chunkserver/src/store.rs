use crate::checksum::{compute_all_blocks_crc32, DEFAULT_BLOCK_SIZE};
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Chunk not found: {0}")]
    ChunkNotFound(u64),
    #[error("Invalid offset {offset} for chunk size {size}")]
    InvalidOffset { offset: u64, size: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub version: u64,
    pub size: u32,
    pub block_size: u32,
    pub block_crc32: Vec<u32>,
    pub created_at: SystemTime,
    pub last_scrubbed: SystemTime,
}

pub struct ChunkStore {
    root_dir: PathBuf,
    locks: DashMapWrapper,
}

// Wrapper for per-chunk locks
struct DashMapWrapper {
    chunk_locks: dashmap::DashMap<u64, Arc<RwLock<()>>>,
}

impl DashMapWrapper {
    fn new() -> Self {
        Self {
            chunk_locks: dashmap::DashMap::new(),
        }
    }

    fn get_lock(&self, handle: u64) -> Arc<RwLock<()>> {
        self.chunk_locks
            .entry(handle)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }
}

impl ChunkStore {
    pub fn new(root_dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(root.join("chunks"))?;
        Ok(Self {
            root_dir: root,
            locks: DashMapWrapper::new(),
        })
    }

    fn chunk_dir(&self, handle: u64) -> PathBuf {
        let bucket = handle % 256;
        self.root_dir
            .join("chunks")
            .join(bucket.to_string())
            .join(handle.to_string())
    }

    fn data_path(&self, handle: u64) -> PathBuf {
        self.chunk_dir(handle).join(format!("chunk_{}.bin", handle))
    }

    fn meta_path(&self, handle: u64) -> PathBuf {
        self.chunk_dir(handle)
            .join(format!("chunk_{}.meta", handle))
    }

    pub fn write_chunk_data(
        &self,
        handle: u64,
        offset: u64,
        data: &[u8],
        version: u64,
    ) -> Result<u32, StoreError> {
        let chunk_lock = self.locks.get_lock(handle);
        let _guard = chunk_lock.write();

        let dir = self.chunk_dir(handle);
        fs::create_dir_all(&dir)?;

        let data_file = self.data_path(handle);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_file)?;

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_data()?;

        let new_size = std::cmp::max(
            file.metadata()?.len() as u32,
            (offset + data.len() as u64) as u32,
        );

        // Read all data to update CRCs
        file.seek(SeekFrom::Start(0))?;
        let mut all_data = vec![0u8; new_size as usize];
        file.read_exact(&mut all_data)?;
        let block_crcs = compute_all_blocks_crc32(&all_data, DEFAULT_BLOCK_SIZE);

        let now = SystemTime::now();
        let meta = ChunkMeta {
            version,
            size: new_size,
            block_size: DEFAULT_BLOCK_SIZE as u32,
            block_crc32: block_crcs,
            created_at: now,
            last_scrubbed: now,
        };

        let meta_bytes = bincode::serialize(&meta)?;
        let meta_file = self.meta_path(handle);
        fs::write(&meta_file, meta_bytes)?;

        Ok(data.len() as u32)
    }

    pub fn read_chunk_data(
        &self,
        handle: u64,
        offset: u64,
        length: u32,
    ) -> Result<(Bytes, ChunkMeta), StoreError> {
        let chunk_lock = self.locks.get_lock(handle);
        let _guard = chunk_lock.read();

        let data_file = self.data_path(handle);
        let meta_file = self.meta_path(handle);

        if !data_file.exists() || !meta_file.exists() {
            return Err(StoreError::ChunkNotFound(handle));
        }

        let meta_bytes = fs::read(&meta_file)?;
        let meta: ChunkMeta = bincode::deserialize(&meta_bytes)?;

        if offset > meta.size as u64 {
            return Err(StoreError::InvalidOffset {
                offset,
                size: meta.size,
            });
        }

        let read_len = std::cmp::min(length as u64, meta.size as u64 - offset) as usize;
        let mut file = File::open(&data_file)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; read_len];
        file.read_exact(&mut buf)?;

        Ok((Bytes::from(buf), meta))
    }

    pub fn get_meta(&self, handle: u64) -> Result<ChunkMeta, StoreError> {
        let chunk_lock = self.locks.get_lock(handle);
        let _guard = chunk_lock.read();
        let meta_file = self.meta_path(handle);
        if !meta_file.exists() {
            return Err(StoreError::ChunkNotFound(handle));
        }
        let bytes = fs::read(&meta_file)?;
        let meta: ChunkMeta = bincode::deserialize(&bytes)?;
        Ok(meta)
    }

    pub fn list_chunks(&self) -> Result<HashMap<u64, ChunkMeta>, StoreError> {
        let mut result = HashMap::new();
        let chunks_dir = self.root_dir.join("chunks");
        if !chunks_dir.exists() {
            return Ok(result);
        }

        for bucket in fs::read_dir(chunks_dir)? {
            let bucket = bucket?;
            if bucket.file_type()?.is_dir() {
                for chunk_entry in fs::read_dir(bucket.path())? {
                    let chunk_entry = chunk_entry?;
                    if chunk_entry.file_type()?.is_dir() {
                        if let Ok(handle) = chunk_entry.file_name().to_string_lossy().parse::<u64>()
                        {
                            if let Ok(meta) = self.get_meta(handle) {
                                result.insert(handle, meta);
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}
