use bytes::Bytes;
use crc32fast::Hasher;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum OplogError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Checksum mismatch on oplog entry seq {0}")]
    ChecksumMismatch(u64),
    #[error("Corrupted oplog entry")]
    CorruptedEntry,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub sequence: u64,
    pub payload: Bytes,
    pub crc32: u32,
}

pub struct OpLog {
    path: PathBuf,
    file: Mutex<File>,
    next_sequence: AtomicU64,
}

impl OpLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OplogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&path)?;

        let mut next_seq = 1;
        file.seek(SeekFrom::Start(0))?;
        let mut reader = std::io::BufReader::new(&file);
        let mut count = 0;

        loop {
            let mut seq_buf = [0u8; 8];
            if reader.read_exact(&mut seq_buf).is_err() {
                break;
            }
            let seq = u64::from_le_bytes(seq_buf);

            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut payload_buf = vec![0u8; len];
            if reader.read_exact(&mut payload_buf).is_err() {
                break;
            }

            let mut crc_buf = [0u8; 4];
            if reader.read_exact(&mut crc_buf).is_err() {
                break;
            }
            let expected_crc = u32::from_le_bytes(crc_buf);

            let mut hasher = Hasher::new();
            hasher.update(&seq_buf);
            hasher.update(&len_buf);
            hasher.update(&payload_buf);
            if hasher.finalize() != expected_crc {
                return Err(OplogError::ChecksumMismatch(seq));
            }

            next_seq = seq + 1;
            count += 1;
        }

        info!(
            "Opened OpLog at {:?} with {} entries (next seq {})",
            path, count, next_seq
        );

        Ok(Self {
            path,
            file: Mutex::new(file),
            next_sequence: AtomicU64::new(next_seq),
        })
    }

    pub fn append(&self, payload: Bytes) -> Result<u64, OplogError> {
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        let seq_buf = seq.to_le_bytes();
        let len_buf = (payload.len() as u32).to_le_bytes();

        let mut hasher = Hasher::new();
        hasher.update(&seq_buf);
        hasher.update(&len_buf);
        hasher.update(&payload);
        let crc32 = hasher.finalize();

        let mut f = self.file.lock();
        f.write_all(&seq_buf)?;
        f.write_all(&len_buf)?;
        f.write_all(&payload)?;
        f.write_all(&crc32.to_le_bytes())?;
        f.sync_data()?;

        Ok(seq)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
