use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NamespaceError {
    #[error("File already exists: {0}")]
    AlreadyExists(String),
    #[error("File not found: {0}")]
    NotFound(String),
    #[error("Not a directory: {0}")]
    NotADirectory(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub chunks: Vec<u64>,
    pub size: u64,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub is_directory: bool,
}

#[derive(Debug, Default)]
pub struct Namespace {
    tree: RwLock<HashMap<PathBuf, FileMetadata>>,
}

impl Namespace {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        let now = SystemTime::now();
        map.insert(
            PathBuf::from("/"),
            FileMetadata {
                chunks: Vec::new(),
                size: 0,
                mtime: now,
                ctime: now,
                is_directory: true,
            },
        );
        Self {
            tree: RwLock::new(map),
        }
    }

    pub fn create_file(&self, path: &Path) -> Result<(), NamespaceError> {
        let mut tree = self.tree.write();
        if tree.contains_key(path) {
            return Err(NamespaceError::AlreadyExists(path.display().to_string()));
        }
        let now = SystemTime::now();
        tree.insert(
            path.to_path_buf(),
            FileMetadata {
                chunks: Vec::new(),
                size: 0,
                mtime: now,
                ctime: now,
                is_directory: false,
            },
        );
        Ok(())
    }

    pub fn get_file_info(&self, path: &Path) -> Result<FileMetadata, NamespaceError> {
        let tree = self.tree.read();
        tree.get(path)
            .cloned()
            .ok_or_else(|| NamespaceError::NotFound(path.display().to_string()))
    }

    pub fn delete_file(&self, path: &Path) -> Result<FileMetadata, NamespaceError> {
        let mut tree = self.tree.write();
        tree.remove(path)
            .ok_or_else(|| NamespaceError::NotFound(path.display().to_string()))
    }

    pub fn list_directory(
        &self,
        path: &Path,
    ) -> Result<Vec<(PathBuf, FileMetadata)>, NamespaceError> {
        let tree = self.tree.read();
        let target_dir = tree
            .get(path)
            .ok_or_else(|| NamespaceError::NotFound(path.display().to_string()))?;
        if !target_dir.is_directory {
            return Err(NamespaceError::NotADirectory(path.display().to_string()));
        }

        let mut results = Vec::new();
        for (k, v) in tree.iter() {
            if k != path && k.starts_with(path) {
                // Direct children only
                if let Ok(rel) = k.strip_prefix(path) {
                    if rel.components().count() == 1 {
                        results.push((k.clone(), v.clone()));
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn append_chunk(&self, path: &Path, handle: u64) -> Result<(), NamespaceError> {
        let mut tree = self.tree.write();
        let meta = tree
            .get_mut(path)
            .ok_or_else(|| NamespaceError::NotFound(path.display().to_string()))?;
        meta.chunks.push(handle);
        meta.mtime = SystemTime::now();
        Ok(())
    }
}
