use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct InodeTable {
    next_ino: AtomicU64,
    ino_to_path: DashMap<u64, PathBuf>,
    path_to_ino: DashMap<PathBuf, u64>,
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl InodeTable {
    pub fn new() -> Self {
        let table = Self {
            next_ino: AtomicU64::new(2), // 1 is root inode
            ino_to_path: DashMap::new(),
            path_to_ino: DashMap::new(),
        };
        table.ino_to_path.insert(1, PathBuf::from("/"));
        table.path_to_ino.insert(PathBuf::from("/"), 1);
        table
    }

    pub fn get_or_insert(&self, path: &Path) -> u64 {
        if let Some(ino) = self.path_to_ino.get(path) {
            return *ino;
        }

        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        self.ino_to_path.insert(ino, path.to_path_buf());
        self.path_to_ino.insert(path.to_path_buf(), ino);
        ino
    }

    pub fn get_path(&self, ino: u64) -> Option<PathBuf> {
        self.ino_to_path.get(&ino).map(|p| p.clone())
    }

    pub fn remove_by_path(&self, path: &Path) {
        if let Some((_, ino)) = self.path_to_ino.remove(path) {
            self.ino_to_path.remove(&ino);
        }
    }
}
