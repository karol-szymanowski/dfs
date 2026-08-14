use gfs_proto::client_master::ChunkLocationsResponse;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct CacheEntry {
    response: ChunkLocationsResponse,
    expires_at: Instant,
}

pub struct ChunkLocationCache {
    default_ttl: Duration,
    entries: RwLock<HashMap<u64, CacheEntry>>,
}

impl ChunkLocationCache {
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            default_ttl,
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, handle: u64) -> Option<ChunkLocationsResponse> {
        let entries = self.entries.read();
        if let Some(entry) = entries.get(&handle) {
            if Instant::now() < entry.expires_at {
                return Some(entry.response.clone());
            }
        }
        None
    }

    pub fn insert(&self, handle: u64, response: ChunkLocationsResponse) {
        let ttl = if response.lease_expiry_unix_millis > 0 {
            Duration::from_millis(response.lease_expiry_unix_millis as u64)
        } else {
            self.default_ttl
        };

        let mut entries = self.entries.write();
        entries.insert(
            handle,
            CacheEntry {
                response,
                expires_at: Instant::now() + ttl,
            },
        );
    }

    pub fn invalidate(&self, handle: u64) {
        let mut entries = self.entries.write();
        entries.remove(&handle);
    }
}
