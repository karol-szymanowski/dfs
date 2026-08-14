use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    pub version: u64,
    pub locations: HashSet<String>,
    pub primary: Option<String>,
    pub lease_expiry: Option<Instant>,
    pub pending_delete: bool,
}

#[derive(Debug, Default)]
pub struct ChunkTable {
    pub inner: DashMap<u64, ChunkMetadata>,
    next_handle: AtomicU64,
}

impl ChunkTable {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
            next_handle: AtomicU64::new(1),
        }
    }

    pub fn allocate_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::SeqCst)
    }

    pub fn insert(&self, handle: u64, meta: ChunkMetadata) {
        self.inner.insert(handle, meta);
    }

    pub fn get(&self, handle: u64) -> Option<ChunkMetadata> {
        self.inner.get(&handle).map(|entry| entry.clone())
    }

    pub fn update_locations(&self, handle: u64, node_id: String) {
        self.inner
            .entry(handle)
            .and_modify(|meta| {
                meta.locations.insert(node_id.clone());
            })
            .or_insert_with(|| {
                let mut locations = HashSet::new();
                locations.insert(node_id);
                ChunkMetadata {
                    version: 1,
                    locations,
                    primary: None,
                    lease_expiry: None,
                    pending_delete: false,
                }
            });
    }

    pub fn grant_lease(&self, handle: u64, primary: String, duration: std::time::Duration) -> bool {
        if let Some(mut entry) = self.inner.get_mut(&handle) {
            entry.primary = Some(primary);
            entry.lease_expiry = Some(Instant::now() + duration);
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct NodeState {
    pub last_heartbeat: Instant,
    pub free_bytes: u64,
    pub used_bytes: u64,
    pub addr: String,
}

#[derive(Debug, Default)]
pub struct NodeRegistry {
    pub inner: DashMap<String, NodeState>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    pub fn record_heartbeat(
        &self,
        node_id: String,
        addr: String,
        free_bytes: u64,
        used_bytes: u64,
    ) {
        self.inner.insert(
            node_id,
            NodeState {
                last_heartbeat: Instant::now(),
                free_bytes,
                used_bytes,
                addr,
            },
        );
    }

    pub fn get_live_nodes(&self, timeout: std::time::Duration) -> Vec<(String, NodeState)> {
        let now = Instant::now();
        self.inner
            .iter()
            .filter(|entry| now.duration_since(entry.value().last_heartbeat) < timeout)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    pub fn pick_least_loaded(
        &self,
        count: usize,
        timeout: std::time::Duration,
    ) -> Vec<(String, NodeState)> {
        let mut live = self.get_live_nodes(timeout);
        // Sort primarily by used_bytes ascending (least used first)
        live.sort_by(|a, b| {
            a.1.used_bytes.cmp(&b.1.used_bytes).then_with(|| {
                let total_a = a.1.free_bytes + a.1.used_bytes;
                let ratio_a = if total_a == 0 {
                    0.0
                } else {
                    a.1.used_bytes as f64 / total_a as f64
                };
                let total_b = b.1.free_bytes + b.1.used_bytes;
                let ratio_b = if total_b == 0 {
                    0.0
                } else {
                    b.1.used_bytes as f64 / total_b as f64
                };
                ratio_a
                    .partial_cmp(&ratio_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });
        live.truncate(count);
        live
    }
}
