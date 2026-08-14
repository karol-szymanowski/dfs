use crate::chunk_table::{ChunkTable, NodeRegistry};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub const REPLICATION_FACTOR: usize = 3;
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(20);

pub struct ReplicationManager {
    pub chunk_table: Arc<ChunkTable>,
    pub node_registry: Arc<NodeRegistry>,
}

impl ReplicationManager {
    pub fn new(chunk_table: Arc<ChunkTable>, node_registry: Arc<NodeRegistry>) -> Self {
        Self {
            chunk_table,
            node_registry,
        }
    }

    pub async fn run_detector_loop(&self, scan_interval: Duration, token: CancellationToken) {
        let mut ticker = tokio::time::interval(scan_interval);
        while !token.is_cancelled() {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {
                    self.detect_under_replication();
                }
            }
        }
        info!("Under-replication detector loop stopped");
    }

    pub async fn run_reaper_loop(&self, reap_interval: Duration, token: CancellationToken) {
        let mut ticker = tokio::time::interval(reap_interval);
        while !token.is_cancelled() {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {
                    self.reap_dead_nodes();
                }
            }
        }
        info!("Dead node reaper loop stopped");
    }

    pub fn detect_under_replication(&self) {
        // Scans chunk table for chunks where locations.len() < REPLICATION_FACTOR
        for entry in self.chunk_table.inner.iter() {
            let handle = *entry.key();
            let meta = entry.value();
            if meta.locations.len() < REPLICATION_FACTOR && !meta.pending_delete {
                warn!(
                    "Chunk {} is under-replicated: {} / {} replicas",
                    handle,
                    meta.locations.len(),
                    REPLICATION_FACTOR
                );
            }
        }
    }

    pub fn reap_dead_nodes(&self) {
        let live_nodes = self.node_registry.get_live_nodes(HEARTBEAT_TIMEOUT);
        let live_ids: std::collections::HashSet<_> =
            live_nodes.into_iter().map(|(id, _)| id).collect();

        for mut entry in self.chunk_table.inner.iter_mut() {
            let meta = entry.value_mut();
            meta.locations.retain(|loc| live_ids.contains(loc));
        }
    }
}
