use crate::chunk_table::{ChunkTable, NodeRegistry};
use dashmap::DashMap;
use gfs_proto::common::{ChunkHandle, ChunkLocation, ChunkVersion, NodeId};
use gfs_proto::master_chunkserver::{master_command, CloneToCommand, CommandType, MasterCommand};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub const REPLICATION_FACTOR: usize = 3;
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(20);

pub struct ReplicationManager {
    pub chunk_table: Arc<ChunkTable>,
    pub node_registry: Arc<NodeRegistry>,
    pub pending_commands: Arc<DashMap<String, Vec<MasterCommand>>>,
}

impl ReplicationManager {
    pub fn new(
        chunk_table: Arc<ChunkTable>,
        node_registry: Arc<NodeRegistry>,
        pending_commands: Arc<DashMap<String, Vec<MasterCommand>>>,
    ) -> Self {
        Self {
            chunk_table,
            node_registry,
            pending_commands,
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
        let live_nodes = self.node_registry.get_live_nodes(HEARTBEAT_TIMEOUT);
        if live_nodes.is_empty() {
            return;
        }

        for entry in self.chunk_table.inner.iter() {
            let handle = *entry.key();
            let meta = entry.value();
            if meta.locations.is_empty()
                || meta.locations.len() >= REPLICATION_FACTOR
                || meta.pending_delete
            {
                continue;
            }

            warn!(
                "Chunk {} is under-replicated: {} / {} replicas",
                handle,
                meta.locations.len(),
                REPLICATION_FACTOR
            );

            let src_node_id = match meta.locations.iter().next() {
                Some(id) => id.clone(),
                None => continue,
            };

            let target_candidate = live_nodes
                .iter()
                .find(|(id, _)| !meta.locations.contains(id));

            if let Some((tgt_id, tgt_state)) = target_candidate {
                let tgt_addr = tgt_state.addr.clone();
                let already_queued = self
                    .pending_commands
                    .get(&src_node_id)
                    .map(|cmds| {
                        cmds.iter().any(|c| {
                            if let Some(master_command::Payload::CloneTo(ref clone_cmd)) = c.payload
                            {
                                clone_cmd.handle.as_ref().map(|h| h.id) == Some(handle)
                            } else {
                                false
                            }
                        })
                    })
                    .unwrap_or(false);

                if !already_queued {
                    info!(
                        "Queuing self-healing CloneTo command: chunk {} from {} -> {} ({})",
                        handle, src_node_id, tgt_id, tgt_addr
                    );
                    let cmd = MasterCommand {
                        command_type: CommandType::CloneTo as i32,
                        payload: Some(master_command::Payload::CloneTo(CloneToCommand {
                            handle: Some(ChunkHandle { id: handle }),
                            version: Some(ChunkVersion {
                                value: meta.version,
                            }),
                            target: Some(ChunkLocation {
                                node: Some(NodeId {
                                    value: tgt_id.clone(),
                                }),
                                grpc_addr: tgt_addr,
                            }),
                        })),
                    };
                    self.pending_commands
                        .entry(src_node_id)
                        .or_default()
                        .push(cmd);
                }
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
