use crate::chunk_table::{ChunkTable, NodeRegistry};
use gfs_proto::master_chunkserver::master_chunk_service_server::MasterChunkService;
use gfs_proto::master_chunkserver::{
    HeartbeatRequest, HeartbeatResponse, LeaseRequest, LeaseResponse,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::instrument;

pub struct MasterChunkServiceImpl {
    pub chunk_table: Arc<ChunkTable>,
    pub node_registry: Arc<NodeRegistry>,
    pub lease_duration: Duration,
}

impl MasterChunkServiceImpl {
    pub fn new(
        chunk_table: Arc<ChunkTable>,
        node_registry: Arc<NodeRegistry>,
        lease_duration: Duration,
    ) -> Self {
        Self {
            chunk_table,
            node_registry,
            lease_duration,
        }
    }
}

#[tonic::async_trait]
impl MasterChunkService for MasterChunkServiceImpl {
    #[instrument(skip(self, request))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node.map(|n| n.value).unwrap_or_default();

        self.node_registry.record_heartbeat(
            node_id.clone(),
            req.grpc_addr,
            req.free_bytes,
            req.used_bytes,
        );

        for chunk_report in req.chunks {
            if let Some(handle) = chunk_report.handle {
                self.chunk_table
                    .update_locations(handle.id, node_id.clone());
            }
        }

        // Commands to send back (e.g. clone, invalidate, delete)
        let commands = Vec::new();

        Ok(Response::new(HeartbeatResponse { commands }))
    }

    #[instrument(skip(self, request))]
    async fn request_lease(
        &self,
        request: Request<LeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node.map(|n| n.value).unwrap_or_default();
        let handle = req.handle.map(|h| h.id).unwrap_or(0);

        let granted = self
            .chunk_table
            .grant_lease(handle, node_id, self.lease_duration);
        let expiry = if granted {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64
                + self.lease_duration.as_millis() as i64
        } else {
            0
        };

        Ok(Response::new(LeaseResponse {
            granted,
            handle: Some(gfs_proto::common::ChunkHandle { id: handle }),
            version: Some(gfs_proto::common::ChunkVersion { value: 1 }),
            lease_expiry_unix_millis: expiry,
        }))
    }
}
