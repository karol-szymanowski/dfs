use crate::store::ChunkStore;
use gfs_proto::p2p_clone::clone_service_server::CloneService;
use gfs_proto::p2p_clone::{CloneChunkRequest, CloneChunkResponse};
use std::sync::Arc;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, instrument};

pub struct CloneServiceImpl {
    pub store: Arc<ChunkStore>,
}

impl CloneServiceImpl {
    pub fn new(store: Arc<ChunkStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl CloneService for CloneServiceImpl {
    #[instrument(skip(self, request))]
    async fn clone_push(
        &self,
        request: Request<Streaming<CloneChunkRequest>>,
    ) -> Result<Response<CloneChunkResponse>, Status> {
        let mut stream = request.into_inner();
        let mut handle = 0;
        let mut version = 1;
        let mut all_payload = Vec::new();

        while let Some(chunk_req) = stream.next().await {
            let req = chunk_req.map_err(|e| Status::internal(e.to_string()))?;
            if let Some(h) = req.chunk {
                handle = h.id;
            }
            if let Some(v) = req.version {
                version = v.value;
            }
            all_payload.extend_from_slice(&req.payload);

            if req.is_last_frame {
                break;
            }
        }

        if handle == 0 {
            return Err(Status::invalid_argument(
                "Missing chunk handle in clone stream",
            ));
        }

        match self
            .store
            .write_chunk_data(handle, 0, &all_payload, version)
        {
            Ok(_) => {
                info!("Successfully cloned chunk {}", handle);
                Ok(Response::new(CloneChunkResponse {
                    success: true,
                    message: "Cloned successfully".to_string(),
                }))
            }
            Err(e) => {
                error!("Failed to write cloned chunk {}: {}", handle, e);
                Err(Status::internal(e.to_string()))
            }
        }
    }
}
