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

pub async fn send_clone(
    store: Arc<ChunkStore>,
    handle: u64,
    version: u64,
    target_addr: String,
) -> anyhow::Result<()> {
    use crate::checksum::compute_block_crc32;
    use gfs_proto::common::{ChunkHandle, ChunkVersion};
    use gfs_proto::p2p_clone::clone_service_client::CloneServiceClient;

    let meta = store
        .get_meta(handle)
        .map_err(|e| anyhow::anyhow!("Chunk {} not found locally for clone: {}", handle, e))?;

    let (data, _) = store.read_chunk_data(handle, 0, meta.size)?;
    let full_chunk_crc32 = compute_block_crc32(&data);

    let frame_size = 1024 * 1024; // 1MB frames
    let total_len = data.len();
    let num_frames = if total_len == 0 {
        1
    } else {
        total_len.div_ceil(frame_size)
    };

    let mut requests = Vec::new();
    if total_len == 0 {
        requests.push(CloneChunkRequest {
            chunk: Some(ChunkHandle { id: handle }),
            version: Some(ChunkVersion { value: version }),
            payload: Vec::new(),
            meta: Vec::new(),
            offset: 0,
            frame_crc32: 0,
            full_chunk_crc32,
            is_last_frame: true,
        });
    } else {
        for (i, chunk_slice) in data.chunks(frame_size).enumerate() {
            let is_last = i + 1 == num_frames;
            requests.push(CloneChunkRequest {
                chunk: Some(ChunkHandle { id: handle }),
                version: Some(ChunkVersion { value: version }),
                payload: chunk_slice.to_vec(),
                meta: Vec::new(),
                offset: (i * frame_size) as u64,
                frame_crc32: compute_block_crc32(chunk_slice),
                full_chunk_crc32,
                is_last_frame: is_last,
            });
        }
    }

    let connect_url = if target_addr.starts_with("http://") {
        target_addr.clone()
    } else {
        format!("http://{}", target_addr)
    };

    let mut client = CloneServiceClient::connect(connect_url)
        .await?
        .max_decoding_message_size(128 * 1024 * 1024)
        .max_encoding_message_size(128 * 1024 * 1024);

    let stream = tokio_stream::iter(requests);
    let resp = client.clone_push(stream).await?.into_inner();

    if resp.success {
        info!(
            "Successfully completed P2P clone of chunk {} to {}",
            handle, target_addr
        );
        Ok(())
    } else {
        anyhow::bail!("Clone push failed: {}", resp.message)
    }
}
