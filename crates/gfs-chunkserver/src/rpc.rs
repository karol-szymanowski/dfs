use crate::checksum::compute_block_crc32;
use crate::store::ChunkStore;
use bytes::Bytes;
use dashmap::DashMap;
use gfs_proto::chunk_data::chunk_data_service_client::ChunkDataServiceClient;
use gfs_proto::chunk_data::chunk_data_service_server::ChunkDataService;
use gfs_proto::chunk_data::{
    ApplyMutationRequest, ApplyMutationResponse, DataPacket, PushDataAck, ReadChunkResponse,
    ReadRequest, RecordAppendRequest, RecordAppendResponse, WriteChunkRequest, WriteChunkResponse,
};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, instrument};

pub const MAX_CHUNK_SIZE: u64 = 64 * 1024 * 1024; // 64 MB GFS chunk size

pub struct ChunkDataServiceImpl {
    pub store: Arc<ChunkStore>,
    pub data_buffer: Arc<DashMap<Vec<u8>, Bytes>>,
}

impl ChunkDataServiceImpl {
    pub fn new(store: Arc<ChunkStore>) -> Self {
        Self {
            store,
            data_buffer: Arc::new(DashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl ChunkDataService for ChunkDataServiceImpl {
    #[instrument(skip(self, request))]
    async fn push_data(
        &self,
        request: Request<Streaming<DataPacket>>,
    ) -> Result<Response<PushDataAck>, Status> {
        let mut stream = request.into_inner();
        let mut data_id = Vec::new();
        let mut accumulated = Vec::new();

        while let Some(packet_res) = stream.next().await {
            let packet = packet_res.map_err(|e| Status::internal(e.to_string()))?;
            if data_id.is_empty() {
                data_id = packet.data_id;
            }
            accumulated.extend_from_slice(&packet.payload);
        }

        let bytes_len = accumulated.len() as u32;
        self.data_buffer
            .insert(data_id.clone(), Bytes::from(accumulated));

        Ok(Response::new(PushDataAck {
            data_id,
            success: true,
            bytes_received: bytes_len,
        }))
    }

    #[instrument(skip(self, request))]
    async fn write_chunk(
        &self,
        request: Request<WriteChunkRequest>,
    ) -> Result<Response<WriteChunkResponse>, Status> {
        let req = request.into_inner();
        let handle = req
            .chunk
            .map(|h| h.id)
            .ok_or_else(|| Status::invalid_argument("Missing chunk handle"))?;

        let data = self
            .data_buffer
            .get(&req.data_id)
            .map(|d| d.clone())
            .ok_or_else(|| Status::not_found("Buffered data not found for data_id"))?;

        // 1. Primary writes locally
        let written = self
            .store
            .write_chunk_data(handle, req.offset, &data, 1)
            .map_err(|e| Status::internal(e.to_string()))?;

        // 2. Forward ApplyMutation to secondaries
        for sec in req.secondaries {
            let addr = format!("http://{}", sec.grpc_addr);
            match ChunkDataServiceClient::connect(addr.clone()).await {
                Ok(mut client) => {
                    let apply_req = ApplyMutationRequest {
                        data_id: req.data_id.clone(),
                        chunk: Some(gfs_proto::common::ChunkHandle { id: handle }),
                        offset: req.offset,
                        mutation_sequence: 1,
                        is_pad: false,
                    };
                    if let Err(e) = client.apply_mutation(apply_req).await {
                        error!("Secondary replication failed for chunk {}: {}", handle, e);
                        return Err(Status::internal(format!("Secondary write failed: {}", e)));
                    }
                }
                Err(e) => {
                    error!("Failed to connect to secondary at {}: {}", addr, e);
                    return Err(Status::internal(format!(
                        "Failed to connect to secondary at {}: {}",
                        addr, e
                    )));
                }
            }
        }

        Ok(Response::new(WriteChunkResponse {
            success: true,
            bytes_written: written,
        }))
    }

    #[instrument(skip(self, request))]
    async fn record_append(
        &self,
        request: Request<RecordAppendRequest>,
    ) -> Result<Response<RecordAppendResponse>, Status> {
        let req = request.into_inner();
        let handle = req
            .chunk
            .map(|h| h.id)
            .ok_or_else(|| Status::invalid_argument("Missing chunk handle"))?;

        let data = self
            .data_buffer
            .get(&req.data_id)
            .map(|d| d.clone())
            .ok_or_else(|| Status::not_found("Buffered data not found for data_id"))?;

        let current_size = self
            .store
            .get_meta(handle)
            .map(|m| m.size as u64)
            .unwrap_or(0);

        let will_overflow = current_size + data.len() as u64 > MAX_CHUNK_SIZE;
        if will_overflow {
            return Ok(Response::new(RecordAppendResponse {
                success: false,
                offset: current_size,
                padded: true,
            }));
        }

        let offset = current_size;
        self.store
            .write_chunk_data(handle, offset, &data, 1)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Forward to secondaries
        for sec in req.secondaries {
            let addr = format!("http://{}", sec.grpc_addr);
            match ChunkDataServiceClient::connect(addr.clone()).await {
                Ok(mut client) => {
                    let apply_req = ApplyMutationRequest {
                        data_id: req.data_id.clone(),
                        chunk: Some(gfs_proto::common::ChunkHandle { id: handle }),
                        offset,
                        mutation_sequence: 1,
                        is_pad: false,
                    };
                    if let Err(e) = client.apply_mutation(apply_req).await {
                        error!("Secondary record append failed for chunk {}: {}", handle, e);
                        return Err(Status::internal(format!("Secondary append failed: {}", e)));
                    }
                }
                Err(e) => {
                    error!("Failed to connect to secondary at {}: {}", addr, e);
                    return Err(Status::internal(format!(
                        "Failed to connect to secondary at {}: {}",
                        addr, e
                    )));
                }
            }
        }

        Ok(Response::new(RecordAppendResponse {
            success: true,
            offset,
            padded: false,
        }))
    }

    #[instrument(skip(self, request))]
    async fn apply_mutation(
        &self,
        request: Request<ApplyMutationRequest>,
    ) -> Result<Response<ApplyMutationResponse>, Status> {
        let req = request.into_inner();
        let handle = req
            .chunk
            .map(|h| h.id)
            .ok_or_else(|| Status::invalid_argument("Missing chunk handle"))?;

        let data = self
            .data_buffer
            .get(&req.data_id)
            .map(|d| d.clone())
            .ok_or_else(|| Status::not_found("Buffered data not found for data_id"))?;

        self.store
            .write_chunk_data(handle, req.offset, &data, 1)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ApplyMutationResponse { success: true }))
    }

    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<ReadChunkResponse, Status>> + Send + 'static>>;

    #[instrument(skip(self, request))]
    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();
        let handle = req
            .chunk
            .map(|h| h.id)
            .ok_or_else(|| Status::invalid_argument("Missing chunk handle"))?;

        let (data, _meta) = self
            .store
            .read_chunk_data(handle, req.offset, req.length)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let frame_size = 1024 * 1024; // 1 MB streaming frames
        let responses: Vec<ReadChunkResponse> = data
            .chunks(frame_size)
            .enumerate()
            .map(|(i, chunk_slice)| {
                let crc = compute_block_crc32(chunk_slice);
                ReadChunkResponse {
                    payload: chunk_slice.to_vec(),
                    crc32: crc,
                    offset: req.offset + (i * frame_size) as u64,
                }
            })
            .collect();

        let stream = tokio_stream::iter(responses).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }
}
