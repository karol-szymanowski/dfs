pub mod common {
    tonic::include_proto!("gfs.common");
}

pub mod master_chunkserver {
    tonic::include_proto!("gfs.master_chunkserver");
}

pub mod client_master {
    tonic::include_proto!("gfs.client_master");
}

pub mod chunk_data {
    tonic::include_proto!("gfs.chunk_data");
}

pub mod p2p_clone {
    tonic::include_proto!("gfs.p2p_clone");
}

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("Invalid socket address format: {0}")]
    InvalidSocketAddr(#[from] std::net::AddrParseError),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("Missing required field: {0}")]
    MissingField(&'static str),
}

// -----------------------------------------------------------------------------
// ChunkHandle conversions
// -----------------------------------------------------------------------------
impl From<u64> for common::ChunkHandle {
    fn from(id: u64) -> Self {
        Self { id }
    }
}

impl From<common::ChunkHandle> for u64 {
    fn from(handle: common::ChunkHandle) -> Self {
        handle.id
    }
}

// -----------------------------------------------------------------------------
// ChunkVersion conversions
// -----------------------------------------------------------------------------
impl From<u64> for common::ChunkVersion {
    fn from(value: u64) -> Self {
        Self { value }
    }
}

impl From<common::ChunkVersion> for u64 {
    fn from(version: common::ChunkVersion) -> Self {
        version.value
    }
}

// -----------------------------------------------------------------------------
// NodeId conversions
// -----------------------------------------------------------------------------
impl From<String> for common::NodeId {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<&str> for common::NodeId {
    fn from(value: &str) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

impl From<common::NodeId> for String {
    fn from(node_id: common::NodeId) -> Self {
        node_id.value
    }
}

// -----------------------------------------------------------------------------
// Timestamp conversions
// -----------------------------------------------------------------------------
impl From<SystemTime> for common::Timestamp {
    fn from(st: SystemTime) -> Self {
        let millis = match st.duration_since(UNIX_EPOCH) {
            Ok(dur) => dur.as_millis() as i64,
            Err(_) => 0,
        };
        Self {
            unix_millis: millis,
        }
    }
}

impl From<common::Timestamp> for SystemTime {
    fn from(ts: common::Timestamp) -> Self {
        if ts.unix_millis >= 0 {
            UNIX_EPOCH + Duration::from_millis(ts.unix_millis as u64)
        } else {
            UNIX_EPOCH
        }
    }
}

// -----------------------------------------------------------------------------
// ChunkLocation conversions
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Location {
    pub node_id: String,
    pub addr: SocketAddr,
}

impl From<Location> for common::ChunkLocation {
    fn from(loc: Location) -> Self {
        Self {
            node: Some(common::NodeId { value: loc.node_id }),
            grpc_addr: loc.addr.to_string(),
        }
    }
}

impl TryFrom<common::ChunkLocation> for Location {
    type Error = ConversionError;

    fn try_from(proto: common::ChunkLocation) -> Result<Self, Self::Error> {
        let node_id = proto.node.map(|n| n.value).unwrap_or_default();
        let addr: SocketAddr = proto.grpc_addr.parse()?;
        Ok(Self { node_id, addr })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_handle_conversion() {
        let raw = 42u64;
        let proto: common::ChunkHandle = raw.into();
        assert_eq!(proto.id, 42);
        let back: u64 = proto.into();
        assert_eq!(back, 42);
    }

    #[test]
    fn test_chunk_version_conversion() {
        let raw = 100u64;
        let proto: common::ChunkVersion = raw.into();
        assert_eq!(proto.value, 100);
        let back: u64 = proto.into();
        assert_eq!(back, 100);
    }

    #[test]
    fn test_node_id_conversion() {
        let raw = "node-1".to_string();
        let proto: common::NodeId = raw.clone().into();
        assert_eq!(proto.value, "node-1");
        let back: String = proto.into();
        assert_eq!(back, raw);
    }

    #[test]
    fn test_timestamp_conversion() {
        let now = SystemTime::now();
        let proto: common::Timestamp = now.into();
        let back: SystemTime = proto.into();
        let diff = if now >= back {
            now.duration_since(back).unwrap()
        } else {
            back.duration_since(now).unwrap()
        };
        // Should be accurate to millisecond granularity (< 2ms diff due to rounding)
        assert!(diff.as_millis() <= 2);
    }

    #[test]
    fn test_location_conversion() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let loc = Location {
            node_id: "chunksrv-1".to_string(),
            addr,
        };
        let proto: common::ChunkLocation = loc.clone().into();
        let parsed = Location::try_from(proto).unwrap();
        assert_eq!(loc, parsed);
    }

    #[test]
    fn test_location_invalid_addr() {
        let proto = common::ChunkLocation {
            node: Some(common::NodeId {
                value: "n1".to_string(),
            }),
            grpc_addr: "invalid-addr".to_string(),
        };
        assert!(Location::try_from(proto).is_err());
    }
}
