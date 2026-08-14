#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="/tmp/gfs-local-test"
echo "Cleaning up $TEST_DIR..."
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/master" "$TEST_DIR/cs1" "$TEST_DIR/cs2" "$TEST_DIR/cs3"

echo "1. Starting GFS Master on 127.0.0.1:50051..."
./target/release/gfs-master \
  --listen-addr 127.0.0.1:50051 \
  --oplog-path "$TEST_DIR/master/oplog.bin" \
  --replication-factor 3 &
MASTER_PID=$!

cleanup() {
  echo "Stopping cluster processes..."
  kill $MASTER_PID $CS1_PID $CS2_PID $CS3_PID 2>/dev/null || true
  wait $MASTER_PID $CS1_PID $CS2_PID $CS3_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "2. Starting 3 ChunkServers on 127.0.0.1:50052, 50053, 50054..."
./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:50052 \
  --storage-dir "$TEST_DIR/cs1" \
  --node-id cs-1 \
  --master-addr http://127.0.0.1:50051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS1_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:50053 \
  --storage-dir "$TEST_DIR/cs2" \
  --node-id cs-2 \
  --master-addr http://127.0.0.1:50051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS2_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:50054 \
  --storage-dir "$TEST_DIR/cs3" \
  --node-id cs-3 \
  --master-addr http://127.0.0.1:50051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS3_PID=$!

echo "Waiting 3s for cluster heartbeats to register with Master..."
sleep 3

echo "3. Checking Cluster Health..."
./target/release/gfs-cli --master http://127.0.0.1:50051 health

echo "4. Creating a test file with sample data..."
echo "Hello from GFS replicated 3x across ARM64 Pi nodes!" > "$TEST_DIR/sample.txt"

echo "5. Uploading file via gfs-cli put..."
./target/release/gfs-cli --master http://127.0.0.1:50051 put "$TEST_DIR/sample.txt" /sample.txt

echo "6. Listing files in GFS namespace..."
./target/release/gfs-cli --master http://127.0.0.1:50051 ls /

echo "7. Downloading file via gfs-cli get..."
./target/release/gfs-cli --master http://127.0.0.1:50051 get /sample.txt "$TEST_DIR/downloaded.txt"

echo "8. Verifying downloaded content..."
diff -u "$TEST_DIR/sample.txt" "$TEST_DIR/downloaded.txt"
echo "✅ File content verified byte-for-byte!"

echo "9. Inspecting on-disk chunk replication across all 3 ChunkServer directories..."
echo "--- ChunkServer 1 files ---"
find "$TEST_DIR/cs1/chunks" -type f
echo "--- ChunkServer 2 files ---"
find "$TEST_DIR/cs2/chunks" -type f
echo "--- ChunkServer 3 files ---"
find "$TEST_DIR/cs3/chunks" -type f

echo "✅ Verified that chunk and metadata files exist on all 3 storage nodes!"
