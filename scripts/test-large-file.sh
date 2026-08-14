#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="/tmp/gfs-large-test"
echo "Cleaning up $TEST_DIR..."
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/master" "$TEST_DIR/cs1" "$TEST_DIR/cs2" "$TEST_DIR/cs3"

echo "1. Starting Master & 3 ChunkServers..."
./target/release/gfs-master \
  --listen-addr 127.0.0.1:50051 \
  --oplog-path "$TEST_DIR/master/oplog.bin" \
  --replication-factor 3 &
MASTER_PID=$!

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

cleanup() {
  echo "Stopping processes..."
  kill $MASTER_PID $CS1_PID $CS2_PID $CS3_PID 2>/dev/null || true
  wait $MASTER_PID $CS1_PID $CS2_PID $CS3_PID 2>/dev/null || true
}
trap cleanup EXIT

sleep 3

echo "2. Generating 100MB test data file..."
dd if=/dev/urandom of="$TEST_DIR/100mb.dat" bs=1048576 count=100 2>/dev/null
ORIG_SHA=$(shasum -a 256 "$TEST_DIR/100mb.dat" | awk '{print $1}')
echo "Original 100MB SHA256: $ORIG_SHA"

echo "3. Uploading 100MB file via gfs-cli put (spanning multiple 64MB chunks)..."
./target/release/gfs-cli --master http://127.0.0.1:50051 put "$TEST_DIR/100mb.dat" /100mb.dat

echo "4. Checking chunks on disk across ChunkServers..."
echo "--- CS1 Chunks ---"
ls -lh "$TEST_DIR"/cs1/chunks/*/*/chunk_*.bin
echo "--- CS2 Chunks ---"
ls -lh "$TEST_DIR"/cs2/chunks/*/*/chunk_*.bin
echo "--- CS3 Chunks ---"
ls -lh "$TEST_DIR"/cs3/chunks/*/*/chunk_*.bin

echo "5. Downloading 100MB file via gfs-cli get..."
./target/release/gfs-cli --master http://127.0.0.1:50051 get /100mb.dat "$TEST_DIR/downloaded_100mb.dat"

DOWNLOAD_SHA=$(shasum -a 256 "$TEST_DIR/downloaded_100mb.dat" | awk '{print $1}')
echo "Downloaded SHA256: $DOWNLOAD_SHA"

if [ "$ORIG_SHA" = "$DOWNLOAD_SHA" ]; then
  echo "🎉 SUCCESS: 100MB multi-chunk file matched SHA256 exactly across all replicas!"
else
  echo "❌ MISMATCH: Hashes did not match!"
  exit 1
fi
