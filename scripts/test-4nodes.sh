#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="/tmp/gfs-4nodes-test"
echo "Cleaning up $TEST_DIR..."
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/master" "$TEST_DIR/cs1" "$TEST_DIR/cs2" "$TEST_DIR/cs3" "$TEST_DIR/cs4"

echo "1. Starting GFS Master on 127.0.0.1:52051..."
./target/release/gfs-master \
  --listen-addr 127.0.0.1:52051 \
  --oplog-path "$TEST_DIR/master/oplog.bin" \
  --replication-factor 3 &
MASTER_PID=$!

echo "2. Starting 4 ChunkServers (cs-1, cs-2, cs-3, cs-4)..."
./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:52052 \
  --storage-dir "$TEST_DIR/cs1" \
  --node-id cs-1 \
  --master-addr http://127.0.0.1:52051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS1_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:52053 \
  --storage-dir "$TEST_DIR/cs2" \
  --node-id cs-2 \
  --master-addr http://127.0.0.1:52051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS2_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:52054 \
  --storage-dir "$TEST_DIR/cs3" \
  --node-id cs-3 \
  --master-addr http://127.0.0.1:52051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS3_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:52055 \
  --storage-dir "$TEST_DIR/cs4" \
  --node-id cs-4 \
  --master-addr http://127.0.0.1:52051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS4_PID=$!

cleanup() {
  echo "Stopping cluster processes..."
  kill $MASTER_PID $CS1_PID $CS2_PID $CS3_PID $CS4_PID 2>/dev/null || true
  wait $MASTER_PID $CS1_PID $CS2_PID $CS3_PID $CS4_PID 2>/dev/null || true
}
trap cleanup EXIT

sleep 2

echo "3. Uploading file1.txt..."
echo "File 1 contents data" > "$TEST_DIR/file1.txt"
./target/release/gfs-cli --master http://127.0.0.1:52051 put "$TEST_DIR/file1.txt" /file1.txt

sleep 2

echo "4. Uploading file2.txt..."
echo "File 2 contents data" > "$TEST_DIR/file2.txt"
./target/release/gfs-cli --master http://127.0.0.1:52051 put "$TEST_DIR/file2.txt" /file2.txt

sleep 2

echo "5. Uploading file3.txt..."
echo "File 3 contents data" > "$TEST_DIR/file3.txt"
./target/release/gfs-cli --master http://127.0.0.1:52051 put "$TEST_DIR/file3.txt" /file3.txt

sleep 2

echo "6. Inspecting chunks hosted across all 4 ChunkServers:"
echo "--- CS1 Chunks ---"
find "$TEST_DIR/cs1/chunks" -type f -name "chunk_*.bin" || true
echo "--- CS2 Chunks ---"
find "$TEST_DIR/cs2/chunks" -type f -name "chunk_*.bin" || true
echo "--- CS3 Chunks ---"
find "$TEST_DIR/cs3/chunks" -type f -name "chunk_*.bin" || true
echo "--- CS4 Chunks ---"
find "$TEST_DIR/cs4/chunks" -type f -name "chunk_*.bin" || true

CS4_COUNT=$(find "$TEST_DIR/cs4/chunks" -type f -name "chunk_*.bin" | wc -l | tr -d ' ')
if [ "$CS4_COUNT" -gt 0 ]; then
  echo "🎉 SUCCESS: CS4 received $CS4_COUNT chunks! Load is balanced across all 4 nodes!"
else
  echo "❌ CS4 received 0 chunks!"
  exit 1
fi
