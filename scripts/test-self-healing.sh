#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="/tmp/gfs-self-healing-test"
echo "Cleaning up $TEST_DIR..."
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/master" "$TEST_DIR/cs1" "$TEST_DIR/cs2" "$TEST_DIR/cs3"

echo "1. Starting GFS Master on 127.0.0.1:51051..."
./target/release/gfs-master \
  --listen-addr 127.0.0.1:51051 \
  --oplog-path "$TEST_DIR/master/oplog.bin" \
  --replication-factor 3 &
MASTER_PID=$!

echo "2. Starting ONLY 2 ChunkServers (cs-1, cs-2)..."
./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:51052 \
  --storage-dir "$TEST_DIR/cs1" \
  --node-id cs-1 \
  --master-addr http://127.0.0.1:51051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS1_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:51053 \
  --storage-dir "$TEST_DIR/cs2" \
  --node-id cs-2 \
  --master-addr http://127.0.0.1:51051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS2_PID=$!

CS3_PID=""
cleanup() {
  echo "Stopping processes..."
  kill $MASTER_PID $CS1_PID $CS2_PID 2>/dev/null || true
  if [ -n "$CS3_PID" ]; then
    kill $CS3_PID 2>/dev/null || true
  fi
  wait $MASTER_PID $CS1_PID $CS2_PID 2>/dev/null || true
}
trap cleanup EXIT

sleep 2

echo "3. Uploading file while only 2 ChunkServers are running..."
echo "Under-replication self-healing test data 12345" > "$TEST_DIR/heal_sample.txt"
./target/release/gfs-cli --master http://127.0.0.1:51051 put "$TEST_DIR/heal_sample.txt" /heal_sample.txt

echo "Checking on-disk state (Chunk should exist ONLY on cs1 and cs2)..."
echo "CS1 chunks:" $(find "$TEST_DIR/cs1/chunks" -type f 2>/dev/null || echo "None")
echo "CS2 chunks:" $(find "$TEST_DIR/cs2/chunks" -type f 2>/dev/null || echo "None")
echo "CS3 chunks:" $(find "$TEST_DIR/cs3/chunks" -type f 2>/dev/null || echo "None")

echo "4. Starting 3rd ChunkServer (cs-3) now..."
./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:51054 \
  --storage-dir "$TEST_DIR/cs3" \
  --node-id cs-3 \
  --master-addr http://127.0.0.1:51051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS3_PID=$!

echo "5. Waiting 4 seconds for Master to detect under-replication and issue P2P CloneTo command..."
sleep 4

echo "6. Checking on-disk state after 3rd node joined..."
echo "--- CS1 Chunks ---"
find "$TEST_DIR/cs1/chunks" -type f
echo "--- CS2 Chunks ---"
find "$TEST_DIR/cs2/chunks" -type f
echo "--- CS3 Chunks (should now have received chunk via P2P Clone!) ---"
find "$TEST_DIR/cs3/chunks" -type f

if [ -f "$TEST_DIR/cs3/chunks/1/1/chunk_1.bin" ]; then
  echo "🎉 SUCCESS: Chunk was automatically cloned to the newly joined 3rd ChunkServer!"
else
  echo "❌ FAILURE: Chunk was not cloned to cs-3!"
  exit 1
fi
