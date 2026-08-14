#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="/tmp/gfs-gc-test"
echo "Cleaning up $TEST_DIR..."
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/master" "$TEST_DIR/cs1" "$TEST_DIR/cs2" "$TEST_DIR/cs3"

echo "1. Starting GFS Master on 127.0.0.1:53051..."
./target/release/gfs-master \
  --listen-addr 127.0.0.1:53051 \
  --oplog-path "$TEST_DIR/master/oplog.bin" \
  --replication-factor 3 &
MASTER_PID=$!

echo "2. Starting 3 ChunkServers (cs-1, cs-2, cs-3)..."
./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:53052 \
  --storage-dir "$TEST_DIR/cs1" \
  --node-id cs-1 \
  --master-addr http://127.0.0.1:53051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS1_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:53053 \
  --storage-dir "$TEST_DIR/cs2" \
  --node-id cs-2 \
  --master-addr http://127.0.0.1:53051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS2_PID=$!

./target/release/gfs-chunkserver \
  --listen-addr 127.0.0.1:53054 \
  --storage-dir "$TEST_DIR/cs3" \
  --node-id cs-3 \
  --master-addr http://127.0.0.1:53051 \
  --skip-disk-isolation \
  --heartbeat-interval-secs 1 &
CS3_PID=$!

cleanup() {
  echo "Stopping processes..."
  kill $MASTER_PID $CS1_PID $CS2_PID $CS3_PID 2>/dev/null || true
  wait $MASTER_PID $CS1_PID $CS2_PID $CS3_PID 2>/dev/null || true
}
trap cleanup EXIT

sleep 2

echo "3. Uploading file to delete..."
echo "Delete test data payload" > "$TEST_DIR/temp.txt"
./target/release/gfs-cli --master http://127.0.0.1:53051 put "$TEST_DIR/temp.txt" /temp.txt

echo "Checking on-disk chunks before deletion:"
find "$TEST_DIR"/cs*/chunks -type f

echo "4. Deleting file from GFS namespace via gfs-cli rm..."
./target/release/gfs-cli --master http://127.0.0.1:53051 rm /temp.txt

echo "5. Verifying file is gone from namespace:"
./target/release/gfs-cli --master http://127.0.0.1:53051 ls /

echo "6. Waiting 2 seconds for heartbeat Garbage Collection (GC)..."
sleep 2

echo "7. Checking on-disk chunks after GC:"
REMAINING_CHUNKS=$(find "$TEST_DIR"/cs*/chunks -type f | wc -l | tr -d ' ')
echo "Remaining chunk files across all ChunkServers: $REMAINING_CHUNKS"

if [ "$REMAINING_CHUNKS" -eq 0 ]; then
  echo "🎉 SUCCESS: All chunk files were reclaimed asynchronously via heartbeat GC!"
else
  echo "❌ FAILURE: Chunks were not deleted from disk!"
  exit 1
fi
