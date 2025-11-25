#!/bin/bash
set -e

RESULTS_DIR="benchmark_results"
mkdir -p "$RESULTS_DIR"

echo "=== Phase 0: Profiling Current Implementation ==="
echo ""
echo "Building release binary with debug symbols..."
CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release --bin bench-single-lock

echo ""
echo "Starting server..."
./target/release/bench-single-lock &
SERVER_PID=$!

echo "Server PID: $SERVER_PID"
echo "Waiting 3 seconds for server to start..."
sleep 3

# Verify server is running
if ! ps -p $SERVER_PID > /dev/null; then
    echo "ERROR: Server failed to start"
    exit 1
fi

echo ""
echo "Starting profiling with 'sample' command..."
sample $SERVER_PID 30 -file "$RESULTS_DIR/sample-output.txt" &
SAMPLE_PID=$!

echo "Running memtier_benchmark for 30 seconds..."
memtier_benchmark \
    -p 6379 \
    -t 8 \
    -c 25 \
    --ratio=10:1 \
    --test-time=30 \
    --hide-histogram \
    > "$RESULTS_DIR/profiling-benchmark-results.txt" 2>&1

echo ""
echo "Waiting for profiling to complete..."
wait $SAMPLE_PID 2>/dev/null || true

echo "Stopping server..."
kill -TERM $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=== Profiling Complete ==="
echo "Sample output: $RESULTS_DIR/sample-output.txt"
echo "Benchmark results: $RESULTS_DIR/profiling-benchmark-results.txt"
echo ""
echo "Analyzing allocation hotspots..."
