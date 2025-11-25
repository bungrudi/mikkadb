#!/bin/bash
set -e

# Profile the benchmark server under load

RESULTS_DIR="benchmark_results"
mkdir -p "$RESULTS_DIR"

echo "Starting profiling session..."
echo "This will run the server with flamegraph profiling for 30 seconds"

# Start the server with flamegraph profiling in the background
CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph \
    --bin bench-single-lock \
    --output "$RESULTS_DIR/flamegraph.svg" \
    --freq 997 \
    --root \
    -- &

FLAMEGRAPH_PID=$!

echo "Waiting 3 seconds for server to start..."
sleep 3

echo "Running memtier_benchmark load test..."
memtier_benchmark \
    -p 6379 \
    -t 8 \
    -c 25 \
    --ratio=10:1 \
    --test-time=30 \
    --hide-histogram \
    > "$RESULTS_DIR/memtier-profiling-run.txt" 2>&1

echo "Benchmark complete. Stopping server..."
# Send SIGTERM to the cargo flamegraph process
kill -TERM $FLAMEGRAPH_PID 2>/dev/null || true

# Wait for flamegraph to finish processing
echo "Waiting for flamegraph generation..."
sleep 5

# Try to kill any remaining processes
pkill -f bench-single-lock || true

echo ""
echo "Profiling complete!"
echo "Flamegraph saved to: $RESULTS_DIR/flamegraph.svg"
echo "Benchmark results saved to: $RESULTS_DIR/memtier-profiling-run.txt"
