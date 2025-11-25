#!/bin/bash
set -e

RESULTS_DIR="./benchmark_results"
mkdir -p "$RESULTS_DIR"

echo "=== MikkaDB Performance Benchmark Suite ==="
echo "Testing: Actor Model vs SingleLockStore vs Redis"
echo "Workload: 90/10 read/write ratio, 200 concurrent clients"
echo ""

# Function to wait for server to be ready
wait_for_server() {
    local port=$1
    local max_attempts=30
    local attempt=0

    echo "Waiting for server on port $port..."
    while [ $attempt -lt $max_attempts ]; do
        if nc -z 127.0.0.1 $port 2>/dev/null; then
            echo "Server ready on port $port"
            sleep 1  # Give it one more second to fully initialize
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    echo "ERROR: Server failed to start on port $port"
    return 1
}

# Function to safely kill processes on a port
kill_server() {
    local port=$1
    echo "Killing any process on port $port..."
    lsof -ti:$port | xargs kill -9 2>/dev/null || true
    sleep 2
}

# Benchmark 1: Actor Model (baseline)
echo ""
echo "=== 1. Actor Model Baseline (port 6380) ==="
kill_server 6380

# Start Actor model server with modified port
PORT=6380 ./target/release/mikkadb-rust > /dev/null 2>&1 &
ACTOR_PID=$!
echo "Started Actor server (PID: $ACTOR_PID)"

if wait_for_server 6380; then
    echo "Running benchmark..."
    memtier_benchmark -p 6380 -t 8 -c 25 --ratio=10:1 --test-time=30 \
        --hide-histogram \
        > "$RESULTS_DIR/actor_model.txt" 2>&1

    echo "Actor model benchmark complete"
    cat "$RESULTS_DIR/actor_model.txt" | grep -E "(Totals|GET|SET)"
else
    echo "Failed to start Actor server"
fi

kill $ACTOR_PID 2>/dev/null || true
kill_server 6380
sleep 2

# Benchmark 2: SingleLockStore
echo ""
echo "=== 2. SingleLockStore (port 6379) ==="
kill_server 6379

./target/release/bench-single-lock > /dev/null 2>&1 &
SINGLE_PID=$!
echo "Started SingleLockStore server (PID: $SINGLE_PID)"

if wait_for_server 6379; then
    echo "Running benchmark..."
    memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30 \
        --hide-histogram \
        > "$RESULTS_DIR/single_lock.txt" 2>&1

    echo "SingleLockStore benchmark complete"
    cat "$RESULTS_DIR/single_lock.txt" | grep -E "(Totals|GET|SET)"
else
    echo "Failed to start SingleLockStore server"
fi

kill $SINGLE_PID 2>/dev/null || true
kill_server 6379
sleep 2

# Benchmark 3: Redis (reference)
echo ""
echo "=== 3. Redis Reference (port 6379) ==="
kill_server 6379

redis-server --port 6379 --save "" --appendonly no > /dev/null 2>&1 &
REDIS_PID=$!
echo "Started Redis server (PID: $REDIS_PID)"

if wait_for_server 6379; then
    echo "Running benchmark..."
    memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30 \
        --hide-histogram \
        > "$RESULTS_DIR/redis.txt" 2>&1

    echo "Redis benchmark complete"
    cat "$RESULTS_DIR/redis.txt" | grep -E "(Totals|GET|SET)"
else
    echo "Failed to start Redis server"
fi

kill $REDIS_PID 2>/dev/null || true
kill_server 6379

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to $RESULTS_DIR/"
echo ""
echo "Summary files created:"
ls -lh "$RESULTS_DIR/"
