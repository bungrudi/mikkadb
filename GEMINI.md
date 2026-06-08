# MikkaDB (Rust)

**MikkaDB** is a high-performance, multithreaded, Redis-compatible database implementation in Rust. It serves as a reference for advanced systems programming, focusing on concurrency, zero-copy I/O, and protocol compliance.

## Project Identity

*   **Type:** Database System (Redis-compatible)
*   **Language:** Rust (2024 Edition)
*   **Runtime:** Tokio (Async I/O)
*   **Protocol:** RESP (Redis Serialization Protocol)
*   **Performance:** ~118k ops/sec (Benchmarks vs Redis ~65k ops/sec)

## Architecture

MikkaDB employs a **Thread-Per-Shard (Shared-Nothing)** architecture to maximize throughput and minimize lock contention.

1.  **Sharding:**
    *   The keyspace is partitioned across `N` shards (configurable, defaults to CPU core count).
    *   Each shard runs on a dedicated OS thread with its own single-threaded Tokio runtime.
    *   **State:** Each shard owns a completely independent `Db` instance (`HashMap<String, DataType>`). There is no global lock on the dataset.

2.  **Networking:**
    *   **`SO_REUSEPORT`:** All shard threads listen on the same port (default `6379`). The OS kernel handles load balancing of incoming connections across threads.
    *   **I/O Handling:** Each shard manages its own connection set using `RespHandler`.

3.  **Engine:**
    *   The `Engine` struct acts as the core command processor within each shard.
    *   It executes commands sequentially to ensure atomicity (mirroring Redis behavior) but handles I/O concurrently.
    *   **Replication:** Cross-shard communication (e.g., for replication) uses `tokio::sync::mpsc` channels.

## Key Components

*   **`src/main.rs`**: Entry point. Spawns shard threads, binds sockets with `SO_REUSEPORT`.
*   **`src/engine.rs`**: The brain of the shard. Handles command dispatch, replication logic, and client management.
*   **`src/db.rs`**: In-memory data structures (`String`, `List`, `Set`, `Hash`, `Stream`, `ZSet`) and operations.
*   **`src/resp.rs`**: RESP protocol parser and serializer.
*   **`src/config.rs`**: CLI argument parsing (`--port`, `--replicaof`, `--shards`).

## Performance & Optimization

*   **Current Status:** Phase 1 Complete.
*   **Throughput:** ~118,335 ops/sec (vs Redis 64,690 ops/sec).
*   **Bottleneck:** Network I/O (~91% CPU time). Memory allocation is *not* the primary bottleneck (~3%).
*   **Next Priorities:**
    1.  **I/O Batching:** Implement command pipelining and opportunistic batching (Priority P0).
    2.  **Syscall Reduction:** Explore `io_uring` (Linux) or `kqueue` batching.

## Development Workflow

### Build & Run

```bash
# Development Build
cargo build

# Release Build (Recommended for benchmarks)
cargo build --release

# Run Server
./target/release/mikkadb-rust --port 6379 --shards 8

# Cross-Compile for Linux (from macOS)
./build_linux_binaries.sh
```

### Testing

*   **Unit Tests:**
    ```bash
    cargo test
    ```
*   **Integration/Verification:**
    *   Python verification scripts in root: `verify_lists.py`, `verify_streams.py`.
    *   Helper script `spawn_redis_server.sh` is used by tests to launch the binary.

### Agent Protocols

**CRITICAL:** This project uses the `openspec` standard for managing complex changes.

*   **Planning:** BEFORE starting major refactors or architectural changes, check `openspec/AGENTS.md`. You must create a proposal and get it approved.
*   **Documentation:** Consult `claudedocs/` for deep dives into infrastructure (`INFRASTRUCTURE.md`) and performance history (`PERFORMANCE-REPORT.md`).
*   **Subagents:** Delegate complex tasks (benchmarking, refactoring) to specialized subagents to conserve context.

## Infrastructure

*   **Remote Benchmark Server:**
    *   **IP:** `34.50.117.27`
    *   **Instance:** `instance-20251125-121024` (GCP)
    *   **Services:** Mikkadb (6379), Redis (6380).
    *   **Usage:** Use this server for definitive performance benchmarking using `memtier_benchmark`. See `claudedocs/INFRASTRUCTURE.md` for access details.