## Why

The current architecture uses Tokio's work-stealing thread pool to schedule all shard engines and connection handlers across 8+ OS threads. This introduces:
1. **Channel overhead**: Every command requires 2 channel operations (send request + receive response)
2. **Thread count mismatch**: 9 threads regardless of shard count (2 shards should need only 3 threads)
3. **Cache thrashing**: Connection handlers and engines may run on different physical cores

## What Changes

- **BREAKING**: Each shard now runs on a dedicated OS thread with its own single-threaded Tokio runtime
- **BREAKING**: Each shard thread owns its own `TcpListener` using `SO_REUSEPORT` for kernel-level load balancing
- Connection handlers run on the same thread as their shard engine (zero channel overhead for reads)
- Only write replication requires cross-thread channels (fire-and-forget broadcast)
- Thread count formula: `N shards = N + 1 threads` (N shard threads + 1 main thread for coordination)

## Impact

- Affected specs: `networking`
- Affected code: `src/main.rs`, `src/engine.rs`
- Expected improvement: Elimination of channel overhead for read path, better cache locality
- Platform dependency: `SO_REUSEPORT` (available on Linux 3.9+, macOS, FreeBSD)
