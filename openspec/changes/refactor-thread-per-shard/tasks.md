## 1. Dependencies and Setup

- [x] 1.1 Add `socket2` crate to `Cargo.toml` for SO_REUSEPORT support (with "all" feature)
- [x] 1.2 Verify SO_REUSEPORT works on macOS with a minimal test

## 2. Engine Refactoring

- [x] 2.1 Add `Engine::execute_command_direct()` method for direct (non-channel) command execution
- [x] 2.2 Add `Engine::execute_batch_direct()` method for direct batch execution
- [x] 2.3 Add `Engine::process_replication()` for handling peer writes
- [x] 2.4 Add `Engine::process_timeouts()` and `Engine::handle_disconnect()`

## 3. Shard Thread Implementation

- [x] 3.1 Create `shard_main()` async function that owns TcpListener and Engine
- [x] 3.2 Implement SO_REUSEPORT listener setup in `shard_main()`
- [x] 3.3 Create `handle_connection()` that uses direct engine access via `Rc<RefCell<Engine>>`
- [x] 3.4 Use `tokio::task::spawn_local()` for connection handlers within shard thread

## 4. Main Thread Refactoring

- [x] 4.1 Refactor `main()` to spawn dedicated OS threads per shard using `std::thread::Builder`
- [x] 4.2 Each shard thread creates its own `current_thread` Tokio runtime with LocalSet
- [x] 4.3 Set up replication channels between shard threads
- [x] 4.4 Remove central `TcpListener` and connection dispatch logic

## 5. Replication Updates

- [x] 5.1 Existing `broadcast_to_peers()` uses fire-and-forget with `tokio::spawn`
- [x] 5.2 Peer channels correctly passed to each shard thread
- [x] 5.3 Replication processed via `process_replication()` in event loop

## 6. Testing and Validation

- [x] 6.1 Verify basic GET/SET operations work across multiple clients
- [x] 6.2 Verify write replication propagates to all shards
- [x] 6.3 Benchmark read throughput vs previous implementation
- [x] 6.4 Benchmark with 2 shards, confirm only 3 threads active
- [x] 6.5 Thread count verified: N shards = N+1 threads

## Benchmark Results

| Config | P=1 | P=10 | P=100 |
|--------|-----|------|-------|
| 2 Shards (3 threads) | 74K | 595K | **1.92M** |
| 3 Shards (4 threads) | - | - | **2.07M** |

**Previous (8-9 threads):**
| Config | P=100 (Read) |
|--------|--------------|
| 2 Shards | ~1.73M |

**Improvements:**
- Thread count reduced from 9 to N+1 (e.g., 3 for 2 shards)
- Read throughput improved: 1.73M → 1.92M (+11%)
- Zero channel overhead for read path
