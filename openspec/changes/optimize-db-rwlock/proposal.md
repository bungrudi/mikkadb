## Why

At P=10 pipelining, MikkaDB achieves 673K ops/sec but Redis reaches 749K ops/sec (11% gap). Profiling indicates the bottleneck is lock contention in the database layer. Currently, all operations (including read-only GET) require exclusive access to the `Db` struct, serializing all command execution.

**Current Benchmark Results** (GCP, optimize-io-and-zero-copy branch):
| Pipeline | MikkaDB | Redis | Gap |
|----------|---------|-------|-----|
| P=1 | 169K | 114K | +47.6% ✓ |
| P=5 | 556K | 472K | +17.8% ✓ |
| P=10 | 673K | 749K | **-11.0%** ✗ |

**Root Cause**: The `Engine` holds a `Db` instance and processes commands sequentially. At high pipelining, multiple connections compete for access. GET operations (read-only, ~50-80% of workload) could proceed concurrently but are serialized.

**Solution**: Replace implicit exclusive access with `RwLock<Db>`:
- Multiple GET operations can hold read locks concurrently
- SET/INCR operations take exclusive write lock
- Expected improvement: 8-15% at P=10, closing or exceeding Redis gap

## What Changes

- Wrap `Db` in `RwLock` within `Engine`
- Add `Db::get_concurrent()` method for read-only access
- Modify engine command execution to use read lock for GET, write lock for SET/INCR
- Add benchmark tests to verify concurrent read performance

**Non-Breaking**: Internal optimization only, no API changes.

## Impact

- **Affected specs**: `networking` (new concurrency requirements)
- **Affected code**: `src/engine.rs`, `src/db.rs`
- **Performance**: Expected +8-15% improvement at P=10 (target: 730K+ ops/sec)
- **Risk**: Low - RwLock is well-tested, GET is truly read-only
- **Testing**: Unit tests for concurrent access, benchmark validation
