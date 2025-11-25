## Why

Phase 1 (Response Coalescing) addresses write-side bottleneck, reducing write syscalls by 80-90% and improving P=10/P=100 throughput by ~50%. However, read-side overhead remains: each pipelined command still requires individual blocking reads.

**Phase 1 Results** (Expected after response coalescing):
- P=10: ~580K ops/sec (read: 10 syscalls, write: 1 syscall)
- P=100: ~800K ops/sec (read: 100 syscalls, write: 1 syscall)

**Remaining Gap to Redis**:
- P=10: Redis 749K vs MikkaDB ~580K (**23% slower**)
- P=100: Redis 1,651K vs MikkaDB ~800K (**52% slower**)

**Root Cause**: Read syscall overhead. At P=100, we make 100 individual `read()` calls where 1-2 could suffice.

Phase 2 implements "read-many, process-many, write-many" pattern to drain the socket buffer in one read operation, reducing total syscalls by another 70-85% and delivering an additional 20-40% throughput improvement.

This is Phase 2 of a 3-phase pipelining optimization plan. Phase 2 focuses on the read-side bottleneck.

## What Changes

- Decouple command reading from command processing (async read-ahead)
- Parse all available commands from socket buffer before processing batch
- Add `try_read_value_from_buf()` for non-blocking parse (already exists from adaptive buffering work)
- Implement read-ahead loop: drain buffer until `Incomplete` or batch limit
- Add safety limits: max 1024 commands per batch, max 4MB total bytes
- Integrate with Phase 1 response coalescing for full read→process→write batching

**Key Principle**: Read as much as available, parse into batch, process batch, flush responses together.

## Impact

- **Affected specs**: `networking` (new command batching requirements)
- **Affected code**: `src/main.rs` (connection handler read loop), `src/resp.rs` (already has `try_read_value_from_buf`)
- **Performance**: Expected additional 20-40% improvement over Phase 1 (addressing remaining read-side overhead)
- **Compatibility**: Zero breaking changes - protocol-level optimization transparent to clients
- **Architecture**: Completes the read→process→write batching pipeline started in Phase 1
- **Metrics**: Target P=10: ~700K ops/sec (93% of Redis), P=100: ~1,100K ops/sec (67% of Redis)
- **Prerequisites**: Phase 1 (Response Coalescing) must be completed first
