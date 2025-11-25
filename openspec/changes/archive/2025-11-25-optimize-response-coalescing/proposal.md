## Why

Phase 0 profiling showed network I/O consumes 91% of CPU time. Current implementation flushes TCP socket after every single response (`write_value()` → `flush()`), generating excessive write syscalls. At P=10 (10 pipelined commands), this means 10 individual write syscalls where 1 could suffice.

**Current State** (Read-Heavy Workload, 1:10 SET:GET):
- P=1: 109K ops/sec (matches Redis)
- P=10: 377K ops/sec (Redis: 749K - **2x slower**)
- P=100: 528K ops/sec (Redis: 1,651K - **3x slower**)

**Root Cause**: Write syscall overhead dominates at higher pipeline depths. Response coalescing (batching writes before flush) can reduce write syscalls by 80-90%, delivering 40-60% throughput improvement for pipelined workloads.

This is Phase 1 of a 3-phase pipelining optimization plan. Phase 1 focuses exclusively on the write-side bottleneck.

## What Changes

- Add `write_batch()` method to `RespHandler` for batching multiple responses
- Implement vectored I/O (`writev`) to send multiple responses in single syscall
- Add smart flush policies: flush after batch complete, 16KB threshold, or before blocking commands
- Preserve existing `write_value()` for non-pipelined clients (backward compatibility)
- Integrate with connection handler to batch responses before flushing

**Key Principle**: Minimal change focused purely on response coalescing. Command parsing (Phase 2) remains unchanged.

## Impact

- **Affected specs**: `networking` (new response batching requirements)
- **Affected code**: `src/resp.rs` (add `write_batch()`), `src/main.rs` (batch response accumulation)
- **Performance**: Expected 40-60% throughput improvement at P=10/P=100 (reducing write syscall overhead)
- **Compatibility**: Zero breaking changes - single-command mode identical to current behavior
- **Architecture**: Builds on existing `BufWriter` and adaptive buffering from previous work
- **Metrics**: Target P=10: ~580K ops/sec (vs current 377K), P=100: ~800K ops/sec (vs current 528K)
