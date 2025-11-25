## Why

Profiling revealed that per-command channel overhead is the primary bottleneck limiting pipelined throughput. Current architecture sends each command individually through mpsc channel to the Engine, requiring N channel round-trips for N pipelined commands. This adds significant latency and limits throughput to ~400K ops/sec at P=10 (target: 1M) and ~500K ops/sec at P=100 (target: 2M).

**Current Performance** (4 shards, 1:10 write:read):
- P=10: 391K ops/sec (target: 1M - **2.56x gap**)
- P=100: 491K ops/sec (target: 2M - **4.07x gap**)

**Root Cause**: Each command requires:
1. mpsc channel send (connection → engine)
2. Engine wake-up and process
3. oneshot channel response (engine → connection)

With pipelining, we batch reads and writes but NOT the channel communication, wasting the batching benefit.

## What Changes

- Modify `CommandRequest` to support batch mode with `BatchCommandRequest`
- Add `send_batch()` method to send multiple commands in single channel message
- Modify Engine `run()` loop to process command batches atomically
- Return batch responses via single oneshot channel
- Preserve FIFO ordering and error handling semantics
- Maintain backward compatibility for single-command mode

## Impact

- **Affected specs**: `networking` (batch processing requirements)
- **Affected code**: `src/engine.rs` (batch processing), `src/main.rs` (batch sending)
- **Performance**: Expected 2-3x improvement (P=10: 800K+, P=100: 1.2M+)
- **Compatibility**: Zero breaking changes - transparent optimization
- **Architecture**: Reduces channel overhead from O(N) to O(1) per batch
