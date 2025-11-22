## Why
Phase 0 profiling revealed network I/O as the primary bottleneck (91% of active CPU time). Current implementation processes one command per syscall pair (recvfrom + sendto), resulting in excessive syscall overhead. Command pipelining can reduce syscalls by batching multiple commands and responses, delivering 15-25% throughput improvement.

## What Changes
- Modify connection handler to read all available commands from the socket buffer before processing
- Batch command processing: collect multiple commands into a queue
- Coalesce responses: buffer all responses and flush once per batch
- Maintain strict request/response ordering (FIFO)
- Preserve existing buffered I/O implementation (BufWriter)
- Support both pipelined and non-pipelined clients transparently

## Impact
- Affected specs: `networking`
- Affected code: `src/main.rs` (connection handler), `src/resp.rs` (response buffering)
- Performance: Expected 15-25% throughput improvement (addressing the 91% I/O bottleneck)
- Compatibility: No breaking changes, transparent to clients
- Architecture: Builds on existing Arc<RwLock<Db>> shared-state from Phase 1
