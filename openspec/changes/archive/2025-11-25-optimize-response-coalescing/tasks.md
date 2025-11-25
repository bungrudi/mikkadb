## 1. Analysis & Baseline
- [ ] 1.1 Review current `write_value()` implementation in `src/resp.rs:274`
- [ ] 1.2 Establish performance baseline with memtier (P=1, P=10, P=100)
- [ ] 1.3 Measure current syscall counts with strace/dtrace
- [ ] 1.4 Document baseline metrics in `claudedocs/phase1-baseline.md`

## 2. Implementation - write_batch() Method
- [ ] 2.1 Add `write_batch()` method to `RespHandler` in `src/resp.rs`
- [ ] 2.2 Implement empty batch guard (return early)
- [ ] 2.3 Implement single-response optimization (use existing `write_value`)
- [ ] 2.4 Implement vectored I/O with `write_vectored()` + IoSlice
- [ ] 2.5 Add proper error handling and flush

## 3. Unit Tests - write_batch()
- [ ] 3.1 Test empty batch handling
- [ ] 3.2 Test single response (verify uses write_value path)
- [ ] 3.3 Test multiple responses (2-10 responses)
- [ ] 3.4 Test large batch (100+ responses)
- [ ] 3.5 Test FIFO ordering preservation
- [ ] 3.6 Test error handling (write failure, flush failure)

## 4. Integration - Connection Handler
- [ ] 4.1 Add `response_batch: Vec<Value>` to connection handler state
- [ ] 4.2 Modify command processing loop to accumulate responses
- [ ] 4.3 Implement `should_flush_before()` for blocking commands
- [ ] 4.4 Implement `should_flush_after_batch()` for size threshold (16KB)
- [ ] 4.5 Add graceful cleanup on connection close

## 5. Integration Tests - Flush Triggers
- [ ] 5.1 Test non-pipelined mode (P=1, single response per batch)
- [ ] 5.2 Test pipelined mode (P=10, batch of 10 responses)
- [ ] 5.3 Test flush before BLPOP (verify pending responses sent first)
- [ ] 5.4 Test flush before EXEC (transaction boundary)
- [ ] 5.5 Test size-based flush (100 SETs exceeding 16KB)
- [ ] 5.6 Test FIFO ordering (100 pipelined GETs in order)

## 6. Performance Testing
- [ ] 6.1 Build Phase 1 implementation (release mode)
- [ ] 6.2 Benchmark P=1 (verify no regression: ~109K ops/sec)
- [ ] 6.3 Benchmark P=10 (target: ≥540K ops/sec, +43%)
- [ ] 6.4 Benchmark P=100 (target: ≥750K ops/sec, +42%)
- [ ] 6.5 Measure syscall reduction with strace (target: ≥70%)
- [ ] 6.6 Profile CPU usage (verify write % reduction)
- [ ] 6.7 Test latency (p50, p99, p999 - verify no regression)

## 7. Validation & Edge Cases
- [ ] 7.1 Test connection drop mid-batch (graceful cleanup)
- [ ] 7.2 Test mixed pipelined and non-pipelined commands
- [ ] 7.3 Test with redis-cli (verify compatibility)
- [ ] 7.4 Test with official Redis clients (redis-py, node-redis)
- [ ] 7.5 Test large response values (>1MB each)
- [ ] 7.6 Test rapid connect/disconnect cycles

## 8. Documentation
- [ ] 8.1 Update `claudedocs/PERFORMANCE-REPORT.md` with Phase 1 results
- [ ] 8.2 Create `benchmark_results/phase1-response-coalescing.txt`
- [ ] 8.3 Document syscall reduction metrics
- [ ] 8.4 Add code comments explaining batching logic
- [ ] 8.5 Update `CLAUDE.md` with Phase 1 completion status

## 9. OpenSpec Validation
- [ ] 9.1 Run `openspec validate optimize-response-coalescing --strict`
- [ ] 9.2 Resolve any validation issues
- [ ] 9.3 Verify spec deltas are correct

## 10. Commit & Review
- [ ] 10.1 Run all existing tests (ensure no regression)
- [ ] 10.2 Review code changes for correctness
- [ ] 10.3 Commit with descriptive message
- [ ] 10.4 Prepare Phase 2 baseline (current Phase 1 results)
