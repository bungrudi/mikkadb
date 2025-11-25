## 1. Prerequisites & Baseline
- [ ] 1.1 Verify Phase 1 (Response Coalescing) is complete and deployed
- [ ] 1.2 Establish Phase 1 baseline with memtier (P=1, P=10, P=100)
- [ ] 1.3 Measure Phase 1 syscall counts with strace (target: 11 syscalls at P=10)
- [ ] 1.4 Document Phase 1 metrics in `claudedocs/phase1-baseline.md`

## 2. Analysis & Design Review
- [ ] 2.1 Review existing `try_read_value_from_buf()` in `src/resp.rs:280`
- [ ] 2.2 Review current connection handler loop in `src/main.rs`
- [ ] 2.3 Identify integration points with Phase 1 `write_batch()`
- [ ] 2.4 Confirm batch limits and fairness parameters

## 3. Implementation - Batch Limits & State
- [ ] 3.1 Add `MAX_BATCH_COMMANDS`, `MAX_BATCH_BYTES`, `MAX_BUDGET_PER_TICK` constants
- [ ] 3.2 Implement `BatchLimits` struct with exceeded() and reset() methods
- [ ] 3.3 Add `command_batch: Vec<Value>` to connection handler state
- [ ] 3.4 Initialize BatchLimits in connection handler

## 4. Implementation - Buffer Draining Loop
- [ ] 4.1 Add initial blocking read (tokio::select branch)
- [ ] 4.2 Implement non-blocking drain loop with `try_read_value_from_buf()`
- [ ] 4.3 Add batch limit check (1024 commands OR 4MB bytes)
- [ ] 4.4 Add budget limit check (128 commands per tick)
- [ ] 4.5 Handle `ParseResult::Complete`, `Incomplete`, `Error` cases

## 5. Implementation - Batch Processing
- [ ] 5.1 Modify command processing to iterate over command_batch
- [ ] 5.2 Accumulate responses in response_batch (Phase 1 integration)
- [ ] 5.3 Preserve flush trigger logic (blocking commands, EXEC)
- [ ] 5.4 Add final batch flush after processing
- [ ] 5.5 Reset batch limits for next iteration

## 6. Unit Tests - Buffer Draining
- [ ] 6.1 Test single complete command in buffer
- [ ] 6.2 Test multiple complete commands (2-10)
- [ ] 6.3 Test partial command handling (Incomplete result)
- [ ] 6.4 Test malformed command mid-batch (Error result)
- [ ] 6.5 Test batch limit exceeded (1025 commands)
- [ ] 6.6 Test byte limit exceeded (>4MB)
- [ ] 6.7 Test budget limit (>128 commands, yields to event loop)

## 7. Integration Tests - Pipelined Workloads
- [ ] 7.1 Test P=1 (single command, verify no regression)
- [ ] 7.2 Test P=10 (10 pipelined commands, verify batching)
- [ ] 7.3 Test P=100 (100 pipelined commands, verify batching)
- [ ] 7.4 Test mixed pipelined and non-pipelined commands
- [ ] 7.5 Test FIFO ordering (100 commands in correct order)

## 8. Integration Tests - Edge Cases
- [ ] 8.1 Test partial command reads (TCP packet boundary)
- [ ] 8.2 Test flush before BLPOP in batched commands
- [ ] 8.3 Test flush before EXEC in batched commands
- [ ] 8.4 Test malformed command mid-batch (error + continue)
- [ ] 8.5 Test DoS prevention (10,000 commands)
- [ ] 8.6 Test fairness (one huge batch doesn't starve others)

## 9. Performance Testing
- [ ] 9.1 Build Phase 2 implementation (release mode)
- [ ] 9.2 Benchmark P=1 (verify no regression vs Phase 1: ~580K)
- [ ] 9.3 Benchmark P=10 (target: ≥680K ops/sec, +17% over Phase 1)
- [ ] 9.4 Benchmark P=100 (target: ≥1,000K ops/sec, +25% over Phase 1)
- [ ] 9.5 Measure total syscall reduction (target: ≥85% vs baseline)
- [ ] 9.6 Measure read syscall reduction (target: ≥70% vs Phase 1)
- [ ] 9.7 Profile CPU usage (verify read + write % both reduced)
- [ ] 9.8 Test latency (p50, p99, p999 - no significant regression)

## 10. Comparison with Redis
- [ ] 10.1 Benchmark Redis with same config (P=1, P=10, P=100)
- [ ] 10.2 Create comparison table (MikkaDB Phase 2 vs Redis)
- [ ] 10.3 Analyze remaining performance gap
- [ ] 10.4 Document architectural differences (Tokio vs epoll)

## 11. Validation & Compatibility
- [ ] 11.1 Test with redis-cli (pipelined and non-pipelined)
- [ ] 11.2 Test with redis-py (Python client)
- [ ] 11.3 Test with node-redis (Node.js client)
- [ ] 11.4 Test rapid connect/disconnect cycles
- [ ] 11.5 Test long-lived connections with mixed workloads

## 12. Documentation
- [ ] 12.1 Update `claudedocs/PERFORMANCE-REPORT.md` with Phase 2 results
- [ ] 12.2 Create `benchmark_results/phase2-batch-command-processing.txt`
- [ ] 12.3 Document syscall reduction metrics (read + write breakdown)
- [ ] 12.4 Create comparison chart (baseline → Phase 1 → Phase 2 → Redis)
- [ ] 12.5 Add code comments explaining batching logic
- [ ] 12.6 Update `CLAUDE.md` with Phase 2 completion status

## 13. OpenSpec Validation
- [ ] 13.1 Run `openspec validate optimize-batch-command-processing --strict`
- [ ] 13.2 Resolve any validation issues
- [ ] 13.3 Verify spec deltas are correct

## 14. Commit & Archive
- [ ] 14.1 Run all existing tests (ensure no regression)
- [ ] 14.2 Review code changes for correctness
- [ ] 14.3 Commit Phase 2 with descriptive message
- [ ] 14.4 Consider Phase 3 (io_uring) if gap to Redis still >30%
