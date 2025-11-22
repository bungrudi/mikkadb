## 1. Analysis & Design
- [ ] 1.1 Review current connection handler in `src/main.rs`
- [ ] 1.2 Analyze RespHandler read/write patterns in `src/resp.rs`
- [ ] 1.3 Design command queue structure and buffering strategy
- [ ] 1.4 Document edge cases: partial reads, command errors, blocking commands

## 2. Implementation - Command Reading
- [ ] 2.1 Modify connection handler to read all available data from socket
- [ ] 2.2 Implement command queue to store parsed commands before processing
- [ ] 2.3 Add loop to parse multiple commands from BytesMut buffer
- [ ] 2.4 Handle partial command reads (wait for complete commands)
- [ ] 2.5 Preserve existing error handling for malformed commands

## 3. Implementation - Command Processing
- [ ] 3.1 Modify processing loop to handle batches of commands
- [ ] 3.2 Maintain FIFO ordering for command execution
- [ ] 3.3 Collect responses into buffer before flushing
- [ ] 3.4 Ensure transaction isolation (MULTI/EXEC) works with pipelining
- [ ] 3.5 Handle blocking commands (BLPOP, XREAD BLOCK) correctly in pipeline

## 4. Implementation - Response Buffering
- [ ] 4.1 Integrate with existing BufWriter for response coalescing
- [ ] 4.2 Implement batch flush after processing all commands in queue
- [ ] 4.3 Add fallback to immediate flush for non-pipelined clients
- [ ] 4.4 Verify buffer size limits and overflow handling

## 5. Testing - Functional
- [ ] 5.1 Test single command (non-pipelined mode)
- [ ] 5.2 Test multiple pipelined commands with correct ordering
- [ ] 5.3 Test mixed pipelined and non-pipelined commands
- [ ] 5.4 Test partial command reads and buffering
- [ ] 5.5 Test error responses in pipelined batches
- [ ] 5.6 Test transactions (MULTI/EXEC) with pipelining
- [ ] 5.7 Test blocking commands in pipeline context
- [ ] 5.8 Test large batches (100+ commands)

## 6. Testing - Performance
- [ ] 6.1 Establish baseline: Run memtier_benchmark with current implementation
- [ ] 6.2 Benchmark with pipelining: Same workload with new implementation
- [ ] 6.3 Measure syscall reduction with strace/dtrace
- [ ] 6.4 Verify 15-25% throughput improvement target
- [ ] 6.5 Test latency impact (p50, p99, p999)
- [ ] 6.6 Profile CPU usage (verify I/O % reduction)
- [ ] 6.7 Compare against Redis pipelining behavior

## 7. Documentation
- [ ] 7.1 Update PERFORMANCE-REPORT.md with pipelining results
- [ ] 7.2 Document benchmark methodology for pipelining
- [ ] 7.3 Add code comments explaining batching logic
- [ ] 7.4 Update CLAUDE.md with performance status

## 8. Validation
- [ ] 8.1 Run all existing tests (ensure no regression)
- [ ] 8.2 Verify backward compatibility with redis-cli
- [ ] 8.3 Test with official Redis clients (redis-py, node-redis)
- [ ] 8.4 Validate spec compliance with `openspec validate optimize-command-pipelining --strict`
