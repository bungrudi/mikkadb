# Command Pipelining Implementation Design

**Version**: 1.0
**Date**: 2025-11-22
**Status**: Ready for Implementation
**Consensus Score**: 9/10 (Claude + Gemini collaborative review)

---

## Executive Summary

This design implements Redis-style command pipelining to reduce network I/O syscall overhead from 91% CPU time to ~70-80%, delivering 15-25% throughput improvement. The approach batches command reads and response writes while maintaining strict FIFO ordering, transaction isolation, and backward compatibility.

**Key Metrics**:
- **Baseline**: 118,335 ops/sec, 91% CPU in I/O syscalls
- **Target**: 136,000-148,000 ops/sec (15-25% improvement)
- **Syscall Reduction**: 80-90% (from 2N to ~2 per batch)
- **Latency**: No regression for non-pipelined, slight improvement for pipelined

---

## Architecture Decision

### Core Strategy

Evolve the connection handler from synchronous "read-process-write-flush" per command to asynchronous "read-all-process-all-write-all-flush" per batch.

**Current Flow** (1 command at a time):
```
loop {
    read_value() → [blocks on I/O]
    parse_command()
    send_to_engine()
    await_response()
    write_value(response) → [flush]
}
```

**New Flow** (batch-oriented):
```
loop {
    // Phase 1: Initial blocking read
    read_value() → [blocks on I/O, wakes on data]

    // Phase 2: Drain buffer (non-blocking)
    while can_parse_from_buffer && batch_size < limits {
        command = try_read_value_from_buf()
        command_batch.push(command)
    }

    // Phase 3: Process batch with strategic flushing
    for command in command_batch {
        if is_flush_trigger(command) {
            flush_responses()
        }
        response = process_command(command)
        response_batch.push(response)
    }

    // Phase 4: Final flush
    write_batch(response_batch) → [single flush]
}
```

### Why Connection Handler Owns Batching

**Decision**: Batching logic lives in `ConnectionHandler`, not `RespHandler`

**Rationale**:
1. **Separation of Concerns**: `RespHandler` is a stateless parser, connection handler manages I/O lifecycle
2. **Special Command Handling**: Flush triggers (BLPOP, MULTI/EXEC) require connection-level state
3. **Clean Architecture**: Parser should not be aware of I/O patterns or event loops

---

## Technical Design

### 1. RespHandler Changes (src/resp.rs)

#### 1.1 New Method: `try_read_value_from_buf()`

Replace current `read_value()` with pure parsing function:

```rust
pub enum ParseResult {
    Complete(Value, usize),  // (parsed_value, bytes_consumed)
    Incomplete,              // Need more data
    Error(anyhow::Error),    // Malformed RESP
}

impl RespHandler {
    /// Parse RESP value from buffer without I/O
    pub fn try_read_value_from_buf(&self, buffer: &[u8]) -> ParseResult {
        match parse_message(buffer) {
            Ok((value, consumed)) => ParseResult::Complete(value, consumed),
            Err(e) if e.is_incomplete() => ParseResult::Incomplete,
            Err(e) => ParseResult::Error(e),
        }
    }
}
```

**Key Properties**:
- No I/O operations (pure function)
- Returns bytes consumed (allows buffer advancement)
- Distinguishes incomplete vs malformed data

#### 1.2 New Method: `write_batch()`

```rust
impl RespHandler {
    /// Write multiple responses with single flush
    pub async fn write_batch(&mut self, responses: Vec<Value>) -> Result<()> {
        // Option A: Vectored I/O (optimal)
        #[cfg(feature = "writev")]
        {
            let iovecs: Vec<IoSlice> = responses.iter()
                .map(|v| IoSlice::new(&v.serialize_bytes()))
                .collect();
            self.writer.write_vectored(&iovecs).await?;
        }

        // Option B: BufWriter accumulation (90% solution)
        #[cfg(not(feature = "writev"))]
        {
            for response in responses {
                self.writer.write_all(&response.serialize_bytes()).await?;
            }
        }

        self.writer.flush().await?;  // Single flush
        Ok(())
    }
}
```

**Performance**: Single `flush()` vs N flushes = 80-90% syscall reduction

### 2. Connection Handler Changes (src/main.rs)

#### 2.1 Safety Limits

```rust
const MAX_BATCH_COMMANDS: usize = 1024;  // Configurable
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;  // 4MB
const MAX_BUDGET_PER_TICK: usize = 128;  // Fairness limit

struct BatchLimits {
    command_count: usize,
    total_bytes: usize,
}
```

**Rationale**:
- **Dual limits**: Prevent DoS via 1000 × 1MB commands
- **Budget per tick**: Prevent head-of-line blocking of other connections
- **4MB byte limit**: Conservative estimate for typical workloads

#### 2.2 Main Loop Structure

```rust
tokio::spawn(async move {
    let mut handler = resp::RespHandler::new(stream);
    let mut command_batch = Vec::with_capacity(128);
    let mut response_batch = Vec::with_capacity(128);
    let mut batch_limits = BatchLimits::default();

    loop {
        // Phase 1: Initial blocking read (tokio::select)
        tokio::select! {
            value = handler.read_value() => {
                let Some(v) = value? else { break };
                command_batch.push(v);
                batch_limits.total_bytes += v.size_estimate();
            }
            // ... other select branches (pub/sub, replication)
        }

        // Phase 2: Drain buffer (non-blocking, budgeted)
        let mut processed = 0;
        loop {
            if batch_limits.exceeded() || processed >= MAX_BUDGET_PER_TICK {
                break;  // Yield to event loop
            }

            match handler.try_read_value_from_buf(&handler.buffer) {
                ParseResult::Complete(value, consumed) => {
                    handler.buffer.advance(consumed);
                    command_batch.push(value);
                    batch_limits.command_count += 1;
                    batch_limits.total_bytes += value.size_estimate();
                    processed += 1;
                }
                ParseResult::Incomplete => break,  // Need more data
                ParseResult::Error(e) => {
                    response_batch.push(Value::Error(format!("ERR {}", e)));
                    break;  // Stop processing batch
                }
            }
        }

        // Phase 3: Process commands with strategic flushing
        for command in command_batch.drain(..) {
            // Check flush triggers
            if should_flush_before(&command) {
                if !response_batch.is_empty() {
                    handler.write_batch(response_batch.drain(..).collect()).await?;
                }
            }

            // Process command
            let response = process_command(command, &tx, client_id).await;
            response_batch.push(response);
        }

        // Phase 4: Final flush
        if !response_batch.is_empty() {
            handler.write_batch(response_batch.drain(..).collect()).await?;
        }

        // Reset limits
        batch_limits.reset();
    }
});
```

#### 2.3 Flush Triggers

Commands that require flushing responses **before** execution:

```rust
fn should_flush_before(command: &RedisCommand) -> bool {
    matches!(command,
        RedisCommand::BLPop { .. }         // Blocking commands
        | RedisCommand::XRead { block: Some(_), .. }
        | RedisCommand::Subscribe { .. }   // Pub/Sub mode change
        | RedisCommand::PSubscribe { .. }
        | RedisCommand::Multi              // Transaction boundaries
        | RedisCommand::Exec
    )
}
```

**Rationale**:
- **Blocking commands**: Client must receive pending responses before server blocks
- **Pub/Sub**: Mode change requires protocol state synchronization
- **Transactions**: `EXEC` delivers entire transaction result atomically

### 3. Backpressure Mechanism

Prevent memory exhaustion from slow-reading clients:

```rust
const OUTPUT_BUFFER_HIGH_WATER: usize = 8 * 1024 * 1024;  // 8MB
const OUTPUT_BUFFER_LOW_WATER: usize = 2 * 1024 * 1024;   // 2MB

enum ConnectionState {
    Reading,
    Draining,  // Output buffer full, stop reading
}

// Before reading commands
if handler.output_buffer_size() > OUTPUT_BUFFER_HIGH_WATER {
    state = ConnectionState::Draining;
    // Wait for write readiness, skip read operations
}

// After successful write
if handler.output_buffer_size() < OUTPUT_BUFFER_LOW_WATER {
    state = ConnectionState::Reading;
}
```

---

## Correctness Guarantees

### FIFO Ordering

**Guarantee**: Commands C1, C2, C3 → Responses R1, R2, R3 (strict order)

**Mechanism**:
1. Commands added to batch in socket read order
2. Processed sequentially from batch
3. Responses added to output batch in same order
4. Single batch write preserves order

### Error Handling

**Mid-Batch Error Strategy**:
```
Commands: [GET key1, INVALID syntax, SET key2 value2]
Responses: [Value("val1"), Error("ERR syntax"), Value("OK")]
```

**Behavior**:
- Continue processing subsequent commands in batch
- Generate error response for failed command
- Flush entire batch (successes + errors)
- Mirrors Redis behavior

**Connection Drop Strategy**:
- If error is catastrophic (state corruption), close connection
- If error is command-specific, continue batch processing
- Log all errors for debugging

### Transaction Isolation

**MULTI/EXEC Handling**:
```
Client sends: MULTI, INCR x, INCR x, EXEC
Server batches: [MULTI, INCR, INCR, EXEC]
Processing:
  - MULTI: Flush pending responses, enter transaction mode
  - INCR commands: Queue (no execution)
  - EXEC: Execute queued commands atomically, flush results
```

**Result**: Client receives entire transaction outcome in single batch

### Blocking Commands

**BLPOP Example**:
```
Client sends: SET key1 val1, BLPOP list1 0
Server batches: [SET, BLPOP]
Processing:
  - SET: Execute, add response to batch
  - BLPOP: Flush batch (client gets "OK" for SET), then block
```

**Prevention**: Flushing before BLPOP prevents deadlock (client waiting for SET response while server waits for list push)

---

## Performance Analysis

### Syscall Reduction

**Current (per command)**:
- 1 × `read()` syscall
- 1 × `write()` syscall
- **Total**: 2N syscalls for N commands

**Pipelined (per batch)**:
- 1 × `read()` syscall (or few for large batches)
- 1 × `write()` syscall
- **Total**: ~2 syscalls for N commands

**Reduction**: (2N - 2) / 2N = ~90% for large N

### Expected Throughput

**Calculation**:
```
Current: 118,335 ops/sec with 91% CPU in I/O
I/O overhead per op: 91% / 118,335 = 7.69µs
Processing per op: 9% / 118,335 = 0.76µs

With 90% syscall reduction:
New I/O overhead: 7.69µs × 0.1 = 0.77µs
Total time per op: 0.77µs + 0.76µs = 1.53µs
New throughput: 1 / 1.53µs = 653,595 ops/sec (theoretical)

Conservative estimate (accounting for batching overhead):
15% improvement: 136,085 ops/sec
25% improvement: 147,919 ops/sec
```

**Note**: Actual gains depend on client pipelining behavior

### Latency Impact

**Non-Pipelined Clients** (batch size = 1):
- No latency change (immediate flush)
- Behavior identical to current implementation

**Pipelined Clients**:
- Slight improvement from reduced syscall overhead
- No additional buffering delay (flush immediately after batch)

---

## Edge Cases & Risks

### 1. Memory Exhaustion DoS

**Attack**: Client sends 1024 × 1MB commands
**Mitigation**: Dual limits (1024 commands AND 4MB total bytes)

### 2. Head-of-Line Blocking

**Problem**: Large batch from one connection starves others
**Mitigation**: Budgeted processing (128 commands per event loop tick, then yield)

### 3. Partial Command Reads

**Scenario**: TCP buffer ends mid-command
**Handling**: `try_read_value_from_buf()` returns `Incomplete`, preserves partial data

### 4. Connection Drop Mid-Batch

**Handling**: Log error, clean up resources, no state corruption

### 5. Slow-Reading Clients

**Problem**: Output buffer grows unbounded
**Mitigation**: Backpressure (stop reading at 8MB high-water mark)

---

## Testing Strategy

### Unit Tests

1. **Parser Tests**:
   - `try_read_value_from_buf()` with empty buffer → `Incomplete`
   - Partial command → `Incomplete`
   - Complete command → `Complete(value, bytes)`
   - Malformed command → `Error`

2. **Batch Writer Tests**:
   - Single response vs batch write (verify identical output)
   - Large batch (1024 responses)
   - Empty batch handling

### Integration Tests

1. **FIFO Ordering**:
   - Send 100 pipelined commands
   - Verify 100 responses in correct order

2. **Error Handling**:
   - Batch with invalid command mid-way
   - Verify all responses received (including error)

3. **Special Commands**:
   - MULTI/EXEC within batch
   - BLPOP at end of batch
   - Mixed normal and blocking commands

4. **Edge Cases**:
   - Exactly 1024 commands (at limit)
   - 1025 commands (exceed limit, split batches)
   - Partial command reads
   - Interleaved pub/sub messages

### Performance Tests

```bash
# Baseline (current implementation)
memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30 -P 1
memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30 -P 10
memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30 -P 50
memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30 -P 100

# Measure syscalls
strace -c -f ./target/release/mikkadb-rust &
memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=10

# Compare: ops/sec, latency (p50, p99, p999), syscall counts
```

**Success Criteria**:
- ✅ Throughput: ≥15% improvement for pipelined workloads
- ✅ Syscalls: ≥70% reduction in `read()`/`write()`
- ✅ Latency: p50 ≤1.55ms, p99 ≤1.75ms
- ✅ All tests pass (no regression)

---

## Implementation Phases

### Phase 1: Foundation (2-3 hours)
- [ ] Implement `try_read_value_from_buf()` in `RespHandler`
- [ ] Add unit tests for parser
- [ ] Implement `write_batch()` with BufWriter (vectored I/O later)
- [ ] Add unit tests for batch writer

### Phase 2: Basic Pipelining (3-4 hours)
- [ ] Modify connection handler main loop
- [ ] Implement buffer draining logic
- [ ] Add safety limits (command count + byte size)
- [ ] Test with simple pipelined commands (GET/SET)

### Phase 3: Special Commands (2-3 hours)
- [ ] Implement `should_flush_before()` logic
- [ ] Handle BLPOP, SUBSCRIBE, MULTI/EXEC
- [ ] Add integration tests for each special case

### Phase 4: Robustness (2-3 hours)
- [ ] Implement budgeted processing (fairness)
- [ ] Add backpressure mechanism
- [ ] Error handling for mid-batch failures
- [ ] Connection drop handling

### Phase 5: Performance Validation (3-4 hours)
- [ ] Benchmark baseline (current implementation)
- [ ] Benchmark pipelined implementation
- [ ] Measure syscall reduction with strace/dtrace
- [ ] Profile CPU usage (verify I/O % reduction)
- [ ] Compare latency distributions

### Phase 6: Documentation (1-2 hours)
- [ ] Update PERFORMANCE-REPORT.md
- [ ] Add code comments
- [ ] Document benchmarking methodology
- [ ] Update CLAUDE.md

**Total Estimated Time**: 13-19 hours

---

## Rollback Plan

**Simplicity**: Changes isolated to 2 files (`main.rs`, `resp.rs`)

**Procedure**:
```bash
git revert <commit-hash>
cargo build --release
./target/release/mikkadb-rust
# Verify: memtier_benchmark (should match baseline)
```

**Recovery Time**: <5 minutes
**Data Loss**: None (no schema changes)

---

## Success Metrics

### Primary Metrics

1. **Throughput**: ≥15% improvement for pipelined workloads (P=10+)
2. **Syscall Reduction**: ≥70% reduction in `read()`/`write()` calls
3. **No Regression**: Non-pipelined (P=1) performance unchanged

### Secondary Metrics

4. **Latency**: p99 ≤1.75ms (no significant increase)
5. **Fairness**: No connection starvation under mixed load
6. **Memory**: No unbounded growth under malicious clients

### Validation

- All existing tests pass
- New integration tests pass (FIFO, errors, special commands)
- redis-cli compatibility maintained
- Official Redis clients (redis-py, node-redis) work correctly

---

## References

- **Phase 0 Profiling**: `claudedocs/PERFORMANCE-REPORT.md`
- **OpenSpec Proposal**: `openspec/changes/optimize-command-pipelining/proposal.md`
- **Tasks Checklist**: `openspec/changes/optimize-command-pipelining/tasks.md`
- **Gemini Review**: Consensus score 9/10 (collaborative review)
