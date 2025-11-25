# Batch Command Processing Design (Phase 2)

**Version**: 1.0
**Date**: 2025-11-23
**Status**: Ready for Implementation
**Prerequisites**: Phase 1 (Response Coalescing) must be complete

---

## Executive Summary

This design implements command batching to reduce read syscall overhead from per-command to per-batch. By draining the socket buffer in one read and parsing all available commands before processing, we reduce read syscalls by 80-90% and deliver an additional 20-40% throughput improvement over Phase 1.

**Scope**: Read-side optimization. Builds on Phase 1 (write-side coalescing).

**Key Metrics**:
- **Phase 1 Baseline**: P=10: ~580K ops/sec, P=100: ~800K ops/sec
- **Phase 2 Target**: P=10: ~700K ops/sec (+21%), P=100: ~1,100K ops/sec (+38%)
- **Total Syscall Reduction**: P=10: 20→2 syscalls (90%), P=100: 200→2 syscalls (99%)

---

## Architecture Decision

### Core Strategy

**Phase 1 Flow** (batched writes, individual reads):
```rust
loop {
    command1 = read_value().await;  // ← 1 read syscall
    response1 = process(command1);
    batch.push(response1);

    command2 = read_value().await;  // ← 1 read syscall
    response2 = process(command2);
    batch.push(response2);

    write_batch(batch).await;  // ← 1 write syscall (Phase 1)
}
```

**Phase 2 Flow** (batched reads + batched writes):
```rust
loop {
    // Read first command (blocking)
    command = read_value().await;  // ← 1 read syscall (blocks until data)
    command_batch.push(command);

    // Drain buffer non-blocking (parse all available)
    while let ParseResult::Complete(cmd, _) = try_read_value_from_buf() {
        command_batch.push(cmd);
        if batch_limits.exceeded() { break; }
    }

    // Process entire batch
    for cmd in command_batch {
        response_batch.push(process(cmd));
    }

    // Flush all responses
    write_batch(response_batch).await;  // ← 1 write syscall (Phase 1)
}
```

### Why Non-Blocking Parse

**Problem**: `read_value()` blocks waiting for next command, even if buffer already contains 9 more commands.

**Solution**: After initial blocking read:
1. Check if buffer contains more complete commands
2. Parse them without additional I/O
3. Process entire batch before next blocking read

**Benefit**: At P=10, we make 1 blocking read (gets all 10 commands) + 0 additional reads, vs 10 individual blocking reads.

---

## Technical Design

### 1. RespHandler Enhancement (src/resp.rs)

**Already Implemented** (from adaptive buffering work):

```rust
pub enum ParseResult {
    Complete(Value, usize),  // Parsed value + bytes consumed
    Incomplete,              // Need more data
    Error(anyhow::Error),    // Malformed RESP
}

impl RespHandler {
    /// Parse RESP value from buffer without I/O
    pub fn try_read_value_from_buf(&mut self) -> ParseResult {
        match parse_message(&self.buffer) {
            Ok((value, consumed)) => {
                let _ = self.buffer.split_to(consumed);  // Advance buffer
                ParseResult::Complete(value, consumed)
            }
            Err(e) if e.is_incomplete() => ParseResult::Incomplete,
            Err(e) => ParseResult::Error(e),
        }
    }
}
```

**No changes needed**: Method already exists and works correctly.

---

### 2. Connection Handler Changes (src/main.rs)

#### 2.1 Safety Limits

```rust
const MAX_BATCH_COMMANDS: usize = 1024;  // Prevent DoS
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;  // 4MB limit
const MAX_BUDGET_PER_TICK: usize = 128;  // Fairness: yield to event loop

struct BatchLimits {
    command_count: usize,
    total_bytes: usize,
}

impl BatchLimits {
    fn exceeded(&self) -> bool {
        self.command_count >= MAX_BATCH_COMMANDS || self.total_bytes >= MAX_BATCH_BYTES
    }

    fn reset(&mut self) {
        self.command_count = 0;
        self.total_bytes = 0;
    }
}
```

**Rationale**:
- **1024 command limit**: Prevents DoS via massive command batches
- **4MB byte limit**: Prevents memory exhaustion from large commands
- **128 budget per tick**: Prevents head-of-line blocking (one connection starving others)

#### 2.2 Main Loop Structure

```rust
tokio::spawn(async move {
    let mut handler = resp::RespHandler::new(stream);
    let mut command_batch = Vec::with_capacity(128);
    let mut response_batch = Vec::with_capacity(128);
    let mut batch_limits = BatchLimits::default();

    loop {
        // Phase 1: Initial blocking read (tokio::select for pub/sub, replication)
        tokio::select! {
            value = handler.read_value() => {
                let Some(cmd_value) = value? else { break };
                command_batch.push(cmd_value);
                batch_limits.command_count += 1;
                batch_limits.total_bytes += cmd_value.size_estimate();
            }
            // ... other select branches (pub/sub notifications, replication)
        }

        // Phase 2: Drain buffer non-blocking (budget-limited)
        let mut processed = 0;
        loop {
            if batch_limits.exceeded() || processed >= MAX_BUDGET_PER_TICK {
                break;  // Yield to event loop
            }

            match handler.try_read_value_from_buf() {
                ParseResult::Complete(value, consumed) => {
                    command_batch.push(value);
                    batch_limits.command_count += 1;
                    batch_limits.total_bytes += consumed;
                    processed += 1;
                }
                ParseResult::Incomplete => break,  // Buffer exhausted
                ParseResult::Error(e) => {
                    // Malformed command: send error, stop processing batch
                    response_batch.push(Value::Error(format!("ERR {}", e)));
                    break;
                }
            }
        }

        // Phase 3: Process commands with strategic flushing
        for cmd_value in command_batch.drain(..) {
            let command = parse_redis_command(&cmd_value)?;

            // Flush before blocking commands (Phase 1 logic)
            if should_flush_before(&command) && !response_batch.is_empty() {
                handler.write_batch(mem::take(&mut response_batch)).await?;
            }

            // Process command
            let response = process_command(command, &tx, client_id).await;
            response_batch.push(response);
        }

        // Phase 4: Final flush (Phase 1 write_batch)
        if !response_batch.is_empty() {
            handler.write_batch(mem::take(&mut response_batch)).await?;
        }

        // Reset limits for next batch
        batch_limits.reset();
    }
});
```

#### 2.3 Budgeted Processing

**Problem**: Large batch from one connection could starve others.

**Solution**: Process max 128 commands per event loop tick, then yield:

```rust
let mut processed = 0;
loop {
    if processed >= MAX_BUDGET_PER_TICK {
        break;  // Yield to Tokio scheduler
    }
    // ... parse and accumulate
    processed += 1;
}
```

**Effect**: Connection can't monopolize CPU for >128 commands without yielding.

---

## Performance Analysis

### Syscall Reduction

**Baseline** (current, before Phase 1):
- P=10: 10 × `read()` + 10 × `write()` = **20 syscalls**

**Phase 1** (response coalescing):
- P=10: 10 × `read()` + 1 × `writev()` = **11 syscalls** (45% reduction)

**Phase 2** (+ batch command processing):
- P=10: 1 × `read()` + 1 × `writev()` = **2 syscalls** (90% total reduction)

**At P=100**:
- Baseline: 200 syscalls
- Phase 1: 101 syscalls (49% reduction)
- Phase 2: 2 syscalls (99% total reduction)

### Expected Throughput

**P=10 Workload**:
```
Phase 1 baseline: 580K ops/sec
Remaining read overhead: 91% CPU × (10 reads / 11 total syscalls) = 82.7% CPU
Phase 2 read overhead: 91% CPU × (1 read / 2 total syscalls) = 45.5% CPU
CPU savings: 82.7% - 45.5% = 37.2% → 580K × (1 + 0.372) = 795K ops/sec (theoretical)

Conservative estimate: 700K ops/sec (+21% over Phase 1)
```

**P=100 Workload**:
```
Phase 1 baseline: 800K ops/sec
Expected Phase 2: ~1,100K ops/sec (+38%)
```

**Note**: Still won't match Redis (749K at P=10, 1,651K at P=100) due to Rust async overhead vs Redis single-threaded event loop, but closes the gap significantly.

---

## Edge Cases & Correctness

### 1. Partial Command Reads

**Scenario**: TCP packet boundary splits a command

```
Buffer: "GET key1\r\nGET key" (incomplete)
```

**Handling**:
```rust
match handler.try_read_value_from_buf() {
    ParseResult::Complete(value, _) => { /* process */ }
    ParseResult::Incomplete => break,  // Stop parsing, wait for more data
    // ...
}
```

**Next iteration**: Blocking `read_value()` completes the command.

### 2. Buffer Exhaustion

**Scenario**: All available commands parsed

**Handling**: `try_read_value_from_buf()` returns `Incomplete`, loop breaks, processing begins.

### 3. Malformed Command Mid-Batch

**Scenario**: Batch contains `["GET key1", "INVALID syntax", "SET key2 val"]`

**Handling**:
```rust
ParseResult::Error(e) => {
    response_batch.push(Value::Error(format!("ERR {}", e)));
    break;  // Stop parsing, send error, continue processing rest
}
```

**Result**: Client receives error response, connection stays open (mirrors Redis behavior).

### 4. DoS Prevention

**Attack**: Client sends 10,000 × 1KB commands

**Mitigation**:
```rust
if batch_limits.exceeded() {  // 1024 commands OR 4MB bytes
    break;  // Process current batch, next batch gets remaining commands
}
```

**Effect**: Attacker can't exhaust server memory.

### 5. Head-of-Line Blocking

**Attack**: One connection sends huge batch, starves others

**Mitigation**:
```rust
if processed >= MAX_BUDGET_PER_TICK {  // 128 commands
    break;  // Yield to event loop, other connections get CPU
}
```

**Effect**: Fair scheduling across connections.

---

## Testing Strategy

### Unit Tests

```rust
#[tokio::test]
async fn test_batch_parsing_complete_commands() {
    // Buffer: "GET key1\r\nSET key2 val\r\nDEL key3\r\n"
    // Verify: 3 commands parsed
}

#[tokio::test]
async fn test_batch_parsing_incomplete_command() {
    // Buffer: "GET key1\r\nSET key2" (incomplete)
    // Verify: 1 command parsed, 1 incomplete (waits for more data)
}

#[tokio::test]
async fn test_batch_limit_exceeded() {
    // 1025 commands
    // Verify: First 1024 processed, 1 remains for next batch
}

#[tokio::test]
async fn test_budget_limit_fairness() {
    // Batch > 128 commands
    // Verify: 128 processed, yields to event loop
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_pipelined_batch_fifo_ordering() {
    // Send 100 pipelined commands
    // Verify: All 100 responses in correct order
}

#[tokio::test]
async fn test_mixed_complete_partial_commands() {
    // Send 5 complete + 1 partial command in single write
    // Verify: 5 processed immediately, 6th waits for completion
}

#[tokio::test]
async fn test_malformed_command_mid_batch() {
    // Batch: [valid, invalid, valid]
    // Verify: Error response for invalid, other commands processed
}

#[tokio::test]
async fn test_flush_before_blocking_in_batch() {
    // Batch: [SET key val, BLPOP list 0]
    // Verify: SET response flushed before BLPOP blocks
}
```

### Performance Tests

```bash
# Phase 1 baseline (response coalescing only)
git checkout feature/response-coalescing
cargo build --release
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 1
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 10
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 100

# Phase 2 (batch command processing)
git checkout feature/batch-command-processing
cargo build --release
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 1
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 10
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 100

# Syscall measurement
strace -c -f ./target/release/mikkadb-rust &
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=10 -P 10
# Compare read() + writev() counts vs Phase 1
```

**Success Criteria**:
- ✅ P=1: No regression (~109K ops/sec)
- ✅ P=10: ≥680K ops/sec (+17% over Phase 1)
- ✅ P=100: ≥1,000K ops/sec (+25% over Phase 1)
- ✅ Total syscalls: ≥85% reduction vs baseline
- ✅ Read syscalls: ≥70% reduction vs Phase 1

---

## Implementation Phases

### Phase 2.1: Buffer Draining Logic (2-3 hours)
- [ ] Add command batch Vec to connection handler
- [ ] Implement buffer drain loop with `try_read_value_from_buf()`
- [ ] Add batch limits (command count, byte size)
- [ ] Add budget limit (fairness)

### Phase 2.2: Integration with Phase 1 (1-2 hours)
- [ ] Integrate with existing response batching from Phase 1
- [ ] Ensure flush triggers still work (blocking commands)
- [ ] Verify FIFO ordering preserved

### Phase 2.3: Testing (3-4 hours)
- [ ] Unit tests for batch parsing
- [ ] Integration tests for pipelined workloads
- [ ] Edge case testing (partial reads, errors)

### Phase 2.4: Performance Validation (2-3 hours)
- [ ] Benchmark Phase 1 baseline
- [ ] Benchmark Phase 2 implementation
- [ ] Measure syscall reduction
- [ ] Profile CPU usage

### Phase 2.5: Documentation (1 hour)
- [ ] Update `claudedocs/PERFORMANCE-REPORT.md`
- [ ] Document Phase 2 results
- [ ] Compare against Redis final numbers

**Total Estimated Time**: 9-13 hours

---

## Rollback Plan

**Changes**: Isolated to `src/main.rs` (connection handler read loop)

**Procedure**:
```bash
git revert <phase-2-commit>
cargo build --release
./target/release/mikkadb-rust
# Verify: Phase 1 performance restored
```

**Recovery Time**: <5 minutes
**Data Loss**: None

---

## Dependencies & Sequencing

**Prerequisites**: Phase 1 (Response Coalescing) MUST be complete

**Why Sequential**: Phase 2 relies on `write_batch()` from Phase 1 for response flushing.

**Enables**: Phase 3 (io_uring) if needed for further optimization

**Independence**: Phase 1 can be deployed alone with 40-60% gains; Phase 2 adds incremental 20-40%.

---

## Success Metrics

### Primary Metrics
1. **P=10 Throughput**: ≥680K ops/sec (Phase 1: 580K, target: +17%)
2. **P=100 Throughput**: ≥1,000K ops/sec (Phase 1: 800K, target: +25%)
3. **Total Syscalls**: ≥85% reduction vs baseline (P=10: 20→2 syscalls)

### Secondary Metrics
4. **Read Syscalls**: ≥70% reduction vs Phase 1 (P=10: 10→1 read)
5. **P=1 Regression**: ≤5% (non-pipelined unchanged)
6. **Fairness**: No connection starvation under mixed load

### Validation
- All existing tests pass
- New integration tests pass
- redis-cli compatibility maintained
- No memory growth under malicious load

---

## References

- **Phase 1 Results**: `benchmark_results/phase1-response-coalescing.txt` (after Phase 1 complete)
- **Current Implementation**: `src/resp.rs:280` (try_read_value_from_buf already exists)
- **Gemini Consultation**: Phase 2 of 3-phase pipelining plan
- **Baseline**: `benchmark_results/adaptive_buffering_final.txt`
