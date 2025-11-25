# Response Coalescing Design (Phase 1)

**Version**: 1.0
**Date**: 2025-11-23
**Status**: Ready for Implementation

---

## Executive Summary

This design implements response coalescing to reduce write syscall overhead from per-command to per-batch. By batching multiple responses and using vectored I/O (`writev`), we reduce write syscalls by 80-90% and deliver 40-60% throughput improvement for pipelined workloads (P=10+).

**Scope**: Write-side optimization only. Command reading/parsing (Phase 2) is explicitly out of scope.

**Key Metrics**:
- **Current**: P=10: 377K ops/sec, P=100: 528K ops/sec
- **Target**: P=10: 580K ops/sec (+54%), P=100: 800K ops/sec (+52%)
- **Syscall Reduction**: 80-90% fewer `write()` calls

---

## Architecture Decision

### Core Strategy

**Current Flow** (flush after every response):
```rust
loop {
    command = read_value().await;
    response = process(command).await;
    write_value(response).await;  // ← Flush here (1 syscall per response)
}
```

**New Flow** (batch responses, flush once):
```rust
let mut response_batch = Vec::new();

loop {
    command = read_value().await;
    response = process(command).await;
    response_batch.push(response);

    // Smart flush triggers
    if batch_complete || should_flush_before_next() || response_batch.len() >= 16KB {
        write_batch(response_batch).await;  // ← Single flush for entire batch
        response_batch.clear();
    }
}
```

### Why `writev` (Vectored I/O)

**Problem**: Even with batching, serializing N responses into single buffer requires:
1. N × `serialize_bytes()` allocations
2. N × `memcpy` into accumulation buffer
3. 1 × `write()` syscall

**Solution**: `writev` (vectored I/O) writes multiple buffers in single syscall:
1. N × `serialize_bytes()` allocations (unavoidable)
2. 0 × `memcpy` - writev sends buffers directly from memory
3. 1 × `writev()` syscall

**Benefit**: Eliminates memcpy overhead, saves ~10-15% CPU on large batches.

---

## Technical Design

### 1. RespHandler Changes (src/resp.rs)

#### 1.1 New Method: `write_batch()`

```rust
impl RespHandler {
    /// Write multiple responses with single flush using vectored I/O
    pub async fn write_batch(&mut self, responses: Vec<Value>) -> Result<()> {
        if responses.is_empty() {
            return Ok(());
        }

        // Single response: use existing write_value for efficiency
        if responses.len() == 1 {
            return self.write_value(responses.into_iter().next().unwrap()).await;
        }

        // Serialize all responses
        let serialized: Vec<Bytes> = responses.iter()
            .map(|v| v.serialize_bytes())
            .collect();

        // Vectored I/O: write all buffers in single syscall
        let iovecs: Vec<IoSlice> = serialized.iter()
            .map(|b| IoSlice::new(b))
            .collect();

        self.writer.write_vectored(&iovecs).await?;
        self.writer.flush().await?;  // Single flush

        Ok(())
    }
}
```

**Key Properties**:
- Zero-copy: `writev` sends buffers directly from serialized Vec
- Backward compatible: Single response uses existing `write_value()`
- Simple: No complex buffer management, just batching + flush

#### 1.2 Existing Method: `write_value()` (Unchanged)

```rust
pub async fn write_value(&mut self, value: Value) -> Result<()> {
    self.writer.write_all(&value.serialize_bytes()).await?;
    self.writer.flush().await?;
    Ok(())
}
```

**Preserved for**:
- Non-pipelined clients (P=1)
- Single-response batches
- Backward compatibility

---

### 2. Connection Handler Changes (src/main.rs)

#### 2.1 Response Batching Pattern

```rust
tokio::spawn(async move {
    let mut handler = resp::RespHandler::new(stream);
    let mut response_batch = Vec::with_capacity(16);  // Typical pipeline depth

    loop {
        // Read command (unchanged - Phase 1 doesn't optimize reads)
        let Some(value) = handler.read_value().await? else { break };
        let command = parse_redis_command(&value)?;

        // Check if we should flush before processing this command
        if should_flush_before(&command) && !response_batch.is_empty() {
            handler.write_batch(mem::take(&mut response_batch)).await?;
        }

        // Process command
        let response = process_command(command, &tx, client_id).await;
        response_batch.push(response);

        // Check flush triggers
        if should_flush_after_batch(&response_batch) {
            handler.write_batch(mem::take(&mut response_batch)).await?;
        }
    }

    // Flush any remaining responses on connection close
    if !response_batch.is_empty() {
        let _ = handler.write_batch(response_batch).await;
    }
});
```

#### 2.2 Flush Trigger Logic

**Flush BEFORE command** (must see previous responses first):
```rust
fn should_flush_before(command: &RedisCommand) -> bool {
    matches!(command,
        RedisCommand::BLPop { .. }         // Blocking commands
        | RedisCommand::XRead { block: Some(_), .. }
        | RedisCommand::Subscribe { .. }   // Pub/Sub mode change
        | RedisCommand::PSubscribe { .. }
        | RedisCommand::Exec               // Transaction completion
    )
}
```

**Flush AFTER batch complete**:
```rust
fn should_flush_after_batch(batch: &[Value]) -> bool {
    // Size-based trigger (prevent unbounded buffering)
    let total_bytes: usize = batch.iter().map(|v| v.size_estimate()).sum();
    total_bytes >= 16 * 1024  // 16KB threshold
}
```

**Rationale**:
- **Blocking commands**: Client must receive all pending responses before server blocks
- **16KB threshold**: Limit memory usage, prevent unbounded batching
- **Transaction boundary**: EXEC returns entire transaction result atomically

---

## Performance Analysis

### Syscall Reduction Math

**Current** (P=10 scenario):
- 10 commands → 10 × `read()` + 10 × `write()` = **20 syscalls**

**Phase 1** (response coalescing):
- 10 commands → 10 × `read()` + 1 × `writev()` = **11 syscalls** (45% reduction)

**Phase 2** (+ batch command processing):
- 10 commands → 1 × `read()` + 1 × `writev()` = **2 syscalls** (90% reduction)

### Expected Throughput

**P=10 Workload**:
```
Current: 377K ops/sec
Write overhead per batch: 91% CPU × (10 write syscalls / 11 total syscalls) = 82.7% CPU
New write overhead: 91% CPU × (1 write syscall / 11 total syscalls) = 8.3% CPU
CPU savings: 82.7% - 8.3% = 74.4% → 377K × (1 + 0.744) = 658K ops/sec (theoretical)

Conservative estimate (accounting for overhead): 580K ops/sec (+54%)
```

**P=100 Workload**:
```
Current: 528K ops/sec
Expected: ~800K ops/sec (+52%)
```

**Note**: Phase 1 doesn't address read-side overhead (still 10 × `read()` at P=10), so gains are limited to ~50-60%. Phase 2 will address the remaining read-side bottleneck.

---

## Edge Cases & Correctness

### 1. FIFO Ordering Guarantee

**Guarantee**: Commands C1, C2, C3 → Responses R1, R2, R3 (strict order)

**Mechanism**:
- Responses added to batch in processing order (Vec preserves insertion order)
- `writev` writes IoSlices in array order
- Single batch flush preserves order

### 2. Empty Batch Handling

```rust
if responses.is_empty() {
    return Ok(());  // No-op
}
```

### 3. Single Response Optimization

```rust
if responses.len() == 1 {
    return self.write_value(responses[0]).await;  // Use existing code path
}
```

**Rationale**: Non-pipelined clients (P=1) use optimized single-value path.

### 4. Connection Drop Mid-Batch

```rust
// Graceful cleanup on connection close
if !response_batch.is_empty() {
    let _ = handler.write_batch(response_batch).await;  // Ignore errors
}
```

### 5. Blocking Command Correctness

**Example**: `SET key val, BLPOP list 0`

```
1. Process SET → batch = ["OK"]
2. See BLPOP (blocking) → flush batch (client receives "OK")
3. Process BLPOP → may block waiting for data
```

**Critical**: Client MUST receive "OK" before server blocks on BLPOP.

---

## Testing Strategy

### Unit Tests (src/resp.rs)

```rust
#[tokio::test]
async fn test_write_batch_empty() {
    let mut handler = create_test_handler();
    assert!(handler.write_batch(vec![]).await.is_ok());
}

#[tokio::test]
async fn test_write_batch_single() {
    // Verify single response uses write_value path
}

#[tokio::test]
async fn test_write_batch_multiple() {
    let responses = vec![
        Value::SimpleString("OK".into()),
        Value::Integer(42),
        Value::BulkString("value".into()),
    ];
    // Verify all written in order
}

#[tokio::test]
async fn test_write_batch_large() {
    // 1000 responses, verify no corruption
}
```

### Integration Tests (Connection Handler)

```rust
#[tokio::test]
async fn test_batch_flush_on_blocking_command() {
    // Send: SET key val, BLPOP list 0
    // Verify: "OK" received before BLPOP blocks
}

#[tokio::test]
async fn test_batch_flush_on_size_threshold() {
    // Send: 100 × SET (exceeds 16KB)
    // Verify: Responses flushed in chunks
}

#[tokio::test]
async fn test_fifo_ordering() {
    // Send: 100 pipelined GET commands
    // Verify: All 100 responses in correct order
}
```

### Performance Tests

```bash
# Baseline (current implementation)
git checkout master
cargo build --release
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 1
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 10
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 100

# Phase 1 (response coalescing)
git checkout feature/response-coalescing
cargo build --release
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 1
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 10
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=30 -P 100

# Measure syscalls (verify write reduction)
strace -c -f ./target/release/mikkadb-rust &
memtier_benchmark -t 8 -c 25 --ratio=1:10 --test-time=10 -P 10
```

**Success Criteria**:
- ✅ P=1: No regression (109K ops/sec ±5%)
- ✅ P=10: ≥540K ops/sec (+43% minimum)
- ✅ P=100: ≥750K ops/sec (+42% minimum)
- ✅ Write syscalls: ≥70% reduction
- ✅ All tests pass

---

## Implementation Phases

### Phase 1.1: Foundation (1-2 hours)
- [ ] Implement `write_batch()` in `RespHandler`
- [ ] Add unit tests for batch writing
- [ ] Verify vectored I/O correctness

### Phase 1.2: Integration (2-3 hours)
- [ ] Add response batch Vec to connection handler
- [ ] Implement flush trigger logic
- [ ] Add integration tests

### Phase 1.3: Validation (2-3 hours)
- [ ] Benchmark baseline vs Phase 1
- [ ] Measure syscall reduction with strace
- [ ] Profile CPU usage

### Phase 1.4: Documentation (1 hour)
- [ ] Update `claudedocs/PERFORMANCE-REPORT.md`
- [ ] Document Phase 1 results
- [ ] Prepare Phase 2 baseline

**Total Estimated Time**: 6-9 hours

---

## Rollback Plan

**Changes**: Isolated to `src/resp.rs` (1 new method) + `src/main.rs` (batching logic)

**Procedure**:
```bash
git revert <phase-1-commit>
cargo build --release
./target/release/mikkadb-rust
# Verify: memtier_benchmark should match baseline
```

**Recovery Time**: <5 minutes
**Data Loss**: None (protocol-level change only)

---

## Dependencies & Sequencing

**Prerequisites**: None (builds on existing adaptive buffering)

**Enables**: Phase 2 (Batch Command Processing) - can now efficiently flush batched command responses

**Blocks**: None - Phase 1 is independent and delivers value standalone

---

## Success Metrics

### Primary Metrics
1. **P=10 Throughput**: ≥540K ops/sec (current: 377K, target: +43%)
2. **P=100 Throughput**: ≥750K ops/sec (current: 528K, target: +42%)
3. **Write Syscalls**: ≥70% reduction (measured with strace)

### Secondary Metrics
4. **P=1 Regression**: ≤5% (non-pipelined clients unaffected)
5. **Latency**: p99 ≤2ms (no significant increase)
6. **Memory**: Response batch ≤16KB per connection (bounded)

### Validation
- All existing tests pass
- New integration tests pass (flush triggers, FIFO ordering)
- redis-cli compatibility maintained

---

## References

- **Baseline Metrics**: `benchmark_results/adaptive_buffering_final.txt`
- **Phase 0 Profiling**: `claudedocs/PERFORMANCE-REPORT.md`
- **Gemini Consultation**: Phase 1 of 3-phase pipelining plan
- **Current Implementation**: `src/resp.rs:274` (write_value)
