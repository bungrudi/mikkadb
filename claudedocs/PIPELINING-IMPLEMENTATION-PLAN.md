# Redis Command Pipelining Implementation Plan

## Executive Summary

This document provides a comprehensive technical design for implementing command pipelining optimization in MikkaDB. The current implementation processes commands one-at-a-time with syscall overhead consuming 91% of CPU time. Command pipelining will batch commands and responses, targeting 15-25% throughput improvement.

**Current Performance:** 118,335 ops/sec (111,578 ops/sec with 8 threads)
**Target:** 15-25% improvement → ~136,000-153,000 ops/sec
**Primary Bottleneck:** Network I/O syscalls (91% CPU time)

---

## 1. Architecture Decision: Option A (Connection Handler Draining)

### Recommended Approach: **Option A - Drain Buffer in Connection Handler**

**Rationale:**
- Maintains clear separation of concerns (RespHandler = parsing, Connection Handler = orchestration)
- Minimal changes to RespHandler (already handles partial reads correctly)
- Easier to reason about control flow for special commands (MULTI/EXEC, blocking)
- Natural place to implement batching logic alongside existing tokio::select! branching

### How It Works

```rust
// Current (one command at a time):
loop {
    tokio::select! {
        value = handler.read_value() => { /* process */ }
        // other branches...
    }
}

// Proposed (batch draining):
loop {
    tokio::select! {
        first_value = handler.read_value() => {
            // 1. Got first command
            let mut batch = vec![first_value?];

            // 2. Try to read more without blocking (drain buffer)
            while let Ok(Some(v)) = handler.try_read_value_nonblocking() {
                batch.push(v);
            }

            // 3. Process entire batch
            let mut responses = Vec::with_capacity(batch.len());
            for value in batch {
                let response = process_command(value).await;
                responses.push(response);
            }

            // 4. Flush all responses at once
            handler.write_batch(responses).await?;
        }
        // other branches unchanged...
    }
}
```

### Why Not Option B (Expose read_all_available)?

- Violates single responsibility: RespHandler should parse, not orchestrate batching
- Forces RespHandler to understand pipelining semantics
- Makes blocking command handling more complex (RespHandler doesn't know about BLPOP)
- Harder to test in isolation

---

## 2. Detailed Technical Design

### 2.1 New Methods in RespHandler (src/resp.rs)

```rust
impl RespHandler {
    /// Try to read one value without blocking if buffer is empty
    /// Returns Ok(None) if buffer is empty and would block
    pub fn try_read_value_nonblocking(&mut self) -> Result<Option<Value>> {
        // If buffer already has a complete message, parse it
        if let Ok((v, consumed)) = parse_message(&self.buffer) {
            let _ = self.buffer.split_to(consumed);
            return Ok(Some(v));
        }

        // Buffer doesn't have complete message, return None without reading from socket
        Ok(None)
    }

    /// Write multiple values in one batch with single flush
    pub async fn write_batch(&mut self, values: Vec<Value>) -> Result<()> {
        for value in values {
            // Write to BufWriter without flushing
            self.writer.write_all(&value.serialize_bytes()).await?;
        }
        // Single flush for entire batch
        self.writer.flush().await?;
        Ok(())
    }

    /// Keep existing write_value for special cases
    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.writer.write_all(&value.serialize_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }
}
```

**Key Design Points:**
- `try_read_value_nonblocking()` only checks buffer, never performs I/O
- Returns `Ok(None)` when buffer exhausted (not an error condition)
- `write_batch()` leverages existing BufWriter, just delays flush
- Existing `write_value()` preserved for single-command responses

### 2.2 Connection Handler Modifications (src/main.rs)

```rust
tokio::spawn(async move {
    let mut handler = resp::RespHandler::new(stream);
    let mut repl_rx: Option<mpsc::Receiver<crate::resp::Value>> = None;
    let (msg_tx, mut msg_rx) = mpsc::channel(32);

    loop {
        tokio::select! {
            // PRIMARY CHANGE: Batch command processing
            value = handler.read_value() => {
                match value {
                    Ok(Some(first_cmd)) => {
                        // Collect all available commands from buffer
                        let mut commands = vec![first_cmd];

                        // Drain buffer without blocking
                        while let Ok(Some(v)) = handler.try_read_value_nonblocking() {
                            commands.push(v);

                            // Safety limit: prevent unbounded batches
                            if commands.len() >= 1000 {
                                break;
                            }
                        }

                        // Process batch
                        handle_command_batch(&mut handler, commands, client_id, &tx, &msg_tx).await;
                    }
                    Ok(None) => break,
                    Err(_e) => break,
                }
            }

            // Existing branches unchanged
            Some(cmd) = async {
                if let Some(rx) = &mut repl_rx {
                    rx.recv().await
                } else {
                    std::future::pending().await
                }
            } => {
                let _ = handler.write_value(cmd).await;
            }
            Some(msg) = msg_rx.recv() => {
                let _ = handler.write_value(msg).await;
            }
        }
    }

    // Disconnect logic unchanged...
});
```

**Key Design Points:**
- First `read_value()` is blocking (normal tokio::select behavior)
- Subsequent `try_read_value_nonblocking()` drains buffer without I/O
- Safety limit of 1000 commands prevents DoS via massive batches
- Existing tokio::select branches (replication, pub/sub) unchanged

### 2.3 Batch Processing Function

```rust
async fn handle_command_batch(
    handler: &mut resp::RespHandler,
    commands: Vec<resp::Value>,
    client_id: u64,
    tx: &mpsc::Sender<CommandRequest>,
    msg_tx: &mpsc::Sender<Value>,
) {
    let mut responses = Vec::with_capacity(commands.len());
    let mut pending_blocks = Vec::new();

    for command_value in commands {
        match RedisCommand::from_resp(command_value) {
            Ok(command) => {
                // Check for blocking commands
                if is_blocking_command(&command) {
                    // Process what we have so far
                    if !responses.is_empty() {
                        let _ = handler.write_batch(responses.drain(..).collect()).await;
                    }

                    // Handle blocking command individually (existing logic)
                    handle_blocking_command(handler, command, client_id, tx, msg_tx).await;
                    continue;
                }

                // Check for transaction commands
                if is_transaction_command(&command) {
                    // Flush batch before transaction boundary
                    if !responses.is_empty() {
                        let _ = handler.write_batch(responses.drain(..).collect()).await;
                    }

                    // Handle transaction command individually
                    handle_transaction_command(handler, command, client_id, tx).await;
                    continue;
                }

                // Normal command: queue for batch processing
                let (resp_tx, resp_rx) = oneshot::channel();
                let req = CommandRequest {
                    client_id,
                    command,
                    response_tx: resp_tx,
                    replica_tx: None,
                    pub_sub_tx: Some(msg_tx.clone()),
                };

                if let Err(_) = tx.send(req).await {
                    break;
                }

                match resp_rx.await {
                    Ok(Ok(response)) => {
                        if let resp::Value::Error(msg) = &response {
                            if msg == "NO_REPLY" {
                                continue;
                            }
                        }
                        responses.push(response);
                    }
                    Ok(Err(e)) => {
                        responses.push(resp::Value::Error(format!("ERR {}", e)));
                    }
                    Err(_) => break,
                }
            }
            Err(e) => {
                responses.push(resp::Value::Error(format!("ERR {}", e)));
            }
        }
    }

    // Flush remaining responses
    if !responses.is_empty() {
        let _ = handler.write_batch(responses).await;
    }
}

fn is_blocking_command(cmd: &RedisCommand) -> bool {
    matches!(cmd,
        RedisCommand::BLPop { .. } |
        RedisCommand::XRead { block: Some(_), .. }
    )
}

fn is_transaction_command(cmd: &RedisCommand) -> bool {
    matches!(cmd,
        RedisCommand::Multi |
        RedisCommand::Exec |
        RedisCommand::Discard
    )
}
```

**Key Design Points:**
- Accumulates responses in vector without flushing
- Detects blocking commands and flushes batch before blocking
- Detects transaction boundaries and flushes batch before MULTI/EXEC
- Preserves exact FIFO ordering (request N → response N)
- Handles parse errors inline (adds error to response batch)

---

## 3. Command Processing Strategy

### 3.1 Normal Commands (GET, SET, RPUSH, etc.)

**Behavior:** Full pipelining support
- Commands queued in batch
- Responses accumulated in vector
- Single flush after all commands processed

**Example:**
```
Client sends: GET key1, SET key2 val2, GET key3
Server reads: All 3 commands in one syscall
Server processes: Sequential execution (FIFO)
Server writes: All 3 responses in one syscall
```

### 3.2 Blocking Commands (BLPOP, XREAD BLOCK)

**Behavior:** Flush batch before blocking
- Detect blocking command in batch
- Flush all accumulated responses
- Handle blocking command with existing logic (separate response)
- Resume batching for subsequent commands

**Example:**
```
Client sends: GET key1, BLPOP list1 0, GET key2
Server:
  1. Process GET key1 → accumulate response
  2. Detect BLPOP → flush GET key1 response
  3. Handle BLPOP with blocking logic (may wait indefinitely)
  4. Resume: GET key2 would be in next batch
```

**Rationale:** Blocking commands can wait indefinitely. Must send accumulated responses before blocking.

### 3.3 Transactions (MULTI/EXEC/DISCARD)

**Behavior:** Flush batch at transaction boundaries
- MULTI: Flush batch, enter transaction mode, respond OK
- Queued commands: Normal batching within transaction
- EXEC: Flush batch, execute transaction, respond with array
- DISCARD: Flush batch, discard transaction, respond OK

**Example:**
```
Client sends: SET a 1, MULTI, SET b 2, SET c 3, EXEC, GET a
Server:
  1. Process SET a 1 → accumulate
  2. Detect MULTI → flush SET response, enter tx mode, respond OK
  3. SET b 2 → queue in transaction (respond QUEUED)
  4. SET c 3 → queue in transaction (respond QUEUED)
  5. Detect EXEC → flush QUEUED responses, execute tx, respond array
  6. GET a → would be in next batch
```

**Rationale:** Transaction semantics require immediate QUEUED responses for queued commands.

### 3.4 Pub/Sub (SUBSCRIBE/PUBLISH)

**Behavior:** Flush batch before mode change
- SUBSCRIBE: Flush batch, enter pub/sub mode, send subscription messages
- Messages delivered via separate channel (msg_rx) - unchanged
- UNSUBSCRIBE: Flush batch, update subscriptions, send unsubscribe messages

**Existing Logic:** Pub/sub messages already delivered via `msg_rx` in tokio::select
**No Change Required:** Pub/sub already bypasses normal command flow

### 3.5 Replication (PSYNC, REPLCONF)

**Behavior:** Single command processing (no batching)
- Replication commands processed individually
- Master propagation unchanged (sends to replicas after each write command)

**Existing Logic:** Replication handshake uses separate connection, propagation via replica channels
**No Change Required:** Replication already isolated from client command flow

---

## 4. Response Buffering Strategy

### 4.1 BufWriter Integration

**Current State:**
- `RespHandler` uses `BufWriter<OwnedWriteHalf>` with 8KB default buffer
- Each `write_value()` calls `flush()` immediately (defeats buffering)

**New Behavior:**
- `write_batch(Vec<Value>)` writes all values before single `flush()`
- Leverages BufWriter's existing buffer
- No additional buffering layer needed

### 4.2 Flush Strategy

**When to Flush:**
1. After processing entire batch of normal commands
2. Before blocking command (BLPOP, XREAD BLOCK)
3. Before/after transaction boundaries (MULTI, EXEC, DISCARD)
4. Before pub/sub mode change (SUBSCRIBE)
5. After single command if no pipelining detected

**Code Pattern:**
```rust
// Accumulate responses
responses.push(resp1);
responses.push(resp2);
responses.push(resp3);

// Flush all at once
handler.write_batch(responses).await?;
```

### 4.3 Non-Pipelined Client Support

**Detection:** If `try_read_value_nonblocking()` returns `Ok(None)` immediately
**Behavior:** Batch size = 1, single command processed, single response flushed
**Performance:** Identical to current implementation (no regression)

**Example:**
```
Client: GET key1 (waits for response)
Server:
  1. read_value() blocks, reads GET key1
  2. try_read_value_nonblocking() returns None (buffer empty)
  3. Batch size = 1
  4. Process GET key1
  5. write_batch([response]) → single flush
  6. Client receives response
  7. Client sends next command
```

### 4.4 Buffer Size Limits

**Safety Constraints:**
- Max batch size: 1000 commands (prevents DoS)
- BufWriter buffer: 8KB (existing default)
- BytesMut buffer: 512 bytes initial, grows as needed

**Overflow Handling:**
- If batch reaches 1000 commands, break and process
- BufWriter automatically flushes if internal buffer full
- BytesMut grows dynamically for large commands

---

## 5. Edge Cases & Error Handling

### 5.1 Partial Command in Buffer

**Scenario:** Buffer contains "SET key" but no value yet
**Current Behavior:** `parse_message()` returns `Err("Incomplete")`
**Pipeline Behavior:**
- `try_read_value_nonblocking()` returns `Ok(None)` (incomplete is not an error)
- Incomplete command stays in buffer
- Next `read_value()` performs I/O to complete the command

**Code:**
```rust
pub fn try_read_value_nonblocking(&mut self) -> Result<Option<Value>> {
    if let Ok((v, consumed)) = parse_message(&self.buffer) {
        self.buffer.split_to(consumed);
        return Ok(Some(v));
    }
    // Incomplete command, don't treat as error
    Ok(None)
}
```

### 5.2 Parse Error in Batch

**Scenario:** Client sends "SET key1 val1, INVALID, GET key2"
**Behavior:**
- SET key1 val1 → processes successfully
- INVALID → `RedisCommand::from_resp()` fails
- Error response added to batch: `Value::Error("ERR parse error")`
- GET key2 → processes successfully
- All 3 responses flushed together

**Critical:** Do NOT break batch processing on parse error, continue with remaining commands

### 5.3 Error Mid-Batch

**Scenario:** Engine returns error for one command in batch
**Behavior:**
- Successful command → `Ok(Value::BulkString("result"))`
- Failed command → `Ok(Value::Error("ERR message"))`
- Remaining commands → continue processing

**Example:**
```rust
match resp_rx.await {
    Ok(Ok(response)) => responses.push(response),
    Ok(Err(e)) => responses.push(Value::Error(format!("ERR {}", e))),
    Err(_) => break, // Engine dropped channel, stop processing
}
```

### 5.4 Connection Drop Mid-Batch

**Scenario:** Client disconnects while batch is being processed
**Detection:** `resp_rx.await` returns `Err(_)` (Engine dropped channel)
**Behavior:**
- Break out of batch processing loop
- Accumulated responses discarded (client gone)
- Connection handler exits
- Engine receives `InternalDisconnect` command (existing logic)

**No Special Handling Required:** Existing disconnect logic handles cleanup

### 5.5 Client Sends Single Command Then Waits

**Scenario:** Traditional request/response pattern (no pipelining)
**Detection:** `try_read_value_nonblocking()` returns `Ok(None)` immediately
**Behavior:**
- Batch contains 1 command
- Processed immediately
- Single response flushed immediately
- Latency identical to current implementation

**Performance:** No overhead for non-pipelined clients

---

## 6. Performance Considerations

### 6.1 Memory Overhead

**Command Queue:**
- Max 1000 commands × ~100 bytes each = 100KB worst case
- Typical batch: 10-50 commands = 1-5KB
- Negligible compared to existing buffers

**Response Queue:**
- Max 1000 responses × ~100 bytes each = 100KB worst case
- Typical batch: 10-50 responses = 1-5KB
- Temporary allocation, freed after flush

**Total Memory Impact:** <200KB per connection worst case, <10KB typical

### 6.2 Latency Impact

**Non-Pipelined Clients:**
- Batch size = 1
- No additional processing delay
- Latency unchanged: ~1.49ms (current p50)

**Pipelined Clients:**
- First command in batch: slightly higher latency (wait for batch to complete)
- Last command in batch: slightly lower latency (already processed)
- Average latency per command: should remain similar
- Total batch latency: lower (fewer syscalls)

**Expected Impact:**
- p50 latency: 1.45-1.50ms (negligible change)
- p99 latency: 1.65-1.75ms (slight improvement from fewer syscalls)

### 6.3 Syscall Reduction

**Current (per command):**
```
Client: 1 command
Server: recvfrom() → 1 syscall
Server: sendto() → 1 syscall
Total: 2 syscalls per command
```

**With Pipelining (10 commands):**
```
Client: 10 commands sent together
Server: recvfrom() → 1 syscall (reads all 10)
Server: sendto() → 1 syscall (writes all 10)
Total: 2 syscalls for 10 commands (10x reduction)
```

**Theoretical Maximum:**
- Current: 2N syscalls for N commands
- Pipelined: ~2 syscalls per batch
- Reduction: Up to 90% for large batches

**Real-World Expectation:**
- Typical batch size: 10-50 commands
- Syscall reduction: 80-95%
- Addresses 91% I/O bottleneck directly

### 6.4 CPU Efficiency

**Current CPU Distribution:**
- 91% network I/O (syscalls)
- 9% actual work (parsing, command execution)

**Expected After Pipelining:**
- 50-70% network I/O (much fewer syscalls)
- 30-50% actual work
- More balanced CPU utilization

### 6.5 Throughput Projection

**Baseline:** 111,577 ops/sec (8 threads, memtier benchmark)

**Conservative Estimate (15% improvement):**
- 111,577 × 1.15 = 128,314 ops/sec

**Optimistic Estimate (25% improvement):**
- 111,577 × 1.25 = 139,471 ops/sec

**Target Range:** 128,000-140,000 ops/sec

---

## 7. Testing Strategy

### 7.1 Unit Tests

**Test `try_read_value_nonblocking()`:**
```rust
#[tokio::test]
async fn test_try_read_nonblocking_with_complete_message() {
    let mut handler = create_handler_with_buffer(b"*1\r\n$4\r\nPING\r\n");
    let result = handler.try_read_value_nonblocking().unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn test_try_read_nonblocking_with_empty_buffer() {
    let mut handler = create_handler_with_buffer(b"");
    let result = handler.try_read_value_nonblocking().unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_try_read_nonblocking_with_incomplete_message() {
    let mut handler = create_handler_with_buffer(b"*1\r\n$4\r\nPI");
    let result = handler.try_read_value_nonblocking().unwrap();
    assert!(result.is_none()); // Incomplete, not an error
}
```

**Test `write_batch()`:**
```rust
#[tokio::test]
async fn test_write_batch_single_flush() {
    let mut handler = create_test_handler();
    let responses = vec![
        Value::SimpleString("OK".to_string()),
        Value::BulkString("result".to_string()),
        Value::Integer(42),
    ];
    handler.write_batch(responses).await.unwrap();

    // Verify all written in one batch
    let output = get_handler_output(&handler);
    assert_eq!(output, "+OK\r\n$6\r\nresult\r\n:42\r\n");
}
```

### 7.2 Integration Tests

**Test Single Command (Non-Pipelined):**
```rust
#[tokio::test]
async fn test_single_command_no_pipelining() {
    let mut client = connect_to_server().await;

    client.send("*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").await;
    let response = client.recv().await;
    assert_eq!(response, "$-1\r\n"); // Null response
}
```

**Test Multiple Pipelined Commands:**
```rust
#[tokio::test]
async fn test_pipelined_commands() {
    let mut client = connect_to_server().await;

    // Send 3 commands without waiting
    client.send("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n").await;
    client.send("*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n").await;
    client.send("*2\r\n$3\r\nGET\r\n$1\r\na\r\n").await;

    // Receive all 3 responses in order
    assert_eq!(client.recv().await, "+OK\r\n");
    assert_eq!(client.recv().await, "+OK\r\n");
    assert_eq!(client.recv().await, "$1\r\n1\r\n");
}
```

**Test FIFO Ordering:**
```rust
#[tokio::test]
async fn test_response_ordering() {
    let mut client = connect_to_server().await;

    // Pipeline 100 SET commands
    for i in 0..100 {
        client.send(format!("*3\r\n$3\r\nSET\r\n$3\r\nkey{}\r\n$3\r\nval{}\r\n", i, i)).await;
    }

    // Verify 100 OK responses in order
    for _ in 0..100 {
        assert_eq!(client.recv().await, "+OK\r\n");
    }
}
```

**Test Error in Batch:**
```rust
#[tokio::test]
async fn test_error_in_pipelined_batch() {
    let mut client = connect_to_server().await;

    client.send("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n").await; // OK
    client.send("*1\r\n$7\r\nINVALID\r\n").await; // Error
    client.send("*2\r\n$3\r\nGET\r\n$1\r\na\r\n").await; // OK

    assert_eq!(client.recv().await, "+OK\r\n");
    assert!(client.recv().await.starts_with("-ERR"));
    assert_eq!(client.recv().await, "$1\r\n1\r\n");
}
```

**Test Blocking Command Flush:**
```rust
#[tokio::test]
async fn test_blpop_flushes_batch() {
    let mut client = connect_to_server().await;

    // Pipeline: SET, BLPOP, GET
    client.send("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n").await;
    client.send("*3\r\n$5\r\nBLPOP\r\n$4\r\nlist\r\n$1\r\n1\r\n").await;

    // SET response should arrive before BLPOP blocks
    assert_eq!(client.recv_with_timeout(100).await, Some("+OK\r\n"));

    // BLPOP should timeout and respond
    assert_eq!(client.recv_with_timeout(1500).await, Some("*-1\r\n"));
}
```

**Test Transaction Boundary:**
```rust
#[tokio::test]
async fn test_transaction_batch_boundary() {
    let mut client = connect_to_server().await;

    // Pipeline: SET, MULTI, SET, SET, EXEC
    client.send("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n").await;
    client.send("*1\r\n$5\r\nMULTI\r\n").await;
    client.send("*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n").await;
    client.send("*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n").await;
    client.send("*1\r\n$4\r\nEXEC\r\n").await;

    // Responses: OK (SET a), OK (MULTI), QUEUED, QUEUED, [OK, OK] (EXEC)
    assert_eq!(client.recv().await, "+OK\r\n");
    assert_eq!(client.recv().await, "+OK\r\n");
    assert_eq!(client.recv().await, "+QUEUED\r\n");
    assert_eq!(client.recv().await, "+QUEUED\r\n");
    assert_eq!(client.recv().await, "*2\r\n+OK\r\n+OK\r\n");
}
```

### 7.3 Performance Tests

**Measure Syscall Reduction (macOS dtrace):**
```bash
# Before pipelining
sudo dtrace -n 'syscall::read:entry,syscall::write:entry /execname == "mikkadb"/ { @[probefunc] = count(); }'

# After pipelining
# Expect 80-90% reduction in read/write syscalls
```

**Benchmark Throughput:**
```bash
# Baseline (before pipelining)
memtier_benchmark -p 6379 --protocol=resp3 --pipeline=1 --clients=25 --threads=8 --test-time=30 --data-size=100 --key-pattern=R:R --ratio=10:1

# With pipelining (pipeline=10)
memtier_benchmark -p 6379 --protocol=resp3 --pipeline=10 --clients=25 --threads=8 --test-time=30 --data-size=100 --key-pattern=R:R --ratio=10:1

# Expected: 15-25% improvement in ops/sec
```

**Latency Distribution:**
```bash
# Compare p50, p99, p999 latencies
# Expect: p50 stable, p99 slightly improved
```

### 7.4 Compatibility Tests

**Redis-CLI:**
```bash
redis-cli -p 6379 PING
# Should work identically (non-pipelined)

echo -e "SET a 1\nGET a\nSET b 2\nGET b" | redis-cli -p 6379
# Should pipeline correctly
```

**Redis-Py:**
```python
import redis
r = redis.Redis(port=6379)

# Non-pipelined
r.set('key1', 'value1')
assert r.get('key1') == b'value1'

# Pipelined
pipe = r.pipeline()
pipe.set('a', '1')
pipe.set('b', '2')
pipe.get('a')
pipe.get('b')
results = pipe.execute()
assert results == [True, True, b'1', b'2']
```

---

## 8. Implementation Phases

### Phase 1: Foundation (2-3 hours)
- [ ] Add `try_read_value_nonblocking()` to RespHandler
- [ ] Add `write_batch()` to RespHandler
- [ ] Write unit tests for new methods
- [ ] Verify tests pass

**Deliverable:** RespHandler supports non-blocking reads and batch writes

### Phase 2: Basic Pipelining (3-4 hours)
- [ ] Modify connection handler to drain buffer
- [ ] Implement `handle_command_batch()` for normal commands
- [ ] Add batch size limit (1000 commands)
- [ ] Test single command (ensure no regression)
- [ ] Test multiple pipelined commands

**Deliverable:** Basic pipelining works for normal commands (GET, SET)

### Phase 3: Special Command Handling (2-3 hours)
- [ ] Implement batch flush before blocking commands
- [ ] Implement batch flush at transaction boundaries
- [ ] Handle pub/sub command flushing
- [ ] Test BLPOP in pipeline
- [ ] Test MULTI/EXEC in pipeline

**Deliverable:** All command types handled correctly

### Phase 4: Edge Cases (2-3 hours)
- [ ] Test partial command reads
- [ ] Test parse errors in batch
- [ ] Test connection drop mid-batch
- [ ] Test error responses in batch
- [ ] Test large batches (100+ commands)

**Deliverable:** Robust error handling

### Phase 5: Performance Validation (3-4 hours)
- [ ] Run baseline benchmarks (before pipelining)
- [ ] Run pipelined benchmarks (pipeline=10, 50, 100)
- [ ] Measure syscall reduction with dtrace
- [ ] Compare latency distributions
- [ ] Profile CPU usage
- [ ] Document results

**Deliverable:** Performance improvement validated (15-25% target)

### Phase 6: Documentation (1-2 hours)
- [ ] Update PERFORMANCE-REPORT.md
- [ ] Add code comments
- [ ] Update OpenSpec tasks
- [ ] Document benchmark methodology

**Deliverable:** Complete documentation

**Total Estimated Time:** 13-19 hours

---

## 9. Risk Analysis

### 9.1 High Risks (Must Address)

**Risk:** Response ordering bug (responses out of order)
**Likelihood:** Medium
**Impact:** Critical (breaks Redis protocol)
**Mitigation:**
- Strict FIFO processing in `handle_command_batch()`
- Integration tests verifying order with 100+ commands
- Manual testing with redis-cli pipelining

**Risk:** Blocking command deadlock (batch doesn't flush before BLPOP)
**Likelihood:** Low
**Impact:** High (client hangs)
**Mitigation:**
- Explicit detection of blocking commands
- Forced flush before blocking
- Integration tests for BLPOP/XREAD BLOCK in pipelines

**Risk:** Transaction isolation broken (commands leak across MULTI/EXEC)
**Likelihood:** Low
**Impact:** High (data corruption)
**Mitigation:**
- Flush batch before MULTI
- Existing transaction logic unchanged
- Integration tests for MULTI/EXEC in pipelines

### 9.2 Medium Risks (Monitor)

**Risk:** Memory exhaustion from large batches
**Likelihood:** Low
**Impact:** Medium (DoS)
**Mitigation:**
- Hard limit of 1000 commands per batch
- Existing buffer limits in BytesMut/BufWriter

**Risk:** Latency regression for non-pipelined clients
**Likelihood:** Low
**Impact:** Medium
**Mitigation:**
- Batch size = 1 for non-pipelined clients
- Performance tests with pipeline=1

**Risk:** Partial command handling bug (infinite loop or panic)
**Likelihood:** Low
**Impact:** Medium
**Mitigation:**
- Existing `parse_message()` logic unchanged
- `try_read_value_nonblocking()` returns None on incomplete
- Unit tests for partial commands

### 9.3 Low Risks (Accept)

**Risk:** Throughput improvement below target (15-25%)
**Likelihood:** Low
**Impact:** Low (still an improvement)
**Mitigation:**
- Conservative target (15% minimum)
- Profiling identifies actual bottleneck if target missed

**Risk:** Compatibility issue with obscure Redis client
**Likelihood:** Low
**Impact:** Low
**Mitigation:**
- Test with redis-cli, redis-py (most common)
- Follow Redis protocol exactly (no custom behavior)

---

## 10. Rollback Plan

**If Severe Issues Found:**
1. Revert `src/main.rs` connection handler changes
2. Revert `src/resp.rs` new methods
3. System reverts to one-command-at-a-time (current behavior)
4. No data loss (no schema changes)

**Criteria for Rollback:**
- Response ordering bug found in production
- Performance regression >5%
- Critical compatibility issue with major client

**Rollback Time:** <5 minutes (git revert + cargo build)

---

## 11. Success Metrics

**Functional Success:**
- [ ] All existing tests pass (no regression)
- [ ] All new integration tests pass
- [ ] redis-cli pipelining works correctly
- [ ] redis-py pipelining works correctly

**Performance Success:**
- [ ] Throughput improvement: ≥15% (target: 15-25%)
- [ ] Syscall reduction: ≥70% (target: 80-90%)
- [ ] p50 latency: ≤1.55ms (current: 1.49ms, allow 4% variance)
- [ ] p99 latency: ≤1.75ms (current: 1.70ms, allow 3% variance)

**Operational Success:**
- [ ] No memory leaks (run for 1 hour, check RSS)
- [ ] No connection leaks (run for 1 hour, check open FDs)
- [ ] No CPU regressions (idle CPU <5%)

---

## 12. Specific Recommendations

### 12.1 Code Structure

**Recommendation:** Keep `handle_command_batch()` as a separate function, not inline

**Rationale:**
- Easier to test in isolation
- Clearer separation of concerns
- Easier to add features (e.g., adaptive batch sizing)

### 12.2 Performance Tuning

**Recommendation:** Start with conservative batch limit (1000), tune later

**Rationale:**
- Safety first (prevent DoS)
- 1000 commands = ~100KB memory, negligible
- Can increase limit based on production metrics

### 12.3 Testing Priority

**Recommendation:** Focus on integration tests over unit tests

**Rationale:**
- Pipelining is end-to-end behavior
- Unit tests for `try_read_value_nonblocking()` and `write_batch()` sufficient
- Integration tests catch ordering bugs, flush bugs, compatibility issues

### 12.4 Deployment Strategy

**Recommendation:** Deploy with feature flag (compile-time or runtime)

**Example:**
```rust
const ENABLE_PIPELINING: bool = true; // Feature flag

if ENABLE_PIPELINING {
    // New pipelining logic
} else {
    // Original one-at-a-time logic
}
```

**Rationale:**
- Easy rollback without recompile
- A/B testing in production
- Gradual rollout

### 12.5 Monitoring

**Recommendation:** Add metrics for:
- Average batch size per connection
- Max batch size observed
- Commands processed per second
- Flush count per second

**Rationale:**
- Understand real-world pipelining behavior
- Detect performance anomalies
- Tune batch limits based on data

---

## 13. Alternative Approaches Considered

### Option B: Expose `read_all_available()` in RespHandler

**Description:** Add method to RespHandler that reads and returns all available commands

**Pros:**
- Encapsulates buffer draining in RespHandler
- Connection handler only deals with Vec<Value>

**Cons:**
- Violates single responsibility (RespHandler knows about batching)
- Harder to handle blocking commands (RespHandler doesn't know about BLPOP)
- More complex testing (RespHandler does more)

**Decision:** Rejected in favor of Option A

### Option C: Async Command Queue with Tokio Channels

**Description:** Use mpsc channel for commands, separate task for batch processing

**Pros:**
- Fully async batch assembly
- Could enable more sophisticated batching (time-based, size-based)

**Cons:**
- Significant architectural change
- Adds complexity (additional task per connection)
- Harder to preserve FIFO ordering
- More latency overhead

**Decision:** Rejected (over-engineered for current needs)

### Option D: TCP_CORK / TCP_NODELAY Tuning

**Description:** Use TCP_CORK to batch writes at kernel level

**Pros:**
- No application code changes
- Kernel handles batching

**Cons:**
- Doesn't reduce read syscalls
- Less control over batching behavior
- Platform-specific (TCP_CORK is Linux-only)
- macOS uses TCP_NOPUSH with different semantics

**Decision:** Rejected (insufficient control, doesn't address read syscalls)

---

## 14. Conclusion

The proposed implementation provides a clean, minimal solution to command pipelining with strong guarantees around correctness and compatibility. By draining the buffer in the connection handler and flushing batches at appropriate boundaries, we achieve significant syscall reduction while maintaining Redis protocol semantics.

**Key Strengths:**
- Minimal code changes (focused on connection handler)
- Preserves existing RespHandler parsing logic
- Handles all command types correctly (blocking, transactions, pub/sub)
- No regression for non-pipelined clients
- Clear rollback plan

**Expected Impact:**
- 15-25% throughput improvement
- 80-90% syscall reduction
- Addresses 91% I/O bottleneck directly
- Positions system for further optimizations (zero-copy parsing)

**Next Steps:**
1. Implement Phase 1 (RespHandler foundation)
2. Validate with unit tests
3. Implement Phase 2 (basic pipelining)
4. Iterate through remaining phases
5. Performance validation
6. Production deployment

---

## Appendix A: Code Diffs

### A.1 RespHandler Changes (src/resp.rs)

```diff
impl RespHandler {
    // ... existing methods ...

+   /// Try to read one value without blocking if buffer is empty
+   /// Returns Ok(None) if buffer is empty and would block
+   pub fn try_read_value_nonblocking(&mut self) -> Result<Option<Value>> {
+       // If buffer already has a complete message, parse it
+       if let Ok((v, consumed)) = parse_message(&self.buffer) {
+           let _ = self.buffer.split_to(consumed);
+           return Ok(Some(v));
+       }
+
+       // Buffer doesn't have complete message, return None without reading
+       Ok(None)
+   }
+
+   /// Write multiple values in one batch with single flush
+   pub async fn write_batch(&mut self, values: Vec<Value>) -> Result<()> {
+       for value in values {
+           // Write to BufWriter without flushing
+           self.writer.write_all(&value.serialize_bytes()).await?;
+       }
+       // Single flush for entire batch
+       self.writer.flush().await?;
+       Ok(())
+   }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.writer.write_all(&value.serialize_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }
}
```

### A.2 Connection Handler Changes (src/main.rs)

```diff
tokio::spawn(async move {
    let mut handler = resp::RespHandler::new(stream);
    let mut repl_rx: Option<mpsc::Receiver<crate::resp::Value>> = None;
    let (msg_tx, mut msg_rx) = mpsc::channel(32);

    loop {
        tokio::select! {
            value = handler.read_value() => {
                match value {
                    Ok(Some(v)) => {
-                       match RedisCommand::from_resp(v) {
-                           Ok(command) => {
-                               // ... process single command ...
-                           }
-                           Err(e) => {
-                               let _ = handler.write_value(resp::Value::Error(format!("ERR {}", e))).await;
-                           }
-                       }
+                       // Collect all available commands from buffer
+                       let mut commands = vec![v];
+
+                       // Drain buffer without blocking
+                       while let Ok(Some(v)) = handler.try_read_value_nonblocking() {
+                           commands.push(v);
+
+                           // Safety limit: prevent unbounded batches
+                           if commands.len() >= 1000 {
+                               break;
+                           }
+                       }
+
+                       // Process batch
+                       handle_command_batch(&mut handler, commands, client_id, &tx, &msg_tx).await;
                    }
                    Ok(None) => break,
                    Err(_e) => break,
                }
            }
            // ... other branches unchanged ...
        }
    }
});

+async fn handle_command_batch(
+    handler: &mut resp::RespHandler,
+    commands: Vec<resp::Value>,
+    client_id: u64,
+    tx: &mpsc::Sender<CommandRequest>,
+    msg_tx: &mpsc::Sender<Value>,
+) {
+    let mut responses = Vec::with_capacity(commands.len());
+
+    for command_value in commands {
+        // ... (see section 2.3 for full implementation) ...
+    }
+
+    // Flush remaining responses
+    if !responses.is_empty() {
+        let _ = handler.write_batch(responses).await;
+    }
+}
```

---

## Appendix B: Benchmark Commands

```bash
# Baseline (no pipelining)
memtier_benchmark -p 6379 \
  --protocol=resp3 \
  --pipeline=1 \
  --clients=25 \
  --threads=8 \
  --test-time=30 \
  --data-size=100 \
  --key-pattern=R:R \
  --ratio=10:1 \
  --hide-histogram

# Small pipeline
memtier_benchmark -p 6379 \
  --protocol=resp3 \
  --pipeline=10 \
  --clients=25 \
  --threads=8 \
  --test-time=30 \
  --data-size=100 \
  --key-pattern=R:R \
  --ratio=10:1 \
  --hide-histogram

# Medium pipeline
memtier_benchmark -p 6379 \
  --protocol=resp3 \
  --pipeline=50 \
  --clients=25 \
  --threads=8 \
  --test-time=30 \
  --data-size=100 \
  --key-pattern=R:R \
  --ratio=10:1 \
  --hide-histogram

# Large pipeline
memtier_benchmark -p 6379 \
  --protocol=resp3 \
  --pipeline=100 \
  --clients=25 \
  --threads=8 \
  --test-time=30 \
  --data-size=100 \
  --key-pattern=R:R \
  --ratio=10:1 \
  --hide-histogram
```

---

## Appendix C: Syscall Tracing

**macOS (dtrace):**
```bash
# Count syscalls during benchmark
sudo dtrace -n 'syscall::read:entry,syscall::write:entry /execname == "mikkadb"/ { @[probefunc] = count(); }' &
DTRACE_PID=$!

# Run benchmark
memtier_benchmark -p 6379 --protocol=resp3 --pipeline=10 --test-time=30

# Stop dtrace
sudo kill $DTRACE_PID

# Expected output:
# Before: read=200000, write=200000 (for 100k ops)
# After: read=10000, write=10000 (90% reduction)
```

**Linux (strace):**
```bash
# Count syscalls
strace -c -p $(pgrep mikkadb) &
STRACE_PID=$!

# Run benchmark
memtier_benchmark -p 6379 --protocol=resp3 --pipeline=10 --test-time=30

# Stop strace
kill $STRACE_PID

# Check read/write syscall counts
```

---

**Document Version:** 1.0
**Author:** Claude (Sonnet 4.5)
**Date:** 2025-11-22
**Status:** Final Implementation Plan
