# Engine Batch Processing Design

**Version**: 1.0
**Date**: 2025-11-25
**Status**: Ready for Implementation

---

## Executive Summary

This design implements batch command processing through the Engine channel to reduce per-command overhead. By sending entire command batches in single channel messages, we reduce channel round-trips from O(N) to O(1), delivering 2-3x throughput improvement for pipelined workloads.

**Key Metrics**:
- **Current**: P=10: 391K ops/sec, P=100: 491K ops/sec
- **Target**: P=10: 800K+ ops/sec, P=100: 1.2M+ ops/sec
- **Channel Reduction**: N messages → 1 message per batch

---

## Architecture

### Current Flow (N channel round-trips)

```
Connection Handler:
  for cmd in command_batch:
    tx.send(CommandRequest { cmd }).await     # N sends
    response = rx.recv().await                 # N receives
    responses.push(response)
  write_batch(responses)
```

### New Flow (1 channel round-trip)

```
Connection Handler:
  batch_req = BatchCommandRequest {
    commands: command_batch,
    response_tx: oneshot::channel()
  }
  tx.send(batch_req).await                    # 1 send
  responses = response_rx.await               # 1 receive
  write_batch(responses)
```

---

## Technical Design

### 1. New Types (src/engine.rs)

```rust
/// Batch of commands for efficient channel transport
pub struct BatchCommandRequest {
    pub client_id: u64,
    pub commands: Vec<RedisCommand>,
    pub response_tx: oneshot::Sender<Vec<Result<Value>>>,
    pub pub_sub_tx: Option<mpsc::Sender<Value>>,
}

/// Engine request - supports both single and batch modes
pub enum EngineRequest {
    Single(CommandRequest),
    Batch(BatchCommandRequest),
}
```

### 2. Engine Changes (src/engine.rs)

```rust
impl Engine {
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(req) = self.rx.recv() => {
                    match req {
                        EngineRequest::Single(cmd) => {
                            self.handle_command(cmd).await;
                        }
                        EngineRequest::Batch(batch) => {
                            self.handle_command_batch(batch).await;
                        }
                    }
                }
                // ... other select branches unchanged
            }
        }
    }

    async fn handle_command_batch(&mut self, batch: BatchCommandRequest) {
        let BatchCommandRequest { client_id, commands, response_tx, pub_sub_tx } = batch;
        
        let mut responses = Vec::with_capacity(commands.len());
        
        for command in commands {
            // Reuse existing handle_command logic
            let result = self.execute_command(client_id, command, &pub_sub_tx).await;
            responses.push(result);
        }
        
        let _ = response_tx.send(responses);
    }
    
    // Extract core logic from handle_command for reuse
    async fn execute_command(
        &mut self, 
        client_id: u64, 
        command: RedisCommand,
        pub_sub_tx: &Option<mpsc::Sender<Value>>
    ) -> Result<Value> {
        // ... existing command execution logic
    }
}
```

### 3. Connection Handler Changes (src/main.rs)

```rust
// After collecting command_batch from buffer...

// Create batch request
let (resp_tx, resp_rx) = oneshot::channel();
let batch_req = BatchCommandRequest {
    client_id,
    commands: command_batch.iter()
        .filter_map(|v| RedisCommand::from_resp(v.clone()).ok())
        .collect(),
    response_tx: resp_tx,
    pub_sub_tx: Some(msg_tx.clone()),
};

// Single channel send
let shard_idx = (client_id as usize) % num_shards;
if let Err(_) = shard_channels[shard_idx].send(EngineRequest::Batch(batch_req)).await {
    break;
}

// Single response receive
match resp_rx.await {
    Ok(responses) => {
        let values: Vec<Value> = responses.into_iter()
            .map(|r| r.unwrap_or_else(|e| Value::Error(e.to_string().into())))
            .collect();
        handler.write_batch(values).await?;
    }
    Err(_) => break,
}
```

---

## Edge Cases

### 1. Blocking Commands (BLPOP, XREAD with BLOCK)

**Problem**: Blocking commands can't be batched - they need special handling.

**Solution**: Detect blocking commands during batch creation and split:
```rust
// If batch contains blocking command, flush non-blocking first
if has_blocking_command(&command_batch) {
    let (before, blocking, after) = split_at_blocking(&command_batch);
    // Process 'before' as batch
    // Process 'blocking' as single (will block)
    // Continue with 'after' in next iteration
}
```

### 2. Transaction Commands (MULTI/EXEC)

**Handling**: Transactions already accumulate commands - batch processing is orthogonal.
- MULTI: Starts transaction state (processed normally)
- Queued commands: Added to transaction buffer (processed normally)
- EXEC: Executes transaction atomically (processed normally)

### 3. Error Mid-Batch

**Handling**: Continue processing remaining commands, collect error in response Vec.
```rust
for command in commands {
    let result = match self.execute_command(...).await {
        Ok(v) => Ok(v),
        Err(e) => Err(e),  // Collect error, continue
    };
    responses.push(result);
}
```

---

## Performance Analysis

### Channel Overhead Reduction

**P=10 scenario**:
- Current: 10 × mpsc::send + 10 × oneshot::recv = 20 async operations
- New: 1 × mpsc::send + 1 × oneshot::recv = 2 async operations
- **Reduction: 90%**

**P=100 scenario**:
- Current: 100 × mpsc::send + 100 × oneshot::recv = 200 async operations
- New: 1 × mpsc::send + 1 × oneshot::recv = 2 async operations
- **Reduction: 99%**

### Expected Throughput

Assuming channel overhead accounts for ~50% of current latency:
- P=10: 391K × 2 = **~780K ops/sec** (conservative)
- P=100: 491K × 2.5 = **~1.2M ops/sec** (conservative)

---

## Implementation Order

1. **Add BatchCommandRequest struct** - No behavior change
2. **Add EngineRequest enum** - Backward compatible
3. **Implement handle_command_batch()** - New code path
4. **Update connection handler** - Switch to batch mode
5. **Test and benchmark**

---

## Rollback Plan

Changes are additive. To rollback:
```bash
# Revert to single-command mode in main.rs
# Keep batch infrastructure for future use
```

**Recovery Time**: <5 minutes
