# Migration Risk Matrix & Quick Reference

## Critical Decision Points

### 1. State Partitioning Strategy

**Decision**: Split Engine into 5 independent domains

| Domain | Implementation | Rationale | Complexity |
|--------|----------------|-----------|------------|
| Core DB | `Arc<RwLock<Db>>` | Concurrent reads, exclusive writes | LOW ✅ |
| Transactions | Client-local `Option<Vec<Command>>` | No shared state needed | LOW ✅ |
| Pub/Sub | `Arc<DashMap<Channel, Subscribers>>` | Lock-free hot path | MEDIUM ⚠️ |
| Blocking Ops | `Arc<RwLock<WaitList>> + Notify` | Event-driven coordination | HIGH 🔴 |
| Replication | Dedicated Actor with channel | Maintain strict ordering | MEDIUM ⚠️ |

### 2. Lock Ordering Hierarchy

**CRITICAL**: Always acquire in this order to prevent deadlocks

```
1. db: Arc<RwLock<Db>>                     [LEVEL 1 - Highest]
2. wait_list: Arc<RwLock<WaitList>>        [LEVEL 2]
3. pubsub: Arc<DashMap<...>>               [LEVEL 3 - Lock-free]
4. repl_tx: mpsc::Sender<...>              [LEVEL 4 - Lock-free]
```

**Enforcement**: Code review checklist + Clippy lints

---

## Risk Assessment Matrix

| Risk | Likelihood | Impact | Severity | Mitigation | Status |
|------|------------|--------|----------|------------|--------|
| Deadlocks from lock order violation | MEDIUM | CRITICAL | 🔴 HIGH | Lock ordering rules + static analysis | MITIGATED |
| Transaction isolation violations | LOW | CRITICAL | 🟡 MEDIUM | Client-local state + single EXEC lock | MITIGATED |
| Pub/Sub message loss during PUBLISH | MEDIUM | HIGH | 🟡 MEDIUM | DashMap atomic iteration + ignore send errors | MITIGATED |
| BLPOP waiters not woken after LPUSH | MEDIUM | HIGH | 🟡 MEDIUM | Fair FIFO queue + retry loop | MITIGATED |
| Replication command out-of-order | LOW | CRITICAL | 🟡 MEDIUM | Dedicated single-threaded actor | MITIGATED |
| Performance regression (no speedup) | LOW | MEDIUM | 🟢 LOW | Benchmark before/after + profiling | ACCEPTABLE |
| Thundering herd on blocking wake | MEDIUM | MEDIUM | 🟢 LOW | Fair queueing + metrics | ACCEPTABLE |
| Lock held during async await | MEDIUM | CRITICAL | 🔴 HIGH | Code review + tokio-console | MITIGATED |

**Legend:**
- 🔴 HIGH: Requires immediate attention, must be addressed before migration
- 🟡 MEDIUM: Important to address, monitor closely during migration
- 🟢 LOW: Acceptable risk, address if encountered

---

## Critical Implementation Patterns

### Pattern 1: Read-Only Command (Concurrent)

```rust
// ✅ CORRECT: Short critical section, concurrent
async fn handle_get(db: Arc<RwLock<Db>>, key: String) -> Result<Value> {
    let lock = db.read().await;  // Multiple readers allowed
    match lock.get(&key) {
        Some(v) => Ok(Value::BulkString(String::from_utf8_lossy(&v).to_string())),
        None => Ok(Value::Null),
    }
}  // Lock released automatically

// ❌ WRONG: Holding lock during send
async fn handle_get_wrong(db: Arc<RwLock<Db>>, key: String, tx: mpsc::Sender<Value>) {
    let lock = db.read().await;
    let value = lock.get(&key);
    tx.send(value).await;  // 🔴 Lock held during await - WRONG!
}
```

### Pattern 2: Write Command with Replication

```rust
// ✅ CORRECT: Separate DB write and replication
async fn handle_set(
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>,
    key: String,
    value: String,
    px: Option<u64>
) -> Result<Value> {
    // Step 1: Write to DB (exclusive lock, short critical section)
    {
        let mut lock = db.write().await;
        lock.set(key.clone(), Bytes::from(value.clone()), px);
    }  // Lock released BEFORE propagation

    // Step 2: Propagate to replicas (async, no lock)
    repl_tx.send(ReplicationCommand::Propagate(/* ... */)).await?;

    Ok(Value::SimpleString("OK".to_string()))
}

// ❌ WRONG: Holding lock during replication
async fn handle_set_wrong(db: Arc<RwLock<Db>>, replicas: Vec<mpsc::Sender<Value>>) {
    let mut lock = db.write().await;
    lock.set(key, value, px);

    // 🔴 Lock held during replication - blocks all readers!
    for replica in replicas {
        replica.send(cmd.clone()).await;
    }
}
```

### Pattern 3: Blocking Command (BLPOP)

```rust
// ✅ CORRECT: Lock → Check → Unlock → Wait → Lock → Check
async fn handle_blpop(
    keys: Vec<String>,
    timeout: f64,
    db: Arc<RwLock<Db>>,
    wait_list: Arc<RwLock<WaitList>>
) -> Result<Value> {
    // Phase 1: Try immediate pop (lock held briefly)
    for key in &keys {
        let result = {
            let mut lock = db.write().await;
            lock.lpop(key, None)
        };  // Lock released

        if let Ok(Some(values)) = result {
            return Ok(build_response(key, values));
        }
    }

    // Phase 2: Register waiter (separate lock)
    let notify = {
        let mut waiters = wait_list.write().await;
        let notify = Arc::new(Notify::new());
        for key in &keys {
            waiters.entry(key.clone())
                .or_insert_with(Vec::new)
                .push(notify.clone());
        }
        notify
    };  // Lock released

    // Phase 3: Wait (NO LOCKS HELD - critical!)
    if timeout > 0.0 {
        let _ = tokio::time::timeout(
            Duration::from_secs_f64(timeout),
            notify.notified()
        ).await;
    } else {
        notify.notified().await;
    }

    // Phase 4: Retry pop (lock held briefly)
    for key in &keys {
        let result = {
            let mut lock = db.write().await;
            lock.lpop(key, None)
        };

        if let Ok(Some(values)) = result {
            return Ok(build_response(key, values));
        }
    }

    Ok(Value::NullArray)
}

// ❌ WRONG: Holding lock during wait
async fn handle_blpop_wrong(db: Arc<RwLock<Db>>, keys: Vec<String>) {
    let mut lock = db.write().await;
    if let Some(value) = lock.lpop(&keys[0], None) {
        return Ok(value);
    }

    // 🔴 Lock held during wait - DEADLOCK!
    // No other task can write because we hold the lock
    tokio::time::sleep(Duration::from_secs(5)).await;
}
```

### Pattern 4: Transaction Execution (EXEC)

```rust
// ✅ CORRECT: Single lock for entire transaction
async fn handle_exec(
    commands: Vec<RedisCommand>,
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>
) -> Result<Value> {
    let mut results = Vec::new();

    // Acquire write lock ONCE for atomicity
    let mut lock = db.write().await;

    for cmd in commands {
        let result = execute_in_transaction(&mut lock, &cmd)?;
        results.push(result);

        // Queue replication (don't send yet - lock held)
        if cmd.is_write() {
            // Store for batch propagation
        }
    }

    drop(lock);  // Explicit lock release

    // Propagate all writes in order (lock released)
    for write_cmd in write_commands {
        repl_tx.send(ReplicationCommand::Propagate(write_cmd)).await?;
    }

    Ok(Value::Array(results))
}

// ❌ WRONG: Separate locks per command
async fn handle_exec_wrong(commands: Vec<RedisCommand>, db: Arc<RwLock<Db>>) {
    let mut results = Vec::new();

    for cmd in commands {
        // 🔴 Each command gets separate lock - NOT ATOMIC!
        let mut lock = db.write().await;
        let result = execute(&mut lock, cmd);
        drop(lock);
        results.push(result);
    }
}
```

### Pattern 5: Pub/Sub with DashMap

```rust
// ✅ CORRECT: Lock-free concurrent access
async fn handle_publish(
    channel: String,
    message: String,
    pubsub: Arc<DashMap<String, DashMap<u64, mpsc::Sender<Value>>>>
) -> Result<Value> {
    let count = if let Some(subs) = pubsub.get(&channel) {
        let msg = Value::Array(vec![
            Value::BulkString("message".to_string()),
            Value::BulkString(channel.clone()),
            Value::BulkString(message),
        ]);

        let mut sent = 0;
        for sender in subs.value().iter() {
            // DashMap ensures atomic iteration
            if sender.value().send(msg.clone()).await.is_ok() {
                sent += 1;
            }
        }
        sent
    } else {
        0
    };

    Ok(Value::Integer(count))
}

// ✅ CORRECT: Concurrent subscribe (no coordination needed)
async fn handle_subscribe(
    client_id: u64,
    channels: Vec<String>,
    msg_tx: mpsc::Sender<Value>,
    pubsub: Arc<DashMap<String, DashMap<u64, mpsc::Sender<Value>>>>
) -> Result<Value> {
    for channel in channels {
        // Lock-free insert
        let subs = pubsub.entry(channel.clone())
            .or_insert_with(DashMap::new);
        subs.insert(client_id, msg_tx.clone());

        // Send confirmation
        let count = count_client_subscriptions(&pubsub, client_id);
        msg_tx.send(confirmation_message(&channel, count)).await?;
    }

    Ok(Value::Error("NO_REPLY".to_string()))
}
```

---

## Deadlock Prevention Checklist

Before merging any PR that acquires locks:

- [ ] Locks acquired in documented hierarchy order (db → wait_list → ...)
- [ ] No locks held during `await` points (async operations)
- [ ] Critical sections are minimal (lock → work → unlock)
- [ ] Timeout on all blocking operations (no infinite waits)
- [ ] Lock guards dropped explicitly before async calls
- [ ] Tested with `tokio-console` for lock contention
- [ ] Reviewed by second engineer for lock ordering

---

## Testing Strategy by Risk

### 🔴 HIGH Priority Tests (Must Pass Before Migration)

1. **Deadlock Detection**
   ```rust
   #[tokio::test(timeout = "5s")]
   async fn test_no_deadlock_under_concurrent_load() {
       // 100 clients executing mixed operations
       // If test times out = deadlock detected
   }
   ```

2. **Transaction Isolation**
   ```rust
   #[tokio::test]
   async fn test_exec_atomicity() {
       // Client A: MULTI → SET key1 → SET key2 → EXEC
       // Client B: GET key1 (concurrent)
       // Verify: B sees either old values or new values, never partial
   }
   ```

3. **Replication Ordering**
   ```rust
   #[tokio::test]
   async fn test_replica_consistency() {
       // Master: SET a 1 → SET b 2 → SET c 3
       // Replica: Verify exact same order
       // WAIT 1 1000: Verify synced
   }
   ```

### 🟡 MEDIUM Priority Tests (Should Pass Before Production)

4. **Pub/Sub Message Delivery**
   ```rust
   #[tokio::test]
   async fn test_pubsub_no_message_loss() {
       // 10 subscribers to channel1
       // 1 publisher sends 100 messages
       // Verify: All subscribers receive all 100 messages
   }
   ```

5. **Blocking Wake Coordination**
   ```rust
   #[tokio::test]
   async fn test_blpop_fair_wakeup() {
       // 5 clients BLPOP on same key
       // 5 LPUSHes (one per waiter)
       // Verify: Each waiter gets exactly one value
   }
   ```

### 🟢 LOW Priority Tests (Nice to Have)

6. **Performance Benchmarks**
   ```rust
   #[bench]
   fn bench_concurrent_reads(b: &mut Bencher) {
       // Measure: 1000 concurrent GETs
       // Target: <1ms p99 latency
   }
   ```

---

## Migration Phases with Success Criteria

### Phase 1: Preparation (2 weeks)

**Goal**: Code ready for parallel execution (no behavioral changes)

**Success Criteria**:
- ✅ All Engine methods extracted to standalone functions
- ✅ Commands classified by lock requirements
- ✅ Integration test suite passes (baseline)
- ✅ Benchmark baseline established

**Risk**: LOW - No architectural changes yet

---

### Phase 2: State Migration (3 weeks)

**Goal**: Replace Engine with shared state structures

**Success Criteria**:
- ✅ Arc<RwLock<Db>> created and all commands updated
- ✅ Replication actor implemented and tested
- ✅ WaitList coordination working (BLPOP tests pass)
- ✅ Client-local transaction state implemented
- ✅ DashMap pub/sub working (message delivery tests pass)
- ✅ All integration tests pass
- ✅ No deadlocks detected in stress tests

**Risk**: HIGH - Breaking changes, careful testing required

---

### Phase 3: Parallel Execution (2 weeks)

**Goal**: Enable concurrent reads, remove Engine actor

**Success Criteria**:
- ✅ Engine actor loop removed
- ✅ Direct command execution in client tasks
- ✅ Concurrent read operations measured (>1 concurrent reader)
- ✅ Performance improvement measured (>2x latency reduction)
- ✅ All tests still passing

**Risk**: MEDIUM - Performance validation critical

---

### Phase 4: Optimization (1 week)

**Goal**: Fine-tune for production

**Success Criteria**:
- ✅ Lock contention profiled (tokio-console)
- ✅ Hot paths optimized (critical section size reduced)
- ✅ Load testing completed (production-like workload)
- ✅ Performance targets met (3x latency improvement)

**Risk**: LOW - Incremental improvements only

---

## Quick Command Reference

### Lock Requirements by Command

| Command | Lock Type | Duration | Coordination |
|---------|-----------|----------|--------------|
| GET, LRANGE, ZRANGE | Read | Short | None |
| SET, LPUSH, ZADD | Write | Short | Replication |
| INCR, LPOP | Write | Short | Replication |
| BLPOP | Write → Wait → Write | Long | WaitList + Notify |
| MULTI | None | N/A | Client-local |
| EXEC | Write | Long | None |
| SUBSCRIBE | Lock-free | Short | DashMap |
| PUBLISH | Lock-free | Short | DashMap |
| WAIT | None | Long | Replication Actor |

### Lock Acquisition Patterns

```
Read-Only:
  db.read().await → execute → return

Write-Only:
  db.write().await → execute → drop(lock) → repl_tx.send()

Read-Modify-Write:
  db.write().await → read → modify → write → drop(lock) → repl_tx.send()

Blocking:
  db.write().await → check → drop(lock)
  → wait_list.write().await → register → drop(lock)
  → notify.notified().await  // No locks
  → db.write().await → retry → drop(lock)

Transaction:
  db.write().await → execute_all → drop(lock) → repl_tx.send_batch()
```

---

## Final Recommendations

### ✅ DO

1. Follow lock ordering hierarchy strictly
2. Keep critical sections minimal
3. Release locks before async operations
4. Use DashMap for pub/sub (lock-free)
5. Use dedicated replication actor (ordering)
6. Test with tokio-console (lock contention)
7. Benchmark before/after (validate improvement)

### ❌ DON'T

1. Acquire locks in reverse order (deadlock)
2. Hold locks during `await` points
3. Share transaction state between clients
4. Skip integration tests (race conditions)
5. Assume writes are concurrent (they're exclusive)
6. Propagate replication outside actor (ordering)
7. Use RwLock for pub/sub (contention)

---

## Success Metrics

### Correctness (Must Achieve)

- ✅ All integration tests pass
- ✅ No deadlocks detected (timeout tests)
- ✅ Transaction isolation verified (property tests)
- ✅ Replication consistency validated
- ✅ Pub/Sub message delivery 100%

### Performance (Target)

- 🎯 Latency: 3x reduction (3ms → 1ms)
- 🎯 Throughput: 3x increase for read-heavy workloads
- 🎯 Concurrent readers: Scales linearly with cores
- 🎯 Write performance: No regression

### Operational (Monitor)

- 📊 Lock contention: <5% of execution time
- 📊 BLPOP wake time: <100ms p99
- 📊 Memory usage: No significant increase
- 📊 CPU utilization: Better across all cores
