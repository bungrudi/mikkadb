# Final Implementation Plan: Shared State Architecture
## Gemini-Reviewed & Simplified for Performance Focus

**Goal**: Beat Redis performance for read-heavy workloads while maintaining correctness

**Non-Goal**: Complex replication optimization (can optimize later)

---

## Critical Decisions from Gemini Review

### 1. Start Simple, Add Complexity Only When Proven Necessary

**Decision**: Use single `Arc<RwLock<Db>>` initially, not sharding
- **Rationale**: Your workload is 90% reads - RwLock handles this well
- **Extensibility**: Abstract behind `trait` to swap to DashMap later if needed
- **Validation**: Benchmark will prove if sharding is needed

### 2. Proper BLPOP/Blocking Operations

**Decision**: FIFO queue with lazy cleanup
- **Rationale**: Correct semantics, fair wakeup, no thundering herd
- **Implementation**: `DashMap<String, VecDeque<oneshot::Sender>>`
- **Race-free**: Check-register-recheck pattern under single lock

### 3. Replication Ordering (Simplified)

**Decision**: Sequence after write, before unlock
- **Rationale**: Simple, correct, matches Redis semantics
- **Implementation**: `AtomicU64` sequence counter
- **Complexity**: Low - just assign sequence numbers

---

## Architecture Overview

```rust
// Shared state structure
struct SharedState {
    // Core database with concurrent read capability
    db: Arc<RwLock<Db>>,

    // Blocking operations coordination
    blpop_waiters: Arc<DashMap<String, VecDeque<oneshot::Sender<Value>>>>,
    xread_waiters: Arc<DashMap<String, VecDeque<oneshot::Sender<Value>>>>,

    // Pub/Sub (lock-free)
    pub_sub: Arc<DashMap<String, DashMap<ClientId, mpsc::Sender<Value>>>>,

    // Replication (simplified)
    replication_tx: mpsc::Sender<(u64, ReplicatedCommand)>,
    write_sequence: Arc<AtomicU64>,
}

// Client-local state (no global lock needed!)
struct ClientState {
    client_id: u64,
    transaction_queue: Option<Vec<RedisCommand>>,
}
```

---

## Implementation Phases

### Phase 1: Foundation (Week 1)

**Goal**: Set up abstraction layer and baseline tests

#### Tasks:
1. **Define Storage Trait** (2 hours)
   ```rust
   #[async_trait]
   pub trait KeyValueStore: Send + Sync {
       async fn get(&self, key: &str) -> Option<Bytes>;
       async fn set(&self, key: String, value: Bytes, px: Option<u64>);
       async fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, String>;
       async fn lpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, String>;
       // ... other methods
   }
   ```

2. **Implement SingleLockStore** (4 hours)
   ```rust
   pub struct SingleLockStore {
       db: Arc<RwLock<Db>>,
   }

   impl KeyValueStore for SingleLockStore {
       async fn get(&self, key: &str) -> Option<Bytes> {
           self.db.read().await.get(key).cloned()
       }

       async fn set(&self, key: String, value: Bytes, px: Option<u64>) {
           self.db.write().await.set(key, value, px);
       }
       // ...
   }
   ```

3. **Create Benchmark Suite** (6 hours)
   - Use `criterion` or custom harness
   - Test: 90% read / 10% write at 8, 32, 128, 512 concurrent clients
   - Measure: throughput (ops/sec), latency (p50, p99, p999)
   - Baseline: Run against current Actor model

4. **Add Integration Tests** (4 hours)
   - Test all command combinations
   - Test concurrent operations
   - Test edge cases (empty keys, type mismatches, etc.)

**Deliverable**: Working baseline with trait abstraction + benchmark results

---

### Phase 2: Core Shared State Migration (Week 2)

**Goal**: Replace Engine actor with shared state for simple commands

#### Tasks:
1. **Create SharedState struct** (2 hours)
   ```rust
   pub struct SharedState {
       store: Arc<dyn KeyValueStore>,
       blpop_waiters: Arc<DashMap<String, VecDeque<oneshot::Sender<Value>>>>,
       pub_sub: Arc<DashMap<String, DashMap<u64, mpsc::Sender<Value>>>>,
       replication_tx: mpsc::Sender<(u64, ReplicatedCommand)>,
       write_sequence: Arc<AtomicU64>,
   }

   impl SharedState {
       pub fn new() -> Self {
           let store = Arc::new(SingleLockStore::new());
           // ... initialize other fields
       }
   }
   ```

2. **Refactor Simple Read Commands** (4 hours)
   - GET, KEYS, TYPE, LLEN, ZCARD, etc.
   - Pattern:
   ```rust
   async fn handle_get(state: &SharedState, key: String) -> Result<Value> {
       match state.store.get(&key).await {
           Some(value) => Ok(Value::BulkString(value)),
           None => Ok(Value::Null),
       }
   }
   ```

3. **Refactor Simple Write Commands** (6 hours)
   - SET, LPUSH, ZADD, etc.
   - Pattern with replication:
   ```rust
   async fn handle_set(state: &SharedState, key: String, value: String) -> Result<Value> {
       // Write to DB
       state.store.set(key.clone(), value.clone(), None).await;

       // Sequence for replication
       let seq = state.write_sequence.fetch_add(1, Ordering::Relaxed);
       state.replication_tx.send((seq, ReplicatedCommand::Set { key, value }))
           .await
           .expect("Replication channel closed");

       Ok(Value::SimpleString("OK".to_string()))
   }
   ```

4. **Update Client Handler** (4 hours)
   ```rust
   // Before: Send to Engine via channel
   tx.send(CommandRequest { ... }).await;

   // After: Execute directly in client task
   let state = shared_state.clone();
   tokio::spawn(async move {
       let response = match command {
           RedisCommand::Get { key } => handle_get(&state, key).await,
           RedisCommand::Set { key, value, px } => handle_set(&state, key, value).await,
           // ...
       };
       handler.write_value(response).await;
   });
   ```

**Deliverable**: Simple commands working with shared state, all tests passing

---

### Phase 3: Blocking Operations (Week 3)

**Goal**: Implement correct BLPOP with FIFO queue and race-free coordination

#### Tasks:
1. **Implement Race-Free BLPOP** (8 hours)
   ```rust
   async fn handle_blpop(
       state: &SharedState,
       keys: Vec<String>,
       timeout: Duration
   ) -> Result<Value> {
       let (tx, rx) = oneshot::channel();

       // Critical section: check + register atomically
       {
           let mut db = state.store.lock_for_blpop().await; // Special method

           // Try immediate pop
           for key in &keys {
               if let Some(value) = db.lpop(key, Some(1))? {
                   return Ok(Value::Array(vec![
                       Value::BulkString(key.clone()),
                       Value::BulkString(value[0].clone()),
                   ]));
               }
           }

           // No data, register waiter for all keys
           for key in &keys {
               state.blpop_waiters
                   .entry(key.clone())
                   .or_default()
                   .push_back(tx.clone());
           }
       } // Lock released here

       // Wait with timeout (no lock held)
       match tokio::time::timeout(timeout, rx).await {
           Ok(Ok(value)) => Ok(value),
           _ => Ok(Value::NullArray),
       }
   }
   ```

2. **Update LPUSH to Wake Waiters** (4 hours)
   ```rust
   async fn handle_lpush(state: &SharedState, key: String, values: Vec<String>) -> Result<Value> {
       // Perform write
       let len = state.store.lpush(key.clone(), values).await?;

       // Wake waiters with lazy cleanup
       if let Some(mut queue) = state.blpop_waiters.get_mut(&key) {
           while let Some(waiter) = queue.pop_front() {
               // Try to pop value for waiter
               if let Some(value) = state.store.lpop(&key, Some(1)).await? {
                   // Try to send (might fail if waiter timed out)
                   if waiter.send(Ok(Value::Array(vec![
                       Value::BulkString(key.clone()),
                       Value::BulkString(value[0].clone()),
                   ]))).is_ok() {
                       break; // Successfully notified one waiter
                   }
                   // If send failed, waiter timed out, try next one
               } else {
                   break; // No more values to give out
               }
           }
       }

       Ok(Value::Integer(len as i64))
   }
   ```

3. **Implement XREAD BLOCK** (4 hours)
   - Similar pattern to BLPOP
   - Per-stream waiters
   - Notify on XADD

**Deliverable**: Blocking operations working correctly with no race conditions

---

### Phase 4: Transactions & Pub/Sub (Week 4)

**Goal**: Handle stateful features correctly

#### Tasks:
1. **Move Transaction State to Client** (4 hours)
   ```rust
   struct ClientHandler {
       handler: RespHandler,
       state: SharedState,
       client_id: u64,
       transaction_queue: Option<Vec<RedisCommand>>, // Client-local!
   }

   async fn handle_multi(&mut self) -> Result<Value> {
       if self.transaction_queue.is_some() {
           return Err("MULTI calls cannot be nested");
       }
       self.transaction_queue = Some(Vec::new());
       Ok(Value::SimpleString("OK".to_string()))
   }

   async fn handle_exec(&mut self) -> Result<Value> {
       let commands = self.transaction_queue.take()
           .ok_or("EXEC without MULTI")?;

       // Execute all commands atomically by holding write lock
       let mut results = Vec::new();
       for cmd in commands {
           let result = execute_command(&self.state, cmd).await;
           results.push(result);
       }

       Ok(Value::Array(results))
   }
   ```

2. **Implement Pub/Sub with DashMap** (6 hours)
   ```rust
   async fn handle_subscribe(
       state: &SharedState,
       client_id: u64,
       channels: Vec<String>,
       msg_tx: mpsc::Sender<Value>
   ) -> Result<()> {
       for channel in channels {
           state.pub_sub
               .entry(channel.clone())
               .or_default()
               .insert(client_id, msg_tx.clone());

           // Send subscription confirmation
           msg_tx.send(Value::Array(vec![
               Value::BulkString("subscribe".to_string()),
               Value::BulkString(channel),
               Value::Integer(1), // subscription count
           ])).await?;
       }
       Ok(())
   }

   async fn handle_publish(
       state: &SharedState,
       channel: String,
       message: String
   ) -> Result<Value> {
       let mut count = 0;
       if let Some(subscribers) = state.pub_sub.get(&channel) {
           let msg = Value::Array(vec![
               Value::BulkString("message".to_string()),
               Value::BulkString(channel),
               Value::BulkString(message),
           ]);

           for sub in subscribers.iter() {
               let _ = sub.value().send(msg.clone()).await;
               count += 1;
           }
       }
       Ok(Value::Integer(count))
   }
   ```

**Deliverable**: Transactions and Pub/Sub working correctly

---

### Phase 5: Performance Validation & Optimization (Week 5)

**Goal**: Confirm performance improvements and optimize bottlenecks

#### Tasks:
1. **Run Full Benchmark Suite** (2 hours)
   - Compare against baseline from Phase 1
   - Test with memtier_benchmark: `memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1`
   - Expected: 3-5x throughput improvement for read-heavy workload

2. **Profile with tokio-console** (4 hours)
   - Identify lock contention hotspots
   - Measure task scheduling efficiency
   - Check for any blocking operations

3. **Optimize Lock Scope** (4 hours)
   - Minimize critical sections
   - Ensure no locks held across `.await` points
   - Consider `parking_lot::RwLock` if needed

4. **Load Testing** (4 hours)
   - Sustained load test (30+ minutes)
   - Memory leak detection
   - CPU utilization analysis

**Deliverable**: Confirmed 3x+ performance improvement, no regressions

---

## Success Criteria

### Correctness (Must Pass)
- ✅ All existing tests pass
- ✅ No deadlocks in 10-minute stress test (200 clients, mixed operations)
- ✅ Transaction isolation verified (concurrent MULTI/EXEC tests)
- ✅ BLPOP fairness verified (FIFO ordering)
- ✅ Pub/Sub 100% message delivery

### Performance (Must Achieve)
- 🎯 **Throughput**: 3x improvement for 90/10 read/write workload
  - Current: ~68k ops/sec
  - Target: >200k ops/sec
- 🎯 **Latency**: 2x reduction in average latency
  - Current: ~2.91ms
  - Target: <1.5ms
- 🎯 **p99 Latency**: Better than current
  - Current: 3.23ms
  - Target: <2ms
- 🎯 **CPU Utilization**: Multi-core scaling
  - Current: 1 core maxed out
  - Target: Distributed across all cores

---

## Risk Mitigation

### High Risk Items

1. **Deadlocks**
   - **Mitigation**: Strict lock ordering documented and enforced
   - **Detection**: Timeout tests, tokio-console monitoring
   - **Prevention**: Code review checklist

2. **Race Conditions in BLPOP**
   - **Mitigation**: Check-register-recheck pattern under lock
   - **Testing**: Specific race condition tests with precise timing
   - **Validation**: Property-based testing with `proptest`

3. **Performance Regression for Writes**
   - **Mitigation**: Benchmark write-heavy workloads
   - **Threshold**: No more than 10% regression acceptable
   - **Fallback**: Can keep Actor model for comparison

### Medium Risk Items

1. **Lock Contention**
   - **Detection**: Profile with tokio-console
   - **Solution**: Consider parking_lot::RwLock or sharding if needed
   - **Decision Point**: If contention >5% of execution time

2. **Memory Leaks in Waitlists**
   - **Detection**: Long-running load test with memory monitoring
   - **Solution**: Periodic cleanup or Weak pointers
   - **Testing**: Timeout tests with many waiters

---

## Testing Strategy

### Unit Tests
- Lock acquisition patterns
- BLPOP lazy cleanup
- Transaction state management
- Pub/Sub subscription lifecycle

### Integration Tests
- Concurrent operations
- Mixed read/write workloads
- Blocking operations with timeouts
- Multi-key commands

### Stress Tests
- 200 clients, 10-minute duration
- Random command mix
- Check for deadlocks, panics, memory leaks

### Performance Tests
- Benchmark suite from Phase 1
- Comparison against baseline
- Regression detection

---

## Decision Log

| Decision | Rationale | Status |
|----------|-----------|--------|
| Use single RwLock initially | Workload is 90% reads, simpler to implement | ✅ Approved |
| Abstract storage behind trait | Easy to swap to DashMap later if needed | ✅ Approved |
| FIFO queue for BLPOP | Correct semantics, fair wakeup, no thundering herd | ✅ Approved |
| Sequence writes after lock | Simple, correct, matches Redis | ✅ Approved |
| Client-local transactions | Zero contention, simple cleanup | ✅ Approved |
| DashMap for Pub/Sub | Lock-free reads, fine-grained locking | ✅ Approved |
| Skip sharding initially | YAGNI - add only if benchmarks prove it's needed | ✅ Approved |

---

## Timeline Summary

| Week | Phase | Focus | Risk |
|------|-------|-------|------|
| 1 | Foundation | Abstraction + baseline | LOW |
| 2 | Core Migration | Simple commands | MEDIUM |
| 3 | Blocking Ops | BLPOP/XREAD | HIGH |
| 4 | Stateful Features | Transactions + Pub/Sub | MEDIUM |
| 5 | Validation | Performance + optimization | LOW |

**Total**: 5 weeks from start to validated implementation

---

## Next Steps

1. **Review this plan** - Confirm approach makes sense
2. **Start Phase 1** - Create abstraction layer and benchmark
3. **Iterate based on data** - Let benchmarks guide optimization decisions

**Ready to begin implementation!**
