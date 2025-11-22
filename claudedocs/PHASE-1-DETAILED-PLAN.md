# Phase 1: Foundation - Detailed Implementation Plan
## Gemini-Reviewed & Refined

**Duration**: Week 1 (16-20 hours)
**Goal**: Trait abstraction + baseline benchmarks to prove shared state is faster

---

## Critical Insights from Gemini Review

### 1. ✅ Use `&self` for ALL Trait Methods
**Problem**: Original plan had `&mut self` for writes
**Why Wrong**: With `Arc<RwLock<Db>>`, we have interior mutability - `&mut self` prevents sharing!
**Correct**: All methods take `&self`, interior mutability handled by RwLock

### 2. ✅ Create Baseline First (ActorStore)
**Insight**: Must benchmark against current Actor model to prove improvement
**Implementation**: Wrap existing Engine in trait, compare apples-to-apples
**Impact**: Data-driven decisions instead of assumptions

### 3. ✅ Reduce Scope for Week 1
**Original**: All data types (String, List, SortedSet, Stream)
**Refined**: Core types only (String, List, Hash) - defer complex types
**Why**: Avoid trait rigidity, faster to validate architecture

### 4. ✅ Proper Error Handling
**Original**: `Result<T, String>` or panic
**Refined**: Custom `StoreError` enum
**Why**: Structured errors, better error handling

### 5. ✅ Use `Bytes` Consistently
**Original**: Mix of `String` and `Bytes`
**Refined**: `Bytes` for values, `&[u8]` for keys (more Redis-like)
**Why**: Byte arrays are fundamental Redis type

---

## Week 1 Task Breakdown

### Task 1: Define Core Storage Trait (3 hours)

**File**: `src/storage.rs`

```rust
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

/// Error types for storage operations
#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
    WrongType,
    KeyNotFound,
    InvalidArguments(String),
    ParseError(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StoreError::WrongType => write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value"),
            StoreError::KeyNotFound => write!(f, "Key not found"),
            StoreError::InvalidArguments(msg) => write!(f, "ERR {}", msg),
            StoreError::ParseError(msg) => write!(f, "ERR {}", msg),
        }
    }
}

impl std::error::Error for StoreError {}

/// Core abstraction for key-value storage
///
/// All methods take `&self` because implementations use interior mutability
/// (Arc<RwLock<T>>) to enable concurrent access.
#[async_trait]
pub trait KeyValueStore: Send + Sync {
    // === String Operations ===

    /// Get value for key. Returns None if key doesn't exist.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, StoreError>;

    /// Set key to value with optional TTL in milliseconds
    async fn set(&self, key: String, value: Bytes, px: Option<u64>) -> Result<(), StoreError>;

    /// Increment integer value at key (atomic read-modify-write)
    async fn incr(&self, key: &str) -> Result<i64, StoreError>;

    /// Delete keys. Returns number of keys deleted.
    async fn del(&self, keys: &[&str]) -> Result<usize, StoreError>;

    /// Check if keys exist. Returns count of existing keys.
    async fn exists(&self, keys: &[&str]) -> Result<usize, StoreError>;

    /// Get TTL in milliseconds. Returns -2 if key doesn't exist, -1 if no TTL.
    async fn pttl(&self, key: &str) -> Result<i64, StoreError>;

    // === List Operations ===

    /// Push values to left of list. Returns new length.
    async fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError>;

    /// Pop count values from left of list. Returns None if list is empty.
    async fn lpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError>;

    /// Push values to right of list. Returns new length.
    async fn rpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError>;

    /// Get list length
    async fn llen(&self, key: &str) -> Result<usize, StoreError>;

    /// Get range of elements from list
    async fn lrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<Bytes>, StoreError>;

    // === Hash Operations ===

    /// Set field in hash to value
    async fn hset(&self, key: String, field: String, value: Bytes) -> Result<bool, StoreError>;

    /// Get field from hash
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, StoreError>;

    /// Get all fields and values from hash
    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, StoreError>;

    /// Delete fields from hash. Returns number deleted.
    async fn hdel(&self, key: &str, fields: &[&str]) -> Result<usize, StoreError>;

    // === Utility Operations ===

    /// Get all keys matching pattern (simple glob-style)
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, StoreError>;

    /// Get type of key ("string", "list", "hash", "none")
    async fn key_type(&self, key: &str) -> Result<String, StoreError>;
}
```

**Deliverable**: Trait compiles, well-documented

---

### Task 2: Create ActorStore Baseline (3 hours)

**File**: `src/storage.rs`

```rust
use tokio::sync::{mpsc, oneshot};
use crate::engine::{Engine, CommandRequest};
use crate::command::RedisCommand;
use crate::resp::Value;

/// Wrapper around existing Actor-based Engine that implements KeyValueStore trait
///
/// This provides a performance baseline to compare against SingleLockStore.
pub struct ActorStore {
    tx: mpsc::Sender<CommandRequest>,
}

impl ActorStore {
    pub fn new(tx: mpsc::Sender<CommandRequest>) -> Self {
        Self { tx }
    }

    async fn execute_command(&self, command: RedisCommand) -> Result<Value, StoreError> {
        let (resp_tx, resp_rx) = oneshot::channel();

        let req = CommandRequest {
            client_id: 0, // Benchmark client
            command,
            response_tx: resp_tx,
            replica_tx: None,
            pub_sub_tx: None,
        };

        self.tx.send(req).await
            .map_err(|_| StoreError::InvalidArguments("Engine channel closed".to_string()))?;

        resp_rx.await
            .map_err(|_| StoreError::InvalidArguments("Response channel closed".to_string()))?
            .map_err(|e| StoreError::InvalidArguments(e.to_string()))
    }
}

#[async_trait]
impl KeyValueStore for ActorStore {
    async fn get(&self, key: &str) -> Result<Option<Bytes>, StoreError> {
        let cmd = RedisCommand::Get { key: key.to_string() };
        let value = self.execute_command(cmd).await?;

        match value {
            Value::BulkString(s) => Ok(Some(Bytes::from(s))),
            Value::Null => Ok(None),
            _ => Err(StoreError::WrongType),
        }
    }

    async fn set(&self, key: String, value: Bytes, px: Option<u64>) -> Result<(), StoreError> {
        let cmd = RedisCommand::Set {
            key,
            value: String::from_utf8_lossy(&value).to_string(),
            px,
        };

        let result = self.execute_command(cmd).await?;
        match result {
            Value::SimpleString(_) => Ok(()),
            Value::Error(e) => Err(StoreError::InvalidArguments(e)),
            _ => Err(StoreError::WrongType),
        }
    }

    // ... implement all other trait methods similarly
}
```

**Deliverable**: ActorStore wraps existing Engine, all methods implemented

---

### Task 3: Implement SingleLockStore (4 hours)

**File**: `src/storage.rs`

```rust
use tokio::sync::RwLock;
use std::sync::Arc;
use crate::db::Db;

/// Shared-state implementation using a single RwLock
///
/// This is the new architecture being tested. Multiple concurrent readers
/// can access the database simultaneously, while writers get exclusive access.
pub struct SingleLockStore {
    db: Arc<RwLock<Db>>,
}

impl SingleLockStore {
    pub fn new() -> Self {
        Self {
            db: Arc::new(RwLock::new(Db::new())),
        }
    }
}

#[async_trait]
impl KeyValueStore for SingleLockStore {
    async fn get(&self, key: &str) -> Result<Option<Bytes>, StoreError> {
        let db = self.db.read().await;  // Concurrent read lock
        Ok(db.get(key))
    }

    async fn set(&self, key: String, value: Bytes, px: Option<u64>) -> Result<(), StoreError> {
        let mut db = self.db.write().await;  // Exclusive write lock
        db.set(key, value, px);
        Ok(())
    }

    async fn incr(&self, key: &str) -> Result<i64, StoreError> {
        // CRITICAL: Hold write lock for entire read-modify-write operation
        let mut db = self.db.write().await;

        let current = match db.get(key) {
            Some(bytes) => {
                std::str::from_utf8(&bytes)
                    .map_err(|_| StoreError::WrongType)?
                    .parse::<i64>()
                    .map_err(|_| StoreError::WrongType)?
            }
            None => 0,
        };

        let new_value = current + 1;
        db.set(key.to_string(), Bytes::from(new_value.to_string()), None);

        Ok(new_value)
    }

    async fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError> {
        let mut db = self.db.write().await;
        db.lpush(key, values)
            .map_err(|e| StoreError::InvalidArguments(e))
    }

    async fn lpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError> {
        let mut db = self.db.write().await;
        db.lpop(key, count)
            .map_err(|e| StoreError::InvalidArguments(e))
    }

    // ... implement all other trait methods
}
```

**Deliverable**: SingleLockStore implements all trait methods correctly

---

### Task 4: Create Comparative Benchmark Suite (6 hours)

**File**: `benches/storage_benchmark.rs`

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use tokio::runtime::Runtime;
use bytes::Bytes;
use std::sync::Arc;
use mikkadb_rust::storage::{KeyValueStore, ActorStore, SingleLockStore};

/// Benchmark helper: run workload against any KeyValueStore implementation
async fn run_workload(
    store: Arc<dyn KeyValueStore>,
    num_tasks: usize,
    ops_per_task: usize,
    read_ratio: f64, // 0.9 = 90% reads, 10% writes
) {
    // Pre-populate with data
    for i in 0..10000 {
        store.set(
            format!("key{}", i),
            Bytes::from(format!("value{}", i)),
            None
        ).await.unwrap();
    }

    let handles: Vec<_> = (0..num_tasks).map(|_| {
        let store = store.clone();
        tokio::spawn(async move {
            for _ in 0..ops_per_task {
                let op = rand::random::<f64>();
                if op < read_ratio {
                    // READ operation
                    let key = format!("key{}", rand::random::<u32>() % 10000);
                    let _ = store.get(&key).await;
                } else {
                    // WRITE operation
                    let key = format!("key{}", rand::random::<u32>() % 10000);
                    let _ = store.set(key, Bytes::from("value"), None).await;
                }
            }
        })
    }).collect();

    for h in handles {
        h.await.unwrap();
    }
}

fn bench_actor_vs_single_lock(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Create both implementations
    let (tx, rx) = mpsc::channel(1000);
    let engine = Engine::new(config, rx);
    tokio::spawn(async move { engine.run().await });

    let actor_store: Arc<dyn KeyValueStore> = Arc::new(ActorStore::new(tx));
    let single_lock_store: Arc<dyn KeyValueStore> = Arc::new(SingleLockStore::new());

    let mut group = c.benchmark_group("read_heavy_90_10");

    for concurrency in [8, 32, 128] {
        // Benchmark ActorStore
        group.bench_with_input(
            BenchmarkId::new("ActorStore", concurrency),
            &concurrency,
            |b, &conc| {
                b.to_async(&rt).iter(|| {
                    run_workload(actor_store.clone(), conc, 100, 0.9)
                });
            }
        );

        // Benchmark SingleLockStore
        group.bench_with_input(
            BenchmarkId::new("SingleLockStore", concurrency),
            &conc,
            |b, &conc| {
                b.to_async(&rt).iter(|| {
                    run_workload(single_lock_store.clone(), conc, 100, 0.9)
                });
            }
        );
    }

    group.finish();
}

criterion_group!(benches, bench_actor_vs_single_lock);
criterion_main!(benches);
```

**Run with**: `cargo bench --bench storage_benchmark`

**Expected Output**:
```
read_heavy_90_10/ActorStore/8     time: [150ms ... ]
read_heavy_90_10/SingleLockStore/8 time: [50ms ... ]   <- 3x faster!

read_heavy_90_10/ActorStore/128    time: [200ms ... ]
read_heavy_90_10/SingleLockStore/128 time: [60ms ... ]  <- 3.3x faster!
```

**Deliverable**: Benchmark proves SingleLockStore is 3x+ faster for read-heavy workload

---

### Task 5: Integration Tests (4 hours)

**File**: `tests/storage_tests.rs`

```rust
use mikkadb_rust::storage::{KeyValueStore, SingleLockStore};
use bytes::Bytes;
use std::sync::Arc;

#[tokio::test]
async fn test_concurrent_reads() {
    let store: Arc<dyn KeyValueStore> = Arc::new(SingleLockStore::new());

    // Set initial value
    store.set("key1".to_string(), Bytes::from("value1"), None).await.unwrap();

    // Spawn 100 concurrent readers
    let handles: Vec<_> = (0..100).map(|_| {
        let store = store.clone();
        tokio::spawn(async move {
            let result = store.get("key1").await.unwrap();
            assert_eq!(result, Some(Bytes::from("value1")));
        })
    }).collect();

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_writes() {
    let store: Arc<dyn KeyValueStore> = Arc::new(SingleLockStore::new());

    // Spawn 100 concurrent writers
    let handles: Vec<_> = (0..100).map(|i| {
        let store = store.clone();
        tokio::spawn(async move {
            store.set(
                format!("key{}", i),
                Bytes::from(format!("value{}", i)),
                None
            ).await.unwrap();
        })
    }).collect();

    for h in handles {
        h.await.unwrap();
    }

    // Verify all writes succeeded
    for i in 0..100 {
        let result = store.get(&format!("key{}", i)).await.unwrap();
        assert_eq!(result, Some(Bytes::from(format!("value{}", i))));
    }
}

#[tokio::test]
async fn test_incr_atomicity() {
    let store: Arc<dyn KeyValueStore> = Arc::new(SingleLockStore::new());

    // Initial value
    store.set("counter".to_string(), Bytes::from("0"), None).await.unwrap();

    // 100 concurrent increments
    let handles: Vec<_> = (0..100).map(|_| {
        let store = store.clone();
        tokio::spawn(async move {
            store.incr("counter").await.unwrap();
        })
    }).collect();

    for h in handles {
        h.await.unwrap();
    }

    // Final value should be exactly 100
    let result = store.get("counter").await.unwrap().unwrap();
    let value = std::str::from_utf8(&result).unwrap().parse::<i64>().unwrap();
    assert_eq!(value, 100);
}

#[tokio::test]
async fn test_error_handling() {
    let store: Arc<dyn KeyValueStore> = Arc::new(SingleLockStore::new());

    // Set a string value
    store.set("mykey".to_string(), Bytes::from("not_a_number"), None).await.unwrap();

    // Try to INCR it - should fail with WrongType
    let result = store.incr("mykey").await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), StoreError::WrongType);
}
```

**Deliverable**: All tests pass, correctness verified under concurrency

---

## Success Criteria

### Must Pass
- ✅ All trait methods compile and run
- ✅ ActorStore wraps existing Engine correctly
- ✅ SingleLockStore implements all operations correctly
- ✅ All integration tests pass (concurrent reads, writes, atomicity)
- ✅ Benchmark shows SingleLockStore is 3x+ faster than ActorStore for 90/10 workload

### Nice to Have
- 📊 Benchmark with varying concurrency levels (8, 32, 128, 512)
- 📊 Benchmark with varying read/write ratios (100/0, 90/10, 50/50)
- 📊 Performance graphs showing scaling characteristics

---

## Time Allocation

| Task | Hours | Priority |
|------|-------|----------|
| Task 1: Trait + Error Type | 3 | CRITICAL |
| Task 2: ActorStore Baseline | 3 | CRITICAL |
| Task 3: SingleLockStore | 4 | CRITICAL |
| Task 4: Benchmark Suite | 6 | CRITICAL |
| Task 5: Integration Tests | 4 | HIGH |
| **Total** | **20** | |

---

## Deliverables Checklist

Week 1 Complete When:
- [ ] `src/storage.rs` with trait, ActorStore, SingleLockStore
- [ ] `benches/storage_benchmark.rs` with comparative benchmarks
- [ ] `tests/storage_tests.rs` with concurrency tests
- [ ] Benchmark results showing 3x+ improvement saved in `results/phase1_baseline.txt`
- [ ] All tests passing: `cargo test --all`
- [ ] Documentation: Brief summary of findings in `claudedocs/phase1_results.md`

---

## Next Steps After Week 1

Based on benchmark results:

**If SingleLockStore is 3x+ faster**: ✅ Proceed to Phase 2 (migrate simple commands)

**If improvements are < 2x**: ⚠️ Investigate:
- Lock contention with tokio-console
- Consider parking_lot::RwLock
- Profile with flamegraph

**If ActorStore is faster**: 🔴 Re-evaluate architecture (unlikely but possible)

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Trait too rigid | Start with core types only, add more in Phase 2 |
| Benchmark inaccurate | Use both criterion and manual timing |
| Integration issues | Keep ActorStore as working baseline |
| Time overrun | Core types (String, List) are minimum viable |

**Ready to start implementation!**
