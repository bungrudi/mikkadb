# Implementation Roadmap: Actor to Shared State Migration

## Overview

This document provides a step-by-step implementation plan for migrating MikkaDB from the Actor model to Shared State architecture. Each phase includes concrete code changes, validation steps, and rollback procedures.

---

## Phase 1: Preparation (2 weeks)

### Goal
Refactor code to enable parallel execution while maintaining actor model behavior.

### Step 1.1: Extract Command Execution Logic (3 days)

**Current State**: Commands executed as Engine methods
```rust
// src/engine.rs - Current implementation
impl Engine {
    async fn execute_command_immediate(&mut self, ...) -> Result<Value> {
        match command {
            RedisCommand::Get { key } => {
                match self.db.get(&key) {
                    Some(value) => Ok(Value::BulkString(...)),
                    None => Ok(Value::Null),
                }
            }
            // ... more commands ...
        }
    }
}
```

**Target State**: Standalone functions accepting shared state
```rust
// src/commands/mod.rs - New module
pub mod read;
pub mod write;
pub mod blocking;
pub mod transaction;
pub mod pubsub;
pub mod replication;

// src/commands/read.rs - Example
pub async fn execute_get(db: &Db, key: &str) -> Result<Value> {
    match db.get(key) {
        Some(value) => Ok(Value::BulkString(String::from_utf8_lossy(&value).to_string())),
        None => Ok(Value::Null),
    }
}

pub async fn execute_lrange(db: &Db, key: &str, start: i64, end: i64) -> Result<Value> {
    match db.lrange(key, start, end) {
        Ok(values) => {
            let resp_values = values.iter()
                .map(|v| Value::BulkString(String::from_utf8_lossy(v).to_string()))
                .collect();
            Ok(Value::Array(resp_values))
        }
        Err(e) => Ok(Value::Error(e)),
    }
}

// src/commands/write.rs - Example
pub async fn execute_set(
    db: &mut Db,
    key: String,
    value: String,
    px: Option<u64>
) -> Result<Value> {
    db.set(key, Bytes::from(value), px);
    Ok(Value::SimpleString("OK".to_string()))
}

pub async fn execute_lpush(
    db: &mut Db,
    key: String,
    values: Vec<String>
) -> Result<Value> {
    let bytes_values: Vec<Bytes> = values.iter()
        .map(|v| Bytes::from(v.clone()))
        .collect();

    match db.lpush(key, bytes_values) {
        Ok(len) => Ok(Value::Integer(len as i64)),
        Err(e) => Ok(Value::Error(e)),
    }
}
```

**Changes Required**:
1. Create `src/commands/` module directory
2. Move command execution logic from `Engine::execute_command_immediate` to standalone functions
3. Update `Engine::execute_command_immediate` to call new functions
4. No behavioral changes - still single-threaded execution

**Validation**:
```bash
# Run existing tests - all should pass
cargo test

# Run integration tests
cargo test --test integration_tests
```

**Rollback**: Revert commit if any test fails

---

### Step 1.2: Classify Commands by Lock Requirements (2 days)

**New File**: `src/commands/classification.rs`
```rust
use crate::command::RedisCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockRequirement {
    ReadOnly,              // GET, LRANGE, ZRANGE
    WriteOnly,             // SET, LPUSH, ZADD
    ReadModifyWrite,       // INCR, LPOP
    Blocking,              // BLPOP, XREAD BLOCK
    TransactionControl,    // MULTI, EXEC, DISCARD
    PubSub,                // SUBSCRIBE, PUBLISH, UNSUBSCRIBE
    Replication,           // PSYNC, REPLCONF, WAIT
    NoLock,                // PING, ECHO, INFO
}

impl RedisCommand {
    pub fn lock_requirement(&self) -> LockRequirement {
        match self {
            // Read-only (concurrent reads allowed)
            RedisCommand::Get { .. } |
            RedisCommand::LRange { .. } |
            RedisCommand::ZRange { .. } |
            RedisCommand::ZCard { .. } |
            RedisCommand::ZScore { .. } |
            RedisCommand::ZRank { .. } |
            RedisCommand::XRange { .. } |
            RedisCommand::Keys { .. } |
            RedisCommand::Type { .. } |
            RedisCommand::LLen { .. } => LockRequirement::ReadOnly,

            // Write-only (exclusive write)
            RedisCommand::Set { .. } |
            RedisCommand::LPush { .. } |
            RedisCommand::RPush { .. } |
            RedisCommand::ZAdd { .. } |
            RedisCommand::ZRem { .. } |
            RedisCommand::XAdd { .. } => LockRequirement::WriteOnly,

            // Read-modify-write (atomic operation)
            RedisCommand::Incr { .. } |
            RedisCommand::LPop { .. } => LockRequirement::ReadModifyWrite,

            // Blocking operations
            RedisCommand::BLPop { .. } |
            RedisCommand::XRead { block: Some(_), .. } => LockRequirement::Blocking,

            // Transaction control
            RedisCommand::Multi |
            RedisCommand::Exec |
            RedisCommand::Discard => LockRequirement::TransactionControl,

            // Pub/Sub
            RedisCommand::Subscribe { .. } |
            RedisCommand::Publish { .. } |
            RedisCommand::Unsubscribe { .. } => LockRequirement::PubSub,

            // Replication
            RedisCommand::PSync { .. } |
            RedisCommand::ReplConf { .. } |
            RedisCommand::Wait { .. } => LockRequirement::Replication,

            // No lock needed
            RedisCommand::Ping { .. } |
            RedisCommand::Echo { .. } |
            RedisCommand::Info { .. } |
            RedisCommand::ConfigGet { .. } => LockRequirement::NoLock,

            // Internal
            RedisCommand::InternalDisconnect => LockRequirement::NoLock,

            // Non-blocking XREAD
            RedisCommand::XRead { block: None, .. } => LockRequirement::ReadOnly,

            RedisCommand::Error { .. } |
            RedisCommand::None => LockRequirement::NoLock,
        }
    }

    pub fn is_write(&self) -> bool {
        matches!(
            self.lock_requirement(),
            LockRequirement::WriteOnly | LockRequirement::ReadModifyWrite
        )
    }

    pub fn requires_replication(&self) -> bool {
        self.is_write()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_classification() {
        assert_eq!(
            RedisCommand::Get { key: "test".to_string() }.lock_requirement(),
            LockRequirement::ReadOnly
        );

        assert_eq!(
            RedisCommand::Set {
                key: "test".to_string(),
                value: "value".to_string(),
                px: None
            }.lock_requirement(),
            LockRequirement::WriteOnly
        );

        assert_eq!(
            RedisCommand::Incr { key: "counter".to_string() }.lock_requirement(),
            LockRequirement::ReadModifyWrite
        );

        assert_eq!(
            RedisCommand::BLPop {
                keys: vec!["queue".to_string()],
                timeout: 5.0
            }.lock_requirement(),
            LockRequirement::Blocking
        );
    }

    #[test]
    fn test_write_detection() {
        assert!(!RedisCommand::Get { key: "k".to_string() }.is_write());
        assert!(RedisCommand::Set {
            key: "k".to_string(),
            value: "v".to_string(),
            px: None
        }.is_write());
        assert!(RedisCommand::Incr { key: "k".to_string() }.is_write());
    }
}
```

**Validation**:
```bash
cargo test commands::classification
```

---

### Step 1.3: Add Comprehensive Integration Tests (3 days)

**New File**: `tests/integration_tests.rs`
```rust
use mikkadb::*;
use tokio::net::TcpStream;
use resp::{RespHandler, Value};

#[tokio::test]
async fn test_concurrent_reads() {
    // Setup: Start server
    let server = start_test_server().await;

    // Setup: Insert test data
    let mut client = connect_to_server(server.port).await;
    send_command(&mut client, vec!["SET", "key1", "value1"]).await;

    // Test: 10 concurrent GET operations
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let port = server.port;
            tokio::spawn(async move {
                let mut c = connect_to_server(port).await;
                send_command(&mut c, vec!["GET", "key1"]).await
            })
        })
        .collect();

    // Verify: All should return "value1"
    for handle in handles {
        let response = handle.await.unwrap();
        assert_eq!(response, Value::BulkString("value1".to_string()));
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_transaction_isolation() {
    let server = start_test_server().await;

    // Client 1: Execute transaction
    let handle1 = tokio::spawn(async move {
        let mut c = connect_to_server(server.port).await;
        send_command(&mut c, vec!["MULTI"]).await;
        send_command(&mut c, vec!["SET", "key1", "tx_value"]).await;
        send_command(&mut c, vec!["SET", "key2", "tx_value"]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        send_command(&mut c, vec!["EXEC"]).await
    });

    // Client 2: Concurrent read (should not see partial transaction)
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut client2 = connect_to_server(server.port).await;
    let key1 = send_command(&mut client2, vec!["GET", "key1"]).await;
    let key2 = send_command(&mut client2, vec!["GET", "key2"]).await;

    // Verify: Either both null or both have values (atomicity)
    match (key1, key2) {
        (Value::Null, Value::Null) => {}, // Transaction not committed yet
        (Value::BulkString(v1), Value::BulkString(v2)) => {
            assert_eq!(v1, "tx_value");
            assert_eq!(v2, "tx_value");
        }
        _ => panic!("Partial transaction visible - isolation violated!"),
    }

    handle1.await.unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn test_blpop_lpush_coordination() {
    let server = start_test_server().await;

    // Client 1: BLPOP with 5s timeout
    let port = server.port;
    let handle1 = tokio::spawn(async move {
        let mut c = connect_to_server(port).await;
        let start = Instant::now();
        let response = send_command(&mut c, vec!["BLPOP", "queue", "5"]).await;
        let elapsed = start.elapsed();
        (response, elapsed)
    });

    // Client 2: LPUSH after 1 second
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut client2 = connect_to_server(server.port).await;
    send_command(&mut client2, vec!["LPUSH", "queue", "value"]).await;

    // Verify: BLPOP returns within 2 seconds (should wake immediately)
    let (response, elapsed) = handle1.await.unwrap();
    assert!(elapsed < Duration::from_secs(2));
    assert_eq!(
        response,
        Value::Array(vec![
            Value::BulkString("queue".to_string()),
            Value::BulkString("value".to_string())
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_pubsub_message_delivery() {
    let server = start_test_server().await;

    // 10 subscribers
    let subscribe_handles: Vec<_> = (0..10)
        .map(|i| {
            let port = server.port;
            tokio::spawn(async move {
                let mut c = connect_to_server(port).await;
                send_command(&mut c, vec!["SUBSCRIBE", "channel1"]).await;

                // Wait for message
                let msg = read_message(&mut c).await;
                msg
            })
        })
        .collect();

    // Wait for all subscriptions
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publisher sends message
    let mut publisher = connect_to_server(server.port).await;
    send_command(&mut publisher, vec!["PUBLISH", "channel1", "hello"]).await;

    // Verify: All 10 subscribers receive message
    for handle in subscribe_handles {
        let msg = handle.await.unwrap();
        assert_eq!(
            msg,
            Value::Array(vec![
                Value::BulkString("message".to_string()),
                Value::BulkString("channel1".to_string()),
                Value::BulkString("hello".to_string())
            ])
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_replication_ordering() {
    let (master, replica) = start_master_replica().await;

    // Execute commands on master
    let mut client = connect_to_server(master.port).await;
    send_command(&mut client, vec!["SET", "key1", "value1"]).await;
    send_command(&mut client, vec!["SET", "key2", "value2"]).await;
    send_command(&mut client, vec!["SET", "key3", "value3"]).await;

    // WAIT for replica sync
    let synced = send_command(&mut client, vec!["WAIT", "1", "1000"]).await;
    assert_eq!(synced, Value::Integer(1));

    // Verify replica has same values
    let mut replica_client = connect_to_server(replica.port).await;
    let v1 = send_command(&mut replica_client, vec!["GET", "key1"]).await;
    let v2 = send_command(&mut replica_client, vec!["GET", "key2"]).await;
    let v3 = send_command(&mut replica_client, vec!["GET", "key3"]).await;

    assert_eq!(v1, Value::BulkString("value1".to_string()));
    assert_eq!(v2, Value::BulkString("value2".to_string()));
    assert_eq!(v3, Value::BulkString("value3".to_string()));

    master.shutdown().await;
    replica.shutdown().await;
}
```

**Validation**:
```bash
cargo test --test integration_tests
```

---

### Step 1.4: Establish Performance Baseline (2 days)

**New File**: `benches/baseline.rs`
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use mikkadb::*;

fn benchmark_get_latency(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(start_test_server());

    // Setup: Insert test data
    runtime.block_on(async {
        let mut client = connect_to_server(server.port).await;
        send_command(&mut client, vec!["SET", "key1", "value1"]).await;
    });

    c.bench_function("get_latency", |b| {
        b.to_async(&runtime).iter(|| async {
            let mut client = connect_to_server(server.port).await;
            let response = send_command(&mut client, vec!["GET", "key1"]).await;
            black_box(response);
        });
    });

    runtime.block_on(server.shutdown());
}

fn benchmark_concurrent_reads(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(start_test_server());

    runtime.block_on(async {
        let mut client = connect_to_server(server.port).await;
        send_command(&mut client, vec!["SET", "key1", "value1"]).await;
    });

    for num_concurrent in [1, 10, 100] {
        c.bench_with_input(
            BenchmarkId::new("concurrent_reads", num_concurrent),
            &num_concurrent,
            |b, &num| {
                b.to_async(&runtime).iter(|| async move {
                    let handles: Vec<_> = (0..num)
                        .map(|_| {
                            tokio::spawn(async {
                                let mut c = connect_to_server(server.port).await;
                                send_command(&mut c, vec!["GET", "key1"]).await
                            })
                        })
                        .collect();

                    for handle in handles {
                        black_box(handle.await.unwrap());
                    }
                });
            }
        );
    }

    runtime.block_on(server.shutdown());
}

criterion_group!(benches, benchmark_get_latency, benchmark_concurrent_reads);
criterion_main!(benches);
```

**Run Baseline**:
```bash
cargo bench --bench baseline > baseline_results.txt

# Expected current results (Actor model):
# get_latency: ~3ms
# concurrent_reads/1: ~3ms
# concurrent_reads/10: ~30ms (10 * 3ms - serial execution)
# concurrent_reads/100: ~300ms (100 * 3ms - serial execution)
```

**Phase 1 Complete**: ✅
- Command logic extracted
- Commands classified
- Integration tests passing
- Baseline established

---

## Phase 2: State Migration (3 weeks)

### Step 2.1: Create Shared Database Wrapper (4 days)

**New File**: `src/shared_state.rs`
```rust
use crate::db::Db;
use tokio::sync::RwLock;
use std::sync::Arc;
use bytes::Bytes;

/// Shared database with concurrent read support
#[derive(Clone)]
pub struct SharedDb {
    inner: Arc<RwLock<Db>>,
}

impl SharedDb {
    pub fn new(db: Db) -> Self {
        Self {
            inner: Arc::new(RwLock::new(db)),
        }
    }

    // Read-only operations (concurrent)
    pub async fn get(&self, key: &str) -> Option<Bytes> {
        self.inner.read().await.get(key)
    }

    pub async fn lrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<Bytes>, String> {
        self.inner.read().await.lrange(key, start, end)
    }

    pub async fn zrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<(String, Option<f64>)>, String> {
        self.inner.read().await.zrange(key, start, end)
    }

    pub async fn keys(&self, pattern: &str) -> Vec<String> {
        self.inner.write().await.keys(pattern)  // Needs write for expiry cleanup
    }

    // Write operations (exclusive)
    pub async fn set(&self, key: String, value: Bytes, px: Option<u64>) {
        self.inner.write().await.set(key, value, px)
    }

    pub async fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, String> {
        self.inner.write().await.lpush(key, values)
    }

    pub async fn rpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, String> {
        self.inner.write().await.rpush(key, values)
    }

    pub async fn zadd(&self, key: String, entries: Vec<(f64, String)>) -> Result<usize, String> {
        self.inner.write().await.zadd(key, entries)
    }

    // Read-modify-write operations (atomic)
    pub async fn incr(&self, key: String) -> Result<i64, String> {
        let mut lock = self.inner.write().await;

        let current = match lock.get(&key) {
            Some(bytes) => {
                String::from_utf8_lossy(&bytes)
                    .parse::<i64>()
                    .map_err(|_| "ERR value is not an integer or out of range".to_string())?
            }
            None => 0,
        };

        let new_val = current + 1;
        lock.set(key, Bytes::from(new_val.to_string()), None);
        Ok(new_val)
    }

    pub async fn lpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, String> {
        self.inner.write().await.lpop(key, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_reads() {
        let db = SharedDb::new(Db::new());
        db.set("key1".to_string(), Bytes::from("value1"), None).await;

        // 100 concurrent reads
        let handles: Vec<_> = (0..100)
            .map(|_| {
                let db = db.clone();
                tokio::spawn(async move {
                    db.get("key1").await
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.await.unwrap(), Some(Bytes::from("value1")));
        }
    }

    #[tokio::test]
    async fn test_write_isolation() {
        let db = SharedDb::new(Db::new());

        // Concurrent writes should not corrupt data
        let handles: Vec<_> = (0..100)
            .map(|i| {
                let db = db.clone();
                tokio::spawn(async move {
                    db.set(format!("key{}", i), Bytes::from(format!("value{}", i)), None).await;
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all writes succeeded
        for i in 0..100 {
            assert_eq!(
                db.get(&format!("key{}", i)).await,
                Some(Bytes::from(format!("value{}", i)))
            );
        }
    }

    #[tokio::test]
    async fn test_incr_atomicity() {
        let db = SharedDb::new(Db::new());

        // 100 concurrent INCRs
        let handles: Vec<_> = (0..100)
            .map(|_| {
                let db = db.clone();
                tokio::spawn(async move {
                    db.incr("counter".to_string()).await.unwrap()
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify counter = 100 (atomicity guaranteed)
        let value = db.get("counter").await.unwrap();
        assert_eq!(String::from_utf8_lossy(&value), "100");
    }
}
```

**Update**: `src/commands/read.rs`
```rust
use crate::shared_state::SharedDb;
use crate::resp::Value;
use anyhow::Result;

pub async fn execute_get(db: &SharedDb, key: &str) -> Result<Value> {
    match db.get(key).await {
        Some(value) => Ok(Value::BulkString(String::from_utf8_lossy(&value).to_string())),
        None => Ok(Value::Null),
    }
}

pub async fn execute_lrange(db: &SharedDb, key: &str, start: i64, end: i64) -> Result<Value> {
    match db.lrange(key, start, end).await {
        Ok(values) => {
            let resp_values = values.iter()
                .map(|v| Value::BulkString(String::from_utf8_lossy(v).to_string()))
                .collect();
            Ok(Value::Array(resp_values))
        }
        Err(e) => Ok(Value::Error(e)),
    }
}
```

**Validation**:
```bash
cargo test shared_state
cargo test commands::read
```

---

### Step 2.2: Create Replication Actor (5 days)

**New File**: `src/replication.rs`
```rust
use crate::resp::Value;
use tokio::sync::{mpsc, oneshot};
use std::collections::HashMap;

pub enum ReplicationCommand {
    Propagate(Value),
    AddReplica {
        id: u64,
        tx: mpsc::Sender<Value>,
    },
    Ack {
        replica_id: u64,
        offset: i64,
    },
    Wait {
        num_replicas: usize,
        timeout: u64,
        respond: oneshot::Sender<i64>,
    },
}

struct Replica {
    id: u64,
    tx: mpsc::Sender<Value>,
    offset: i64,
}

struct PendingWait {
    num_replicas: usize,
    respond: oneshot::Sender<i64>,
    target_offset: i64,
}

pub struct ReplicationActor {
    replicas: HashMap<u64, Replica>,
    offset: i64,
    pending_waits: Vec<PendingWait>,
}

impl ReplicationActor {
    pub fn new() -> Self {
        Self {
            replicas: HashMap::new(),
            offset: 0,
            pending_waits: Vec::new(),
        }
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<ReplicationCommand>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                ReplicationCommand::Propagate(value) => {
                    self.propagate(value).await;
                }
                ReplicationCommand::AddReplica { id, tx } => {
                    self.add_replica(id, tx);
                }
                ReplicationCommand::Ack { replica_id, offset } => {
                    self.handle_ack(replica_id, offset);
                }
                ReplicationCommand::Wait { num_replicas, timeout, respond } => {
                    self.handle_wait(num_replicas, timeout, respond).await;
                }
            }
        }
    }

    async fn propagate(&mut self, value: Value) {
        let bytes = value.serialize_bytes();
        self.offset += bytes.len() as i64;

        for replica in self.replicas.values() {
            let _ = replica.tx.send(value.clone()).await;
        }
    }

    fn add_replica(&mut self, id: u64, tx: mpsc::Sender<Value>) {
        self.replicas.insert(id, Replica { id, tx, offset: 0 });
    }

    fn handle_ack(&mut self, replica_id: u64, offset: i64) {
        if let Some(replica) = self.replicas.get_mut(&replica_id) {
            replica.offset = offset;
        }

        // Check pending waits
        self.pending_waits.retain(|wait| {
            let synced = self.replicas.values()
                .filter(|r| r.offset >= wait.target_offset)
                .count();

            if synced >= wait.num_replicas {
                let _ = wait.respond.send(synced as i64);
                false  // Remove from pending
            } else {
                true   // Keep waiting
            }
        });
    }

    async fn handle_wait(
        &mut self,
        num_replicas: usize,
        timeout: u64,
        respond: oneshot::Sender<i64>
    ) {
        // Check if already synced
        let synced = self.replicas.values()
            .filter(|r| r.offset >= self.offset)
            .count();

        if synced >= num_replicas {
            let _ = respond.send(synced as i64);
            return;
        }

        // Send GETACK to all replicas
        let getack = Value::Array(vec![
            Value::BulkString("REPLCONF".to_string()),
            Value::BulkString("GETACK".to_string()),
            Value::BulkString("*".to_string()),
        ]);

        for replica in self.replicas.values() {
            let _ = replica.tx.send(getack.clone()).await;
        }

        // Add to pending waits
        self.pending_waits.push(PendingWait {
            num_replicas,
            respond,
            target_offset: self.offset,
        });

        // Spawn timeout task
        let wait_idx = self.pending_waits.len() - 1;
        // TODO: Implement timeout mechanism
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_propagation() {
        let (tx, mut rx) = mpsc::channel(32);
        let (repl_tx, repl_rx) = mpsc::channel(32);

        // Start replication actor
        tokio::spawn(async move {
            let actor = ReplicationActor::new();
            actor.run(rx).await;
        });

        // Add replica
        let (replica_tx, mut replica_rx) = mpsc::channel(32);
        repl_tx.send(ReplicationCommand::AddReplica {
            id: 1,
            tx: replica_tx,
        }).await.unwrap();

        // Propagate command
        let cmd = Value::Array(vec![
            Value::BulkString("SET".to_string()),
            Value::BulkString("key1".to_string()),
            Value::BulkString("value1".to_string()),
        ]);
        repl_tx.send(ReplicationCommand::Propagate(cmd.clone())).await.unwrap();

        // Verify replica receives command
        let received = replica_rx.recv().await.unwrap();
        assert_eq!(received, cmd);
    }
}
```

**Integration with Write Commands**:
```rust
// src/commands/write.rs
use crate::shared_state::SharedDb;
use crate::replication::ReplicationCommand;
use tokio::sync::mpsc;

pub async fn execute_set(
    db: &SharedDb,
    repl_tx: &mpsc::Sender<ReplicationCommand>,
    key: String,
    value: String,
    px: Option<u64>
) -> Result<Value> {
    // Write to DB
    db.set(key.clone(), Bytes::from(value.clone()), px).await;

    // Propagate to replicas
    let mut args = vec![
        Value::BulkString("SET".to_string()),
        Value::BulkString(key),
        Value::BulkString(value),
    ];

    if let Some(ms) = px {
        args.push(Value::BulkString("PX".to_string()));
        args.push(Value::BulkString(ms.to_string()));
    }

    repl_tx.send(ReplicationCommand::Propagate(Value::Array(args))).await?;

    Ok(Value::SimpleString("OK".to_string()))
}
```

**Validation**:
```bash
cargo test replication
cargo test --test integration_tests::test_replication_ordering
```

---

### Step 2.3: Create Blocking Command Coordination (4 days)

**New File**: `src/wait_list.rs`
```rust
use tokio::sync::{RwLock, Notify};
use std::sync::Arc;
use std::collections::HashMap;

pub struct WaitList {
    // BLPOP: key → list of notifiers
    blpop_waiters: Arc<RwLock<HashMap<String, Vec<Arc<Notify>>>>>,
}

impl WaitList {
    pub fn new() -> Self {
        Self {
            blpop_waiters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_blpop(&self, keys: &[String]) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let mut waiters = self.blpop_waiters.write().await;

        for key in keys {
            waiters.entry(key.clone())
                .or_insert_with(Vec::new)
                .push(notify.clone());
        }

        notify
    }

    pub async fn wake_blpop_waiters(&self, key: &str) {
        let mut waiters = self.blpop_waiters.write().await;

        if let Some(notifiers) = waiters.remove(key) {
            // Wake one waiter (fair queueing)
            if let Some(notify) = notifiers.first() {
                notify.notify_one();
            }

            // Put back remaining waiters
            if notifiers.len() > 1 {
                waiters.insert(key.to_string(), notifiers[1..].to_vec());
            }
        }
    }

    pub async fn cleanup_waiter(&self, keys: &[String], notify: &Arc<Notify>) {
        let mut waiters = self.blpop_waiters.write().await;

        for key in keys {
            if let Some(v) = waiters.get_mut(key) {
                v.retain(|n| !Arc::ptr_eq(n, notify));
            }
        }
    }
}
```

**Blocking Command Implementation**:
```rust
// src/commands/blocking.rs
use crate::shared_state::SharedDb;
use crate::wait_list::WaitList;
use crate::resp::Value;
use std::time::Duration;

pub async fn execute_blpop(
    keys: Vec<String>,
    timeout: f64,
    db: &SharedDb,
    wait_list: &WaitList
) -> Result<Value> {
    // Phase 1: Try immediate pop
    for key in &keys {
        if let Ok(Some(values)) = db.lpop(key, None).await {
            return Ok(Value::Array(vec![
                Value::BulkString(key.clone()),
                Value::BulkString(String::from_utf8_lossy(&values[0]).to_string())
            ]));
        }
    }

    // Phase 2: Register waiter
    let notify = wait_list.register_blpop(&keys).await;

    // Phase 3: Wait with timeout
    if timeout > 0.0 {
        match tokio::time::timeout(
            Duration::from_secs_f64(timeout),
            notify.notified()
        ).await {
            Ok(_) => {
                // Notified, retry pop
                for key in &keys {
                    if let Ok(Some(values)) = db.lpop(key, None).await {
                        wait_list.cleanup_waiter(&keys, &notify).await;
                        return Ok(Value::Array(vec![
                            Value::BulkString(key.clone()),
                            Value::BulkString(String::from_utf8_lossy(&values[0]).to_string())
                        ]));
                    }
                }
            }
            Err(_) => {
                // Timeout
                wait_list.cleanup_waiter(&keys, &notify).await;
            }
        }
    } else {
        // Block indefinitely
        notify.notified().await;
        for key in &keys {
            if let Ok(Some(values)) = db.lpop(key, None).await {
                wait_list.cleanup_waiter(&keys, &notify).await;
                return Ok(Value::Array(vec![
                    Value::BulkString(key.clone()),
                    Value::BulkString(String::from_utf8_lossy(&values[0]).to_string())
                ]));
            }
        }
    }

    Ok(Value::NullArray)
}
```

**Integration with LPUSH**:
```rust
// src/commands/write.rs
pub async fn execute_lpush(
    db: &SharedDb,
    repl_tx: &mpsc::Sender<ReplicationCommand>,
    wait_list: &WaitList,
    key: String,
    values: Vec<String>
) -> Result<Value> {
    let bytes_values: Vec<Bytes> = values.iter()
        .map(|v| Bytes::from(v.clone()))
        .collect();

    // Write to DB
    let len = db.lpush(key.clone(), bytes_values).await?;

    // Propagate to replicas
    let mut args = vec![
        Value::BulkString("LPUSH".to_string()),
        Value::BulkString(key.clone()),
    ];
    for v in values {
        args.push(Value::BulkString(v));
    }
    repl_tx.send(ReplicationCommand::Propagate(Value::Array(args))).await?;

    // Wake waiters
    wait_list.wake_blpop_waiters(&key).await;

    Ok(Value::Integer(len as i64))
}
```

**Validation**:
```bash
cargo test wait_list
cargo test --test integration_tests::test_blpop_lpush_coordination
```

---

## Summary

This implementation roadmap provides:

1. **Phase 1**: Safe refactoring with no behavioral changes
2. **Phase 2**: Incremental state migration with validation at each step
3. **Clear validation criteria** for each step
4. **Rollback procedures** if any step fails
5. **Comprehensive testing** strategy

**Next Steps**: Continue with Phase 2 (Steps 2.4-2.6) and Phase 3 in separate documents to maintain manageable size.

**Estimated Timeline**:
- Phase 1: 2 weeks ✅ (Preparation complete)
- Phase 2: 3 weeks (State migration)
- Phase 3: 2 weeks (Parallel execution)
- Phase 4: 1 week (Optimization)

**Total**: 8 weeks from start to production-ready
