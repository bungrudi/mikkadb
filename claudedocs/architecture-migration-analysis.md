# Architectural Migration Analysis: Actor to Shared State

## Executive Summary

This analysis examines the migration from a single-threaded Actor model to a concurrent Shared State architecture for MikkaDB. The current Actor pattern ensures correctness through serialization but severely limits performance. The proposed Shared State model enables concurrent reads and reduces latency, but introduces complex state management challenges across multiple subsystems.

**Critical Finding**: The migration requires splitting Engine's monolithic state into 5 distinct ownership domains, each with different concurrency semantics. The most challenging aspects are:
1. Transaction state (client-local, needs isolation)
2. Pub/Sub subscriptions (global shared, needs broadcast)
3. Blocking commands (cannot hold locks, needs coordination)
4. Replication (needs strict ordering, serial writes)

---

## 1. State Ownership Analysis

### 1.1 Current State Distribution (Actor Model)

The Engine struct owns ALL state:

```rust
pub struct Engine {
    // Core data
    db: Db,                                    // HashMap<String, DataType>

    // Replication state
    replicas: Vec<Replica>,                    // Connected replicas
    replication_offset: i64,                   // Current write offset
    pending_waits: Vec<Option<PendingWait>>,   // WAIT command tracking

    // Transaction state (per-client)
    transaction_state: HashMap<u64, Vec<RedisCommand>>,

    // Pub/Sub state (global)
    pub_sub_subs: HashMap<String, HashMap<u64, mpsc::Sender<Value>>>,

    // Blocking operations
    waiting_list_clients: Vec<(Vec<String>, oneshot::Sender)>,  // BLPOP
    pending_reads: Vec<Option<PendingRead>>,                    // XREAD BLOCK

    // Timeout coordination
    timeout_tx/rx: mpsc::Sender/Receiver<usize>,
    read_timeout_tx/rx: mpsc::Sender/Receiver<usize>,
}
```

**Current Flow:**
```
Client Task → CommandRequest → mpsc::channel → Engine.handle_command()
                                                    ↓
                                            Single-threaded execution
                                                    ↓
                                            oneshot::channel → Client Task
```

### 1.2 Proposed State Distribution (Shared State Model)

State must be partitioned into 5 ownership domains:

#### Domain 1: Core Database (Shared Read, Exclusive Write)
```rust
Arc<RwLock<Db>>  // All data structures: String, List, Stream, SortedSet

Access Pattern:
- Read commands: db.read().await  (concurrent)
- Write commands: db.write().await (exclusive)
```

**Commands:**
- **Readers**: GET, LRANGE, ZRANGE, ZCARD, ZSCORE, ZRANK, XRANGE, KEYS, TYPE, LLEN
- **Writers**: SET, INCR, LPUSH, RPUSH, LPOP, ZADD, ZREM, XADD
- **Read-then-Write**: INCR (read current → increment → write)

#### Domain 2: Transaction State (Client-Local)
```rust
// Option A: Client-side storage
struct ClientState {
    transaction_queue: Option<Vec<RedisCommand>>,
}

// Option B: Shared map with client isolation
Arc<RwLock<HashMap<ClientId, TransactionState>>>

struct TransactionState {
    commands: Vec<RedisCommand>,
}
```

**Critical Requirements:**
- Must be isolated per-client (Client A's MULTI doesn't affect Client B)
- Must survive across multiple requests (MULTI → command → command → EXEC)
- Must be cleaned up on disconnect
- Cannot use single RwLock<Db> because transactions need to queue commands

**Recommended Approach**: Client-local storage (Option A)
- Each client task owns its transaction state
- No contention between clients
- Simple cleanup on disconnect
- Trade-off: Cannot inspect transaction state globally (acceptable)

#### Domain 3: Pub/Sub Subscriptions (Global Shared, Broadcast)
```rust
Arc<RwLock<HashMap<Channel, HashMap<ClientId, mpsc::Sender<Value>>>>>

// Simpler alternative:
Arc<DashMap<Channel, DashMap<ClientId, mpsc::Sender<Value>>>>
```

**Critical Requirements:**
- Global visibility (PUBLISH needs to find all subscribers)
- Dynamic updates (clients SUBSCRIBE/UNSUBSCRIBE frequently)
- Push messages asynchronously (non-blocking)
- Cleanup on disconnect

**Concurrency Challenge:**
```
Thread 1: SUBSCRIBE(channel1) → acquire write lock → insert
Thread 2: PUBLISH(channel1) → acquire read lock → iterate subscribers → send
Thread 3: Client disconnects → acquire write lock → remove from all channels
```

**Recommended Approach**: DashMap (concurrent HashMap)
- Lock-free reads for PUBLISH hot path
- Fine-grained locking per channel
- Trade-off: Extra dependency, but superior performance

#### Domain 4: Blocking Operations (Coordination Points)
```rust
// BLPOP waiting clients
Arc<RwLock<Vec<(Vec<String>, oneshot::Sender<Result<Value>>)>>>

// XREAD BLOCK pending reads
Arc<RwLock<Vec<Option<PendingRead>>>>

// Timeout coordination
Arc<Mutex<HashMap<TaskId, oneshot::Sender<()>>>>
```

**Critical Requirements:**
- Cannot hold Db write lock while waiting (deadlock risk)
- Must coordinate between LPUSH (waker) and BLPOP (waiter)
- Must handle timeouts (spawn tasks that send on timeout)
- Must cleanup on client disconnect

**The Deadlock Problem:**
```rust
// WRONG: This will deadlock
async fn handle_blpop(db: Arc<RwLock<Db>>, keys: Vec<String>) {
    let lock = db.write().await;  // Acquire write lock
    if let Some(value) = lock.lpop(&keys[0]) {
        return value;
    }
    // Block here waiting for value → DEADLOCK
    // No other task can write because we hold the lock!
}
```

**Correct Approach - Lock/Unlock Pattern:**
```rust
async fn handle_blpop(
    db: Arc<RwLock<Db>>,
    keys: Vec<String>,
    waiters: Arc<RwLock<WaitList>>
) {
    // Step 1: Try immediate pop (lock → check → unlock)
    {
        let mut lock = db.write().await;
        for key in &keys {
            if let Some(value) = lock.lpop(key, None) {
                return Ok(value);  // Lock released here
            }
        }
    }  // Lock MUST be released before blocking

    // Step 2: Register as waiter
    let (tx, rx) = oneshot::channel();
    {
        let mut waiter_lock = waiters.write().await;
        waiter_lock.push((keys.clone(), tx));
    }

    // Step 3: Wait for notification (no locks held)
    match timeout(duration, rx).await {
        Ok(Ok(value)) => Ok(value),
        _ => Ok(Value::NullArray),
    }
}

async fn handle_lpush(
    db: Arc<RwLock<Db>>,
    key: String,
    values: Vec<Bytes>,
    waiters: Arc<RwLock<WaitList>>
) {
    // Step 1: Perform write
    let len = {
        let mut lock = db.write().await;
        lock.lpush(key.clone(), values)?
    };  // Lock released

    // Step 2: Check waiters (separate lock)
    let mut waiter_lock = waiters.write().await;
    waiter_lock.retain(|(keys, tx)| {
        if keys.contains(&key) {
            // Try to pop and notify
            // Need to re-acquire db lock!
            false  // Remove from wait list
        } else {
            true   // Keep waiting
        }
    });
}
```

**Lock Ordering Issue**: The above approach has a subtle bug - we need to reacquire `db` lock while holding `waiters` lock. This creates potential for deadlock.

**Correct Solution - Single Lock with Coordination:**
```rust
// Combine DB access and waiter notification in single critical section
async fn handle_lpush(
    db: Arc<RwLock<Db>>,
    key: String,
    values: Vec<Bytes>,
    waiters: Arc<RwLock<WaitList>>
) {
    // Acquire both locks in consistent order
    let mut db_lock = db.write().await;
    let mut waiter_lock = waiters.write().await;

    // Perform write
    let len = db_lock.lpush(key.clone(), values)?;

    // Immediately check and wake waiters (locks held)
    if let Some((_, tx)) = waiter_lock.remove_waiter_for(&key) {
        if let Ok(Some(value)) = db_lock.lpop(&key, None) {
            let _ = tx.send(Ok(value));
        }
    }

    // Both locks released together
}
```

**Better Solution - Event-Driven Notification:**
```rust
// Use tokio::sync::Notify for coordination
struct WaitList {
    waiters: HashMap<String, Vec<(oneshot::Sender, tokio::sync::Notify)>>,
}

async fn handle_lpush(...) {
    let len = {
        let mut lock = db.write().await;
        lock.lpush(key.clone(), values)?
    };

    // Notify waiters (no lock needed)
    if let Some(notifier) = get_notifier_for_key(&key).await {
        notifier.notify_one();
    }
}

async fn handle_blpop(...) {
    loop {
        // Try pop
        if let Some(value) = try_pop(&db, &keys).await {
            return value;
        }

        // Wait for notification
        let notifier = register_waiter(&keys).await;
        notifier.notified().await;
        // Loop back to try again
    }
}
```

#### Domain 5: Replication (Serial Write Stream)
```rust
Arc<RwLock<ReplicationState>>

struct ReplicationState {
    replicas: Vec<Replica>,
    offset: i64,
    pending_waits: Vec<PendingWait>,
}

struct Replica {
    id: u64,
    tx: mpsc::Sender<Value>,
    offset: i64,
}
```

**Critical Requirements:**
- Strict ordering of write commands
- Offset tracking for consistency
- WAIT command coordination
- REPLCONF ACK offset updates

**Concurrency Challenge:**
```
Thread 1: SET key1 → write DB → propagate → update offset
Thread 2: SET key2 → write DB → propagate → update offset
Thread 3: WAIT 1 1000 → read offsets → wait for ACKs
Thread 4: REPLCONF ACK → update replica offset → check pending waits
```

**The Ordering Problem:**
```rust
// WRONG: Race condition in offset tracking
async fn handle_set(db: Arc<RwLock<Db>>, repl: Arc<RwLock<Replication>>) {
    // Thread 1 and 2 execute concurrently
    db.write().await.set(key, value);  // T1: set key1, T2: set key2

    // Race: which command propagates first?
    let mut repl_lock = repl.write().await;
    repl_lock.propagate(cmd);  // Order not guaranteed!
    repl_lock.offset += cmd_size;
}
```

**Correct Solution - Single Write Lock:**
```rust
async fn handle_write_command(
    db: Arc<RwLock<Db>>,
    repl: Arc<RwLock<Replication>>,
    cmd: RedisCommand
) {
    // Acquire both locks in consistent order
    let mut db_lock = db.write().await;
    let mut repl_lock = repl.write().await;

    // Execute write
    let result = execute_write(&mut db_lock, cmd);

    // Propagate in same critical section
    repl_lock.propagate(cmd.to_resp());
    repl_lock.offset += cmd.serialized_size();

    result
}
```

**Alternative - Replication Channel:**
```rust
// Separate replication from DB writes
async fn handle_set(
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>
) {
    // Write to DB (concurrent writes still exclusive via RwLock)
    let result = {
        let mut lock = db.write().await;
        lock.set(key, value)
    };

    // Send to replication actor (maintains order via channel)
    repl_tx.send(ReplicationCommand::Propagate(cmd)).await?;

    result
}

// Dedicated replication task (single-threaded)
async fn replication_loop(mut rx: mpsc::Receiver<ReplicationCommand>) {
    let mut offset = 0;
    let mut replicas = Vec::new();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            ReplicationCommand::Propagate(cmd) => {
                let bytes = cmd.serialize();
                offset += bytes.len();
                for replica in &replicas {
                    replica.tx.send(cmd.clone()).await;
                }
            }
            ReplicationCommand::Wait { num, timeout, respond } => {
                // Handle WAIT command
            }
            ReplicationCommand::Ack { replica_id, offset } => {
                // Update replica offset
            }
        }
    }
}
```

**Recommended**: Replication channel approach maintains strict ordering and simplifies write command implementation.

---

## 2. Concurrency Strategy by Feature

### 2.1 Transactions (MULTI/EXEC)

**State Requirements:**
- Client-local command queue
- Isolation from other clients
- Atomic execution of queued commands

**Implementation Strategy:**

```rust
// Client task owns transaction state
struct ClientHandler {
    client_id: u64,
    transaction_queue: Option<Vec<RedisCommand>>,
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>,
}

impl ClientHandler {
    async fn handle_command(&mut self, cmd: RedisCommand) -> Result<Value> {
        match cmd {
            RedisCommand::Multi => {
                if self.transaction_queue.is_some() {
                    return Err("ERR MULTI calls can not be nested");
                }
                self.transaction_queue = Some(Vec::new());
                Ok(Value::SimpleString("OK"))
            }

            RedisCommand::Exec => {
                let commands = self.transaction_queue.take()
                    .ok_or("ERR EXEC without MULTI")?;

                // Acquire write lock once for entire transaction
                let mut db_lock = self.db.write().await;
                let mut results = Vec::new();

                for cmd in commands {
                    let result = self.execute_in_transaction(&mut db_lock, cmd).await?;
                    results.push(result);
                }

                Ok(Value::Array(results))
            }

            RedisCommand::Discard => {
                self.transaction_queue.take()
                    .ok_or("ERR DISCARD without MULTI")?;
                Ok(Value::SimpleString("OK"))
            }

            other => {
                if let Some(queue) = &mut self.transaction_queue {
                    queue.push(other);
                    Ok(Value::SimpleString("QUEUED"))
                } else {
                    self.execute_command(other).await
                }
            }
        }
    }
}
```

**Key Insights:**
- Transaction state stays in client task (no shared state)
- EXEC acquires single write lock for all commands (atomic)
- No possibility of interleaving with other clients
- Cleanup automatic on client disconnect

**Limitations:**
- SUBSCRIBE/UNSUBSCRIBE still forbidden in transactions (separate state)
- Blocking commands (BLPOP) must return immediately or error

### 2.2 Pub/Sub

**State Requirements:**
- Global subscription map: Channel → Set of ClientIDs
- Per-client sender channel for push messages
- Dynamic updates (subscribe/unsubscribe)

**Implementation Strategy:**

```rust
use dashmap::DashMap;

type PubSubState = Arc<DashMap<String, DashMap<u64, mpsc::Sender<Value>>>>;

async fn handle_subscribe(
    client_id: u64,
    channels: Vec<String>,
    msg_tx: mpsc::Sender<Value>,
    pubsub: PubSubState
) -> Result<Value> {
    for channel in channels {
        // Insert subscriber
        let subs = pubsub.entry(channel.clone())
            .or_insert_with(DashMap::new);
        subs.insert(client_id, msg_tx.clone());

        // Send subscription confirmation
        let count = count_client_subscriptions(&pubsub, client_id);
        let msg = Value::Array(vec![
            Value::BulkString("subscribe".to_string()),
            Value::BulkString(channel),
            Value::Integer(count),
        ]);
        msg_tx.send(msg).await?;
    }

    Ok(Value::Error("NO_REPLY".to_string()))  // Don't send normal response
}

async fn handle_publish(
    channel: String,
    message: String,
    pubsub: PubSubState
) -> Result<Value> {
    let count = if let Some(subs) = pubsub.get(&channel) {
        let msg = Value::Array(vec![
            Value::BulkString("message".to_string()),
            Value::BulkString(channel.clone()),
            Value::BulkString(message),
        ]);

        let mut sent = 0;
        for sender in subs.value().iter() {
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

async fn cleanup_client_subscriptions(
    client_id: u64,
    pubsub: PubSubState
) {
    // Iterate all channels and remove client
    for channel in pubsub.iter() {
        channel.value().remove(&client_id);
    }
}
```

**Key Insights:**
- DashMap provides lock-free reads (PUBLISH hot path)
- Per-channel locking (independent subscriptions don't block)
- Must cleanup on disconnect (iterate all channels)
- Subscription state separate from command execution

**Challenges:**
- Client in subscribed mode can only execute subset of commands
- Need to track subscription state in client task
- Channel cleanup on disconnect is O(channels) but infrequent

### 2.3 Blocking Commands (BLPOP, XREAD BLOCK)

**State Requirements:**
- Waiter registration (keys → channels to notify)
- Timeout coordination
- Wake on data availability or timeout

**Implementation Strategy (Event-Driven):**

```rust
use tokio::sync::Notify;
use std::sync::Arc;

type WaitList = Arc<RwLock<HashMap<String, Vec<Arc<Notify>>>>>;

async fn handle_blpop(
    keys: Vec<String>,
    timeout: f64,
    db: Arc<RwLock<Db>>,
    wait_list: WaitList
) -> Result<Value> {
    // Try immediate pop
    for key in &keys {
        let result = {
            let mut lock = db.write().await;
            lock.lpop(key, None)
        };

        if let Ok(Some(values)) = result {
            return Ok(Value::Array(vec![
                Value::BulkString(key.clone()),
                Value::BulkString(String::from_utf8_lossy(&values[0]).to_string())
            ]));
        }
    }

    // Register waiter
    let notify = Arc::new(Notify::new());
    {
        let mut waiters = wait_list.write().await;
        for key in &keys {
            waiters.entry(key.clone())
                .or_insert_with(Vec::new)
                .push(notify.clone());
        }
    }

    // Wait with timeout
    if timeout > 0.0 {
        match tokio::time::timeout(
            Duration::from_secs_f64(timeout),
            notify.notified()
        ).await {
            Ok(_) => {
                // Notified, try pop again
                for key in &keys {
                    let result = {
                        let mut lock = db.write().await;
                        lock.lpop(key, None)
                    };
                    if let Ok(Some(values)) = result {
                        return Ok(Value::Array(vec![
                            Value::BulkString(key.clone()),
                            Value::BulkString(String::from_utf8_lossy(&values[0]).to_string())
                        ]));
                    }
                }
            }
            Err(_) => {} // Timeout
        }
    } else {
        // Block indefinitely
        notify.notified().await;
        // Try pop after notification
    }

    // Cleanup waiter registration
    {
        let mut waiters = wait_list.write().await;
        for key in &keys {
            if let Some(v) = waiters.get_mut(key) {
                v.retain(|n| !Arc::ptr_eq(n, &notify));
            }
        }
    }

    Ok(Value::NullArray)
}

async fn handle_lpush(
    key: String,
    values: Vec<Bytes>,
    db: Arc<RwLock<Db>>,
    wait_list: WaitList,
    repl_tx: mpsc::Sender<ReplicationCommand>
) -> Result<Value> {
    // Perform write
    let len = {
        let mut lock = db.write().await;
        lock.lpush(key.clone(), values.clone())?
    };

    // Propagate to replicas
    repl_tx.send(ReplicationCommand::Propagate(
        RedisCommand::LPush { key: key.clone(), values: ... }
    )).await?;

    // Wake waiters
    {
        let mut waiters = wait_list.write().await;
        if let Some(notifiers) = waiters.remove(&key) {
            for notify in notifiers {
                notify.notify_one();
            }
        }
    }

    Ok(Value::Integer(len as i64))
}
```

**Key Insights:**
- Notify provides lock-free wait/wake coordination
- Lock released before blocking (no deadlock)
- Waiter cleanup handled in finally block
- Can wake multiple waiters (notify_one vs notify_waiters)

**Challenges:**
- Thundering herd (all waiters wake, race to pop)
- Must retry pop after wake (value might be taken)
- Cleanup on timeout vs notification

**Optimization - Fair Queueing:**
```rust
// Instead of notifying all, maintain FIFO queue
struct WaiterQueue {
    waiters: VecDeque<(Vec<String>, oneshot::Sender<Value>)>,
}

async fn handle_lpush(...) {
    // ... perform write ...

    // Wake ONE waiter
    let mut queue = wait_queue.write().await;
    if let Some((keys, tx)) = queue.pop_front_if_matches(&key) {
        // Try to pop and send to that specific waiter
        if let Ok(Some(value)) = db.write().await.lpop(&key, None) {
            let _ = tx.send(Ok(value));
        }
    }
}
```

### 2.4 Replication

**State Requirements:**
- Replica connections (mpsc::Sender per replica)
- Offset tracking (master and per-replica)
- WAIT command coordination
- REPLCONF ACK handling

**Implementation Strategy (Replication Actor):**

```rust
enum ReplicationCommand {
    Propagate(Value),                    // Write command to propagate
    AddReplica(u64, mpsc::Sender<Value>), // New replica connected
    Ack { replica_id: u64, offset: i64 }, // Replica ACK
    Wait {
        num_replicas: usize,
        timeout: u64,
        respond: oneshot::Sender<i64>,
    },
}

struct ReplicationActor {
    replicas: HashMap<u64, Replica>,
    offset: i64,
    pending_waits: Vec<PendingWait>,
}

impl ReplicationActor {
    async fn run(mut self, mut rx: mpsc::Receiver<ReplicationCommand>) {
        loop {
            tokio::select! {
                Some(cmd) = rx.recv() => {
                    match cmd {
                        ReplicationCommand::Propagate(value) => {
                            let bytes = value.serialize_bytes();
                            self.offset += bytes.len() as i64;

                            for replica in self.replicas.values() {
                                let _ = replica.tx.send(value.clone()).await;
                            }
                        }

                        ReplicationCommand::AddReplica(id, tx) => {
                            self.replicas.insert(id, Replica {
                                id,
                                tx,
                                offset: 0,
                            });
                        }

                        ReplicationCommand::Ack { replica_id, offset } => {
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

                        ReplicationCommand::Wait { num_replicas, timeout, respond } => {
                            // Check if already synced
                            let synced = self.replicas.values()
                                .filter(|r| r.offset >= self.offset)
                                .count();

                            if synced >= num_replicas {
                                let _ = respond.send(synced as i64);
                            } else {
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
                                tokio::spawn(async move {
                                    tokio::time::sleep(Duration::from_millis(timeout)).await;
                                    // Send timeout notification
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}
```

**Key Insights:**
- Single-threaded actor ensures strict ordering
- Channel provides natural serialization
- Offset tracking straightforward (no races)
- WAIT command coordination centralized

**Integration with Write Commands:**
```rust
async fn handle_set(
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>,
    key: String,
    value: String,
    px: Option<u64>
) -> Result<Value> {
    // Write to DB
    {
        let mut lock = db.write().await;
        lock.set(key.clone(), Bytes::from(value.clone()), px);
    }

    // Propagate to replicas (async, non-blocking)
    let cmd = Value::Array(vec![
        Value::BulkString("SET".to_string()),
        Value::BulkString(key),
        Value::BulkString(value),
        // ... px arguments ...
    ]);

    repl_tx.send(ReplicationCommand::Propagate(cmd)).await?;

    Ok(Value::SimpleString("OK".to_string()))
}
```

---

## 3. Lock Acquisition Patterns

### 3.1 Read-Only Commands

**Pattern**: Shared read lock, no coordination needed

```rust
async fn handle_get(
    db: Arc<RwLock<Db>>,
    key: String
) -> Result<Value> {
    let lock = db.read().await;
    match lock.get(&key) {
        Some(value) => Ok(Value::BulkString(String::from_utf8_lossy(&value).to_string())),
        None => Ok(Value::Null),
    }
}
```

**Commands**: GET, LRANGE, ZRANGE, ZCARD, ZSCORE, ZRANK, XRANGE, KEYS, TYPE, LLEN

**Performance**: Multiple readers can execute concurrently

### 3.2 Write-Only Commands

**Pattern**: Exclusive write lock + replication propagation

```rust
async fn handle_set(
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>,
    key: String,
    value: String,
    px: Option<u64>
) -> Result<Value> {
    // Critical section: write to DB
    {
        let mut lock = db.write().await;
        lock.set(key.clone(), Bytes::from(value.clone()), px);
    }  // Lock released

    // Propagate to replicas (async)
    repl_tx.send(ReplicationCommand::Propagate(/* ... */)).await?;

    Ok(Value::SimpleString("OK".to_string()))
}
```

**Commands**: SET, LPUSH, RPUSH, ZADD, ZREM, XADD

**Performance**: Exclusive write, but lock held for minimal time

### 3.3 Read-Modify-Write Commands

**Pattern**: Exclusive write lock for entire operation

```rust
async fn handle_incr(
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>,
    key: String
) -> Result<Value> {
    let new_val = {
        let mut lock = db.write().await;

        // Read current value
        let current = match lock.get(&key) {
            Some(bytes) => {
                String::from_utf8_lossy(&bytes)
                    .parse::<i64>()
                    .map_err(|_| "ERR value is not an integer")?
            }
            None => 0,
        };

        // Increment
        let new_val = current + 1;

        // Write back
        lock.set(
            key.clone(),
            Bytes::from(new_val.to_string()),
            None
        );

        new_val
    };  // Lock released

    // Propagate
    repl_tx.send(ReplicationCommand::Propagate(/* SET */)).await?;

    Ok(Value::Integer(new_val))
}
```

**Commands**: INCR, LPOP (pop-and-return)

**Key Point**: Must hold write lock for read-modify-write atomicity

### 3.4 Blocking Commands

**Pattern**: Lock → check → unlock → wait → lock → check

```rust
async fn handle_blpop(
    keys: Vec<String>,
    timeout: f64,
    db: Arc<RwLock<Db>>,
    wait_list: WaitList
) -> Result<Value> {
    // Phase 1: Try immediate pop (lock held briefly)
    for key in &keys {
        if let Some(value) = {
            let mut lock = db.write().await;
            lock.lpop(key, None)?
        } {
            return Ok(build_response(key, value));
        }
    }

    // Phase 2: Register and wait (no lock)
    let notify = register_waiter(&wait_list, &keys).await;

    // Phase 3: Wait (no lock held)
    wait_with_timeout(notify, timeout).await?;

    // Phase 4: Retry pop (lock held briefly)
    for key in &keys {
        if let Some(value) = {
            let mut lock = db.write().await;
            lock.lpop(key, None)?
        } {
            return Ok(build_response(key, value));
        }
    }

    Ok(Value::NullArray)
}
```

**Commands**: BLPOP, XREAD BLOCK

**Critical**: Never hold DB lock during wait phase

### 3.5 Transaction Execution

**Pattern**: Single write lock for entire transaction

```rust
async fn handle_exec(
    client_id: u64,
    commands: Vec<RedisCommand>,
    db: Arc<RwLock<Db>>,
    repl_tx: mpsc::Sender<ReplicationCommand>
) -> Result<Value> {
    let mut results = Vec::new();

    // Acquire write lock ONCE
    let mut lock = db.write().await;

    for cmd in commands {
        let result = match cmd {
            RedisCommand::Set { key, value, px } => {
                lock.set(key, Bytes::from(value), px);
                Value::SimpleString("OK".to_string())
            }
            RedisCommand::Get { key } => {
                match lock.get(&key) {
                    Some(v) => Value::BulkString(String::from_utf8_lossy(&v).to_string()),
                    None => Value::Null,
                }
            }
            // ... other commands ...
        };

        results.push(result);

        // Propagate write commands
        if cmd.is_write() {
            repl_tx.send(ReplicationCommand::Propagate(cmd.to_resp())).await?;
        }
    }

    drop(lock);  // Explicit lock release

    Ok(Value::Array(results))
}
```

**Key Point**: Atomicity requires holding lock for entire transaction

### 3.6 Lock Ordering Rules

**Critical**: Consistent lock ordering prevents deadlocks

```rust
// RULE 1: Always acquire in this order
// 1. db (RwLock<Db>)
// 2. wait_list (RwLock<WaitList>)
// 3. pubsub (DashMap - lock-free)
// 4. repl_tx (channel send - lock-free)

// GOOD: Consistent order
async fn operation_a(db: Arc<RwLock<Db>>, wait_list: Arc<RwLock<WaitList>>) {
    let db_lock = db.write().await;        // Lock 1
    let wait_lock = wait_list.write().await;  // Lock 2
    // ... work ...
}

// GOOD: Same order
async fn operation_b(db: Arc<RwLock<Db>>, wait_list: Arc<RwLock<WaitList>>) {
    let db_lock = db.write().await;        // Lock 1
    let wait_lock = wait_list.write().await;  // Lock 2
    // ... work ...
}

// BAD: Reverse order (DEADLOCK RISK)
async fn operation_c(db: Arc<RwLock<Db>>, wait_list: Arc<RwLock<WaitList>>) {
    let wait_lock = wait_list.write().await;  // Lock 2 first
    let db_lock = db.write().await;           // Lock 1 second - WRONG!
}
```

**Deadlock Prevention Strategy**:
1. Define global lock hierarchy
2. Always acquire in same order
3. Release locks in reverse order
4. Minimize critical section size
5. Never hold lock during async operations (await points)

---

## 4. Migration Plan

### Phase 1: Preparation (No Behavioral Changes)

**Goal**: Refactor code to enable parallel execution while maintaining actor model

**Tasks**:
1. Extract command execution logic from Engine into standalone functions
   ```rust
   // Before: method on Engine
   impl Engine {
       async fn execute_set(&mut self, key: String, value: String) { ... }
   }

   // After: standalone function
   async fn execute_set(
       db: &mut Db,
       repl: &mut ReplicationState,
       key: String,
       value: String
   ) -> Result<Value> { ... }
   ```

2. Identify pure read vs write operations
   ```rust
   trait CommandType {
       fn is_read_only(&self) -> bool;
       fn requires_write_lock(&self) -> bool;
       fn requires_coordination(&self) -> bool;
   }
   ```

3. Add comprehensive integration tests
   - Transaction isolation
   - Pub/Sub message ordering
   - Blocking command wake behavior
   - Replication consistency

### Phase 2: State Extraction (Breaking Changes)

**Goal**: Move state into shared Arc structures

**Tasks**:
1. Create shared Db wrapper
   ```rust
   pub struct SharedDb {
       inner: Arc<RwLock<Db>>,
   }

   impl SharedDb {
       pub async fn get(&self, key: &str) -> Option<Bytes> {
           self.inner.read().await.get(key)
       }

       pub async fn set(&self, key: String, value: Bytes, px: Option<u64>) {
           self.inner.write().await.set(key, value, px)
       }
   }
   ```

2. Create ReplicationActor
   ```rust
   spawn(async move {
       replication_actor.run(repl_rx).await;
   });
   ```

3. Create WaitList coordination
   ```rust
   pub struct WaitList {
       blpop: Arc<RwLock<HashMap<String, Vec<Arc<Notify>>>>>,
       xread: Arc<RwLock<Vec<PendingRead>>>,
   }
   ```

4. Move transaction state to client task
   ```rust
   struct ClientHandler {
       transaction_queue: Option<Vec<RedisCommand>>,
   }
   ```

### Phase 3: Parallel Execution (Performance Improvements)

**Goal**: Enable concurrent read operations

**Tasks**:
1. Classify commands by lock requirements
   ```rust
   match command {
       // Concurrent reads
       RedisCommand::Get { .. } |
       RedisCommand::LRange { .. } |
       RedisCommand::ZRange { .. } => {
           execute_read_only(db.clone(), command).await
       }

       // Exclusive writes
       RedisCommand::Set { .. } |
       RedisCommand::LPush { .. } => {
           execute_write(db.clone(), repl_tx.clone(), command).await
       }

       // Coordinated operations
       RedisCommand::BLPop { .. } => {
           execute_blocking(db.clone(), wait_list.clone(), command).await
       }
   }
   ```

2. Remove Engine actor loop
   ```rust
   // Before: single-threaded loop
   loop {
       tokio::select! {
           Some(req) = rx.recv() => handle_command(req).await,
       }
   }

   // After: direct execution in client task
   loop {
       let cmd = read_command(&mut handler).await?;
       let result = execute_command_direct(
           cmd,
           db.clone(),
           repl_tx.clone(),
           wait_list.clone(),
           pubsub.clone()
       ).await?;
       write_response(&mut handler, result).await?;
   }
   ```

3. Benchmark and validate performance
   - Latency reduction (target: 3x improvement)
   - Throughput increase (target: linear with cores)
   - Correctness (all tests pass)

### Phase 4: Optimization

**Goal**: Fine-tune lock granularity and contention

**Tasks**:
1. Profile lock contention
2. Consider lock-free data structures (DashMap for pub/sub)
3. Optimize critical section sizes
4. Add connection pooling if needed

---

## 5. Risk Assessment and Mitigation

### Risk 1: Deadlocks

**Likelihood**: Medium
**Impact**: Critical (server hangs)

**Scenarios**:
- Lock order violation (db → wait_list vs wait_list → db)
- Holding lock during async operation with await point
- Recursive lock acquisition

**Mitigation**:
1. Document and enforce lock ordering hierarchy
2. Code review checklist for lock acquisitions
3. Deadlock detection tests (timeout-based)
4. Use `tokio-console` to monitor lock contention
5. Static analysis tools (Clippy lints)

### Risk 2: Transaction Isolation Violations

**Likelihood**: Low
**Impact**: Critical (data corruption)

**Scenarios**:
- Transaction seeing partial writes from concurrent transaction
- EXEC not holding lock for entire transaction
- Transaction queue not isolated per-client

**Mitigation**:
1. Comprehensive transaction isolation tests
2. Single write lock for entire EXEC
3. Client-local transaction state
4. Property-based testing (concurrent transactions)

### Risk 3: Pub/Sub Message Loss

**Likelihood**: Medium
**Impact**: High (missing notifications)

**Scenarios**:
- Subscriber removed during PUBLISH iteration
- Channel dropped before send completes
- Race between SUBSCRIBE and PUBLISH

**Mitigation**:
1. DashMap ensures atomic iteration
2. Ignore send errors (client disconnect is acceptable)
3. Subscription confirmation before messages
4. Integration tests with concurrent subscribe/publish

### Risk 4: Blocking Command Wake Failures

**Likelihood**: Medium
**Impact**: High (clients hang)

**Scenarios**:
- BLPOP waiter not woken after LPUSH
- Multiple waiters race for single value
- Timeout not firing (task scheduler issue)

**Mitigation**:
1. Fair queueing (FIFO waiter queue)
2. Retry loop after wake (handle races)
3. Timeout validation tests
4. Metrics for waiter queue depth

### Risk 5: Replication Ordering

**Likelihood**: Low
**Impact**: Critical (replica inconsistency)

**Scenarios**:
- Commands propagated out of order
- Offset tracking race condition
- WAIT returning before replica synced

**Mitigation**:
1. Dedicated replication actor (single-threaded)
2. Channel ensures FIFO ordering
3. ACK tracking validation tests
4. Replica consistency verification

### Risk 6: Performance Regression

**Likelihood**: Low
**Impact**: Medium (no improvement)

**Scenarios**:
- Lock contention worse than channel overhead
- RwLock write bias starves readers
- Thundering herd on blocking commands

**Mitigation**:
1. Benchmark before/after migration
2. Profile lock contention (tokio-console)
3. Consider parking_lot (fair RwLock)
4. Load testing with realistic workloads

---

## 6. Testing Strategy

### 6.1 Unit Tests

**Focus**: Individual command correctness with shared state

```rust
#[tokio::test]
async fn test_concurrent_reads() {
    let db = Arc::new(RwLock::new(Db::new()));
    db.write().await.set("key".to_string(), Bytes::from("value"), None);

    // Spawn 100 concurrent GET operations
    let handles: Vec<_> = (0..100)
        .map(|_| {
            let db = db.clone();
            tokio::spawn(async move {
                let lock = db.read().await;
                lock.get("key")
            })
        })
        .collect();

    // All should succeed
    for handle in handles {
        assert_eq!(handle.await.unwrap(), Some(Bytes::from("value")));
    }
}

#[tokio::test]
async fn test_transaction_isolation() {
    let db = Arc::new(RwLock::new(Db::new()));

    // Client 1: MULTI → SET key1 → SET key2 → EXEC
    // Client 2: GET key1 (should not see partial transaction)

    let handle1 = tokio::spawn(async move {
        // Execute transaction
    });

    let handle2 = tokio::spawn(async move {
        // Concurrent read
    });

    // Verify isolation
}
```

### 6.2 Integration Tests

**Focus**: Multi-client scenarios with realistic workloads

```rust
#[tokio::test]
async fn test_blpop_lpush_coordination() {
    // Client 1: BLPOP key1 5
    // Client 2: LPUSH key1 value (after 1s)
    // Verify: Client 1 receives value within 2s
}

#[tokio::test]
async fn test_pubsub_concurrent_subscribe() {
    // 10 clients SUBSCRIBE to channel1
    // 1 client PUBLISH to channel1
    // Verify: All 10 clients receive message
}
```

### 6.3 Stress Tests

**Focus**: Race conditions and deadlocks under load

```rust
#[tokio::test]
async fn test_concurrent_transactions() {
    // 100 clients execute overlapping transactions
    // Verify: No deadlocks, all transactions complete
}

#[tokio::test]
async fn test_lock_contention() {
    // Mixed read/write workload (80% reads, 20% writes)
    // Measure: Latency distribution, throughput
}
```

### 6.4 Property-Based Tests

**Focus**: Invariants under arbitrary command sequences

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_transaction_atomicity(
        commands in prop::collection::vec(arbitrary_redis_command(), 1..10)
    ) {
        // Execute transaction
        // Verify: Either all commands succeed or all fail
    }
}
```

### 6.5 Consistency Tests

**Focus**: Replication consistency

```rust
#[tokio::test]
async fn test_replication_ordering() {
    // Master: SET key1 v1 → SET key2 v2 → SET key3 v3
    // Replica: Verify same order
    // WAIT: Verify replica synced
}
```

---

## 7. Performance Analysis

### 7.1 Expected Improvements

**Latency**:
- Current: ~3ms (channel send + context switch + process + channel send)
- Target: ~1ms (direct lock acquisition + process)
- **Improvement**: 3x reduction

**Throughput**:
- Current: Limited to single core (~50K ops/sec)
- Target: Scales with cores (4 cores = ~150K ops/sec for read-heavy)
- **Improvement**: 3x for read-heavy workloads

### 7.2 Bottleneck Analysis

**Read-Heavy Workload (80% reads)**:
- Bottleneck: RwLock read contention (minimal)
- Scaling: Near-linear up to core count
- Performance: Excellent

**Write-Heavy Workload (50% writes)**:
- Bottleneck: RwLock write exclusion
- Scaling: Limited (writes are serial)
- Performance: Similar to current (no regression)

**Mixed with Blocking (10% BLPOP)**:
- Bottleneck: WaitList lock contention
- Scaling: Depends on key distribution
- Performance: Good if keys are disjoint

### 7.3 Optimization Opportunities

1. **parking_lot::RwLock**: Fairer than tokio::sync::RwLock
2. **DashMap for Db**: Lock-free reads (complex migration)
3. **Sharded Db**: Partition keyspace to reduce write contention
4. **Lock-free replication**: CAS-based offset tracking

---

## 8. Conclusion

### Migration Feasibility: HIGH

The migration from Actor to Shared State model is **feasible** with careful planning. The key challenges are:

1. **Transaction isolation**: Solved by client-local state
2. **Pub/Sub coordination**: Solved by DashMap (or RwLock)
3. **Blocking commands**: Solved by event-driven notification (Notify)
4. **Replication ordering**: Solved by dedicated actor

### Critical Success Factors:

1. ✅ **Lock ordering discipline**: Prevent deadlocks
2. ✅ **Comprehensive testing**: Catch race conditions
3. ✅ **Incremental migration**: Validate at each phase
4. ✅ **Performance benchmarking**: Verify improvements

### Recommended Approach:

**Phase 1**: Refactor (2 weeks)
- Extract logic from Engine
- Add integration tests
- No behavioral changes

**Phase 2**: State migration (3 weeks)
- Create shared structures
- Replication actor
- Update client loop
- Validate correctness

**Phase 3**: Parallel execution (2 weeks)
- Enable concurrent reads
- Remove Engine actor
- Benchmark performance

**Phase 4**: Optimization (1 week)
- Profile and tune
- Consider DashMap
- Load testing

**Total Estimate**: 8 weeks

### Risk Mitigation Priority:

1. 🔴 **Deadlocks**: Lock ordering + static analysis
2. 🔴 **Transaction isolation**: Client-local state
3. 🟡 **Replication consistency**: Dedicated actor
4. 🟡 **Blocking wake failures**: Event-driven + tests
5. 🟢 **Performance regression**: Benchmarking

---

## Appendix A: Lock Acquisition Decision Tree

```
Command received
│
├─ Is read-only? (GET, LRANGE, ZRANGE, etc.)
│  └─ Acquire read lock → Execute → Release
│
├─ Is write-only? (SET, LPUSH, ZADD, etc.)
│  ├─ Acquire write lock → Execute → Release
│  └─ Send to replication actor (async)
│
├─ Is read-modify-write? (INCR, LPOP)
│  ├─ Acquire write lock → Read → Modify → Write → Release
│  └─ Send to replication actor (async)
│
├─ Is blocking? (BLPOP, XREAD BLOCK)
│  ├─ Acquire write lock → Check → Release
│  ├─ If not available: Register waiter (separate lock)
│  ├─ Wait (no locks)
│  └─ On wake: Acquire write lock → Try again → Release
│
├─ Is transaction control? (MULTI, EXEC, DISCARD)
│  ├─ MULTI: Set client-local flag
│  ├─ EXEC: Acquire write lock → Execute all → Release
│  └─ DISCARD: Clear client-local queue
│
└─ Is pub/sub? (SUBSCRIBE, PUBLISH, UNSUBSCRIBE)
   ├─ SUBSCRIBE: Update DashMap (lock-free)
   ├─ PUBLISH: Read DashMap → Send to channels
   └─ UNSUBSCRIBE: Update DashMap (lock-free)
```

## Appendix B: State Ownership Reference

| State | Current Owner | New Owner | Lock Type | Shared? |
|-------|---------------|-----------|-----------|---------|
| Core data | Engine | Arc<RwLock<Db>> | RwLock | Yes |
| Transactions | Engine | Client Task | None | No |
| Pub/Sub subs | Engine | Arc<DashMap<...>> | Lock-free | Yes |
| BLPOP waiters | Engine | Arc<RwLock<WaitList>> | RwLock | Yes |
| XREAD pending | Engine | Arc<RwLock<WaitList>> | RwLock | Yes |
| Replicas | Engine | Replication Actor | Actor | No (serial) |
| Repl offset | Engine | Replication Actor | Actor | No (serial) |
| Pending WAITs | Engine | Replication Actor | Actor | No (serial) |

## Appendix C: Command Classification

### Read-Only (Concurrent)
GET, LRANGE, ZRANGE, ZCARD, ZSCORE, ZRANK, XRANGE, KEYS, TYPE, LLEN

### Write-Only (Exclusive)
SET, LPUSH, RPUSH, ZADD, ZREM, XADD

### Read-Modify-Write (Exclusive, Atomic)
INCR, LPOP

### Blocking (Complex Coordination)
BLPOP, XREAD BLOCK

### Transaction Control (Client-Local)
MULTI, EXEC, DISCARD

### Pub/Sub (Lock-Free)
SUBSCRIBE, PUBLISH, UNSUBSCRIBE

### Replication (Serial Actor)
PSYNC, REPLCONF, WAIT

### Read-Only (No Lock)
PING, ECHO, INFO, CONFIG
