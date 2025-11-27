# Technical Design: Common Redis Commands

## Context

MikkaDB uses a thread-per-shard architecture where:
- Each shard runs on a dedicated OS thread with its own tokio runtime
- Keys are distributed across shards via consistent hashing
- Shards communicate via `mpsc` channels for cross-shard replication
- Current performance: 4.52M ops/sec with 4 shards

Adding new commands must preserve this architecture and performance characteristics.

## Goals
- Add 40+ commonly used Redis commands
- Maintain >4M ops/sec performance
- Achieve 90% test coverage
- Match Redis protocol and error behavior exactly

## Non-Goals
- True ACID atomicity for cross-shard operations (documented trade-off)
- Cluster mode protocol (MOVED, ASK redirects)
- Lua scripting support

## Architecture Decisions

### Decision 1: Hash and Set Data Structures

**What**: Add `HashMap<String, HashMap<String, Bytes>>` for Hashes and `HashMap<String, HashSet<Bytes>>` for Sets to `Db` struct.

**Why**:
- Hashes and Sets are single-key operations (all fields/members stored together)
- Same memory model as existing Lists and Sorted Sets
- No cross-shard complexity for basic operations

**Alternatives considered**:
- Separate storage backend → Rejected: over-engineering for current needs
- Use existing string storage with serialization → Rejected: poor performance for field access

### Decision 2: Key-to-Shard Routing

**What**: Use `hash(key) % num_shards` for consistent key routing.

**Why**: Already implemented for SET/GET. Ensures all operations for a key go to same shard.

**Implementation**:
```rust
fn key_to_shard(key: &[u8], num_shards: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}
```

### Decision 3: Cross-Shard Fan-Out for Multi-Key Operations

**What**: For MGET/MSET/multi-key DEL, fan out to relevant shards and merge results.

**Why**:
- Maintains shard independence (no global locks)
- Preserves performance for single-key operations
- Acceptable trade-off: partial failures documented, not hidden

**Pattern**:
```rust
async fn handle_mget(keys: Vec<Bytes>) -> Vec<Option<Bytes>> {
    // 1. Group keys by target shard
    let mut shard_keys: HashMap<usize, Vec<(usize, Bytes)>> = HashMap::new();
    for (idx, key) in keys.iter().enumerate() {
        let shard = key_to_shard(key, num_shards);
        shard_keys.entry(shard).or_default().push((idx, key.clone()));
    }

    // 2. Fan-out: send to each shard
    let mut results = vec![None; keys.len()];
    let mut futures = Vec::new();

    for (shard_id, keys_with_idx) in shard_keys {
        futures.push(async move {
            // If this is our shard, execute directly
            // Otherwise, send via channel and await response
            (shard_id, get_from_shard(shard_id, keys_with_idx).await)
        });
    }

    // 3. Merge results in original order
    for (shard_id, shard_results) in join_all(futures).await {
        for (original_idx, value) in shard_results {
            results[original_idx] = value;
        }
    }

    results
}
```

### Decision 4: Expiration Storage

**What**: Store expiration as `Option<Instant>` alongside values, check on access (lazy expiration).

**Why**:
- Already implemented for SET PX
- Lazy expiration is simple and efficient
- Background cleanup can be added later if memory pressure occurs

**Enhancement**: Add `EXPIRE`, `TTL` commands to manipulate existing expiration metadata.

### Decision 5: Error Response Compatibility

**What**: Match Redis error messages exactly using `WRONGTYPE` prefix.

**Why**: Client libraries and applications depend on specific error formats.

**Example**:
```rust
// Redis returns this exact message for type mismatch
Value::Error(Bytes::from("WRONGTYPE Operation against a key holding the wrong kind of value"))
```

## Data Structure Changes

### Db Struct (src/db.rs)
```rust
pub struct Db {
    // Existing
    data: HashMap<String, (Bytes, Option<Instant>)>,
    lists: HashMap<String, VecDeque<Bytes>>,
    sorted_sets: HashMap<String, BTreeMap<...>>,
    streams: HashMap<String, Stream>,

    // New
    hashes: HashMap<String, HashMap<String, Bytes>>,
    sets: HashMap<String, HashSet<Bytes>>,
}
```

### Command Enum Additions (src/command.rs)
```rust
pub enum RedisCommand {
    // Existing...

    // Key commands
    Del { keys: Vec<Bytes> },
    Exists { keys: Vec<Bytes> },
    Expire { key: Bytes, seconds: i64 },
    PExpire { key: Bytes, milliseconds: i64 },
    Ttl { key: Bytes },
    PTtl { key: Bytes },
    Persist { key: Bytes },
    Rename { key: Bytes, newkey: Bytes },

    // String commands
    Decr { key: Bytes },
    DecrBy { key: Bytes, decrement: i64 },
    IncrBy { key: Bytes, increment: i64 },
    Append { key: Bytes, value: Bytes },
    StrLen { key: Bytes },
    GetEx { key: Bytes, ex: Option<i64>, px: Option<i64>, persist: bool },
    SetNx { key: Bytes, value: Bytes },
    SetEx { key: Bytes, seconds: i64, value: Bytes },
    MGet { keys: Vec<Bytes> },
    MSet { pairs: Vec<(Bytes, Bytes)> },

    // Hash commands
    HSet { key: String, fields: Vec<(String, Bytes)> },
    HGet { key: String, field: String },
    HMGet { key: String, fields: Vec<String> },
    HGetAll { key: String },
    HDel { key: String, fields: Vec<String> },
    HExists { key: String, field: String },
    HKeys { key: String },
    HVals { key: String },
    HLen { key: String },
    HIncrBy { key: String, field: String, increment: i64 },

    // Set commands
    SAdd { key: String, members: Vec<Bytes> },
    SRem { key: String, members: Vec<Bytes> },
    SMembers { key: String },
    SIsMember { key: String, member: Bytes },
    SCard { key: String },
    SPop { key: String, count: Option<usize> },
}
```

### Decision 6: Concurrency Model and Race Condition Safety

**What**: Leverage thread-per-shard architecture for natural serialization of operations on same key.

**Why**: In our architecture:
- Each key hashes to exactly one shard
- Each shard runs on a single thread (single-threaded tokio runtime)
- Operations within a shard are naturally serialized
- No explicit locking needed for same-key operations

**How It Works**:
```
Client A: INCR counter → Hash("counter") = shard-2 → queued in shard-2
Client B: INCR counter → Hash("counter") = shard-2 → queued in shard-2
                                                      ↓
                                             Shard-2 processes sequentially:
                                             1. INCR counter → 1
                                             2. INCR counter → 2
```

**Guarantees**:
- Operations on same key are always serialized (no race conditions)
- Operations on different keys (same shard) are serialized
- Operations on different shards can execute in parallel

**Testing Strategy**:
1. **Correctness Tests**: Concurrent INCR must yield exact sum
2. **Integrity Tests**: Concurrent mutations must not corrupt data structures
3. **Stress Tests**: High contention to verify no deadlocks or livelocks
4. **Producer-Consumer**: RPUSH/BLPOP queue pattern exactly-once semantics

**Edge Cases to Test**:
- BLPOP wake race: multiple clients blocked, one LPUSH → exactly one client unblocked
- DEL during iteration: SMEMBERS while DEL happens (should return consistent snapshot or error)
- Expiry race: GET on key that's expiring (atomic check-and-return)

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Cross-shard MGET performance degradation | Medium | Medium | Benchmark before/after, optimize hot path |
| Memory growth from Hashes/Sets | Low | Medium | Document memory model, add MEMORY commands later |
| Incompatible error messages | Medium | High | Test against real Redis, use exact error strings |
| Partial failure in MSET | Medium | Medium | Document behavior, return error count |

## Migration Plan

No migration needed - additive changes only. Existing data unaffected.

## Rollback Plan

Revert commit and redeploy. No data migration required.

## Open Questions

1. Should MSET be atomic within each shard (all-or-nothing per shard)?
   - **Proposed**: Yes, atomic per shard, best-effort across shards

2. Should we implement SCAN/HSCAN/SSCAN for iteration?
   - **Proposed**: Defer to Phase 5, not critical for common use cases

3. How to handle RENAME when source and dest are on different shards?
   - **Proposed**: GET from source shard, SET to dest shard, DEL from source (not atomic)
