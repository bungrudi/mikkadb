# Add Common Redis Commands

## Why

MikkaDB currently supports only basic Redis commands (GET, SET, INCR, Lists, Sorted Sets, Pub/Sub). Production applications commonly use additional commands for key management (DEL, EXISTS, EXPIRE, TTL), batch operations (MGET, MSET), and data structures (Hashes, Sets). Without these, MikkaDB cannot serve as a drop-in Redis replacement for typical workloads.

## What Changes

### Phase 1: Single-Key Commands (No cross-shard complexity)
- **DEL** - Delete one or more keys
- **EXISTS** - Check if keys exist
- **EXPIRE** - Set key expiration in seconds
- **PEXPIRE** - Set key expiration in milliseconds
- **TTL** - Get remaining time-to-live in seconds
- **PTTL** - Get remaining time-to-live in milliseconds
- **PERSIST** - Remove expiration from key
- **RENAME** - Rename a key
- **DECR** - Decrement integer value
- **DECRBY** - Decrement by specific amount
- **INCRBY** - Increment by specific amount
- **APPEND** - Append string to value
- **STRLEN** - Get string length
- **GETEX** - Get and set expiration atomically
- **SETNX** - Set if not exists
- **SETEX** - Set with expiration (seconds)

### Phase 2: Hash Commands (Single-shard, new data structure)
- **HSET** - Set hash field(s)
- **HGET** - Get hash field value
- **HMSET** - Set multiple hash fields (deprecated but common)
- **HMGET** - Get multiple hash field values
- **HGETALL** - Get all fields and values
- **HDEL** - Delete hash field(s)
- **HEXISTS** - Check if hash field exists
- **HKEYS** - Get all field names
- **HVALS** - Get all values
- **HLEN** - Get number of fields
- **HINCRBY** - Increment hash field integer
- **HINCRBYFLOAT** - Increment hash field float

### Phase 3: Set Commands (Single-shard, new data structure)
- **SADD** - Add members to set
- **SREM** - Remove members from set
- **SMEMBERS** - Get all members
- **SISMEMBER** - Check membership
- **SCARD** - Get set cardinality
- **SPOP** - Remove and return random member(s)
- **SRANDMEMBER** - Get random member(s)

### Phase 4: Multi-Key Commands (Cross-shard coordination required)
- **MGET** - Get multiple keys (fan-out to shards, merge results)
- **MSET** - Set multiple keys (fan-out to shards)
- **DEL** with multiple keys - Delete across shards
- **EXISTS** with multiple keys - Count across shards
- **SUNION** - Union of sets (if sets on different shards)
- **SINTER** - Intersection of sets (if sets on different shards)
- **SDIFF** - Difference of sets (if sets on different shards)

## Cross-Shard Atomicity Design

### Problem
In the thread-per-shard architecture, each shard owns a subset of keys (determined by hash). Multi-key commands like MGET, MSET, DEL may span multiple shards.

### Solution Options

#### Option A: Fan-Out Pattern (Recommended for Phase 4)
```
Client → Connection Handler → Fan-out to relevant shards → Merge responses
```
- MGET: Send GET to each shard, merge results in order
- MSET: Send SET to each shard, wait for all ACKs
- DEL: Send DEL to each shard, sum deleted counts

**Pros**: Simple, no global lock, good performance
**Cons**: Not truly atomic (partial failures possible)

#### Option B: Coordinator Pattern (Future consideration)
- Designate one shard as coordinator for multi-key ops
- Two-phase commit for atomicity

**Pros**: True atomicity
**Cons**: Complex, performance overhead, single point of coordination

### Recommendation
Implement Option A (Fan-Out) for Phase 4. Most applications tolerate eventual consistency for batch operations. Document that multi-key operations are not atomic across shards.

## Impact

- **Affected specs**: commands (new capability)
- **Affected code**:
  - `src/command.rs` - Add new command variants
  - `src/engine.rs` - Add command handlers
  - `src/db.rs` - Add Hash and Set data structures
  - `src/main.rs` - Add cross-shard fan-out logic for Phase 4
- **New tests**: ~60 integration tests for 90% coverage

## Test Coverage Strategy (Target: 90%)

### Test Categories

1. **Unit Tests** (`src/db.rs`, `src/command.rs`)
   - Data structure operations (Hash, Set)
   - Command parsing
   - Edge cases (empty keys, negative values, overflow)

2. **Integration Tests** (`tests/`)
   - Single-shard command behavior
   - Multi-shard fan-out correctness
   - Error handling and edge cases
   - Redis protocol compliance

3. **Cross-Shard Tests** (Phase 4)
   - MGET with keys on different shards
   - MSET atomicity (or documented non-atomicity)
   - DEL counting across shards

### Test Matrix

| Command | Happy Path | Edge Cases | Error Cases | Cross-Shard |
|---------|------------|------------|-------------|-------------|
| DEL | 1 key, multi-key | Non-existent key | Wrong type | Yes |
| EXISTS | 1 key, multi-key | Non-existent | - | Yes |
| EXPIRE/TTL | Set, Get, Persist | Already expired, Non-existent | Wrong type | No |
| HSET/HGET | Single field, Multi-field | Empty hash, Large values | Wrong type | No |
| SADD/SMEMBERS | Single member, Multi | Empty set, Duplicates | Wrong type | No |
| MGET/MSET | All exist, Some exist | Empty, Large batch | - | Yes |

### Coverage Metrics
- Line coverage target: 90%
- Branch coverage target: 85%
- All Redis error responses must match official Redis behavior
