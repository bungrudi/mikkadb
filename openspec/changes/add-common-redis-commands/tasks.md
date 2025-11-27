# Implementation Tasks

## Phase 1: Single-Key Commands (Priority: HIGH)

### 1.1 Key Management Commands
- [ ] 1.1.1 Add `Del` command variant to `command.rs`
- [ ] 1.1.2 Implement `del()` in `db.rs` - delete key from any data structure
- [ ] 1.1.3 Add `Del` handler in `engine.rs` with type-aware deletion
- [ ] 1.1.4 Add `Exists` command variant to `command.rs`
- [ ] 1.1.5 Implement `exists()` in `db.rs` - check across all data structures
- [ ] 1.1.6 Add `Exists` handler in `engine.rs`
- [ ] 1.1.7 Write tests for DEL (single key, multi-key same shard, non-existent)
- [ ] 1.1.8 Write tests for EXISTS (single key, multi-key, non-existent)

### 1.2 Expiration Commands
- [ ] 1.2.1 Add `Expire`, `PExpire`, `Ttl`, `PTtl`, `Persist` command variants
- [ ] 1.2.2 Implement `expire()` in `db.rs` - set expiration on existing key
- [ ] 1.2.3 Implement `ttl()` in `db.rs` - get remaining TTL
- [ ] 1.2.4 Implement `persist()` in `db.rs` - remove expiration
- [ ] 1.2.5 Add handlers in `engine.rs` for all expiration commands
- [ ] 1.2.6 Ensure TTL returns -2 for non-existent, -1 for no expiry
- [ ] 1.2.7 Write tests for EXPIRE/TTL lifecycle
- [ ] 1.2.8 Write tests for PEXPIRE/PTTL (millisecond precision)
- [ ] 1.2.9 Write tests for PERSIST

### 1.3 String Commands
- [ ] 1.3.1 Add `Decr`, `DecrBy`, `IncrBy` command variants
- [ ] 1.3.2 Implement `decr()`, `decrby()`, `incrby()` in `db.rs`
- [ ] 1.3.3 Add `Append`, `StrLen` command variants
- [ ] 1.3.4 Implement `append()`, `strlen()` in `db.rs`
- [ ] 1.3.5 Add `SetNx`, `SetEx`, `GetEx` command variants
- [ ] 1.3.6 Implement `setnx()`, `setex()`, `getex()` in `db.rs`
- [ ] 1.3.7 Add handlers in `engine.rs`
- [ ] 1.3.8 Write tests for DECR/DECRBY (including underflow)
- [ ] 1.3.9 Write tests for INCRBY
- [ ] 1.3.10 Write tests for APPEND/STRLEN
- [ ] 1.3.11 Write tests for SETNX (exists vs not exists)
- [ ] 1.3.12 Write tests for SETEX/GETEX

### 1.4 Rename Command
- [ ] 1.4.1 Add `Rename` command variant
- [ ] 1.4.2 Implement `rename()` in `db.rs` - single shard case
- [ ] 1.4.3 Add handler in `engine.rs`
- [ ] 1.4.4 Write tests for RENAME (same type, non-existent source)

## Phase 2: Hash Commands (Priority: HIGH)

### 2.1 Hash Data Structure
- [ ] 2.1.1 Add `hashes: HashMap<String, HashMap<String, Bytes>>` to `Db` struct
- [ ] 2.1.2 Update `key_type()` to detect hash type
- [ ] 2.1.3 Update `del()` to handle hash cleanup

### 2.2 Hash Write Commands
- [ ] 2.2.1 Add `HSet`, `HMSet`, `HDel` command variants
- [ ] 2.2.2 Implement `hset()` in `db.rs` - create/update fields
- [ ] 2.2.3 Implement `hdel()` in `db.rs` - delete fields
- [ ] 2.2.4 Add handlers in `engine.rs`
- [ ] 2.2.5 Write tests for HSET (single field, multi-field, overwrite)
- [ ] 2.2.6 Write tests for HDEL (single field, multi-field)

### 2.3 Hash Read Commands
- [ ] 2.3.1 Add `HGet`, `HMGet`, `HGetAll`, `HKeys`, `HVals`, `HLen`, `HExists` variants
- [ ] 2.3.2 Implement `hget()`, `hmget()`, `hgetall()` in `db.rs`
- [ ] 2.3.3 Implement `hkeys()`, `hvals()`, `hlen()`, `hexists()` in `db.rs`
- [ ] 2.3.4 Add handlers in `engine.rs`
- [ ] 2.3.5 Write tests for HGET/HMGET (exists, not exists, wrong type)
- [ ] 2.3.6 Write tests for HGETALL/HKEYS/HVALS
- [ ] 2.3.7 Write tests for HLEN/HEXISTS

### 2.4 Hash Numeric Commands
- [ ] 2.4.1 Add `HIncrBy`, `HIncrByFloat` command variants
- [ ] 2.4.2 Implement `hincrby()`, `hincrbyfloat()` in `db.rs`
- [ ] 2.4.3 Add handlers in `engine.rs`
- [ ] 2.4.4 Write tests for HINCRBY (new field, existing, non-numeric)
- [ ] 2.4.5 Write tests for HINCRBYFLOAT

## Phase 3: Set Commands (Priority: MEDIUM)

### 3.1 Set Data Structure
- [ ] 3.1.1 Add `sets: HashMap<String, HashSet<Bytes>>` to `Db` struct
- [ ] 3.1.2 Update `key_type()` to detect set type
- [ ] 3.1.3 Update `del()` to handle set cleanup

### 3.2 Set Mutation Commands
- [ ] 3.2.1 Add `SAdd`, `SRem`, `SPop` command variants
- [ ] 3.2.2 Implement `sadd()` in `db.rs` - add members, return count of new
- [ ] 3.2.3 Implement `srem()` in `db.rs` - remove members, return count removed
- [ ] 3.2.4 Implement `spop()` in `db.rs` - remove random member(s)
- [ ] 3.2.5 Add handlers in `engine.rs`
- [ ] 3.2.6 Write tests for SADD (new set, existing, duplicates)
- [ ] 3.2.7 Write tests for SREM (exists, not exists)
- [ ] 3.2.8 Write tests for SPOP (with and without count)

### 3.3 Set Read Commands
- [ ] 3.3.1 Add `SMembers`, `SIsMember`, `SCard`, `SRandMember` command variants
- [ ] 3.3.2 Implement `smembers()`, `sismember()`, `scard()` in `db.rs`
- [ ] 3.3.3 Implement `srandmember()` in `db.rs`
- [ ] 3.3.4 Add handlers in `engine.rs`
- [ ] 3.3.5 Write tests for SMEMBERS/SISMEMBER
- [ ] 3.3.6 Write tests for SCARD
- [ ] 3.3.7 Write tests for SRANDMEMBER (with and without count)

## Phase 4: Multi-Key Commands (Priority: MEDIUM)

### 4.1 Key Routing Infrastructure
- [ ] 4.1.1 Add `key_to_shard()` utility function
- [ ] 4.1.2 Add cross-shard request/response types to `engine.rs`
- [ ] 4.1.3 Implement fan-out pattern for multi-key operations

### 4.2 MGET/MSET Implementation
- [ ] 4.2.1 Add `MGet`, `MSet` command variants
- [ ] 4.2.2 Implement fan-out logic in connection handler
- [ ] 4.2.3 Implement result merging for MGET (preserve order)
- [ ] 4.2.4 Implement parallel SET for MSET
- [ ] 4.2.5 Write tests for MGET (all same shard, mixed shards)
- [ ] 4.2.6 Write tests for MSET (all same shard, mixed shards)

### 4.3 Multi-Key DEL/EXISTS
- [ ] 4.3.1 Extend DEL handler for cross-shard deletion
- [ ] 4.3.2 Extend EXISTS handler for cross-shard counting
- [ ] 4.3.3 Write tests for multi-key DEL across shards
- [ ] 4.3.4 Write tests for multi-key EXISTS across shards

### 4.4 Cross-Shard Rename
- [ ] 4.4.1 Implement cross-shard RENAME (GET+SET+DEL pattern)
- [ ] 4.4.2 Write tests for RENAME across shards
- [ ] 4.4.3 Document non-atomicity in error cases

## Phase 5: Testing and Coverage (Priority: HIGH)

### 5.1 Unit Test Suite
- [ ] 5.1.1 Create `tests/key_commands_test.rs` with DEL/EXISTS/EXPIRE tests
- [ ] 5.1.2 Create `tests/string_commands_test.rs` with INCR/DECR/APPEND tests
- [ ] 5.1.3 Create `tests/hash_commands_test.rs` with full hash coverage
- [ ] 5.1.4 Create `tests/set_commands_test.rs` with full set coverage
- [ ] 5.1.5 Create `tests/multi_key_test.rs` with MGET/MSET tests

### 5.2 Integration Tests
- [ ] 5.2.1 Add redis-cli based smoke tests for each command
- [ ] 5.2.2 Test WRONGTYPE error responses match Redis exactly
- [ ] 5.2.3 Test edge cases (empty strings, binary data, large values)
- [ ] 5.2.4 Test concurrent operations on same key

### 5.3 Cross-Shard Tests
- [ ] 5.3.1 Multi-shard MGET correctness test
- [ ] 5.3.2 Multi-shard MSET consistency test
- [ ] 5.3.3 Multi-shard DEL count verification
- [ ] 5.3.4 RENAME across shards test

### 5.4 Concurrency and Race Condition Tests
- [ ] 5.4.1 Create `tests/concurrency_test.rs` for race condition testing
- [ ] 5.4.2 Test concurrent INCR: 10 clients × 100 ops = exact final value
- [ ] 5.4.3 Test concurrent LPUSH/LPOP: verify no lost items
- [ ] 5.4.4 Test concurrent SADD/SREM: verify set integrity
- [ ] 5.4.5 Test concurrent HSET/HDEL: verify hash integrity
- [ ] 5.4.6 Test concurrent ZADD/ZREM: verify sorted set integrity
- [ ] 5.4.7 High-contention stress test: 100 clients × 100 INCR ops = 10000
- [ ] 5.4.8 Producer-consumer queue test: 5 producers, 5 consumers, verify exactly-once delivery
- [ ] 5.4.9 Test BLPOP race: multiple clients blocking, single LPUSH wakes exactly one
- [ ] 5.4.10 Test DEL during iteration: concurrent DEL while SMEMBERS/HGETALL running

### 5.5 Coverage Verification
- [ ] 5.4.1 Run `cargo tarpaulin` to measure coverage
- [ ] 5.4.2 Identify and fill coverage gaps
- [ ] 5.4.3 Achieve 90% line coverage for new code
- [ ] 5.4.4 Document coverage report

### 5.5 Performance Validation
- [ ] 5.5.1 Benchmark single-key commands (should match GET/SET perf)
- [ ] 5.5.2 Benchmark hash operations
- [ ] 5.5.3 Benchmark set operations
- [ ] 5.5.4 Benchmark MGET/MSET with varying key counts
- [ ] 5.5.5 Verify no regression in existing command performance

## Completion Criteria

- [ ] All 40+ commands implemented and tested
- [ ] 90% test coverage achieved
- [ ] All tests pass with 4-shard configuration
- [ ] Performance benchmarks show no regression
- [ ] Documentation updated with new command support
