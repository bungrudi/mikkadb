## ADDED Requirements

### Requirement: Key Management Commands

The system SHALL support Redis key management commands for deleting keys, checking existence, and managing expiration.

#### Scenario: DEL removes keys and returns count
- **WHEN** client sends `DEL key1 key2 key3`
- **THEN** system deletes all specified keys from any data structure
- **AND** returns integer count of keys that were deleted

#### Scenario: DEL on non-existent key returns 0
- **WHEN** client sends `DEL nonexistent`
- **THEN** system returns `0`

#### Scenario: EXISTS returns count of existing keys
- **WHEN** client sends `EXISTS key1 key2 key3`
- **THEN** system returns integer count of keys that exist

#### Scenario: EXISTS single key returns 1 or 0
- **WHEN** client sends `EXISTS mykey`
- **THEN** system returns `1` if key exists, `0` otherwise

#### Scenario: EXPIRE sets key expiration
- **WHEN** client sends `EXPIRE mykey 10`
- **THEN** system sets key to expire in 10 seconds
- **AND** returns `1` on success, `0` if key doesn't exist

#### Scenario: TTL returns remaining time
- **WHEN** client sends `TTL mykey`
- **THEN** system returns remaining seconds until expiration
- **OR** returns `-1` if key exists but has no expiration
- **OR** returns `-2` if key does not exist

#### Scenario: PERSIST removes expiration
- **WHEN** client sends `PERSIST mykey`
- **THEN** system removes expiration from key
- **AND** returns `1` on success, `0` if key doesn't exist or had no expiry

### Requirement: String Manipulation Commands

The system SHALL support Redis string manipulation commands for incrementing, decrementing, and modifying string values.

#### Scenario: DECR decrements integer value
- **WHEN** client sends `DECR counter`
- **THEN** system decrements value by 1
- **AND** returns new integer value

#### Scenario: DECR on non-existent key starts from 0
- **WHEN** client sends `DECR newkey`
- **THEN** system sets value to -1 and returns `-1`

#### Scenario: DECRBY decrements by specified amount
- **WHEN** client sends `DECRBY counter 5`
- **THEN** system decrements value by 5
- **AND** returns new integer value

#### Scenario: INCRBY increments by specified amount
- **WHEN** client sends `INCRBY counter 10`
- **THEN** system increments value by 10
- **AND** returns new integer value

#### Scenario: APPEND adds to string value
- **WHEN** client sends `APPEND mykey "world"`
- **THEN** system appends "world" to existing value
- **AND** returns length of new string

#### Scenario: STRLEN returns string length
- **WHEN** client sends `STRLEN mykey`
- **THEN** system returns length of string value
- **OR** returns `0` if key doesn't exist

#### Scenario: SETNX only sets if key doesn't exist
- **WHEN** client sends `SETNX mykey "value"`
- **THEN** system sets key only if it doesn't already exist
- **AND** returns `1` if set, `0` if key already existed

#### Scenario: SETEX sets with expiration
- **WHEN** client sends `SETEX mykey 60 "value"`
- **THEN** system sets key with value and 60-second expiration
- **AND** returns `OK`

### Requirement: Multi-Key Batch Commands

The system SHALL support Redis batch commands for operating on multiple keys efficiently.

#### Scenario: MGET returns values for multiple keys
- **WHEN** client sends `MGET key1 key2 key3`
- **THEN** system returns array of values in same order
- **AND** returns nil for keys that don't exist

#### Scenario: MSET sets multiple key-value pairs
- **WHEN** client sends `MSET key1 val1 key2 val2`
- **THEN** system sets all key-value pairs
- **AND** returns `OK`

#### Scenario: MGET with keys across shards
- **WHEN** client sends `MGET` with keys distributed across multiple shards
- **THEN** system fans out to relevant shards and merges results
- **AND** returns values in correct order

### Requirement: Hash Data Structure Commands

The system SHALL support Redis hash commands for storing and retrieving field-value pairs within a key.

#### Scenario: HSET creates or updates hash fields
- **WHEN** client sends `HSET myhash field1 value1 field2 value2`
- **THEN** system creates/updates specified fields
- **AND** returns count of fields that were added (not updated)

#### Scenario: HGET retrieves single field value
- **WHEN** client sends `HGET myhash field1`
- **THEN** system returns value of field1
- **OR** returns nil if field doesn't exist

#### Scenario: HMGET retrieves multiple field values
- **WHEN** client sends `HMGET myhash field1 field2 field3`
- **THEN** system returns array of values in same order
- **AND** returns nil for fields that don't exist

#### Scenario: HGETALL returns all fields and values
- **WHEN** client sends `HGETALL myhash`
- **THEN** system returns array of alternating field names and values

#### Scenario: HDEL removes fields from hash
- **WHEN** client sends `HDEL myhash field1 field2`
- **THEN** system removes specified fields
- **AND** returns count of fields that were removed

#### Scenario: HEXISTS checks field existence
- **WHEN** client sends `HEXISTS myhash field1`
- **THEN** system returns `1` if field exists, `0` otherwise

#### Scenario: HKEYS returns all field names
- **WHEN** client sends `HKEYS myhash`
- **THEN** system returns array of all field names

#### Scenario: HVALS returns all values
- **WHEN** client sends `HVALS myhash`
- **THEN** system returns array of all values

#### Scenario: HLEN returns field count
- **WHEN** client sends `HLEN myhash`
- **THEN** system returns number of fields in hash

#### Scenario: HINCRBY increments hash field integer
- **WHEN** client sends `HINCRBY myhash counter 5`
- **THEN** system increments field by 5
- **AND** returns new integer value

#### Scenario: WRONGTYPE error for hash commands on wrong type
- **WHEN** client sends hash command on a string key
- **THEN** system returns `WRONGTYPE Operation against a key holding the wrong kind of value`

### Requirement: Set Data Structure Commands

The system SHALL support Redis set commands for storing unique members.

#### Scenario: SADD adds members to set
- **WHEN** client sends `SADD myset member1 member2 member3`
- **THEN** system adds members to set (ignoring duplicates)
- **AND** returns count of members that were added

#### Scenario: SREM removes members from set
- **WHEN** client sends `SREM myset member1 member2`
- **THEN** system removes specified members
- **AND** returns count of members that were removed

#### Scenario: SMEMBERS returns all members
- **WHEN** client sends `SMEMBERS myset`
- **THEN** system returns array of all members (unordered)

#### Scenario: SISMEMBER checks membership
- **WHEN** client sends `SISMEMBER myset member1`
- **THEN** system returns `1` if member exists, `0` otherwise

#### Scenario: SCARD returns set cardinality
- **WHEN** client sends `SCARD myset`
- **THEN** system returns number of members in set

#### Scenario: SPOP removes and returns random member
- **WHEN** client sends `SPOP myset`
- **THEN** system removes random member and returns it
- **OR** returns nil if set is empty

#### Scenario: SPOP with count removes multiple members
- **WHEN** client sends `SPOP myset 3`
- **THEN** system removes up to 3 random members
- **AND** returns array of removed members

#### Scenario: SRANDMEMBER returns random member without removing
- **WHEN** client sends `SRANDMEMBER myset`
- **THEN** system returns random member without removing it

#### Scenario: WRONGTYPE error for set commands on wrong type
- **WHEN** client sends set command on a string key
- **THEN** system returns `WRONGTYPE Operation against a key holding the wrong kind of value`

### Requirement: Concurrent Operation Safety

The system SHALL handle concurrent operations on the same key from multiple connections safely without data corruption or race conditions.

#### Scenario: Concurrent INCR operations are serialized
- **WHEN** 10 clients send `INCR counter` simultaneously on a key starting at 0
- **THEN** final value equals exactly 10
- **AND** no increments are lost

#### Scenario: Concurrent LPUSH/LPOP maintains list integrity
- **WHEN** client A sends `LPUSH mylist item1 item2 item3`
- **AND** client B sends `LPOP mylist` concurrently
- **THEN** list maintains structural integrity
- **AND** no items are duplicated or lost
- **AND** LPOP returns a valid item or nil

#### Scenario: Concurrent SADD/SREM maintains set integrity
- **WHEN** client A sends `SADD myset member1 member2`
- **AND** client B sends `SREM myset member1` concurrently
- **THEN** set maintains structural integrity
- **AND** member1 is either present or absent (not corrupted)

#### Scenario: Concurrent HSET/HDEL maintains hash integrity
- **WHEN** client A sends `HSET myhash field1 value1`
- **AND** client B sends `HDEL myhash field1` concurrently
- **THEN** hash maintains structural integrity
- **AND** field1 is either present with value1 or absent

#### Scenario: Concurrent ZADD/ZREM maintains sorted set integrity
- **WHEN** client A sends `ZADD myzset 1.0 member1`
- **AND** client B sends `ZREM myzset member1` concurrently
- **THEN** sorted set maintains structural integrity
- **AND** member1 is either present with score 1.0 or absent

#### Scenario: High-contention counter stress test
- **WHEN** 100 concurrent clients each send 100 INCR operations on same key
- **THEN** final value equals exactly 10000
- **AND** no operations fail with errors

#### Scenario: Producer-consumer queue pattern
- **WHEN** 5 producer clients send `RPUSH queue job1..job100` (total 500 jobs)
- **AND** 5 consumer clients send `BLPOP queue 1` concurrently
- **THEN** each job is consumed exactly once
- **AND** no jobs are lost or duplicated

### Requirement: Cross-Shard Operation Semantics

The system SHALL document and handle cross-shard operations with best-effort semantics.

#### Scenario: Multi-key operations fan out to shards
- **WHEN** client sends MGET/MSET/DEL with keys on different shards
- **THEN** system routes requests to appropriate shards
- **AND** merges results correctly

#### Scenario: Partial failure in cross-shard MSET
- **WHEN** one shard fails during MSET
- **THEN** system completes successful shard operations
- **AND** returns error indicating partial failure
- **AND** does not roll back successful operations
