## ADDED Requirements
### Requirement: Shared-State Concurrency Model
The server SHALL execute Redis commands directly in client tasks against a shared in-memory database guarded by a reader-writer lock.

#### Scenario: Concurrent Reads
- **WHEN** two or more clients issue read-only commands (e.g., `GET`, `ZRANGE`) concurrently
- **THEN** the server SHALL allow those commands to execute in parallel without blocking each other
- **AND** the shared database state SHALL remain consistent.

#### Scenario: Serialized Writes
- **WHEN** one or more clients issue write commands (e.g., `SET`, `RPUSH`, `ZADD`)
- **THEN** the server SHALL ensure that each write operation holds an exclusive write lock on the database
- **AND** all writes SHALL appear atomic and totally ordered from the perspective of all clients.

#### Scenario: Replication Stream Ordering
- **WHEN** write commands are executed that must be replicated to followers
- **THEN** the server SHALL append them to a replication stream in the same order they are applied to the database
- **AND** followers SHALL observe writes in that order.
