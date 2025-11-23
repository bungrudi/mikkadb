## ADDED Requirements

### Requirement: Share-Nothing Execution Model
The system SHALL execute commands in a "Share-Nothing" model where multiple `Engine` actors run independently.

#### Scenario: Parallel Execution
- **WHEN** multiple `Engine` actors are running
- **THEN** they SHALL process commands concurrently without sharing memory or acquiring global locks.

### Requirement: Connection-Based Routing
The system SHALL route client connections to `Engine` shards based on a connection balancing strategy (e.g., Round-Robin).

#### Scenario: Connection Assignment
- **WHEN** a new client connects
- **THEN** it SHALL be assigned to a specific `Engine` shard for the duration of its connection.
- **AND** all commands from that client SHALL be processed by the assigned `Engine`.

### Requirement: Active-Active Replication
The system SHALL replicate write operations to all `Engine` shards to ensure eventual consistency.

#### Scenario: Write Broadcast
- **WHEN** a write command (e.g., `SET`) is executed on Shard A
- **THEN** Shard A SHALL update its local state
- **AND** Shard A SHALL broadcast the command to all other shards (B, C, etc.)
- **AND** Shards B and C SHALL apply the update asynchronously.

#### Scenario: Read Isolation
- **WHEN** a read command (e.g., `GET`) is received by Shard A
- **THEN** Shard A SHALL execute it locally without communicating with other shards.
