## ADDED Requirements

### Requirement: Thread-Per-Shard Architecture
The server SHALL assign each database shard to a dedicated OS thread with its own single-threaded Tokio runtime.

#### Scenario: Shard thread isolation
- **WHEN** the server starts with N shards configured
- **THEN** N+1 OS threads are created (N shard threads + 1 main thread)
- **AND** each shard thread runs independently with its own event loop

#### Scenario: Thread naming
- **WHEN** a shard thread is spawned
- **THEN** the thread is named "shard-{id}" for debugging visibility

### Requirement: SO_REUSEPORT Load Balancing
The server SHALL use SO_REUSEPORT to allow each shard thread to own its own TcpListener on the same port.

#### Scenario: Multiple listeners on same port
- **WHEN** the server starts with 2 shards
- **THEN** 2 TcpListeners are created, both bound to the configured port
- **AND** the kernel distributes incoming connections across listeners

#### Scenario: Connection ownership
- **WHEN** a client connects
- **THEN** the connection is owned entirely by one shard thread
- **AND** no cross-thread communication is required for read operations

### Requirement: Zero-Channel Read Path
The server SHALL execute read commands without channel communication by calling the engine directly within the connection handler's thread.

#### Scenario: Direct GET execution
- **WHEN** a client sends a GET command
- **THEN** the command is executed by calling the engine directly (no mpsc send)
- **AND** the response is written immediately to the connection

#### Scenario: Batch command execution
- **WHEN** a client sends multiple pipelined commands
- **THEN** all commands in the batch are executed directly in sequence
- **AND** responses are batched and written together

### Requirement: Cross-Thread Write Replication
The server SHALL replicate write commands to peer shards using fire-and-forget channel sends.

#### Scenario: Write broadcast
- **WHEN** a write command (SET, DEL, etc.) is executed
- **THEN** the command is broadcast to all peer shard channels
- **AND** the local response is returned immediately without waiting for peer acknowledgment

#### Scenario: Replication reception
- **WHEN** a shard receives a replicated write from a peer
- **THEN** the write is applied to the local database
- **AND** no response is sent back to the originating shard
