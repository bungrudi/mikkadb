## ADDED Requirements

### Requirement: Command Batching
The server SHALL read and parse multiple commands from the socket buffer before processing to minimize read system calls.

#### Scenario: Pipelined Command Batching
- **GIVEN** a client sends multiple pipelined commands (P > 1)
- **WHEN** the server performs an initial blocking read
- **THEN** the server drains the socket buffer non-blocking
- **AND** parses all available commands into a batch
- **AND** processes the entire batch before the next blocking read

#### Scenario: Non-Blocking Buffer Parsing
- **GIVEN** commands are available in the internal buffer
- **WHEN** the server attempts to parse commands
- **THEN** parsing occurs without additional I/O operations
- **AND** the buffer is drained until incomplete or limit reached

#### Scenario: Incomplete Command Handling
- **GIVEN** a partial command in the buffer (TCP packet boundary)
- **WHEN** the server attempts to parse
- **THEN** parsing returns Incomplete status
- **AND** the partial data remains in buffer
- **AND** the next blocking read completes the command

#### Scenario: Batch Size Limits
- **GIVEN** the server is accumulating commands in a batch
- **WHEN** the batch reaches 1024 commands OR 4MB total bytes
- **THEN** the server stops accumulating
- **AND** processes the current batch
- **AND** remaining commands go into the next batch

#### Scenario: Fairness Budget Per Tick
- **GIVEN** a connection is processing a large command batch
- **WHEN** 128 commands have been parsed in the current tick
- **THEN** the server yields to the event loop
- **AND** other connections get CPU time
- **AND** parsing resumes in the next tick

#### Scenario: Malformed Command in Batch
- **GIVEN** a batch contains both valid and malformed commands
- **WHEN** a malformed command is detected during parsing
- **THEN** an error response is generated for that command
- **AND** parsing stops for this batch
- **AND** the connection remains open (not closed)

#### Scenario: DoS Prevention
- **GIVEN** a malicious client sends 10,000 commands
- **WHEN** the server applies batch limits
- **THEN** only the first 1024 commands are batched
- **AND** subsequent commands are processed in later batches
- **AND** server memory remains bounded

### Requirement: Integration with Response Batching
The server SHALL coordinate command batching with response batching for optimal syscall efficiency.

#### Scenario: Complete Read-Process-Write Batch
- **GIVEN** multiple pipelined commands
- **WHEN** commands are batched and processed
- **THEN** responses are also batched (from Phase 1)
- **AND** all responses are flushed together
- **AND** total syscalls are minimized (1 read + 1 write per batch)

#### Scenario: Flush Trigger Preservation
- **GIVEN** a batch contains a blocking command (BLPOP)
- **WHEN** the server processes the batch
- **THEN** responses before the blocking command are flushed
- **AND** Phase 1 flush trigger logic is preserved

## MODIFIED Requirements

### Requirement: Buffered I/O
The server SHALL buffer both incoming commands and outgoing responses to minimize system calls.

#### Scenario: Bidirectional Buffering
- **WHEN** the server receives pipelined commands
- **THEN** commands are accumulated in a read batch
- **AND** responses are accumulated in a write batch (Phase 1)
- **AND** both batches are flushed together per pipeline cycle
