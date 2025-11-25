## ADDED Requirements

### Requirement: Response Batching
The server SHALL batch multiple responses before flushing to the TCP socket to minimize write system calls.

#### Scenario: Pipelined Response Coalescing
- **GIVEN** a client sends multiple pipelined commands (P > 1)
- **WHEN** the server processes these commands
- **THEN** responses are accumulated in a batch
- **AND** the batch is flushed in a single write operation using vectored I/O

#### Scenario: Non-Pipelined Fallback
- **GIVEN** a client sends commands without pipelining (P = 1)
- **WHEN** the server processes a single command
- **THEN** the response is sent immediately without batching
- **AND** performance matches existing single-response code path

#### Scenario: Flush Before Blocking Command
- **GIVEN** a batch of responses is pending
- **WHEN** a blocking command (BLPOP, XREAD BLOCK) is about to be processed
- **THEN** all pending responses are flushed to the client
- **AND** the blocking command is processed only after flush completes

#### Scenario: Flush Before Transaction Completion
- **GIVEN** a batch of responses is pending
- **WHEN** an EXEC command is about to be processed
- **THEN** all pending responses are flushed to the client
- **AND** the transaction result is returned separately

#### Scenario: Flush Before Pub/Sub Mode Change
- **GIVEN** a batch of responses is pending
- **WHEN** a SUBSCRIBE or PSUBSCRIBE command is about to be processed
- **THEN** all pending responses are flushed to the client
- **AND** the connection enters pub/sub mode

#### Scenario: Size-Based Flush Trigger
- **GIVEN** responses are being accumulated in a batch
- **WHEN** the total batch size exceeds 16KB
- **THEN** the batch is flushed immediately
- **AND** subsequent responses start a new batch

#### Scenario: FIFO Ordering Guarantee
- **GIVEN** commands C1, C2, C3 are processed in order
- **WHEN** responses R1, R2, R3 are batched and flushed
- **THEN** the client receives responses in exact order: R1, R2, R3
- **AND** no response reordering occurs

#### Scenario: Connection Close Cleanup
- **GIVEN** responses are pending in a batch
- **WHEN** the client connection is closed
- **THEN** pending responses are flushed before connection cleanup
- **AND** no responses are lost

### Requirement: Vectored I/O
The server SHALL use vectored I/O (writev) to send multiple response buffers in a single system call.

#### Scenario: Multiple Response Buffers
- **GIVEN** a batch contains N responses
- **WHEN** the batch is flushed
- **THEN** all N responses are serialized independently
- **AND** sent using a single writev() syscall with N IoSlice buffers

#### Scenario: Zero-Copy Transmission
- **GIVEN** serialized response buffers
- **WHEN** writev is used for transmission
- **THEN** no additional memory copies occur beyond serialization
- **AND** buffers are sent directly from memory

## MODIFIED Requirements

### Requirement: Buffered I/O
The server SHALL buffer outgoing responses to the TCP connection to minimize system calls.

#### Scenario: Buffered Writes
- **WHEN** the server sends a response
- **THEN** the data is written to an internal buffer
- **AND** the buffer is flushed according to batching policy (not per-command)

#### Scenario: Batched Flush
- **WHEN** multiple responses are ready
- **THEN** they are accumulated in a batch
- **AND** flushed together in a single operation
- **AND** syscall overhead is minimized
