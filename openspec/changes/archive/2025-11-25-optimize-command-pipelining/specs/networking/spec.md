## ADDED Requirements

### Requirement: Command Pipelining
The server SHALL support command pipelining by reading and processing multiple commands before sending responses, reducing syscall overhead and improving throughput.

#### Scenario: Single Command (Non-Pipelined)
- **WHEN** a client sends one command and waits
- **THEN** the server reads the command, processes it, and sends the response
- **AND** behavior is identical to pre-pipelining implementation

#### Scenario: Multiple Commands (Pipelined)
- **WHEN** a client sends multiple commands without waiting for responses
- **THEN** the server reads all available commands from the socket buffer
- **AND** processes each command sequentially maintaining FIFO order
- **AND** buffers all responses
- **AND** flushes all responses in a single batch write operation

#### Scenario: Request-Response Ordering
- **WHEN** commands C1, C2, C3 are received in order
- **THEN** responses R1, R2, R3 are sent in the same order
- **AND** no response interleaving occurs

#### Scenario: Partial Buffer Reads
- **WHEN** the socket buffer contains incomplete command data
- **THEN** the server waits for complete commands before processing
- **AND** does not block other connections

#### Scenario: Syscall Reduction
- **WHEN** N commands are pipelined by the client
- **THEN** the server performs at most 1 read syscall (instead of N)
- **AND** the server performs at most 1 write syscall (instead of N)
- **AND** total syscalls are reduced from 2N to approximately 2 per batch

### Requirement: Backward Compatibility
The server SHALL maintain full compatibility with non-pipelined clients, automatically detecting and handling both modes transparently.

#### Scenario: Mixed Client Types
- **WHEN** both pipelined and non-pipelined clients connect
- **THEN** each client receives correct responses regardless of mode
- **AND** no performance degradation occurs for either client type
