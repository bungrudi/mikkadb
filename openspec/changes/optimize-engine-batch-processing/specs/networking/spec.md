## ADDED Requirements

### Requirement: Engine Batch Command Processing
The system SHALL support processing multiple commands in a single Engine request to reduce channel overhead.

#### Scenario: Batch command processing
- **GIVEN** a connection handler with N pipelined commands
- **WHEN** the handler sends a BatchCommandRequest to the Engine
- **THEN** the Engine SHALL process all N commands sequentially
- **AND** return all N responses in a single oneshot message
- **AND** preserve FIFO ordering (response[i] corresponds to command[i])

#### Scenario: Single command optimization
- **GIVEN** a batch containing exactly 1 command
- **WHEN** the batch is processed
- **THEN** the system SHALL process it with minimal overhead
- **AND** return a Vec with exactly 1 response

#### Scenario: Error handling in batch
- **GIVEN** a batch where command[i] produces an error
- **WHEN** the batch is processed
- **THEN** response[i] SHALL contain the error
- **AND** subsequent commands SHALL continue processing
- **AND** the batch response SHALL be complete (N responses for N commands)

### Requirement: Batch Channel Transport
The system SHALL use a single channel message to transport command batches between connection handler and Engine.

#### Scenario: Reduced channel round-trips
- **GIVEN** N pipelined commands from a client
- **WHEN** processed via batch mode
- **THEN** exactly 1 mpsc::send operation SHALL occur (not N)
- **AND** exactly 1 oneshot::recv operation SHALL occur (not N)

#### Scenario: Backward compatibility
- **GIVEN** a single non-pipelined command
- **WHEN** processed by the system
- **THEN** the system SHALL handle it correctly
- **AND** no regression in single-command latency SHALL occur
