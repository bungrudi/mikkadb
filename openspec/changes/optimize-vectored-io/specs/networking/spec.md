## MODIFIED Requirements

### Requirement: Response Batch Writing

The system SHALL write multiple RESP responses efficiently using vectored I/O to minimize memory copies and syscall overhead.

#### Scenario: Pipelined responses with vectored I/O
- **GIVEN** a batch of 10 serialized RESP responses
- **WHEN** `write_batch()` is called
- **THEN** the system uses `write_vectored()` to send all responses in minimal syscalls without intermediate buffer copies

#### Scenario: Single response optimization
- **GIVEN** a batch containing exactly 1 response
- **WHEN** `write_batch()` is called
- **THEN** the system uses direct `write_all()` without vectored I/O overhead

#### Scenario: Partial write handling
- **GIVEN** a vectored write that partially completes
- **WHEN** the kernel returns fewer bytes written than requested
- **THEN** the system advances the IoSlice array and retries until all data is written
