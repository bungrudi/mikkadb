## ADDED Requirements
### Requirement: Buffered I/O
The server SHALL buffer outgoing responses to the TCP connection to minimize system calls.

#### Scenario: Buffered Writes
- **WHEN** the server sends a response
- **THEN** the data is written to an internal buffer
- **AND** the buffer is flushed to the socket at the end of command processing
