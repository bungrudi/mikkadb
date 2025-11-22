## ADDED Requirements
### Requirement: Zero-Copy RESP Parsing
The server SHALL parse RESP messages by slicing the input buffer without unnecessary heap allocations.

#### Scenario: Parse Simple Command Without Copies
- **WHEN** the server receives a simple command like `PING` or `SET key value`
- **THEN** the parser SHALL represent bulk strings and arrays using shared byte storage (e.g., `Bytes`)
- **AND** it SHALL avoid allocating new `String` or `Vec<u8>` values for each token, except where a textual `String` is required by downstream logic.

#### Scenario: Serialize Response Without Large Intermediate Buffers
- **WHEN** the server sends a response to a client
- **THEN** the serializer SHALL write directly into an output buffer (or buffered writer)
- **AND** it SHALL avoid building large intermediate `Vec<u8>` buffers representing the entire response.
