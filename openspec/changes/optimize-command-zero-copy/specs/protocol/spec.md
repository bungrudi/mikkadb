## ADDED Requirements

### Requirement: Zero-Copy Command Parsing

The command parser SHALL extract field data from the RESP buffer without allocating intermediate String objects, using `bytes::Bytes` slices that reference the original buffer.

#### Scenario: GET command zero-copy parsing
- **GIVEN** a pipelined batch of 10 GET commands
- **WHEN** `RedisCommand::from_resp()` parses each command
- **THEN** the key field is extracted as a `Bytes` slice without String allocation

#### Scenario: SET command zero-copy parsing
- **GIVEN** a SET command with key and value
- **WHEN** `RedisCommand::from_resp()` parses the command
- **THEN** both key and value are extracted as `Bytes` slices without allocation

#### Scenario: String conversion at storage boundary
- **GIVEN** a command with `Bytes` key that needs HashMap lookup
- **WHEN** the key is used for database storage
- **THEN** String conversion happens only once at the storage boundary, not during parsing
