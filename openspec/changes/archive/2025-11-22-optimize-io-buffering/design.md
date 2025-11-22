## Context
Redis protocol involves frequent small writes (e.g., `+OK\r\n`). Unbuffered writes trigger a syscall for each write, causing user-kernel mode switches.

## Goals / Non-Goals
- **Goals**: Minimize syscalls by buffering small writes.
- **Non-Goals**: Change the `Value` serialization logic (handled in separate proposal).

## Decisions
- **Decision**: Use `tokio::io::BufWriter` with default capacity (usually 8KB).
- **Rationale**: Standard and efficient wrapper for `AsyncWrite`.

## Risks / Trade-offs
- **Risk**: Delayed responses if flush is not called.
- **Mitigation**: Explicitly call `flush()` after processing each command loop iteration.

## Migration Plan
- Transparent change; no data migration needed.

## Open Questions
- None.
