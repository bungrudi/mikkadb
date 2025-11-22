## Context
The RESP parser and serializer currently allocate new `String` and `Vec<u8>` values for almost every token and response. In a high-throughput Redis workload (many small PING/GET/SET commands), these allocations dominate CPU time and increase GC/allocator pressure.

## Goals / Non-Goals
- **Goals**:
  - Reduce heap allocations during parsing and serialization.
  - Reuse buffers and share underlying byte storage where possible.
- **Non-Goals**:
  - Change observable RESP behavior or error messages.
  - Introduce unsafe Rust; the implementation MUST remain safe Rust.

## Decisions
- **Decision**: Use `bytes::Bytes` (or `BytesMut`) for bulk string storage in `Value`.
  - **Rationale**: `Bytes` provides cheap cloning and slicing over shared storage.
- **Decision**: Make parsing functions operate on `&[u8]`/`BytesMut` slices and avoid `to_vec()` + `String::from_utf8` unless a UTF-8 `String` is required by downstream code.
- **Decision**: Introduce helper functions for converting `Bytes` to `String` only at command interpretation boundaries (e.g., command name, key names if necessary).
- **Decision**: Move toward a streaming serializer that writes directly into an output buffer (or `BufWriter`) without building large intermediate `Vec<u8>` values.

## Risks / Trade-offs
- **Risk**: Lifetime/ownership complexity when sharing byte buffers across commands.
  - **Mitigation**: Keep parsed `Value`s tied to the lifetime of the connection buffer and avoid cross-connection sharing.
- **Risk**: Subtle bugs if UTF-8 assumptions are broken.
  - **Mitigation**: Convert to `String` only where the Redis protocol guarantees text (command names) and keep raw bytes elsewhere.

## Migration Plan
- Step 1: Introduce new `Value` variants using `Bytes` while keeping old code paths behind small adapters.
- Step 2: Incrementally migrate parsing and serialization to the new representation.
- Step 3: Remove legacy allocation-heavy paths once tests and benchmarks pass.

## Open Questions
- Should keys and values at the `Db` layer remain `String` for simplicity, or be migrated to `Bytes` as well?
