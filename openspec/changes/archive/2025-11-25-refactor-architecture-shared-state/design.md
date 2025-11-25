## Context
The single-threaded Actor model with message passing works well for correctness but introduces significant latency (context switching, serialization) and limits throughput to a single core.

## Goals / Non-Goals
- **Goals**: Enable concurrent command execution, reduce latency, increase throughput.
- **Non-Goals**: Change the behavior of Redis commands (except potential concurrency side effects which should be minimal for in-memory DB).

## Decisions
- **Decision**: Use `Arc<tokio::sync::RwLock<Db>>`.
- **Rationale**: Allows multiple readers (GET, etc.) while ensuring exclusive writes. Simpler than lock-free structures but much faster than channels for this workload.
- **Decision**: Handle `Replication` via a dedicated channel from write operations.
- **Rationale**: Replication stream must be serial. We can push to a replication queue *after* acquiring the write lock and mutating DB.

## Risks / Trade-offs
- **Risk**: Deadlocks if we're not careful (though simple commands are atomic).
- **Risk**: Replication ordering if we allow concurrent writes (we won't, RwLock enforces exclusive writes).
- **Trade-off**: Higher complexity in `main.rs` vs simple actor loop.

## Migration Plan
- Rewrite `main.rs` loop.
- Extract `Engine` logic into `Db` or stateless functions.
