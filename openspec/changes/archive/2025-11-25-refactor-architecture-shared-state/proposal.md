## Why
The current architecture uses an Actor model where client tasks send commands to a single `Engine` task via channels. This results in double context switching and sequential execution, causing high latency (3x vs optimized) and limited throughput.

## What Changes
- Replace the `Engine` actor with a shared `Db` instance wrapped in `Arc<tokio::sync::RwLock<Db>>` (or `parking_lot::RwLock` if IO is minimal).
- Client tasks execute commands directly against the shared DB.
- Use `RwLock` to allow concurrent read operations (`GET`, `ZRANGE`, etc.).
- **BREAKING**: Removes `Engine` struct and `CommandRequest` channel.

## Impact
- Affected specs: `concurrency`
- Affected code: `src/main.rs`, `src/engine.rs`, `src/db.rs`
