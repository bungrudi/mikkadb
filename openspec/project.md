# Project Context

## Purpose
`mikkadb-rust` is a high-performance, Redis-compatible database implemented in Rust. It is designed to replicate core Redis functionalities, including string operations, lists, sorted sets, streams, pub/sub, transactions, and replication. The project aims to provide a robust and compliant implementation of the Redis serialization protocol (RESP) and command set, suitable for educational purposes and as a reference implementation for building Redis-like systems in Rust.

## Tech Stack
- **Language:** Rust (Edition 2021+)
- **Runtime:** Tokio (Asynchronous I/O, Actor-like concurrency)
- **Core Libraries:**
    - `anyhow` (Error handling)
    - `bytes` (Zero-copy byte manipulation)
    - `hex` (Encoding/decoding)
- **Build Tool:** Cargo

## Project Conventions

### Code Style
- **Formatting:** Follow standard Rust formatting conventions (`rustfmt`).
- **Naming:**
    - `PascalCase` for structs, enums, and traits (e.g., `RedisCommand`, `Engine`).
    - `snake_case` for functions, methods, variables, and modules (e.g., `handle_command`, `read_value`).
    - `SCREAMING_SNAKE_CASE` for constants.
- **Error Handling:** Use `anyhow::Result` for application-level errors. Specific errors should be descriptive.
- **Async/Await:** Extensive use of `async`/`await` with Tokio for non-blocking I/O and concurrency.

### Architecture Patterns
- **Actor Model:** The core logic resides in the `Engine` struct, which acts as a single-threaded actor managing the database state (`Db`), replication, and pub/sub subscriptions.
- **Message Passing:** Communication between connection handlers (in `main.rs`) and the `Engine` is done via `tokio::sync::mpsc` channels for requests and `tokio::sync::oneshot` channels for responses.
- **Command Pattern:** Redis commands are parsed into a `RedisCommand` enum. The `Engine` matches on these variants to execute logic.
- **RESP Handling:** A dedicated `RespHandler` (in `resp.rs`) abstracts the reading and writing of RESP (Redis Serialization Protocol) data types.
- **Replication:** Implements a Master-Replica model with `PSYNC`, `REPLCONF`, and `WAIT` support. The `Engine` handles propagation of write commands to replicas.

### Testing Strategy
- **Integration Testing:** Uses `spawn_redis_server.sh` to launch the server for external test suites.
- **Verification Scripts:** Python scripts (`verify_*.py`) are used for specific feature verification (e.g., Streams, Lists).
- **Compliance:** Adheres to Redis protocol specifications and behavior (e.g., blocking behavior for `BLPOP`/`XREAD`, transaction isolation).

### Git Workflow
- Main branch contains the stable, working code.
- Feature branches for new commands or architectural changes.
- Commits should be atomic and descriptive.

## Domain Context
- **RESP (Redis Serialization Protocol):** The server communicates exclusively using RESP. Understanding RESP types (Simple Strings, Errors, Integers, Bulk Strings, Arrays) is crucial.
- **Redis Commands:** The system implements a subset of Redis commands. New commands must strictly follow Redis semantic specifications (e.g., return values, error messages, side effects).
- **Concurrency:** While the `Engine` processes commands sequentially (ensuring atomicity), I/O handling is concurrent. Blocking commands (`BLPOP`, `XREAD BLOCK`) must be handled without blocking the main `Engine` loop (using `waiting_list` pattern).

## Important Constraints
- **Single-Threaded Execution:** The `Engine` processes commands one by one to guarantee data consistency without complex locking (mimicking Redis's single-threaded nature for command execution).
- **Blocking Operations:** Blocking commands must be implemented carefully to avoid freezing the entire server. They should park the client request until the condition is met or timeout occurs.
- **Transaction Isolation:** `MULTI`/`EXEC` must ensure commands are executed atomically and isolated from other clients.

## External Dependencies
- No external runtime dependencies (standalone binary).
- Compatible with standard Redis clients (e.g., `redis-cli`).
