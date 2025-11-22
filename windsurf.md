# mikkadb-rust – Windsurf Guide

Redis-compatible database implementation in Rust (CodeCrafters challenge style).  
This document is for humans and AI coding assistants working in Windsurf.

---

## 1. Project Overview

- **Language**: Rust (Tokio async)
- **Binary name**: `mikkadb-rust`
- **Goal**: Implement a subset of Redis protocol and behavior, including:
  - Basic commands: `PING`, `ECHO`, `SET`, `GET`, `INFO`
  - Replication handshake: `REPLCONF`, `PSYNC`
  - Streams: `XADD`, `XREAD`
  - Transactions: `MULTI`, `EXEC`, `DISCARD`
  - Replication consistency: `WAIT`
- **External tester**: `../redis-tester` (Go test harness validating Redis semantics)

---

## 2. Important Files & Modules

- **`Cargo.toml`**
  - Rust crate metadata and dependencies: `tokio`, `anyhow`, `bytes`, `hex`.

- **`your_program.sh`**
  - Entry script used by test harnesses.
  - Builds/runs the local binary and passes all CLI args through:
    - `exec ./target/debug/mikkadb-rust "$@"`

- **`src/main.rs`**
  - Application entrypoint.
  - Responsibilities:
    - Parse config via `Config::parse()`.
    - Create the `Engine` actor and spawn its `run()` loop.
    - Bind `TcpListener` on `127.0.0.1:<port>` (default `6379`).
    - Accept client connections and delegate to a `RespHandler`.
    - Convert RESP messages into `RedisCommand` and send them to the engine via `mpsc`.

- **`src/config.rs`**
  - Handles CLI arguments and builds `Config`.
  - Key fields:
    - `port: u16` – default `6379`, override with `--port <PORT>`.
    - `role: ServerRole` – `Master` or `Slave`.
    - Replication params when `--replicaof <host> <port>` is provided:
      - `master_host: Option<String>`
      - `master_port: Option<u16>`
  - Also holds `master_replid` and `master_repl_offset`.

- **`src/resp.rs`**
  - Defines the RESP `Value` enum (SimpleString, BulkString, Integer, Array, Error, Null, etc.).
  - `RespHandler`:
    - `read_value()` – parse incoming RESP from `TcpStream`.
    - `write_value()` – serialize and send RESP responses.
    - `read_rdb_file()` – helper for replication (RDB transfer).

- **`src/command.rs`**
  - `RedisCommand` enum and `from_resp` parser.
  - Maps RESP arrays to concrete commands:
    - `PING`, `ECHO`, `SET`, `GET`, `INFO`, `REPLCONF`, `PSYNC`, `WAIT`, `XADD`, `XREAD`, `MULTI`, `EXEC`, `DISCARD`, etc.
  - Performs command-specific argument validation and error message shaping.

- **`src/db.rs`**
  - In-memory storage engine.
  - Handles:
    - Key/value set/get with optional TTL.
    - Stream structures and operations (`XADD`, `XREAD`).
    - Helper methods used by `Engine` for replication and blocking reads.

- **`src/engine.rs`**
  - Core "actor" responsible for:
    - Processing `CommandRequest` messages from clients.
    - Maintaining replication state:
      - Replica connections, offsets, propagation offsets.
    - Handling blocking operations:
      - `WAIT` (replication acknowledgment).
      - `XREAD` with `BLOCK` (pending stream reads).
    - Managing transactions per client (`MULTI` / `EXEC` / `DISCARD`).

- **`server.log`**
  - Log file in project root.
  - Used to capture:
    - Compiler logs and runtime output.
    - Test harness output from `redis-tester`.

---

## 3. Building and Running the Server

### Build

From repo root (`mikkadb-rust`):

```sh
cargo build
```

### Run directly

```sh
# Default port 6379 as a master
cargo run

# Custom port
cargo run -- --port 6380

# As a replica of another instance
cargo run -- --replicaof localhost 6379
```

### Run via `your_program.sh`

The external tester and some scripts rely on this wrapper:

```sh
./your_program.sh
./your_program.sh --port 6380
./your_program.sh --replicaof localhost 6379
```

---

## 4. Redis Tester Integration (`../redis-tester`)

The `redis-tester` directory lives **next to** this repo:

- Path: `../redis-tester` (relative to `mikkadb-rust` root)
- Language: Go
- Entry script: `test.sh`
  - This script drives the Redis protocol tests against your implementation.
  - It typically shells out to `./your_program.sh` to start the server.

You don’t need to modify `redis-tester`.  
You only need to implement behavior here so those tests pass.

---

## 5. Running Tests and Logging to `server.log`

From the `mikkadb-rust` root:

### Overwrite `server.log` with latest test run

```sh
../redis-tester/test.sh > server.log 2>&1
```

- Captures **both** stdout and stderr.
- Replaces any previous contents of `server.log`.

### Append to `server.log` instead

```sh
../redis-tester/test.sh >> server.log 2>&1
```

After running tests, you can inspect the log:

```sh
less server.log
grep -i "FAIL" server.log
grep -i "panic" server.log
```

AI assistants should **always** look at `server.log` first when diagnosing failing tests.

---

## 6. Typical Debugging Workflow

1. **Run tests & capture logs**

   ```sh
   ../redis-tester/test.sh > server.log 2>&1
   ```

2. **Inspect failures**

   - Search for failing test names or error messages in `server.log`.
   - Identify which Redis command or subsystem is implicated:
     - Basic commands: `src/command.rs`, `src/engine.rs`, `src/db.rs`
     - Streams: `XADD` / `XREAD` paths in `engine.rs` and `db.rs`
     - Replication: `perform_handshake` in `main.rs`, replication code in `engine.rs`
     - Transactions: `MULTI` / `EXEC` / `DISCARD` logic in `engine.rs`

3. **Locate relevant code paths**

   - Search for command keyword (e.g., `"XADD"`, `"WAIT"`) in:
     - `src/command.rs` – parsing layer
     - `src/engine.rs` – behavior and replication
     - `src/db.rs` – data storage semantics

4. **Make minimal, focused changes**

   - Prefer small patches over large refactors.
   - Keep behavior compatible with existing commands and tests.

5. **Re-run tests and compare logs**

   - Use `server.log` before/after diffs when debugging tricky behavior.

---

## 7. Guidelines for AI Assistants

- **Keep file footprint small**
  - Do **not** create extra top-level files unless necessary.
  - Prefer editing existing modules (`main.rs`, `engine.rs`, `command.rs`, `db.rs`, `resp.rs`, `config.rs`).

- **Respect comments and documentation**
  - Do not delete or rewrite existing comments or docblocks unless explicitly asked.
  - You may add short, focused comments if needed for clarity.

- **Testing discipline**
  - When changing core behavior (especially replication, streams, or transactions):
    - Run `../redis-tester/test.sh > server.log 2>&1`.
    - Inspect and summarize any new failures.

- **Error messages and protocol semantics**
  - Match error strings and behaviors expected by the tester as closely as possible.
  - Be careful with RESP encoding (bulk string lengths, arrays, nulls).

- **Performance and blocking behavior**
  - Avoid blocking the Tokio runtime unnecessarily.
  - Use `tokio::time` and channels (already wired in `Engine`) for timeouts and blocking operations (`WAIT`, `XREAD BLOCK`).

---

## 8. Quick Reference Commands

From `mikkadb-rust` root:

```sh
# Build
cargo build

# Run server
cargo run

# Run Redis tests with logging
../redis-tester/test.sh > server.log 2>&1

# Inspect log
less server.log
```

---

## 9. Future Improvements / TODO Ideas (Optional)

- Add more detailed logging around:
  - Replication offsets and `WAIT` behavior.
  - Stream IDs for `XADD` / `XREAD`.
- Expand support for additional Redis commands if required by tests.
- Add lightweight Rust tests (unit/integration) mirroring key `redis-tester` scenarios.
