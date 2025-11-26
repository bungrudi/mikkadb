## Context

Mikkadb uses a "Connection Sharding" architecture where each shard has a full database copy. Reads are local (zero contention), writes are broadcast to all peers. Currently:
- All shards are async tasks on Tokio's shared thread pool
- Connection handlers send commands via `mpsc::channel` to shard tasks
- Responses return via `oneshot::channel`

This creates unnecessary overhead for the common read path.

## Goals / Non-Goals

**Goals:**
- Eliminate channel overhead for read operations
- Predictable thread count: N shards = N+1 threads
- Better cache locality (connection + engine on same core)
- Maintain existing semantics (Active-Active replication, eventual consistency)

**Non-Goals:**
- CPU pinning (future optimization)
- io_uring integration (future optimization)
- Changing replication model

## Decisions

### Decision 1: Dedicated OS thread per shard with `std::thread::spawn`

**Why**: Guarantees shard isolation, predictable thread count, and avoids work-stealing overhead.

**Alternatives considered:**
- `tokio::task::spawn_blocking`: Still shares Tokio's blocking pool
- `tokio::runtime::Builder::new_multi_thread().worker_threads(N)`: Doesn't guarantee 1:1 shard:thread mapping

### Decision 2: Single-threaded Tokio runtime per shard thread

**Why**: Each shard needs async I/O for TCP handling. `current_thread` runtime is optimal for single-threaded workloads.

```rust
std::thread::Builder::new()
    .name(format!("shard-{}", shard_id))
    .spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(shard_main(shard_id, ...));
    });
```

### Decision 3: `SO_REUSEPORT` for kernel load balancing

**Why**: Allows multiple listeners on the same port. Kernel distributes connections across listeners. No central accept loop needed.

```rust
use socket2::{Socket, Domain, Type};

let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
socket.set_reuse_port(true)?;
socket.bind(&addr)?;
socket.listen(1024)?;
let listener = TcpListener::from_std(socket.into())?;
```

**Platform support:**
- Linux 3.9+ (2013): Full support with load balancing
- macOS: Supported (BSD-style, `0x0200`)
- Windows: Not supported (fallback to single listener)

### Decision 4: Thread-local engine reference via `Rc<RefCell<Engine>>`

**Why**: No `Arc` needed since engine is accessed only from one thread. `Rc` + `spawn_local` avoids atomic overhead.

```rust
let engine = Rc::new(RefCell::new(Engine::new(...)));
loop {
    let (stream, _) = listener.accept().await?;
    let engine = engine.clone();
    tokio::task::spawn_local(async move {
        handle_connection(stream, engine).await;
    });
}
```

### Decision 5: Replication via cross-thread channels (unchanged)

**Why**: Write replication must reach all shards. Fire-and-forget `mpsc::send` is acceptable overhead for writes (less frequent than reads).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Main Thread                             │
│  - Spawns shard threads                                      │
│  - Waits for shutdown signal                                 │
└─────────────────────────────────────────────────────────────┘
            │                           │
            ▼                           ▼
┌─────────────────────────┐   ┌─────────────────────────┐
│     Shard Thread 0      │   │     Shard Thread 1      │
│  ┌───────────────────┐  │   │  ┌───────────────────┐  │
│  │ TcpListener:6379  │  │   │  │ TcpListener:6379  │  │
│  │ (SO_REUSEPORT)    │  │   │  │ (SO_REUSEPORT)    │  │
│  └─────────┬─────────┘  │   │  └─────────┬─────────┘  │
│            ▼            │   │            ▼            │
│  ┌───────────────────┐  │   │  ┌───────────────────┐  │
│  │ Conn Handlers     │  │   │  │ Conn Handlers     │  │
│  │ (spawn_local)     │  │   │  │ (spawn_local)     │  │
│  └─────────┬─────────┘  │   │  └─────────┬─────────┘  │
│            ▼            │   │            ▼            │
│  ┌───────────────────┐  │   │  ┌───────────────────┐  │
│  │ Engine (local)    │──┼───┼──│ Engine (local)    │  │
│  │ Db (local)        │  │   │  │ Db (local)        │  │
│  └───────────────────┘  │   │  └───────────────────┘  │
└─────────────────────────┘   └─────────────────────────┘
         ▲                             ▲
         │      Replication (mpsc)     │
         └─────────────────────────────┘
```

## Data Flow

### Read Path (Zero Channel Overhead)
1. Client connects → kernel assigns to shard thread via SO_REUSEPORT
2. Connection handler receives RESP command
3. Handler calls `engine.borrow_mut().execute(cmd)` directly
4. Response returned inline

### Write Path (Minimal Overhead)
1. Same as read path for local execution
2. After local execution, `engine.broadcast_to_peers(cmd)`
3. Fire-and-forget `mpsc::send` to peer channels
4. Peers receive asynchronously and apply

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| SO_REUSEPORT unavailable (Windows) | Fallback to single-listener mode with channel dispatch |
| Uneven connection distribution | Kernel's SO_REUSEPORT has reasonable balancing; acceptable |
| Connection migration not possible | Acceptable for connection-sharding model |
| Increased code complexity | Well-documented, clear separation of concerns |

## Migration Plan

1. Add `socket2` dependency for SO_REUSEPORT
2. Create `shard_main()` function that owns listener + engine
3. Refactor `main()` to spawn dedicated threads
4. Update `Engine` to use `Rc<RefCell<>>` instead of channel-based dispatch
5. Benchmark and validate

## Open Questions

- Should we support a fallback mode for Windows? (Answer: Yes, for compatibility)
- Should we pin threads to specific CPU cores? (Answer: Future optimization, not in this change)
