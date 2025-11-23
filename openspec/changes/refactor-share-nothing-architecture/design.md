## Context
We need to scale Mikkadb to handle high throughput (1M+ ops/sec) on multi-core systems. The previous architectures (Single Actor, Hybrid DashMap) suffered from lock contention or single-threaded bottlenecks. The "Key Sharding" approach was rejected in favor of a simpler "Connection Sharding" model that better fits the "caching" use case where eventual consistency is acceptable.

## Goals
- **Linear Scalability**: Throughput should increase linearly with the number of CPU cores.
- **Zero Lock Contention**: Read operations must not block or contend with other threads.
- **Simplicity**: Architecture should resemble "mini microservices" - independent workers sharing data.

## Non-Goals
- **Strong Consistency**: We accept eventual consistency. A read on Shard A might not immediately see a write from Shard B.
- **Strict Transaction Isolation**: Transactions across shards are not supported (or rely on eventual sync).

## Decisions

### 1. Connection-Based Sharding
**Decision**: Incoming client connections are assigned to a specific `Engine` shard using a Round-Robin strategy (or `client_id % num_shards`).
**Rationale**: This ensures load balancing of connections. Unlike Key Sharding, it simplifies the routing logic (no parsing needed before routing) and allows `MULTI` transactions to stay local to a connection/shard easily.

### 2. Active-Active Replication (Broadcast)
**Decision**: Every `Engine` shard maintains a **full copy** of the dataset.
**Rationale**: This allows any shard to serve any Read request immediately from local memory without cross-shard communication.
**Mechanism**:
- When Shard A executes a **Write** command (e.g., `SET k v`):
  1. It updates its local state.
  2. It broadcasts the command to all other Shards (B, C, D...).
  3. Shards B, C, D receive the command and apply it to their local state.

### 3. Eventual Consistency
**Decision**: Readers may see stale data for a brief window (microseconds/milliseconds) until the broadcast message is processed.
**Rationale**: Acceptable for caching workloads. Allows for extremely high read throughput.

## Risks / Trade-offs
- **Write Amplification**: A write command is executed `N` times (once per shard). This limits total Write throughput to the speed of a single core (roughly).
    - *Mitigation*: This architecture is optimized for Read-Heavy workloads (Caching).
- **Memory Usage**: Data is replicated `N` times.
    - *Mitigation*: Acceptable for current scope; typically caching nodes have ample RAM or we run fewer shards if RAM is tight.

## Architecture Diagram
```mermaid
graph TD
    Client1 -->|Connection| Shard1[Engine 1 (Full DB)]
    Client2 -->|Connection| Shard2[Engine 2 (Full DB)]
    Client3 -->|Connection| Shard3[Engine 3 (Full DB)]

    Shard1 -- Broadcast Write --> Shard2
    Shard1 -- Broadcast Write --> Shard3
    Shard2 -- Broadcast Write --> Shard1
    Shard2 -- Broadcast Write --> Shard3
```
