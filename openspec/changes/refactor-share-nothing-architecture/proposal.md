## Why
The current architecture suffers from single-threaded write bottlenecks in the `Engine` actor or lock contention in the hybrid `DashMap` approach. To achieve linear scalability with CPU cores (1M+ ops/sec) for caching workloads, we need a "Share-Nothing" architecture where each CPU core handles a subset of connections independently.

## What Changes
- **Connection Sharding**: Route incoming connections to a specific `Engine` shard (Round-Robin) rather than routing individual commands based on keys.
- **Active-Active Replication**: Each `Engine` shard maintains a full copy of the dataset (Eventual Consistency).
- **Write Broadcasting**: Write commands executed on one shard are broadcast asynchronously to all other shards to sync state.
- **Read Isolation**: Read commands are served entirely from the local `Engine`'s memory, ensuring zero lock contention and high throughput.
- **State Isolation**: Each `Engine` instance owns a standard `HashMap` (no `DashMap`).

## Impact
- **Affected specs**: Architecture, Performance, Consistency Model
- **Affected code**: `src/engine.rs`, `src/main.rs`
- **BREAKING**: 
    - Consistency model changes from Strong to Eventual.
    - Internal architecture change.
    - High memory usage (data replicated N times).
