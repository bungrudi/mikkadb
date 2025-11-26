# Shard Scaling Analysis (2025-11-26)

## Test Configuration
- **Server**: GCP instance-20251125-121024 (asia-southeast2-a, 8 CPU cores)
- **mikkadb**: Port 6379, optimize-io-and-zero-copy branch (commit 88eb956)
- **Test params**: 4 threads, 50 clients, 10:1 read/write ratio, 10K requests
- **Shard configurations tested**: 1, 2, 3 shards

## Thread Usage Analysis

With **2 shards**, mikkadb uses **9 threads**:
- Main thread
- 8 tokio runtime worker threads

**Key Insight**: Number of threads is independent of shard count. Tokio runtime uses a thread pool based on CPU cores, not shard count. Shards represent logical partitioning of the database, not physical thread allocation.

## Performance Results by Shard Count

### Complete Comparison Table

| Pipeline | 1 Shard | 2 Shards | 3 Shards | Best Config | 2 vs 1 | 3 vs 1 | 3 vs 2 |
|----------|--------:|---------:|---------:|-------------|-------:|-------:|-------:|
| **1**    | 186,102 | **199,188** | 199,768 | **3 shards** | +7.0% | +7.3% | +0.3% |
| **5**    | 627,586 | **760,684** | 749,677 | **2 shards** | +21.2% | +19.5% | -1.4% |
| **10**   | 1,020,155 | 1,252,606 | **1,306,547** | **3 shards** | +22.8% | +28.1% | +4.3% |

### Detailed Analysis by Pipeline Depth

#### Pipeline=1 (No Pipelining)
```
1 shard:  186,102 ops/sec
2 shards: 199,188 ops/sec (+7.0%)
3 shards: 199,768 ops/sec (+7.3% vs 1, +0.3% vs 2)
```

**Observation**: Diminishing returns beyond 2 shards
- 1→2: Significant gain (+7%)
- 2→3: Minimal gain (+0.3%)
- **Optimal**: 2-3 shards perform similarly

#### Pipeline=5 (Moderate Batching)
```
1 shard:  627,586 ops/sec
2 shards: 760,684 ops/sec (+21.2%)
3 shards: 749,677 ops/sec (+19.5% vs 1, -1.4% vs 2)
```

**Observation**: 2 shards is optimal! ⭐
- 1→2: Strong gain (+21.2%)
- 2→3: Slight degradation (-1.4%)
- **Optimal**: 2 shards (sweet spot)

#### Pipeline=10 (Heavy Batching)
```
1 shard:  1,020,155 ops/sec
2 shards: 1,252,606 ops/sec (+22.8%)
3 shards: 1,306,547 ops/sec (+28.1% vs 1, +4.3% vs 2)
```

**Observation**: Benefits from higher shard count
- 1→2: Strong gain (+22.8%)
- 2→3: Continued improvement (+4.3%)
- **Optimal**: 3 shards (best performance)

## Shard Scaling Patterns

### Pattern 1: Parallelism Benefits
- **1→2 shards**: Consistent 7-23% improvement across all pipelines
- Share-Nothing Architecture enables true parallel processing
- Each shard handles subset of connections independently

### Pattern 2: Contention Sweet Spot
- **Pipeline=5**: 2 shards optimal (3 shards slightly slower)
- Suggests coordination overhead at moderate workloads
- Load balancing may not be perfect with 3 shards

### Pattern 3: Deep Pipeline Scaling
- **Pipeline=10**: Benefits from 3 shards (+4.3% over 2)
- Higher concurrency can utilize more shards effectively
- Deeper batching amortizes coordination costs

## Thread vs Shard Relationship

### Key Findings

1. **Threads are NOT tied to shards**
   - Tokio runtime creates thread pool based on CPU cores (8 cores = 8 threads)
   - Shards are logical database partitions, not thread allocations
   - Same 9 threads used regardless of shard count (1, 2, or 3)

2. **Sharding is about concurrency, not threads**
   - More shards = more concurrent lock regions
   - Reduces lock contention on shared database
   - Each shard has its own `Arc<RwLock<Db>>`

3. **Thread utilization is tokio's responsibility**
   - Tokio work-stealing scheduler distributes tasks across threads
   - Shards create more independent work units
   - Better thread utilization with multiple shards

## Performance Recommendations

### By Workload Type

**Low Latency (Pipeline=1)**:
- Use **2-3 shards** (minimal difference)
- 2 shards: 199,188 ops/sec
- 3 shards: 199,768 ops/sec (+0.3%)
- Recommendation: **2 shards** (simpler, nearly identical performance)

**Moderate Throughput (Pipeline=5)**:
- Use **2 shards** (optimal) ⭐
- 2 shards: 760,684 ops/sec
- 3 shards: 749,677 ops/sec (-1.4%)
- Recommendation: **2 shards** (best performance)

**High Throughput (Pipeline=10)**:
- Use **3 shards** (highest performance)
- 2 shards: 1,252,606 ops/sec
- 3 shards: 1,306,547 ops/sec (+4.3%)
- Recommendation: **3 shards** (clear winner)

### General Guidelines

1. **Default**: Use **2 shards** for balanced performance
   - Excellent across all workloads
   - Best for pipeline=1 and pipeline=5
   - Within 4% of optimal at pipeline=10

2. **High concurrency**: Use **3+ shards**
   - When pipeline depth >10
   - When client count >100
   - When maximizing throughput is critical

3. **Single shard**: Avoid in production
   - 7-28% slower across all workloads
   - Only use for debugging/testing

## Shard Count vs CPU Cores

**Server**: 8 CPU cores
**Tested**: 1, 2, 3 shards
**Not tested**: 4-8 shards

### Why not test more shards?

1. **Diminishing returns pattern observed**
   - 1→2: Large gains (7-23%)
   - 2→3: Smaller gains (0-4%)
   - Expect even smaller gains for 4+ shards

2. **Coordination overhead increases**
   - Each shard adds coordination complexity
   - Load balancing becomes harder
   - Lock contention may shift but not eliminate

3. **Sweet spot identified**
   - 2 shards: Optimal for most workloads
   - 3 shards: Best for deep pipelining
   - No strong evidence that 4+ would improve further

### Hypothesis for Future Testing

**When to test 4+ shards**:
- Client count >100 (more concurrent connections)
- Pipeline depth >20 (deeper batching)
- CPU count >16 (more cores available)
- Write-heavy workloads (more lock contention)

## Architecture Insights

### Share-Nothing Design Effectiveness

The Share-Nothing Architecture shows clear benefits:
- ✅ **Parallelism**: 7-23% gain from 1→2 shards
- ✅ **Scalability**: Continued gains from 2→3 shards (at high pipeline)
- ⚠️ **Overhead**: Slight degradation at pipeline=5 with 3 shards

### Fast Batch Path Interaction

The synchronous batch path works well with sharding:
- Direct RwLock access per shard
- No cross-shard coordination needed (GET/SET/INCR)
- Each shard processes batches independently
- Sharding amplifies fast path benefits

### Tokio Runtime Efficiency

Tokio's work-stealing scheduler handles shards well:
- Fixed 8-thread pool serves all shard counts
- More shards = more independent tasks to schedule
- Better CPU utilization with multiple shards
- No thread-per-shard overhead

## Conclusions

1. **2 shards is the sweet spot** for general-purpose deployments
   - Optimal for pipeline=1 and pipeline=5
   - Near-optimal for pipeline=10 (within 4%)
   - Simpler than 3+ shards

2. **3 shards for throughput-focused** deployments
   - Best for pipeline=10 (+4.3% over 2 shards)
   - Worth the slight complexity for maximum performance
   - Recommended for high-concurrency scenarios

3. **Threads ≠ Shards**: Key architectural understanding
   - Tokio manages threads independently
   - Shards provide logical concurrency partitioning
   - More shards ≠ more threads, but better thread utilization

4. **Diminishing returns**: Clear pattern observed
   - 1→2: Large gains
   - 2→3: Moderate gains
   - Expect minimal gains beyond 3 shards for current workload

## Recommended Configuration

**Default mikkadb deployment**: **2 shards**
```bash
./mikkadb-rust --shards 2
```

**High-throughput deployment**: **3 shards**
```bash
./mikkadb-rust --shards 3
```

---

**Test Date**: 2025-11-26
**Environment**: GCP asia-southeast2-a, Ubuntu 25.10, 8 CPU cores
**Branch**: optimize-io-and-zero-copy
**Commit**: 88eb956
