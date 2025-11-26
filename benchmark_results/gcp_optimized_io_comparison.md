# GCP Optimized I/O Benchmark Results (2025-11-26)

## Test Configuration
- **Server**: GCP instance-20251125-121024 (asia-southeast2-a)
- **mikkadb**: 2 shards, port 6379, **optimize-io-and-zero-copy branch**
- **Redis**: v8.0.2, port 6380
- **Test params**: 4 threads, 50 clients, 10:1 read/write ratio, 10K requests

## Optimizations Implemented
- **Vectored I/O (writev)**: Batch multiple writes into single syscall
- **Zero-copy command parsing**: Minimize allocations during RESP parsing
- **Engine batch processing**: Process pipelined commands efficiently

## Results Summary

### Throughput Comparison (ops/sec)

| Pipeline Depth | mikkadb (optimized) | mikkadb (previous) | Improvement | Redis 8.0.2 | vs Redis |
|----------------|--------------------:|-------------------:|------------:|------------:|----------:|
| 1 (no pipeline)| **169,647**        | 164,824            | **+2.9%**   | 114,952     | **+47.6%** ✅ |
| 5              | **556,779**        | 435,748            | **+27.8%**  | 472,471     | **+17.8%** ✅ |
| 10             | **673,065**        | 659,788            | **+2.0%**   | 756,473     | -11.0% ❌ |

### Detailed Results

#### mikkadb (optimized) - Port 6379
```
Pipeline 1:  169,647 ops/sec (baseline)
Pipeline 5:  556,779 ops/sec  (3.28x vs pipeline=1)
Pipeline 10: 673,065 ops/sec  (3.97x vs pipeline=1)
```

#### mikkadb (previous) - Port 6379
```
Pipeline 1:  164,824 ops/sec (baseline)
Pipeline 5:  435,748 ops/sec  (2.64x vs pipeline=1)
Pipeline 10: 659,788 ops/sec  (4.00x vs pipeline=1)
```

#### Redis 8.0.2 - Port 6380
```
Pipeline 1:  114,952 ops/sec (baseline)
Pipeline 5:  472,471 ops/sec  (4.11x vs pipeline=1)
Pipeline 10: 756,473 ops/sec  (6.58x vs pipeline=1)
```

## Analysis

### Optimization Impact

1. **Pipeline=1 (No pipelining)**:
   - Modest +2.9% improvement
   - mikkadb still 47.6% faster than Redis
   - Vectored I/O has less impact with single commands

2. **Pipeline=5 (Moderate batching)**:
   - **Massive +27.8% improvement** ⭐
   - Now 17.8% faster than Redis (was -7.8% slower)
   - Vectored I/O batching shows clear benefit
   - This is the sweet spot for the optimizations

3. **Pipeline=10 (Heavy batching)**:
   - Modest +2.0% improvement
   - Still 11% slower than Redis
   - Better stability (no more performance drops)
   - Room for further optimization

### Key Insights

1. **Vectored I/O Impact**:
   - Biggest gains at pipeline=5 (+27.8%)
   - Reduces syscall overhead for batched writes
   - Most effective in moderate pipelining scenarios

2. **Competitive Position**:
   - ✅ **Wins at pipeline=1**: 47.6% faster than Redis
   - ✅ **Wins at pipeline=5**: 17.8% faster than Redis (NEW!)
   - ❌ **Loses at pipeline=10**: 11% slower than Redis

3. **Stability Improvement**:
   - Previous pipeline=10 showed inconsistency (660K → 319K drop)
   - Optimized version stable around 673K ops/sec
   - More predictable performance

### Performance Scaling

**mikkadb (optimized)**:
- 1→5: 3.28x improvement
- 1→10: 3.97x improvement

**Redis**:
- 1→5: 4.11x improvement
- 1→10: 6.58x improvement

Redis still scales better with deep pipelining, but the gap is narrowing.

## Recommendations

**Use mikkadb (optimized) when**:
- Low to moderate pipelining (depth ≤ 5)
- Single-request latency critical
- Up to 47.6% faster than Redis at baseline
- Up to 17.8% faster than Redis at pipeline=5

**Use Redis when**:
- Deep pipelining (depth ≥ 10)
- Maximum throughput for batch processing
- 11% faster than mikkadb at pipeline=10

## Next Steps

1. ✅ Vectored I/O implemented - **significant gains at pipeline=5**
2. ✅ Zero-copy parsing implemented - **improved stability**
3. 🔄 Further optimize pipeline=10 performance
   - Investigate why Redis scales better at deep pipelining
   - Consider adaptive buffering strategies
   - Profile CPU usage during pipeline=10 workloads
4. 📊 Test with higher concurrency (threads/clients)
5. 🔍 Analyze Redis pipeline implementation for insights

## Technical Details

**Branch**: `optimize-io-and-zero-copy`

**Key Changes**:
- Vectored I/O using `writev` for batching multiple RESP writes
- Zero-copy command parsing to reduce allocations
- Engine batch processing for pipelined commands

**Build Info**:
```bash
cd ~/mikkadb-rust
git checkout optimize-io-and-zero-copy
cargo build --release
```

---

**Test Date**: 2025-11-26
**Environment**: GCP asia-southeast2-a, Ubuntu 25.10
**Branch**: optimize-io-and-zero-copy
