# GCP Pipeline Benchmark Comparison (2025-11-25)

## Test Configuration
- **Server**: GCP instance-20251125-121024 (asia-southeast2-a)
- **mikkadb**: 2 shards, port 6379
- **Redis**: v8.0.2, port 6380
- **Test params**: 4 threads, 50 clients, 10:1 read/write ratio, 10K requests

## Results Summary

### Throughput (ops/sec)

| Pipeline Depth | mikkadb (2 shards) | Redis 8.0.2 | mikkadb Advantage |
|----------------|--------------------:|-------------:|------------------:|
| 1 (no pipeline)| **164,824**        | 114,952      | **+43.4%** ✅     |
| 5              | 435,748            | **472,471**  | -7.8% ❌          |
| 10             | 659,788            | **756,473**  | -12.8% ❌         |

### Detailed Results

#### mikkadb (2 shards) - Port 6379
```
Pipeline 1:  164,824 ops/sec
Pipeline 5:  435,748 ops/sec  (2.64x increase)
Pipeline 10: 659,788 ops/sec  (4.00x increase)
```

#### Redis 8.0.2 - Port 6380
```
Pipeline 1:  114,952 ops/sec
Pipeline 5:  472,471 ops/sec  (4.11x increase)
Pipeline 10: 756,473 ops/sec  (6.58x increase)
```

## Analysis

### mikkadb Strengths
- **43% faster without pipelining** - Better single-request latency
- Share-Nothing architecture with 2 shards provides good baseline performance
- Efficient for low-pipeline workloads

### Redis Strengths
- **Better pipeline scaling** - Redis scales more efficiently with pipelining
- At pipeline=5: 8% faster than mikkadb
- At pipeline=10: 13% faster than mikkadb
- More mature pipeline implementation

### Key Insights

1. **Without Pipelining (pipeline=1)**: mikkadb wins significantly
   - 43% higher throughput
   - Better for real-world workloads with low latency requirements

2. **With Pipelining (pipeline=5+)**: Redis wins
   - Redis's pipeline implementation scales better
   - Better for batch processing workloads

3. **Scaling Factor**:
   - mikkadb: 4.00x improvement from pipeline=1 to pipeline=10
   - Redis: 6.58x improvement from pipeline=1 to pipeline=10
   - Redis benefits more from pipelining

## Recommendations

**Use mikkadb when**:
- Low latency is critical (no pipelining)
- Single-request performance matters
- 43% throughput advantage at baseline

**Use Redis when**:
- High throughput batch processing
- Pipeline depth > 5
- Mature ecosystem needed

## Next Steps

1. ✅ Investigate why mikkadb's pipeline scaling is lower than Redis
2. ✅ Profile CPU/network usage during pipeline=10 tests
3. ✅ Test with more shards (4, 8) to see if scaling improves
4. ✅ Analyze batch processing implementation in mikkadb

---

**Test Date**: 2025-11-25
**Environment**: GCP asia-southeast2-a, Ubuntu 25.10
