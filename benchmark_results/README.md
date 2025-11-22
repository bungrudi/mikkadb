# Benchmark Results

**Current Status**: Phase 1 Complete, Phase 0 Profiling Complete

**See `../claudedocs/PERFORMANCE-REPORT.md` for complete analysis**

## Quick Results

| Implementation | Ops/sec | Latency | Status |
|----------------|---------|---------|--------|
| **SingleLockStore** | 118,335 | 1.49ms | ✅ Current |
| Actor Model | 111,764 | 1.49ms | Baseline |
| Redis 7.4.1 | 64,690 | 3.09ms | Reference |

**Achievement**: +5.9% vs Actor, +83% vs Redis

## Files

### Phase 1 Results (Shared-State Architecture)
- `single_lock_AFTER_FIX.txt` - Final results after Db API fix
- `actor_model.txt` - Baseline comparison
- `redis.txt` - Redis reference
- `VALIDATION_SUMMARY.txt` - Phase 1 validation

### Phase 0 Results (Profiling)
- `PROFILING-ANALYSIS.md` - **CPU breakdown showing I/O bottleneck**
- `PROFILING-SUMMARY.txt` - Executive summary
- `cpu-breakdown.txt` - Visual breakdown (91% I/O, 3% allocations)
- `profiling-benchmark-results.txt` - Benchmark during profiling
- `allocation-stats.txt` - Allocation statistics

### Deprecated
- Old reports superseded by `../claudedocs/PERFORMANCE-REPORT.md`

## Key Finding (Phase 0)

**Bottleneck**: Network I/O (91% CPU), NOT allocations (3%)

**Recommendation**: Focus on I/O optimization (10-30% gain) before zero-copy (1-2% gain)

## Reproduction

```bash
# Build
cargo build --release --bin bench-single-lock

# Benchmark
./target/release/bench-single-lock &
memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30

# Profile
CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --bin bench-single-lock
```
