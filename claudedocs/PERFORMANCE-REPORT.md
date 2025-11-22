# MikkaDB Performance Optimization Report

**Last Updated**: 2025-11-22
**Status**: Phase 1 Complete, I/O Optimization Recommended

---

## Executive Summary

**Current Performance**: 118,335 ops/sec (+5.9% vs Actor model, +83% vs Redis)
**Primary Bottleneck**: Network I/O (91% CPU time)
**Recommendation**: Focus on I/O optimization (10-30% gain) before zero-copy migration (1-2% gain)

---

## Phase 1: Shared-State Architecture (COMPLETED ✅)

### Implementation

Migrated from single-threaded Actor model to `Arc<RwLock<Db>>` shared-state architecture for concurrent reads.

**Key Changes**:
- `Db::get/keys/key_type` from `&mut self` → `&self` (lazy expiration)
- `SingleLockStore` using read locks for GET operations
- `ActorStore` baseline wrapper for comparison

### Performance Results

| Implementation | Ops/sec | Latency | p99 | Result |
|----------------|---------|---------|-----|--------|
| **SingleLockStore** | **118,335** | **1.49ms** | **1.63ms** | ✅ Winner |
| Actor Model | 111,764 | 1.49ms | 1.62ms | Baseline |
| Redis 7.4.1 | 64,690 | 3.09ms | 3.92ms | Reference |

**Achievements**:
- ✅ +5.9% faster than Actor model
- ✅ +82.9% faster than Redis
- ✅ Concurrent reads validated (RwLock working efficiently)
- ✅ <1% lock contention

**Files**: See git commit `ea69416` for full implementation

---

## Phase 0: Profiling Analysis (COMPLETED ✅)

### Objective

Profile current implementation to identify optimization priorities before investing in zero-copy RESP parser migration.

### Methodology

**Tool**: `cargo flamegraph` with perf sampling
**Workload**: `memtier_benchmark -t 8 -c 25 --ratio=10:1 --test-time=30`
**Baseline**: 118,335 ops/sec, 1.49ms avg latency

### CPU Time Breakdown

```
Active Request Processing (7% of total thread time):
├─ Network I/O:      90.6%  (sendto/recvfrom syscalls)
├─ RESP Parsing:      3.7%  (parse_message, parse_bulk_string)
├─ Allocations:       3.0%  (malloc/free overhead)
├─ String Ops:        2.0%  (to_uppercase, clone)
├─ HashMap/Storage:   1.3%  (get/set operations)
└─ Lock Operations:  <1.0%  (RwLock read/write - negligible!)

Idle Time (93% of total thread time):
└─ Waiting for network events (tokio epoll/kqueue)
```

**Key Insight**: System is **I/O-bound**, not CPU-bound or allocation-bound.

### Allocation Hotspots

Top allocation sites by CPU impact:
1. `parse_integer` String allocation: 1.5% CPU
2. `String::clone` operations: 0.7% CPU
3. `Vec::grow` reallocation: 0.5% CPU
4. `String::to_uppercase`: 0.3% CPU
5. `format!` macro: 0.3% CPU

**Total allocation overhead**: ~3% of active CPU time

### Lock Contention Analysis

- RwLock operations: <1% CPU time
- Read/write ratio: ~9:1 (as expected for 90/10 workload)
- No contention detected

**Validation**: Phase 1 shared-state architecture is working efficiently!

### Decision Matrix

| Optimization | CPU Impact | Effort | Expected Gain | ROI | Priority |
|--------------|------------|--------|---------------|-----|----------|
| I/O optimization | 91% | MEDIUM | 10-30% | ⭐⭐⭐⭐⭐ | **P0** |
| Command batching | 91% | MEDIUM | 15-25% | ⭐⭐⭐⭐ | **P1** |
| TCP tuning | 91% | LOW | 5-10% | ⭐⭐⭐ | **P2** |
| Zero-copy RESP | 3% | HIGH | 1-2% | ⭐ | **P3 (Deferred)** |

**Recommendation**: ⚠️ **DEFER zero-copy migration, prioritize I/O optimization**

---

## Zero-Copy RESP Parser Analysis (DEFERRED)

### Original Plan

Migrate from String-based to Bytes-based RESP parsing to eliminate heap allocations.

**Expected Benefits**:
- 50-70% reduction in allocations
- 10-20% throughput improvement
- Reduced GC/allocator pressure

### Gemini Review Insights

**Consensus (Score: 9/10)**:
1. ✅ Use `ahash` instead of SipHash for `HashMap<Bytes>` (2-3x faster hashing)
2. ✅ Boundary at Command Layer: `match &cmd[..] { b"GET" => ... }`
3. ✅ Profile first - don't assume allocations are bottleneck
4. ✅ Minimal UTF-8 validation (only command names)

### Why Deferred

**Profiling Results**:
- Allocations: only 3% of CPU time (not 20%+ needed to justify)
- Network I/O: 91% of CPU time (30x larger bottleneck)
- Expected gain: 1-2% throughput (vs 10-30% from I/O optimization)

**ROI Analysis**:
- Implementation effort: HIGH (significant refactoring)
- Performance gain: LOW (1-2%)
- Conclusion: Not worth it until I/O is optimized

### Future Consideration

Revisit zero-copy migration IF:
1. I/O optimization brings allocations >15% CPU time
2. Profiling shows allocation pressure after I/O improvements
3. Specific use case requires minimal allocation (embedded/edge)

**Design Ready**: Full implementation plan available if needed (see archive)

---

## Recommended Optimizations (Priority Order)

### P0: Command Pipelining (Highest ROI)

**Problem**: Each command = 2 syscalls (recvfrom + sendto)
**Solution**: Batch multiple commands per syscall

**Expected Impact**:
- 15-25% throughput improvement
- Reduced syscall overhead
- Better CPU cache utilization

**Implementation**:
- Read buffer: process all available commands before responding
- Write buffer: coalesce responses
- Maintain request/response ordering

**Effort**: MEDIUM (2-3 weeks)

### P1: io_uring (Linux) / kqueue Batching (macOS)

**Problem**: Traditional epoll/kqueue = syscall per I/O event
**Solution**: Batch I/O operations with modern async interfaces

**Expected Impact**:
- 20-30% throughput improvement
- Reduced kernel/userspace context switches
- Lower latency variance

**Platform Support**:
- Linux: io_uring (kernel 5.1+)
- macOS: kqueue batching
- Fallback: current tokio epoll/kqueue

**Effort**: MEDIUM-HIGH (3-4 weeks)

### P2: TCP Tuning

**Low-hanging fruit**:
- TCP_NODELAY: disable Nagle's algorithm
- SO_RCVBUF/SO_SNDBUF: tune buffer sizes
- TCP_QUICKACK: reduce delayed ACKs

**Expected Impact**: 5-10% improvement
**Effort**: LOW (1-2 days)

---

## Benchmarking Methodology

### Standard Benchmark

```bash
memtier_benchmark -p 6379 -t 8 -c 25 --ratio=10:1 --test-time=30
```

**Parameters**:
- 8 threads × 25 clients = 200 concurrent connections
- 10:1 read/write ratio (90% GETs)
- 30 second duration

### Profiling

```bash
CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --bin bench-single-lock
```

### Key Metrics

- **Throughput**: ops/sec
- **Latency**: avg, p50, p99, p999
- **CPU**: active vs idle time breakdown
- **Allocations**: per request, per second

---

## Artifacts

### Benchmarks
- `benchmark_results/single_lock_AFTER_FIX.txt` - Phase 1 results
- `benchmark_results/PROFILING-ANALYSIS.md` - Phase 0 profiling
- `benchmark_results/SUMMARY.txt` - All comparisons

### Planning (Historical)
- `claudedocs/PHASE-1-DETAILED-PLAN.md` - Shared-state implementation plan
- Git commit `ea69416` - Phase 1 implementation

---

## Conclusions

### What We've Learned

1. **Shared-state works**: RwLock-based architecture is efficient (<1% lock contention)
2. **I/O is the bottleneck**: 91% CPU time, not allocations (3%)
3. **Current performance is solid**: 118k ops/sec, 83% faster than Redis
4. **ROI matters**: Optimize the 91% bottleneck before the 3% one

### Next Steps

**Option 1**: Implement command pipelining (P0 - recommended)
**Option 2**: Explore io_uring/kqueue for I/O optimization
**Option 3**: Ship current performance as-is (already beating Redis)

**Decision**: Awaiting user direction
