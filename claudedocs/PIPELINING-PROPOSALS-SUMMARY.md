# Pipelining Optimization Proposals Summary

Created: 2025-11-23
Based on: Gemini 3-phase optimization plan

## Overview

Two phased OpenSpec proposals created to optimize MikkaDB pipelining performance and close the gap with Redis.

**Current State** (Read-Heavy 1:10 SET:GET):
- P=1: 109K ops/sec (✅ matches Redis 109K)
- P=10: 377K ops/sec (❌ Redis 749K - 2x slower)
- P=100: 528K ops/sec (❌ Redis 1,651K - 3x slower)

**Root Cause**: 91% CPU time in I/O syscalls (profiled in Phase 0)

## Phase 1: Response Coalescing

**Change ID**: `optimize-response-coalescing`
**Status**: ✅ Validated (0/51 tasks)

### Focus
Write-side optimization: Batch multiple responses before flushing to reduce write syscalls.

### Key Changes
- Add `write_batch()` method with vectored I/O (writev)
- Accumulate responses in Vec before flushing
- Smart flush triggers (blocking commands, 16KB threshold)
- Zero breaking changes (backward compatible)

### Expected Impact
- **P=10**: 377K → ~580K ops/sec (+54%)
- **P=100**: 528K → ~800K ops/sec (+52%)
- **Syscalls**: P=10: 20→11 (45% reduction)
- **Implementation**: 6-9 hours

### Files
```
openspec/changes/optimize-response-coalescing/
├── proposal.md (Why, What, Impact)
├── design.md (Technical design, 560 lines)
├── tasks.md (51 tasks across 10 phases)
└── specs/networking/spec.md (8 new requirements)
```

## Phase 2: Batch Command Processing

**Change ID**: `optimize-batch-command-processing`
**Status**: ✅ Validated (0/70 tasks)
**Prerequisites**: ⚠️ Phase 1 must be complete first

### Focus
Read-side optimization: Drain socket buffer and parse all available commands before processing.

### Key Changes
- Non-blocking buffer draining with `try_read_value_from_buf()` (already exists)
- Command batching with safety limits (1024 cmds, 4MB, 128/tick)
- Integration with Phase 1 response batching
- DoS prevention and fairness mechanisms

### Expected Impact
- **P=10**: ~580K → ~700K ops/sec (+21% over Phase 1, +86% over baseline)
- **P=100**: ~800K → ~1,100K ops/sec (+38% over Phase 1, +108% over baseline)
- **Syscalls**: P=10: 11→2 (90% total reduction), P=100: 101→2 (99% reduction)
- **Implementation**: 9-13 hours

### Files
```
openspec/changes/optimize-batch-command-processing/
├── proposal.md (Why, What, Impact)
├── design.md (Technical design, 400 lines)
├── tasks.md (70 tasks across 14 phases)
└── specs/networking/spec.md (9 new requirements)
```

## Performance Trajectory

| Metric | Baseline | Phase 1 | Phase 2 | Redis | Gap |
|--------|----------|---------|---------|-------|-----|
| **P=1** | 109K | 109K | 109K | 109K | ✅ 0% |
| **P=10** | 377K | 580K | 700K | 749K | ✅ 7% |
| **P=100** | 528K | 800K | 1,100K | 1,651K | ⚠️ 33% |
| **Syscalls (P=10)** | 20 | 11 | 2 | N/A | N/A |
| **Read syscalls (P=10)** | 10 | 10 | 1 | N/A | N/A |
| **Write syscalls (P=10)** | 10 | 1 | 1 | N/A | N/A |

## Syscall Reduction Analysis

**Baseline** (P=10):
- 10 × `read()` syscalls
- 10 × `write()` syscalls
- **Total**: 20 syscalls per 10 commands

**Phase 1** (Response Coalescing):
- 10 × `read()` syscalls (unchanged)
- 1 × `writev()` syscall (vectored I/O)
- **Total**: 11 syscalls (45% reduction)
- **Focus**: Write-side optimization

**Phase 2** (+ Batch Command Processing):
- 1 × `read()` syscall (buffer draining)
- 1 × `writev()` syscall (from Phase 1)
- **Total**: 2 syscalls (90% total reduction)
- **Focus**: Read-side optimization

## Implementation Strategy

### Sequential Phasing (Recommended)

**Phase 1 → Deploy → Validate → Phase 2**

**Rationale**:
- Phase 1 delivers 40-60% improvement standalone
- Phase 2 depends on Phase 1 `write_batch()` method
- Incremental risk reduction
- Clear performance attribution per phase

**Timeline**:
- Week 1: Implement + test Phase 1 (6-9 hours)
- Week 2: Deploy Phase 1, establish baseline
- Week 3: Implement + test Phase 2 (9-13 hours)
- Week 4: Deploy Phase 2, final validation

### Parallel Implementation (Advanced)

**Phase 1 + Phase 2 → Deploy Together**

**Pros**:
- Faster time to full optimization
- Single deployment cycle

**Cons**:
- Higher complexity and risk
- Harder to attribute performance gains
- More difficult debugging if issues arise

## Testing Strategy

### Phase 1 Testing
1. Unit tests: `write_batch()` with various batch sizes
2. Integration: Flush triggers (BLPOP, EXEC, size threshold)
3. Performance: memtier P=1/P=10/P=100
4. Syscalls: strace verification (≥70% write reduction)

### Phase 2 Testing
1. Unit tests: Buffer draining, batch limits, fairness
2. Integration: Pipelined workloads, partial reads, DoS prevention
3. Performance: memtier P=1/P=10/P=100 vs Phase 1 baseline
4. Syscalls: strace verification (≥85% total reduction)

## Success Criteria

### Phase 1 Success
- ✅ P=10 ≥ 540K ops/sec (+43%)
- ✅ P=100 ≥ 750K ops/sec (+42%)
- ✅ Write syscalls reduced ≥70%
- ✅ No P=1 regression (±5%)

### Phase 2 Success
- ✅ P=10 ≥ 680K ops/sec (+17% over Phase 1)
- ✅ P=100 ≥ 1,000K ops/sec (+25% over Phase 1)
- ✅ Total syscalls reduced ≥85%
- ✅ Read syscalls reduced ≥70%

### Combined Success
- ✅ P=10: 93% of Redis performance (vs current 50%)
- ✅ P=100: 67% of Redis performance (vs current 32%)
- ✅ 90% syscall reduction (20→2 at P=10)

## Phase 3 Consideration (Optional)

**If Phase 2 gap to Redis still >30%**:

**Change ID**: `optimize-io-uring` (not yet created)

**Focus**: io_uring for kernel-level I/O batching

**Expected Impact**: Additional 10-20% (diminishing returns)

**Complexity**: HIGH (requires Linux 5.1+, fallback to epoll)

**Recommendation**: Only pursue if Phase 1+2 insufficient for production needs

## Existing Proposal: optimize-command-pipelining

**Status**: 0/41 tasks (created earlier)

**Relationship**: Comprehensive proposal combining both Phase 1 and Phase 2 concepts

**Action**: Consider archiving in favor of the more granular Phase 1 + Phase 2 approach, OR use as reference implementation

## Next Steps

1. ✅ **Proposals Created**: Phase 1 and Phase 2 validated
2. **Implementation Decision**: Choose sequential vs parallel strategy
3. **Phase 1 Implementation**: Start with response coalescing
4. **Benchmarking**: Establish Phase 1 baseline before Phase 2
5. **Phase 2 Implementation**: Build on Phase 1 results
6. **Final Validation**: Compare against Redis, document findings

## References

- **Baseline Metrics**: `benchmark_results/adaptive_buffering_final.txt`
- **Profiling**: `claudedocs/PERFORMANCE-REPORT.md` (Phase 0)
- **Gemini Consultation**: 3-phase optimization plan (2025-11-23)
- **OpenSpec Validation**: Both proposals pass `--strict` validation
- **Remote Server**: `ubuntu@158.69.212.243` (4 cores, 3 shards)
