# Executive Summary: Actor to Shared State Migration

## Overview

MikkaDB currently uses a single-threaded Actor model where all commands are processed sequentially through message passing. This architecture ensures correctness but limits performance to a single CPU core and introduces ~3ms latency overhead from context switching.

**Proposed Solution**: Migrate to a Shared State architecture using `Arc<RwLock<Db>>` to enable concurrent read operations and reduce latency by 3x while maintaining correctness guarantees.

---

## Key Findings

### Performance Impact
- **Current Latency**: ~3ms per operation (channel + context switch + process)
- **Target Latency**: ~1ms per operation (direct lock + process)
- **Improvement**: **3x latency reduction**

- **Current Throughput**: ~50K ops/sec (single core limit)
- **Target Throughput**: ~150K ops/sec (4 cores, read-heavy workload)
- **Improvement**: **3x throughput increase** for read-heavy workloads

### Architectural Complexity

The migration requires splitting the monolithic `Engine` actor into **5 independent state domains**:

| Domain | Implementation | Complexity | Risk Level |
|--------|----------------|------------|------------|
| Core Database | `Arc<RwLock<Db>>` | LOW | 🟢 LOW |
| Transactions | Client-local state | LOW | 🟢 LOW |
| Pub/Sub | `Arc<DashMap<...>>` | MEDIUM | 🟡 MEDIUM |
| Blocking Ops | `Arc<RwLock<WaitList>> + Notify` | HIGH | 🔴 HIGH |
| Replication | Dedicated Actor | MEDIUM | 🟡 MEDIUM |

---

## Critical Challenges

### Challenge 1: Blocking Commands (BLPOP, XREAD BLOCK)
**Problem**: Cannot hold database lock while waiting for data
**Solution**: Lock → Check → Unlock → Wait → Lock → Check pattern using `tokio::sync::Notify`
**Complexity**: HIGH 🔴

### Challenge 2: Deadlock Prevention
**Problem**: Multiple locks (db, wait_list, pubsub) can deadlock if acquired in wrong order
**Solution**: Strict lock ordering hierarchy + code review checklist
**Complexity**: MEDIUM 🟡

### Challenge 3: Transaction Atomicity
**Problem**: EXEC must hold lock for entire transaction (multiple commands)
**Solution**: Client-local transaction queue + single write lock for EXEC
**Complexity**: LOW 🟢

### Challenge 4: Replication Ordering
**Problem**: Concurrent writes must propagate in deterministic order
**Solution**: Dedicated replication actor with channel-based serialization
**Complexity**: MEDIUM 🟡

---

## Migration Strategy

### Phase 1: Preparation (2 weeks)
**Goal**: Refactor code without behavioral changes

✅ Extract command execution logic to standalone functions
✅ Classify commands by lock requirements
✅ Add comprehensive integration tests
✅ Establish performance baseline

**Risk**: LOW - No architectural changes

---

### Phase 2: State Migration (3 weeks)
**Goal**: Replace Engine with shared state structures

1. Create `SharedDb` wrapper with `Arc<RwLock<Db>>`
2. Implement `ReplicationActor` for ordered write propagation
3. Create `WaitList` coordination for blocking commands
4. Move transaction state to client tasks
5. Implement pub/sub with `DashMap` (lock-free)

**Risk**: HIGH - Breaking changes, extensive testing required

**Validation Criteria**:
- All integration tests pass
- No deadlocks detected (timeout tests)
- Transaction isolation verified
- Replication consistency validated

---

### Phase 3: Parallel Execution (2 weeks)
**Goal**: Enable concurrent reads and remove Engine actor

1. Update command routing to execute directly in client tasks
2. Remove Engine actor event loop
3. Benchmark performance improvements
4. Validate correctness under load

**Risk**: MEDIUM - Performance validation critical

**Success Criteria**:
- Latency reduced by >2x
- Throughput increased by >2x for read-heavy workloads
- All tests passing
- No performance regressions for write-heavy workloads

---

### Phase 4: Optimization (1 week)
**Goal**: Fine-tune for production

1. Profile lock contention with `tokio-console`
2. Optimize critical section sizes
3. Load testing with production-like workloads
4. Consider `parking_lot::RwLock` if needed

**Risk**: LOW - Incremental improvements

---

## Risk Assessment

### High Priority Risks (Must Mitigate)

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Deadlocks | CRITICAL | MEDIUM | Lock ordering rules + static analysis |
| Transaction isolation violations | CRITICAL | LOW | Client-local state + single EXEC lock |
| Lock held during async await | CRITICAL | MEDIUM | Code review + tokio-console |

### Medium Priority Risks (Monitor)

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Pub/Sub message loss | HIGH | MEDIUM | DashMap atomic iteration |
| BLPOP waiters not woken | HIGH | MEDIUM | Fair FIFO queue + retry loop |
| Replication out-of-order | CRITICAL | LOW | Dedicated single-threaded actor |

### Low Priority Risks (Acceptable)

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Performance regression | MEDIUM | LOW | Benchmark before/after |
| Thundering herd | MEDIUM | MEDIUM | Fair queueing + metrics |

---

## Testing Strategy

### Critical Tests (Must Pass Before Migration)

1. **Deadlock Detection**: 100 clients, mixed operations, 5s timeout
2. **Transaction Isolation**: Concurrent MULTI/EXEC, verify atomicity
3. **Replication Ordering**: Verify replica consistency after WAIT
4. **Pub/Sub Delivery**: 10 subscribers, 100 messages, 100% delivery
5. **BLPOP Coordination**: 5 waiters, 5 pushes, fair wakeup

### Performance Tests (Validate Improvements)

1. **GET Latency**: Target <1ms p99
2. **Concurrent Reads**: Target linear scaling up to core count
3. **Write Performance**: Target no regression vs Actor model
4. **Mixed Workload**: 80% reads, 20% writes, target 3x throughput

---

## Implementation Timeline

```
Week 1-2:   Phase 1 - Preparation
            ├─ Extract command logic
            ├─ Classify commands
            ├─ Add integration tests
            └─ Establish baseline

Week 3-5:   Phase 2 - State Migration
            ├─ SharedDb implementation
            ├─ ReplicationActor
            ├─ WaitList coordination
            ├─ Transaction state migration
            └─ Pub/Sub DashMap

Week 6-7:   Phase 3 - Parallel Execution
            ├─ Remove Engine actor
            ├─ Direct command execution
            ├─ Performance validation
            └─ Load testing

Week 8:     Phase 4 - Optimization
            ├─ Profile and tune
            ├─ Production load testing
            └─ Final validation
```

**Total**: 8 weeks from start to production-ready

---

## Decision Points

### Decision 1: Use `Arc<RwLock<Db>>` for Core Database
**Rationale**: Allows concurrent reads while ensuring exclusive writes. Simpler than lock-free structures but much faster than channels.
**Alternative Considered**: Lock-free structures (too complex), sharded DB (premature optimization)
**Status**: ✅ Approved

### Decision 2: Dedicated Replication Actor
**Rationale**: Maintains strict write ordering through channel serialization. Simple to reason about correctness.
**Alternative Considered**: CAS-based offset tracking (complex, error-prone)
**Status**: ✅ Approved

### Decision 3: Client-Local Transaction State
**Rationale**: No shared state needed, automatic cleanup on disconnect, zero contention.
**Alternative Considered**: Shared HashMap with client isolation (unnecessary complexity)
**Status**: ✅ Approved

### Decision 4: DashMap for Pub/Sub
**Rationale**: Lock-free reads optimize PUBLISH hot path, fine-grained locking per channel.
**Alternative Considered**: `RwLock<HashMap>` (higher contention)
**Status**: ✅ Approved

### Decision 5: Event-Driven Blocking with `tokio::sync::Notify`
**Rationale**: Lock-free wait/wake coordination, prevents deadlocks, fair wakeup possible.
**Alternative Considered**: Polling with timeout (inefficient), holding lock while waiting (deadlock)
**Status**: ✅ Approved

---

## Success Metrics

### Correctness (Must Achieve)
- ✅ All integration tests pass
- ✅ No deadlocks detected in stress tests
- ✅ Transaction isolation verified
- ✅ Replication consistency validated
- ✅ Pub/Sub message delivery 100%

### Performance (Target)
- 🎯 Latency: 3x reduction (3ms → 1ms)
- 🎯 Throughput: 3x increase for read-heavy workloads
- 🎯 Concurrent readers: Scales linearly with cores
- 🎯 Write performance: No regression

### Operational (Monitor)
- 📊 Lock contention: <5% of execution time
- 📊 BLPOP wake time: <100ms p99
- 📊 Memory usage: No significant increase
- 📊 CPU utilization: Better distribution across cores

---

## Recommendation

**Proceed with migration** following the phased approach:

1. **Phase 1 (2 weeks)**: Low-risk preparation with comprehensive testing
2. **Phase 2 (3 weeks)**: Careful state migration with validation gates
3. **Phase 3 (2 weeks)**: Enable parallelism and measure improvements
4. **Phase 4 (1 week)**: Fine-tune for production

**Confidence Level**: HIGH
- Clear architectural design
- Well-understood risks with mitigations
- Comprehensive testing strategy
- Incremental migration path with rollback capability

**Expected Outcome**: 3x performance improvement while maintaining correctness guarantees and enabling future scalability.

---

## References

- **Detailed Analysis**: `architecture-migration-analysis.md`
- **Risk Matrix**: `migration-risk-matrix.md`
- **Implementation Guide**: `implementation-roadmap.md`

## Questions for Review

1. ✅ Is the lock ordering hierarchy clear and enforceable?
2. ✅ Are the testing requirements comprehensive enough?
3. ✅ Is the timeline realistic (8 weeks)?
4. ✅ Are all critical risks properly mitigated?
5. ✅ Is the performance improvement justifiable for the complexity?

**Status**: Ready for stakeholder approval
