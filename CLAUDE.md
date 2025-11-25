<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# Context Management Guidelines

## Use Subagents to Conserve Context

**IMPORTANT**: Always use specialized subagents for complex tasks to conserve the main context window.

### When to Use Subagents

**Always delegate to subagents for**:
- Benchmarking and performance testing (use `performance-engineer`)
- Multi-file refactoring or migrations
- Complex debugging sessions
- Code reviews and analysis
- Documentation generation
- Testing and validation

**Examples**:
```
❌ Wrong: Run memtier_benchmark directly in main conversation
✅ Right: Spawn performance-engineer subagent to run benchmarks

❌ Wrong: Read and edit 10+ files for migration in main thread
✅ Right: Spawn refactoring-expert subagent for systematic migration

❌ Wrong: Profile, analyze, and optimize in main conversation
✅ Right: Spawn performance-engineer to handle full optimization cycle
```

### Benefits
- **Context preservation**: Keep main conversation focused on high-level decisions
- **Parallel execution**: Subagents can work independently
- **Specialization**: Each subagent has domain expertise and tools
- **Clean separation**: Complex work isolated from planning/discussion

### Subagent Selection Guide

| Task Type | Subagent | Why |
|-----------|----------|-----|
| Benchmarking | `performance-engineer` | Has profiling tools, understands optimization |
| Refactoring | `refactoring-expert` | Systematic code transformation expertise |
| Architecture | `system-architect` | High-level design and trade-off analysis |
| Testing | `quality-engineer` | Comprehensive test strategy and execution |
| Debugging | `root-cause-analyst` | Systematic problem investigation |
| Documentation | `technical-writer` | Clear, comprehensive documentation |

### Implementation Pattern

**Planning Phase** (main conversation):
1. Discuss approach and design
2. Review trade-offs with Gemini if needed
3. Create implementation plan

**Execution Phase** (delegate to subagent):
1. Spawn appropriate subagent with clear task description
2. Subagent executes implementation/benchmarking/testing
3. Subagent returns results

**Review Phase** (main conversation):
1. Review subagent results
2. Make decisions based on findings
3. Plan next steps

This keeps the main conversation lean and focused on strategic decisions while preserving context for long-running projects.

---

# Performance Optimization Status

## Current State (as of 2025-11-22)

**Performance**: 118,335 ops/sec (+5.9% vs Actor, +83% vs Redis)
**Architecture**: Arc<RwLock<Db>> shared-state (Phase 1 complete)
**Bottleneck**: Network I/O (91% CPU time)

See **`claudedocs/PERFORMANCE-REPORT.md`** for:
- Phase 1 shared-state results
- Phase 0 profiling analysis
- Zero-copy analysis (deferred)
- Recommended next optimizations

## Key Findings

✅ **Phase 1 Success**: RwLock shared-state working efficiently (<1% lock contention)
⚠️ **Phase 0 Insight**: I/O is bottleneck (91%), not allocations (3%)
📊 **Priority**: I/O optimization (10-30% gain) over zero-copy (1-2% gain)

## Next Optimization Priorities

1. **P0**: Command pipelining (15-25% gain, MEDIUM effort)
2. **P1**: io_uring/kqueue batching (20-30% gain, MEDIUM-HIGH effort)
3. **P2**: TCP tuning (5-10% gain, LOW effort)
4. **P3**: Zero-copy RESP parser (1-2% gain, HIGH effort) - DEFERRED

---

# Infrastructure & Testing

## Remote Testing Server

**Instance**: `instance-20251125-121024`
**Project**: `ajaib-poc-cs`
**Zone**: `asia-southeast2-a`
**External IP**: `34.50.117.27`

**Services**:
- mikkadb: port 6379
- Redis: port 6380 (both run in parallel)

See **`claudedocs/INFRASTRUCTURE.md`** for:
- SSH access and deployment instructions
- Benchmark execution procedures
- Server setup and configuration details