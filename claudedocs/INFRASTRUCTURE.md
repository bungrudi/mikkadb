# Infrastructure & Testing Servers

## Remote Testing Environments

### Production-Like Testing Server
- **Host**: `ubuntu@158.69.212.243`
- **Purpose**: Remote Linux environment for realistic performance testing and benchmarking
- **OS**: Ubuntu Linux
- **Added**: 2025-11-23

**Usage**:
```bash
# SSH access
ssh ubuntu@158.69.212.243

# Deploy for testing
scp target/release/mikkadb-rust ubuntu@158.69.212.243:~/

# Run remote benchmarks
ssh ubuntu@158.69.212.243 './mikkadb-rust &'
# Run memtier_benchmark from local or another machine targeting 158.69.212.243:6379
```

**Notes**:
- Used for testing shared-nothing architecture (multi-shard setup)
- See `benchmark_results/remote_linux_3shards.txt` for baseline results
- Provides realistic network latency and Linux kernel behavior vs local macOS development

## Local Development
- **Platform**: macOS (Darwin 25.1.0)
- **Development**: `/Users/rudi/Workspace/mikkadb-rust`

## Benchmark Results Location
- Local: `benchmark_results/`
- Remote results should be copied back to `benchmark_results/` with descriptive names
