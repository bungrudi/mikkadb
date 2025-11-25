# Infrastructure & Testing Servers

## Remote Testing Environment

### GCP Development & Testing Server
- **Instance Name**: `instance-20251125-121024`
- **Project**: `ajaib-poc-cs`
- **Zone**: `asia-southeast2-a` (Jakarta, Indonesia)
- **External IP**: `34.50.117.27`
- **Internal IP**: `10.184.0.5`
- **OS**: Ubuntu 25.10 (Questing)
- **Purpose**: Development, testing, and benchmarking in cloud environment
- **Added**: 2025-11-25

**Installed Software**:
- Rust 1.91.1
- Redis 8.0.2
- Build tools (gcc, make, git, pkg-config, libssl-dev)
- mikkadb-rust (branch: `feature/server-pipeline-batching`)

**SSH Access**:
```bash
# Using gcloud CLI
gcloud compute ssh instance-20251125-121024 --zone=asia-southeast2-a --project=ajaib-poc-cs

# Direct SSH (if keys configured)
ssh 34.50.117.27
```

**File Transfer**:
```bash
# Upload files
gcloud compute scp local-file.txt instance-20251125-121024:~/ --zone=asia-southeast2-a --project=ajaib-poc-cs

# Upload directory
gcloud compute scp --recurse local-dir/ instance-20251125-121024:~/ --zone=asia-southeast2-a --project=ajaib-poc-cs

# Download files
gcloud compute scp instance-20251125-121024:~/remote-file.txt ./ --zone=asia-southeast2-a --project=ajaib-poc-cs
```

**Server Setup**:
```bash
# Management scripts (recommended)
~/start_mikkadb.sh      # Start mikkadb with 2 shards
~/stop_mikkadb.sh       # Stop mikkadb
~/status_servers.sh     # Check status of both services

# Manual control
~/mikkadb-rust/target/release/mikkadb-rust --shards 2 &

# Redis control (runs on port 6380)
sudo systemctl {start|stop|restart|status} redis-server

# Check both services
redis-cli -p 6380 ping  # Redis
redis-cli -p 6379 ping  # mikkadb
```

**Rebuild mikkadb**:
```bash
ssh instance-20251125-121024
cd ~/mikkadb-rust
git pull origin feature/server-pipeline-batching
source ~/.cargo/env
cargo build --release
```

**Run Benchmarks**:
```bash
# Install memtier_benchmark on GCP instance
sudo apt install memtier-benchmark

# Start mikkadb (Redis already running)
~/start_mikkadb.sh
sleep 2

# Check status
~/status_servers.sh

# Run benchmark against mikkadb (port 6379, 2 shards)
memtier_benchmark -p 6379 -t 4 -c 50 -n 10000

# Run benchmark against Redis (port 6380)
memtier_benchmark -p 6380 -t 4 -c 50 -n 10000

# Stop mikkadb when done
~/stop_mikkadb.sh
```

**Port Configuration**:
- **mikkadb**: port 6379 (default Redis port)
- **Redis**: port 6380 (configured to avoid conflict)
- Both services can run simultaneously for easy comparison

**Notes**:
- mikkadb is built from the `feature/server-pipeline-batching` branch
- Configured to run with **2 shards** (Share-Nothing Architecture)
- Default would be 8 shards based on CPU cores, but 2 is used for clearer benchmarking
- Redis configuration backup: `/etc/redis/redis.conf.backup`
- mikkadb logs: `/tmp/mikkadb.log`

## Local Development
- **Platform**: macOS (Darwin 25.1.0)
- **Development**: `/Users/rudi/Workspace/mikkadb-rust`

## Benchmark Results Location
- Local: `benchmark_results/`
- Remote results should be copied back to `benchmark_results/` with descriptive names
