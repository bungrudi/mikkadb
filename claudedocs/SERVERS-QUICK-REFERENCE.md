# Server Quick Reference

## GCP Testing & Development Server

**Connection**:
```bash
gcloud compute ssh instance-20251125-121024 --zone=asia-southeast2-a --project=ajaib-poc-cs
# or: ssh 34.50.117.27
```

**Details**:
- **Instance**: `instance-20251125-121024`
- **IP**: `34.50.117.27`
- **Location**: Jakarta (asia-southeast2-a)
- **OS**: Ubuntu 25.10

**Installed**:
- ✅ Rust 1.91.1
- ✅ Redis 8.0.2
- ✅ mikkadb-rust (feature/server-pipeline-batching)
- ✅ Build tools

**Quick Commands**:
```bash
# Start/stop mikkadb (2 shards)
~/start_mikkadb.sh
~/stop_mikkadb.sh
~/status_servers.sh

# Test both
redis-cli -p 6379 ping  # mikkadb
redis-cli -p 6380 ping  # Redis

# Rebuild mikkadb
cd ~/mikkadb-rust && git pull && cargo build --release

# Benchmark mikkadb (2 shards)
memtier_benchmark -p 6379 -t 4 -c 50 -n 10000

# Benchmark Redis
memtier_benchmark -p 6380 -t 4 -c 50 -n 10000
```

---

## File Transfer

**To GCP**:
```bash
gcloud compute scp file.txt instance-20251125-121024:~/ \
  --zone=asia-southeast2-a --project=ajaib-poc-cs
```

**From GCP**:
```bash
gcloud compute scp instance-20251125-121024:~/file.txt ./ \
  --zone=asia-southeast2-a --project=ajaib-poc-cs
```

---

**Full documentation**: See `claudedocs/INFRASTRUCTURE.md`
