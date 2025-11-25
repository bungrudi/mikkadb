## 1. Implementation
- [ ] 1.1 Refactor `Db` to be thread-safe (wrapped in `RwLock`)
- [ ] 1.2 Update `Engine` logic to be stateless or integrated into `Db` methods
- [ ] 1.3 Remove `Engine` actor loop and channel
- [ ] 1.4 Update `main.rs` to spawn client tasks with shared `Arc<RwLock<Db>>`
- [ ] 1.5 Re-implement `Replication` and `PubSub` handling in shared state model
