## 1. Implementation

- [x] 1.1 Import `std::io::IoSlice` in `resp.rs`
- [x] 1.2 Modify `write_batch()` to eliminate intermediate buffer copy (sequential writes to BufWriter)
- [x] 1.3 Implement `write_batch_vectored()` with IoSlice and partial write handling
- [x] 1.4 Maintain single flush after all data written
- [x] 1.5 Add unit test for vectored write behavior

## 2. Verification

- [ ] 2.1 Run memtier benchmark at P=1, P=5, P=10 comparing before/after
- [ ] 2.2 Verify no regression at P=1 (low latency path)
- [ ] 2.3 Document throughput improvement in benchmark results
