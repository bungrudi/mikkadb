## 1. Implementation

- [ ] 1.1 Add `BytesKey` wrapper type for efficient HashMap keys (implements Hash + Eq)
- [ ] 1.2 Modify `RedisCommand::Get` to use `Bytes` instead of `String` for key
- [ ] 1.3 Modify `RedisCommand::Set` to use `Bytes` for key and value
- [ ] 1.4 Update `from_resp()` to extract `Bytes` slices without String conversion
- [ ] 1.5 Update `Db` methods to accept `&[u8]` or `Bytes` for key lookups
- [ ] 1.6 Add `Bytes` → `String` conversion only at storage boundary where needed
- [ ] 1.7 Update remaining command variants (INCR, LPUSH, etc.) incrementally

## 2. Verification

- [ ] 2.1 Run existing unit tests to verify correctness
- [ ] 2.2 Run memtier benchmark at P=1, P=5, P=10
- [ ] 2.3 Profile memory allocations before/after with `heaptrack` or similar
- [ ] 2.4 Document throughput improvement
