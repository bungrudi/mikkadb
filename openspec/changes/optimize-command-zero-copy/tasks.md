## 1. Implementation

- [x] 1.1 Add `Value::as_bytes()` and `clone_bytes()` methods for zero-copy extraction
- [x] 1.2 Modify `RedisCommand::Get` to use `Bytes` instead of `String` for key
- [x] 1.3 Modify `RedisCommand::Set` to use `Bytes` for key and value
- [x] 1.4 Update `from_resp()` to use `clone_bytes()` without String conversion
- [x] 1.5 Add `Db::set_bytes()` and `get_bytes()` methods for Bytes keys
- [x] 1.6 Add `Bytes` → `String` conversion only at storage boundary where needed
- [x] 1.7 Update INCR command to use Bytes key

## 2. Verification

- [x] 2.1 Run existing unit tests to verify correctness (53 tests pass)
- [ ] 2.2 Run memtier benchmark at P=1, P=5, P=10
- [ ] 2.3 Profile memory allocations before/after with `heaptrack` or similar
- [ ] 2.4 Document throughput improvement
