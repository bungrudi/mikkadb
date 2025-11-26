## 1. Test Infrastructure (TDD)

- [x] 1.1 Write unit test for concurrent GET operations (multiple readers)
- [x] 1.2 Write unit test for fast batch path detection
- [x] 1.3 Write unit test for mixed GET/SET batch fast path
- [x] 1.4 Write integration test simulating P=10 workload with mixed GET/SET

## 2. Implementation

- [x] 2.1 Add `parking_lot` to Cargo.toml (for future RwLock use)
- [x] 2.2 Implement fast synchronous batch path for GET/SET/INCR
- [x] 2.3 Add `execute_simple_batch()` method for sync execution
- [x] 2.4 Update `handle_command_batch()` to detect simple batches
- [x] 2.5 Add INCR to fast path
- [x] 2.6 Ensure lazy expiration in GET still works

## 3. Verification

- [x] 3.1 All unit tests pass (24 tests)
- [x] 3.2 All existing tests pass (cargo test)
- [ ] 3.3 Run GCP benchmark to verify improvement
- [ ] 3.4 Document results in benchmark_results/

## 4. Cleanup

- [x] 4.1 Remove any temporary test code
- [x] 4.2 Update OpenSpec tasks to completed
