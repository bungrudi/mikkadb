## 1. Batch Request Structure
- [ ] 1.1 Create `BatchCommandRequest` struct in `src/engine.rs`
- [ ] 1.2 Add batch response type `Vec<Result<Value>>`
- [ ] 1.3 Create enum to support both single and batch modes

## 2. Engine Batch Processing
- [ ] 2.1 Modify `Engine::run()` to handle batch requests
- [ ] 2.2 Implement `handle_command_batch()` method
- [ ] 2.3 Process commands sequentially within batch (preserve FIFO)
- [ ] 2.4 Collect responses into Vec
- [ ] 2.5 Send batch response via single oneshot

## 3. Connection Handler Integration
- [ ] 3.1 Modify connection handler to collect commands into batch
- [ ] 3.2 Create single `BatchCommandRequest` per batch
- [ ] 3.3 Send batch through channel (single send)
- [ ] 3.4 Receive batch response and write with `write_batch()`

## 4. Edge Cases
- [ ] 4.1 Handle empty batch (no-op)
- [ ] 4.2 Handle single command batch (optimize to avoid Vec overhead)
- [ ] 4.3 Handle errors mid-batch (continue processing, collect errors)
- [ ] 4.4 Handle blocking commands (BLPOP) - flush before, process separately

## 5. Testing
- [ ] 5.1 Unit test batch request creation
- [ ] 5.2 Unit test batch response handling
- [ ] 5.3 Integration test P=10 batch processing
- [ ] 5.4 Integration test P=100 batch processing
- [ ] 5.5 Test FIFO ordering preservation

## 6. Performance Validation
- [ ] 6.1 Build release binary
- [ ] 6.2 Deploy to test server (ubuntu@158.69.212.243)
- [ ] 6.3 Benchmark P=10 (target: ≥800K ops/sec)
- [ ] 6.4 Benchmark P=100 (target: ≥1.2M ops/sec)
- [ ] 6.5 Verify no regression at P=1

## 7. Documentation
- [ ] 7.1 Update benchmark results
- [ ] 7.2 Document batch processing architecture
