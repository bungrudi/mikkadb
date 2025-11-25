## 1. Implementation
- [ ] 1.1 Change RESP `Value` representation to use `bytes::Bytes` (or `BytesMut`) instead of `String` where possible
- [ ] 1.2 Update parsing functions in `src/resp.rs` to slice from the input buffer rather than allocate new `String`s
- [ ] 1.3 Update `RedisCommand::from_resp` to accept `Bytes` and convert to `String` only when strictly necessary (e.g., command names)
- [ ] 1.4 Update database APIs (`Db`) to accept/return `Bytes` where safe, or convert once at the boundary
- [ ] 1.5 Optimize `Value::serialize_bytes` (or equivalent) to write directly into an output buffer without intermediate `Vec<u8>` allocations
- [ ] 1.6 Ensure all tests still pass and add a benchmark (even a simple one) to confirm allocation reduction
