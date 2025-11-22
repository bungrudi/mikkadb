## Why
The current parser allocates new `String` instances for every parsed token and creates intermediate `Vec<u8>` buffers during serialization. This causes excessive heap allocations and memory copying, reducing throughput.

## What Changes
- Replace `String` with `bytes::Bytes` in `Value` enum variants.
- Update `RedisCommand` to use `Bytes` (or `String` via conversion only where necessary).
- Optimize `parse_message` to slice from the input `BytesMut` instead of copying.
- Optimize `write_value` to serialize directly to the output buffer (if using `BufWriter`).

## Impact
- Affected specs: `protocol`
- Affected code: `src/resp.rs`, `src/command.rs`, `src/db.rs`
