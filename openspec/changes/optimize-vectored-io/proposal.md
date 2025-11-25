## Why

Current `write_batch()` implementation coalesces all responses into a single buffer before writing, requiring an extra memory copy. At P=10, this means copying ~10 response buffers into one before the syscall. Redis uses vectored I/O (`writev`) to send multiple buffers in a single syscall without the copy overhead.

**Current Performance** (2 shards, 1:10 SET:GET):
- P=1: 164K ops/sec (mikkadb +43% vs Redis)
- P=10: 660K ops/sec (Redis +13% - **losing**)

**Scaling Gap**: mikkadb scales 4.0x (P=1→P=10), Redis scales 6.6x. The per-batch copy overhead compounds with larger batches.

**Root Cause**: Buffer coalescing in `write_batch()`:
```rust
let mut buffer = Vec::with_capacity(total_size);
for bytes in serialized {
    buffer.extend(bytes);  // <-- copy overhead
}
```

Vectored I/O eliminates this copy by passing multiple `IoSlice` references directly to the kernel.

## What Changes

- Replace buffer coalescing in `write_batch()` with `write_vectored()` using `IoSlice`
- Pre-serialize responses into a `Vec<Vec<u8>>` (already done)
- Create `IoSlice` references and call `write_all_vectored()` or manual vectored write loop
- Maintain single flush after all slices written
- Fallback to current coalescing if vectored write unavailable

## Impact

- **Affected specs**: `networking` (response writing optimization)
- **Affected code**: `src/resp.rs` (`write_batch()` method)
- **Performance**: Expected 10-20% improvement at P=10+ by eliminating copy overhead
- **Compatibility**: Zero breaking changes - internal optimization only
- **Risk**: Low - vectored I/O is well-supported on all platforms
