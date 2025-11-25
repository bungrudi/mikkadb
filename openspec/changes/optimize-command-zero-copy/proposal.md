## Why

Current command parsing in `RedisCommand::from_resp()` allocates `String` for every field, even though the parser already uses zero-copy `bytes::Bytes`. For GET commands (90% of the 1:10 workload), this means allocating a `String` for every key lookup.

**Current Performance** (2 shards, 1:10 SET:GET):
- P=1: 164K ops/sec (+43% vs Redis)
- P=10: 660K ops/sec (Redis +13%)

**Root Cause**: Unnecessary String allocations in command parsing:
```rust
// command.rs line 121-125
let key = match items[1].to_string() {
    Ok(s) => s,  // <-- allocates String from Bytes
```

This is called for every command, creating allocation pressure that scales with pipeline depth.

**Expected Impact**: 15-25% throughput improvement at P=10 by eliminating per-command allocations.

## What Changes

- Modify `RedisCommand` variants to use `Bytes` instead of `String` for keys/values
- Update `from_resp()` to pass through `Bytes` slices without conversion
- Defer String conversion to storage boundary only when needed (for HashMap keys)
- Use `Bytes::slice()` for zero-copy field extraction
- Maintain backward compatibility for commands that truly need String

## Impact

- **Affected specs**: `protocol` (command parsing optimization)
- **Affected code**: `src/command.rs`, `src/engine.rs` (execution), `src/db.rs` (storage boundary)
- **Performance**: Expected 15-25% improvement by eliminating allocation overhead
- **Compatibility**: Internal optimization, no protocol changes
- **Risk**: Medium - requires careful handling of String/Bytes boundary at DB layer
