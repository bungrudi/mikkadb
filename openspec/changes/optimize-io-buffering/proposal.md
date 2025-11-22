## Why
The current implementation writes to the TCP stream directly for every response chunk, resulting in a high number of syscalls. This significantly limits throughput and increases latency, especially for fragmented responses.

## What Changes
- Wrap the `TcpStream` in `tokio::io::BufWriter` within `RespHandler`.
- Ensure responses are flushed to the underlying stream after writing a full command response.
- Modify `RespHandler::new` to initialize the buffered writer.

## Impact
- Affected specs: `networking`
- Affected code: `src/resp.rs`
