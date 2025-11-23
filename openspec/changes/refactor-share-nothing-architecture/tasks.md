## 1. Core Infrastructure Updates
- [x] 1.1 Update `CommandRequest` struct:
    - Add `from_replica: bool` (default false).
- [x] 1.2 Update `Engine` struct:
    - Add `peers: Vec<mpsc::Sender<CommandRequest>>`.

## 2. Engine Logic (Active-Active Replication)
- [x] 2.1 Update `handle_command` in `Engine`:
    - Check if command is a Write command.
    - If `Write` AND `!from_replica`:
        - Execute locally.
        - Iterate over `peers` and send the command with `from_replica = true`.
    - If `Write` AND `from_replica`:
        - Execute locally.
        - Do NOT broadcast.
    - If `Read`:
        - Execute locally.

## 3. Main Loop & Routing (Connection Sharding)
- [x] 3.1 Refactor `main.rs` routing logic:
    - Remove `get_target_shard` / `hash_slot` logic.
    - Assign `target_shard = client_id % num_shards` (Round Robin).
    - Route ALL commands from that client to `target_shard`.
- [x] 3.2 Update Startup/Wiring:
    - Create all channels first.
    - Spawn Engines with peer channels.

## 4. Cleanup
- [x] 4.1 Remove `crc16` implementation.
- [x] 4.2 Remove CROSSSLOT checks.
