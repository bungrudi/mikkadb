use crate::command::RedisCommand;
use crate::db::Db;
use crate::resp::Value;
use crate::config::Config;
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;
use std::sync::Arc;
use bytes::Bytes;

use std::collections::HashMap;

pub struct CommandRequest {
    pub client_id: u64,
    pub command: RedisCommand,
    pub response_tx: oneshot::Sender<Result<Value>>,
    pub replica_tx: Option<mpsc::Sender<Value>>,
    pub pub_sub_tx: Option<mpsc::Sender<Value>>,
    pub from_replica: bool,
}

/// Batch of commands for efficient channel transport
/// Reduces channel overhead from O(N) to O(1) per batch
pub struct BatchCommandRequest {
    pub client_id: u64,
    pub commands: Vec<RedisCommand>,
    pub response_tx: oneshot::Sender<Vec<Result<Value>>>,
    pub pub_sub_tx: Option<mpsc::Sender<Value>>,
}

/// Engine request - supports both single and batch modes
pub enum EngineRequest {
    Single(CommandRequest),
    Batch(BatchCommandRequest),
}

struct Replica {
    id: u64,
    tx: mpsc::Sender<Value>,
    offset: i64,
}

struct PendingWait {
    num_replicas: usize,
    response_tx: oneshot::Sender<Result<Value>>,
    target_offset: i64,
}

struct PendingRead {
    response_tx: oneshot::Sender<Result<Value>>,
    streams: Vec<(String, (u64, u64))>, // key, last_id
}

pub struct Engine {
    db: Db,
    shard_id: usize,
    config: Arc<Config>,
    peers: Vec<mpsc::Sender<EngineRequest>>,
    rx: mpsc::Receiver<EngineRequest>,
    replicas: Vec<Replica>,
    pending_waits: Vec<Option<PendingWait>>,
    timeout_rx: mpsc::Receiver<usize>,
    timeout_tx: mpsc::Sender<usize>,
    pending_reads: Vec<Option<PendingRead>>,
    read_timeout_rx: mpsc::Receiver<usize>,
    read_timeout_tx: mpsc::Sender<usize>,
    replication_offset: i64,
    // (keys, response_tx) for clients blocked on BLPOP
    waiting_list_clients: Vec<(Vec<String>, oneshot::Sender<Result<Value>>)>,
    transaction_state: HashMap<u64, Vec<RedisCommand>>,
    pub_sub_subs: HashMap<String, HashMap<u64, mpsc::Sender<Value>>>,
}

impl Engine {
    pub fn new(shard_id: usize, config: Arc<Config>, rx: mpsc::Receiver<EngineRequest>, db: Db, peers: Vec<mpsc::Sender<EngineRequest>>) -> Self {
        let (timeout_tx, timeout_rx) = mpsc::channel(32);
        let (read_timeout_tx, read_timeout_rx) = mpsc::channel(32);
        Engine {
            db,
            shard_id,
            config,
            peers,
            rx,
            replicas: Vec::new(),
            pending_waits: Vec::new(),
            timeout_rx,
            timeout_tx,
            pending_reads: Vec::new(),
            read_timeout_rx,
            read_timeout_tx,
            replication_offset: 0,
            waiting_list_clients: Vec::new(),
            transaction_state: HashMap::new(),
            pub_sub_subs: HashMap::new(),
        }
    }

    async fn handle_blpop(&mut self, keys: Vec<String>, timeout: f64, response_tx: oneshot::Sender<Result<Value>>) {
        // 1. Try to pop immediately from any of the keys
        for key in &keys {
            match self.db.lpop(key, None) {
                Ok(Some(values)) => {
                    // Propagate LPOP (BLPOP acts as LPOP when data is present)
                    let args = vec![
                        Value::BulkString(Bytes::from("LPOP")),
                        Value::BulkString(key.clone().into()),
                    ];
                    self.propagate_command(Value::Array(args)).await;

                    let reply = Value::Array(vec![
                        Value::BulkString(key.clone().into()),
                        Value::BulkString(Bytes::from(String::from_utf8_lossy(&values[0]).to_string())),
                    ]);
                    
                    // Broadcast LPOP to peers
                    let broadcast_cmd = RedisCommand::LPop { key: key.clone(), count: None };
                    self.broadcast_to_peers(broadcast_cmd).await;

                    let _ = response_tx.send(Ok(reply));
                    return;
                }
                Ok(None) => continue,
                Err(e) => {
                    let _ = response_tx.send(Ok(Value::Error(e.into())));
                    return;
                }
            }
        }

        // 2. No data in any key.
        // Use an internal channel so we can support timeouts without
        // blocking the Engine's event loop.
        let (notify_tx, notify_rx) = oneshot::channel::<Result<Value>>();

        if timeout > 0.0 {
            let duration = tokio::time::Duration::from_secs_f64(timeout);
            tokio::spawn(async move {
                match tokio::time::timeout(duration, notify_rx).await {
                    Ok(Ok(val)) => {
                        let _ = response_tx.send(val);
                    }
                    Ok(Err(_)) => {
                        let _ = response_tx.send(Ok(Value::NullArray));
                    }
                    Err(_) => {
                        let _ = response_tx.send(Ok(Value::NullArray));
                    }
                }
            });
        } else {
            tokio::spawn(async move {
                match notify_rx.await {
                    Ok(val) => {
                        let _ = response_tx.send(val);
                    }
                    Err(_) => {
                        let _ = response_tx.send(Ok(Value::NullArray));
                    }
                }
            });
        }

        // Store the notifier and wake one client when a value is pushed.
        self.waiting_list_clients.push((keys, notify_tx));
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(req) = self.rx.recv() => {
                    match req {
                        EngineRequest::Single(cmd) => {
                            self.handle_command(cmd).await;
                        }
                        EngineRequest::Batch(batch) => {
                            self.handle_command_batch(batch).await;
                        }
                    }
                }
                Some(wait_idx) = self.timeout_rx.recv() => {
                    self.complete_wait(wait_idx);
                }
                Some(read_idx) = self.read_timeout_rx.recv() => {
                    self.complete_read(read_idx);
                }
                else => break,
            }
        }
    }

    /// Process a batch of commands efficiently
    /// Returns all responses in a single oneshot message
    async fn handle_command_batch(&mut self, batch: BatchCommandRequest) {
        let BatchCommandRequest { client_id, commands, response_tx, pub_sub_tx } = batch;
        
        let mut responses = Vec::with_capacity(commands.len());
        
        for command in commands {
            // Execute each command and collect response
            // Note: Blocking commands (BLPOP, XREAD with BLOCK) should not be in batches
            // They should be handled separately by the connection handler
            let result = self.execute_command_immediate(
                client_id, 
                command, 
                None,  // replica_tx not needed for batch mode
                pub_sub_tx.clone(), 
                false  // from_replica
            ).await;
            responses.push(result);
        }
        
        // Send all responses in single message
        let _ = response_tx.send(responses);
    }
    
    fn complete_read(&mut self, idx: usize) {
        if let Some(Some(_)) = self.pending_reads.get(idx) {
            // Timeout occurred, check one last time? 
            // Or just return Null as per Redis spec for timeout.
            // "If the timeout is reached, the command returns a Null reply."
            if let Some(pending) = self.pending_reads[idx].take() {
                let _ = pending.response_tx.send(Ok(Value::NullArray));
            }
        }
    }
    
    fn complete_read_with_data(&mut self, idx: usize) {
        if let Some(Some(read)) = self.pending_reads.get_mut(idx) {
            // Fetch data for all streams requested
            let mut result_streams = Vec::new();
            
            for (key, start_id) in &read.streams {
                if let Some(entries) = self.db.read_stream(key, *start_id) {
                    let mut stream_entries = Vec::new();
                    for entry in entries {
                        let id_str = format!("{}-{}", entry.id.0, entry.id.1);
                        let mut fields_val = Vec::new();
                        for (k, v) in entry.fields {
                            fields_val.push(Value::BulkString(k.into()));
                            fields_val.push(Value::BulkString(v.into()));
                        }
                        
                        stream_entries.push(Value::Array(vec![
                            Value::BulkString(id_str.into()),
                            Value::Array(fields_val)
                        ]));
                    }
                    
                    result_streams.push(Value::Array(vec![
                        Value::BulkString(key.clone().into()),
                        Value::Array(stream_entries)
                    ]));
                }
            }
            
            if !result_streams.is_empty() {
                if let Some(pending) = self.pending_reads[idx].take() {
                    let _ = pending.response_tx.send(Ok(Value::Array(result_streams)));
                }
            }
        }
    }

    async fn broadcast_to_peers(&mut self, command: RedisCommand) {
        for peer in &self.peers {
            let peer = peer.clone();
            let command = command.clone();
            
            tokio::spawn(async move {
                let (tx, _) = oneshot::channel();
                let req = CommandRequest {
                    client_id: 0,
                    command,
                    response_tx: tx,
                    replica_tx: None,
                    pub_sub_tx: None,
                    from_replica: true,
                };
                // Wrap in EngineRequest::Single for channel transport
                if let Err(e) = peer.send(EngineRequest::Single(req)).await {
                    eprintln!("Failed to broadcast to peer: {}", e);
                }
            });
        }
    }

    async fn handle_command(&mut self, req: CommandRequest) {
        let CommandRequest { client_id, command, response_tx, replica_tx, pub_sub_tx, from_replica } = req;
        
        if let RedisCommand::InternalDisconnect = &command {
             self.transaction_state.remove(&client_id);
             for subs in self.pub_sub_subs.values_mut() {
                 subs.remove(&client_id);
             }
             let _ = response_tx.send(Ok(Value::SimpleString(Bytes::from("OK"))));
             return;
        }
        
        // Check subscription state
        let is_subscribed = self.pub_sub_subs.values().any(|subs| subs.contains_key(&client_id));
        if is_subscribed {
             match &command {
                RedisCommand::Subscribe { .. } | 
                RedisCommand::Unsubscribe { .. } => {},
                RedisCommand::Ping { message } => {
                    let resp = match message {
                        Some(msg) => Value::Array(vec![
                            Value::BulkString(Bytes::from("pong")),
                            Value::BulkString(msg.clone().into()),
                        ]),
                        None => Value::Array(vec![
                            Value::BulkString(Bytes::from("pong")),
                            Value::BulkString(Bytes::from("")),
                        ]),
                    };
                    let _ = response_tx.send(Ok(resp));
                    return;
                },
                _ => {
                    let _ = response_tx.send(Ok(Value::Error(Bytes::from(format!("ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context", command.name())))));
                    return;
                }
             }
        }
        
        // Check for transaction commands
        match command {
            RedisCommand::Multi => {
                if self.transaction_state.contains_key(&client_id) {
                    let _ = response_tx.send(Ok(Value::Error(Bytes::from("ERR MULTI calls can not be nested"))));
                } else {
                    self.transaction_state.insert(client_id, Vec::new());
                    let _ = response_tx.send(Ok(Value::SimpleString(Bytes::from("OK"))));
                }
                return;
            }
            RedisCommand::Discard => {
                if self.transaction_state.remove(&client_id).is_some() {
                    let _ = response_tx.send(Ok(Value::SimpleString(Bytes::from("OK"))));
                } else {
                    let _ = response_tx.send(Ok(Value::Error(Bytes::from("ERR DISCARD without MULTI"))));
                }
                return;
            }
            RedisCommand::Exec => {
                if let Some(commands) = self.transaction_state.remove(&client_id) {
                    if commands.is_empty() {
                         let _ = response_tx.send(Ok(Value::Array(vec![])));
                         return;
                    }
                    
                    let mut results = Vec::new();
                    for cmd in commands {
                        // For now, we don't support blocking commands in transaction properly
                        // We just execute them. If they block, we might get BLOCKED error or handle it.
                        // But execute_command_immediate returns Result<Value>.
                        
                        // We need to handle replica_tx for each command?
                        // Yes, if we propagate.
                        // But we should probably propagate EXEC/MULTI to replicas?
                        // Or propagate individual commands?
                        // Redis propagates MULTI/EXEC block.
                        // But here our replicas are simple.
                        // Let's just execute and propagate individual commands if they do propagate.
                        // Wait, execute_command_immediate calls propagate_command.
                        // So if we call it, it will propagate.
                        // But it will propagate as individual commands.
                        // That's fine for now.
                        
                        match self.execute_command_immediate(client_id, cmd, None, None, from_replica).await {
                            Ok(val) => results.push(val),
                            Err(e) => {
                                results.push(Value::Error(Bytes::from(e.to_string())));
                            }
                        }
                    }
                    let _ = response_tx.send(Ok(Value::Array(results)));
                } else {
                    let _ = response_tx.send(Ok(Value::Error(Bytes::from("ERR EXEC without MULTI"))));
                }
                return;
            }
            _ => {}
        }
        
        // If in transaction, queue command
        if let Some(queue) = self.transaction_state.get_mut(&client_id) {
             match &command {
                 RedisCommand::Subscribe { .. } | RedisCommand::Unsubscribe { .. } => {
                     let _ = response_tx.send(Ok(Value::Error(Bytes::from("ERR subscribe inside MULTI is not allowed"))));
                     return;
                 }
                 _ => {}
             }
            queue.push(command);
            let _ = response_tx.send(Ok(Value::SimpleString(Bytes::from("QUEUED"))));
            return;
        }

        match command {
            RedisCommand::Wait { num_replicas, timeout } => {
                self.handle_wait(num_replicas, timeout, response_tx).await;
            }
            other => {
                // Handle BLPOP separately to support blocking semantics for normal clients
                if let RedisCommand::BLPop { keys, timeout } = other.clone() {
                    self.handle_blpop(keys, timeout, response_tx).await;
                    return;
                }

                // Check if it's XREAD with BLOCK

                
                let result = self.execute_command_immediate(client_id, other.clone(), replica_tx, pub_sub_tx, from_replica).await;
                
                match result {
                    Ok(Value::Error(msg)) if msg == "BLOCKED" => {
                        // Handle blocking XREAD
                        if let RedisCommand::XRead { block: Some(block_ms), streams } = other {
                            // Resolve streams again? No, we need to pass resolved streams.
                            // But execute_command_immediate already did resolution.
                            // We should probably just move XREAD logic here or split it.
                            // Re-resolving is fine for now as it's cheap (just getting last ID).
                            
                            let mut resolved_streams = Vec::new();
                            for (key, id_str) in streams {
                                let start_id = if id_str == "$" {
                                    self.db.get_last_stream_id(&key).unwrap_or((0, 0))
                                } else {
                                    let parts: Vec<&str> = id_str.split('-').collect();
                                    let ms = parts[0].parse::<u64>().unwrap_or(0);
                                    let seq = parts[1].parse::<u64>().unwrap_or(0);
                                    (ms, seq)
                                };
                                resolved_streams.push((key, start_id));
                            }
                            
                            self.handle_read_block(block_ms, resolved_streams, response_tx).await;
                        }
                    }
                    _ => {
                        let _ = response_tx.send(result);
                    }
                }
            }
        }
    }
    
    async fn handle_read_block(&mut self, block_ms: u64, streams: Vec<(String, (u64, u64))>, response_tx: oneshot::Sender<Result<Value>>) {
        let read_index = self.pending_reads.len();
        
        // If block_ms is 0, it means block indefinitely.
        // We don't spawn a timeout task in that case.
        if block_ms > 0 {
            let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(block_ms);
            let timeout_tx = self.read_timeout_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep_until(deadline).await;
                let _ = timeout_tx.send(read_index).await;
            });
        }
        
        self.pending_reads.push(Some(PendingRead {
            response_tx,
            streams,
        }));
    }

    async fn handle_wait(&mut self, num_replicas: usize, timeout: u64, response_tx: oneshot::Sender<Result<Value>>) {
        // 1. Count replicas that are already synced up to current offset
        let synced_count = self.replicas.iter()
            .filter(|r| r.offset >= self.replication_offset)
            .count();
        
        if synced_count >= num_replicas {
            let _ = response_tx.send(Ok(Value::Integer(synced_count as i64)));
            return;
        }
        
        // 2. Send REPLCONF GETACK * to all replicas
        let getack_cmd = Value::Array(vec![
            Value::BulkString(Bytes::from("REPLCONF")),
            Value::BulkString(Bytes::from("GETACK")),
            Value::BulkString(Bytes::from("*")),
        ]);
        
        for replica in &self.replicas {
            let _ = replica.tx.send(getack_cmd.clone()).await;
        }
        
        // 3. Store pending wait
        let wait_index = self.pending_waits.len();
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout);
        
        self.pending_waits.push(Some(PendingWait {
            num_replicas,
            response_tx,
            target_offset: self.replication_offset,
        }));
        
        // Spawn timeout task
        let timeout_tx = self.timeout_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            let _ = timeout_tx.send(wait_index).await;
        });
    }

    fn complete_wait(&mut self, idx: usize) {
        if let Some(Some(wait)) = self.pending_waits.get_mut(idx) {
            let synced_count = self.replicas.iter()
                .filter(|r| r.offset >= wait.target_offset)
                .count();
                
            if let Some(pending) = self.pending_waits[idx].take() {
                    let _ = pending.response_tx.send(Ok(Value::Integer(synced_count as i64)));
            }
        }
    }

    async fn handle_subscribe(&mut self, client_id: u64, channels: Vec<String>, pub_sub_tx: Option<mpsc::Sender<Value>>) -> Result<Value, anyhow::Error> {
        if let Some(tx) = pub_sub_tx {
            for channel in &channels {
                let subs = self.pub_sub_subs.entry(channel.clone()).or_insert(HashMap::new());
                subs.insert(client_id, tx.clone());
                
                let mut client_sub_count = 0;
                for subs in self.pub_sub_subs.values() {
                    if subs.contains_key(&client_id) {
                        client_sub_count += 1;
                    }
                }
                
                let push_msg = Value::Array(vec![
                    Value::BulkString(Bytes::from("subscribe")),
                    Value::BulkString(channel.clone().into()),
                    Value::Integer(client_sub_count),
                ]);
                
                let _ = tx.send(push_msg).await;
            }
            Ok(Value::Error(Bytes::from("NO_REPLY")))
        } else {
            Ok(Value::Error(Bytes::from("ERR no pub/sub channel")))
        }
    }

    async fn handle_unsubscribe(&mut self, client_id: u64, channels: Vec<String>, pub_sub_tx: Option<mpsc::Sender<Value>>) -> Result<Value, anyhow::Error> {
        if let Some(tx) = pub_sub_tx {
            let channels_to_unsub = if channels.is_empty() {
                let mut all = Vec::new();
                for (channel, subs) in &self.pub_sub_subs {
                    if subs.contains_key(&client_id) {
                        all.push(channel.clone());
                    }
                }
                all
            } else {
                channels
            };
            
            for channel in &channels_to_unsub {
                if let Some(subs) = self.pub_sub_subs.get_mut(channel) {
                    subs.remove(&client_id);
                }
                
                let mut client_sub_count = 0;
                for subs in self.pub_sub_subs.values() {
                    if subs.contains_key(&client_id) {
                        client_sub_count += 1;
                    }
                }
                
                let push_msg = Value::Array(vec![
                    Value::BulkString(Bytes::from("unsubscribe")),
                    Value::BulkString(channel.clone().into()),
                    Value::Integer(client_sub_count),
                ]);
                
                let _ = tx.send(push_msg).await;
            }
            Ok(Value::Error(Bytes::from("NO_REPLY")))
        } else {
            Ok(Value::Error(Bytes::from("ERR no pub/sub channel")))
        }
    }

    async fn handle_publish(&mut self, channel: String, message: String) -> Result<Value, anyhow::Error> {
        let mut count = 0;
        if let Some(subs) = self.pub_sub_subs.get(&channel) {
            let push_msg = Value::Array(vec![
                Value::BulkString(Bytes::from("message")),
                Value::BulkString(channel.clone().into()),
                Value::BulkString(message.into()),
            ]);
            
            for tx in subs.values() {
                let _ = tx.send(push_msg.clone()).await;
                count += 1;
            }
        }
        Ok(Value::Integer(count))
    }

    async fn execute_command_immediate(&mut self, client_id: u64, command: RedisCommand, resp_tx: Option<mpsc::Sender<Value>>, pub_sub_tx: Option<mpsc::Sender<Value>>, from_replica: bool) -> Result<Value, anyhow::Error> {
        match command {
            RedisCommand::Ping { message } => {
                match message {
                    Some(msg) => Ok(Value::BulkString(msg.into())),
                    None => Ok(Value::SimpleString(Bytes::from("PONG"))),
                }
            }
            RedisCommand::Echo { message } => Ok(Value::BulkString(message.into())),
            RedisCommand::ConfigGet { parameter } => {
                let value = match parameter.to_lowercase().as_str() {
                    "dir" => Some(self.config.data_dir.clone()),
                    "dbfilename" => Some(self.config.db_filename.clone()),
                    _ => None,
                };

                if let Some(val) = value {
                    Ok(Value::Array(vec![
                        Value::BulkString(parameter.into()),
                        Value::BulkString(val.into()),
                    ]))
                } else {
                    Ok(Value::Array(vec![]))
                }
            }
            RedisCommand::Subscribe { channels } => {
                self.handle_subscribe(client_id, channels, pub_sub_tx).await
            }
            RedisCommand::Unsubscribe { channels } => {
                self.handle_unsubscribe(client_id, channels, pub_sub_tx).await
            }
            RedisCommand::Publish { channel, message } => {
                self.handle_publish(channel, message).await
            }
            RedisCommand::Keys { pattern } => {
                println!("Engine: Executing KEYS with pattern '{}'", pattern);
                let keys = self.db.keys(&pattern);
                println!("Engine: Found {} keys matching pattern '{}'", keys.len(), pattern);
                let resp_values = keys.into_iter()
                    .map(|s| Value::BulkString(s.into()))
                    .collect();
                Ok(Value::Array(resp_values))
            }
            RedisCommand::Type { key } => {
                let t = self.db.key_type(&key);
                Ok(Value::SimpleString(Bytes::from(t)))
            }
            RedisCommand::Set { key, value, px } => {
                // Use set_bytes() for zero-copy key handling
                self.db.set_bytes(key.clone(), value.clone(), px);
                
                let mut args = vec![
                    Value::BulkString(Bytes::from("SET")),
                    Value::BulkString(key.clone()),
                    Value::BulkString(value.clone()),
                ];
                
                if let Some(ms) = px {
                    args.push(Value::BulkString(Bytes::from("PX")));
                    args.push(Value::BulkString(Bytes::from(ms.to_string())));
                }
                
                let cmd_value = Value::Array(args);
                self.propagate_command(cmd_value).await;
                
                if !from_replica {
                    self.broadcast_to_peers(RedisCommand::Set { key: key.clone(), value: value.clone(), px }).await;
                }
                
                Ok(Value::SimpleString(Bytes::from("OK")))
            }
            RedisCommand::XAdd { key, id, fields } => {
                // Parse ID
                let (ms, seq) = if id == "*" {
                    // Auto-generate
		            let last_id = self.db.get_last_stream_id(&key);
		            let (last_ms, last_seq) = last_id.unwrap_or((0, 0));
		            
		            let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
		                Ok(dur) => dur.as_millis() as u64,
		                Err(_) => last_ms,
		            };
                    
                    if now > last_ms {
                        (now, 0)
                    } else {
                        // If now <= last_ms, we must increment sequence.
                        // Even if now < last_ms (clock skew), we must ensure monotonicity.
                        // Wait, if now < last_ms, we should probably use last_ms?
                        // Redis spec says: "If the ID specified is *, the ID is generated automatically... 
                        // The milliseconds part is the current time... if the current time is smaller or equal to the previous entry time, the sequence number is incremented... if the milliseconds part is smaller than the previous entry time, the previous entry time is used instead."
                        // Actually, let's just use max(now, last_ms).
                        
                        let ms = if now < last_ms { last_ms } else { now };
                        let seq = if ms == last_ms { last_seq + 1 } else { 0 };
                        (ms, seq)
                    }
                } else {
                    // Parse explicit ID
                    let parts: Vec<&str> = id.split('-').collect();
                    if parts.len() != 2 {
                        return Ok(Value::Error(Bytes::from("ERR The ID specified in XADD must be greater than 0-0"))); // Invalid format, but let's return error
                        // Actually, invalid format should be handled.
                        // Let's assume format is valid-ish or return error.
                    }
                    
                    let ms = parts[0].parse::<u64>().map_err(|_| anyhow::Error::msg("Invalid ID"))?;
                    let seq = if parts[1] == "*" {
                        // Partial auto-generation: <ms>-*
                        let last_id = self.db.get_last_stream_id(&key);
                        let (last_ms, last_seq) = last_id.unwrap_or((0, 0));
                        
                        if ms > last_ms {
                            0
                        } else if ms == last_ms {
                            last_seq + 1
                        } else {
                            // ms < last_ms. This is allowed if explicit ID, but we need to validate later.
                            // But for partial auto-generation, we just generate.
                            // Wait, if ms < last_ms, and we generate seq, it will fail validation in db.add_stream_entry.
                            // So just generate 0 or something?
                            // Let's just generate based on logic:
                            // If ms == last_ms, seq = last_seq + 1.
                            // If ms != last_ms, seq = 0.
                            // Validation will happen in db.add_stream_entry.
                            if ms == last_ms { last_seq + 1 } else { 0 }
                        }
                    } else {
                        parts[1].parse::<u64>().map_err(|_| anyhow::Error::msg("Invalid ID"))?
                    };
                    
                    (ms, seq)
                };
                
                match self.db.add_stream_entry(key.clone(), (ms, seq), fields.clone()) {
                    Ok((ms, seq)) => {
                        let id_str = format!("{}-{}", ms, seq);
                        
                        if !from_replica {
                            // Broadcast with generated ID
                            self.broadcast_to_peers(RedisCommand::XAdd { 
                                key: key.clone(), 
                                id: id_str.clone(), 
                                fields: fields.clone() 
                            }).await;
                        }

                        // Check pending reads
                        let mut completed_indices = Vec::new();
                        for (i, read_opt) in self.pending_reads.iter().enumerate() {
                            if let Some(read) = read_opt {
                                for (stream_key, start_id) in &read.streams {
                                    if *stream_key == key {
                                        if ms > start_id.0 || (ms == start_id.0 && seq > start_id.1) {
                                            completed_indices.push(i);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        
                        for i in completed_indices {
                            self.complete_read_with_data(i);
                        }
                        
                        Ok(Value::BulkString(id_str.into()))
                    },
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::XRead { block, streams } => {
                let mut result_streams = Vec::new();
                let mut has_results = false;
                
                // Resolve $ to current last ID if needed
                let mut resolved_streams = Vec::new();
                for (key, id_str) in &streams {
                    let start_id = if id_str == "$" {
                        self.db.get_last_stream_id(key).unwrap_or((0, 0))
                    } else {
                        let parts: Vec<&str> = id_str.split('-').collect();
                        if parts.len() != 2 {
                            if id_str == "0" {
                                (0, 0)
                            } else {
                                return Ok(Value::Error(Bytes::from("ERR Invalid stream ID specified as stream command argument")));
                            }
                        } else {
                            let ms = parts[0].parse::<u64>().unwrap_or(0);
                            let seq = parts[1].parse::<u64>().unwrap_or(0);
                            (ms, seq)
                        }
                    };
                    resolved_streams.push((key.clone(), start_id));
                }
                
                // Check for results immediately
                for (key, start_id) in &resolved_streams {
                    if let Some(entries) = self.db.read_stream(key, *start_id) {
                        has_results = true;
                        let mut stream_entries = Vec::new();
                        for entry in entries {
                            let id_str = format!("{}-{}", entry.id.0, entry.id.1);
                            let mut fields_val = Vec::new();
                            for (k, v) in entry.fields {
                                fields_val.push(Value::BulkString(k.into()));
                                fields_val.push(Value::BulkString(v.into()));
                            }
                            
                            stream_entries.push(Value::Array(vec![
                                Value::BulkString(id_str.into()),
                                Value::Array(fields_val)
                            ]));
                        }
                        
                        result_streams.push(Value::Array(vec![
                            Value::BulkString(key.clone().into()),
                            Value::Array(stream_entries)
                        ]));
                    }
                }
                
                if has_results {
                    return Ok(Value::Array(result_streams));
                }
                
                if let Some(_) = block {
                    // Block client
                    // We need to send the response later, so we can't return Ok(Value) here.
                    // But execute_command_immediate returns Result<Value>.
                    // We need to change how handle_command works for XRead with block.
                    // Or, we can return a special value indicating "Blocked"?
                    // But handle_command sends the result to response_tx immediately.
                    // We need to take ownership of response_tx in handle_command for XRead.
                    // But execute_command_immediate is called by handle_command which holds response_tx.
                    // Refactor needed: handle_xread should be separate like handle_wait.
                    
                    // For now, let's return a special error or value that handle_command recognizes?
                    // No, cleaner to refactor handle_command.
                    
                    // Since I can't easily change the signature of execute_command_immediate in this tool call without changing handle_command too...
                    // I will return a special error "BLOCKED" and handle it in handle_command?
                    // No, that's hacky.
                    
                    // I will return Ok(Value::Null) here, but I need to signal that I've taken over response_tx.
                    // But I haven't taken over response_tx because it's not passed to execute_command_immediate.
                    
                    // I MUST refactor handle_command to handle XRead separately.
                    // So I will return an error here saying "HandledSeparately" and catch it?
                    // Or just move XRead to handle_command.
                    
                    // Let's move XRead to handle_command in the next step.
                    // For this step, I'll just implement the non-blocking logic properly with resolved_streams.
                    // And if block is set and no results, I'll return a special Value::Error("BLOCKED").
                    
                    return Ok(Value::Error(Bytes::from("BLOCKED")));
                }
                
                Ok(Value::Null)
            }
            RedisCommand::XRange { key, start, end } => {
                // Parse start and end IDs
                let parse_id = |s: &str| -> Result<(u64, u64)> {
                    if s == "-" {
                        Ok((0, 0))
                    } else if s == "+" {
                        Ok((u64::MAX, u64::MAX))
                    } else {
                        let parts: Vec<&str> = s.split('-').collect();
                        let ms = parts[0].parse::<u64>().unwrap_or(0);
                        let seq = if parts.len() > 1 {
                            parts[1].parse::<u64>().unwrap_or(0)
                        } else {
                            0
                        };
                        Ok((ms, seq))
                    }
                };
                
                let start_id = match parse_id(&start) {
                    Ok(id) => id,
                    Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid stream ID specified as stream command argument"))),
                };
                
                let end_id = match parse_id(&end) {
                    Ok(id) => id,
                    Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid stream ID specified as stream command argument"))),
                };
                
                if let Some(entries) = self.db.range_stream(&key, start_id, end_id) {
                    let mut stream_entries = Vec::new();
                    for entry in entries {
                        let id_str = format!("{}-{}", entry.id.0, entry.id.1);
                        let mut fields_val = Vec::new();
                        for (k, v) in entry.fields {
                            fields_val.push(Value::BulkString(k.into()));
                            fields_val.push(Value::BulkString(v.into()));
                        }
                        
                        stream_entries.push(Value::Array(vec![
                            Value::BulkString(id_str.into()),
                            Value::Array(fields_val)
                        ]));
                    }
                    Ok(Value::Array(stream_entries))
                } else {
                    Ok(Value::Array(vec![])) // Empty array if key doesn't exist or no entries
                }
            }
            RedisCommand::RPush { key, values } => {
                let bytes_values: Vec<bytes::Bytes> = values.iter()
                    .map(|v| bytes::Bytes::from(v.clone()))
                    .collect();
                    
                match self.db.rpush(key.clone(), bytes_values) {
                    Ok(len) => {
                        // Propagate RPUSH
                        let mut args = vec![
                            Value::BulkString(Bytes::from("RPUSH")),
                            Value::BulkString(key.clone().into()),
                        ];
                        for v in &values {
                            args.push(Value::BulkString(v.clone().into()));
                        }
                        self.propagate_command(Value::Array(args)).await;
                        // Wake any clients blocked on BLPOP for this key
                        self.check_blocked_list_clients(&key).await;
                        
                        if !from_replica {
                            self.broadcast_to_peers(RedisCommand::RPush { key: key.clone(), values: values.clone() }).await;
                        }

                        Ok(Value::Integer(len as i64))
                    }
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::LRange { key, start, end } => {
                match self.db.lrange(&key, start, end) {
                    Ok(values) => {
                        let resp_values = values.iter()
                            .map(|v| Value::BulkString(Bytes::from(String::from_utf8_lossy(v).to_string())))
                            .collect();
                        Ok(Value::Array(resp_values))
                    }
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::LPush { key, values } => {
                let bytes_values: Vec<bytes::Bytes> = values.iter()
                    .map(|v| bytes::Bytes::from(v.clone()))
                    .collect();
                    
                match self.db.lpush(key.clone(), bytes_values) {
                    Ok(len) => {
                        // Propagate LPUSH
                        let mut args = vec![
                            Value::BulkString(Bytes::from("LPUSH")),
                            Value::BulkString(key.clone().into()),
                        ];
                        for v in &values {
                            args.push(Value::BulkString(v.clone().into()));
                        }
                        self.propagate_command(Value::Array(args)).await;
                        
                        // Check if any blocked clients are waiting for this key
                        self.check_blocked_list_clients(&key).await;
                        
                        if !from_replica {
                            self.broadcast_to_peers(RedisCommand::LPush { key: key.clone(), values: values.clone() }).await;
                        }
                        
                        Ok(Value::Integer(len as i64))
                    }
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::LLen { key } => {
                match self.db.llen(&key) {
                    Ok(len) => Ok(Value::Integer(len as i64)),
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::LPop { key, count } => {
                match self.db.lpop(&key, count) {
                    Ok(Some(values)) => {
                        // Propagate LPOP
                        let mut args = vec![
                            Value::BulkString(Bytes::from("LPOP")),
                            Value::BulkString(key.clone().into()),
                        ];
                        if let Some(c) = count {
                            args.push(Value::BulkString(Bytes::from(c.to_string())));
                        }
                        let cmd_value = Value::Array(args);
                        self.propagate_command(cmd_value).await;
                        
                        if !from_replica {
                            self.broadcast_to_peers(RedisCommand::LPop { key: key.clone(), count }).await;
                        }
                        
                        if count.is_none() {
                            // Single value
                            Ok(Value::BulkString(Bytes::from(String::from_utf8_lossy(&values[0]).to_string())))
                        } else {
                            // Array
                            let resp_values = values.iter()
                                .map(|v| Value::BulkString(Bytes::from(String::from_utf8_lossy(v).to_string())))
                                .collect();
                            Ok(Value::Array(resp_values))
                        }
                    }
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::BLPop { keys, timeout: _timeout } => {
                // BLPOP inside MULTI/EXEC behaves like LPOP (non-blocking)
                for key in &keys {
                    match self.db.lpop(key, None) {
                        Ok(Some(values)) => {
                            let args = vec![
                                Value::BulkString(Bytes::from("LPOP")),
                                Value::BulkString(key.clone().into()),
                            ];
                            self.propagate_command(Value::Array(args)).await;

                            return Ok(Value::Array(vec![
                                Value::BulkString(key.clone().into()),
                                Value::BulkString(Bytes::from(String::from_utf8_lossy(&values[0]).to_string()))
                            ]));
                        }
                        Ok(None) => continue,
                        Err(e) => return Ok(Value::Error(e.into())),
                    }
                }

                Ok(Value::Null)
            }
            RedisCommand::InternalDisconnect => Ok(Value::SimpleString(Bytes::from("OK"))),
            RedisCommand::ZAdd { key, entries } => {
                match self.db.zadd(key.clone(), entries.clone()) {
                    Ok(added) => {
                        if !from_replica {
                            self.broadcast_to_peers(RedisCommand::ZAdd { key, entries }).await;
                        }
                        Ok(Value::Integer(added as i64))
                    },
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::ZRange { key, start, end, with_scores } => {
                match self.db.zrange(&key, start, end) {
                    Ok(items) => {
                         let mut resp = Vec::new();
                         for (member, score) in items {
                             resp.push(Value::BulkString(member.into()));
                             if with_scores {
                                 if let Some(s) = score {
                                     resp.push(Value::BulkString(Bytes::from(s.to_string())));
                                 } else {
                                     resp.push(Value::Null);
                                 }
                             }
                         }
                         Ok(Value::Array(resp))
                    }
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::ZCard { key } => {
                match self.db.zcard(&key) {
                    Ok(count) => Ok(Value::Integer(count as i64)),
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::ZScore { key, member } => {
                match self.db.zscore(&key, &member) {
                    Ok(Some(score)) => Ok(Value::BulkString(Bytes::from(score.to_string()))),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::ZRank { key, member } => {
                match self.db.zrank(&key, &member) {
                    Ok(Some(rank)) => Ok(Value::Integer(rank as i64)),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::ZRem { key, members } => {
                match self.db.zrem(&key, &members) {
                    Ok(removed) => {
                        if !from_replica {
                            self.broadcast_to_peers(RedisCommand::ZRem { key: key.clone(), members: members.clone() }).await;
                        }
                        Ok(Value::Integer(removed as i64))
                    },
                    Err(e) => Ok(Value::Error(e.into())),
                }
            }
            RedisCommand::Get { key } => {
                // Use get_bytes() for zero-copy key lookup
                match self.db.get_bytes(&key) {
                    Some(value) => Ok(Value::BulkString(value)),
                    None => Ok(Value::Null),
                }
            }
            RedisCommand::Incr { key } => {
                // Use get_bytes() for zero-copy key lookup
                let current_val = match self.db.get_bytes(&key) {
                    Some(bytes) => {
                        let s = String::from_utf8_lossy(&bytes);
                        match s.parse::<i64>() {
                            Ok(n) => n,
                            Err(_) => return Ok(Value::Error(Bytes::from("ERR value is not an integer or out of range"))),
                        }
                    }
                    None => 0,
                };
                
                let new_val = current_val + 1;
                let new_val_bytes = Bytes::from(new_val.to_string());
                // Convert key to String at storage boundary
                let key_str = String::from_utf8_lossy(&key).to_string();
                self.db.set(key_str, new_val_bytes.clone(), None);
                
                // Propagate as SET for simplicity
                let args = vec![
                    Value::BulkString(Bytes::from("SET")),
                    Value::BulkString(key.clone()),
                    Value::BulkString(new_val_bytes.clone()),
                ];
                self.propagate_command(Value::Array(args)).await;
                
                if !from_replica {
                    // Broadcast SET for convergence
                    self.broadcast_to_peers(RedisCommand::Set { key: key.clone(), value: new_val_bytes, px: None }).await;
                }
                
                Ok(Value::Integer(new_val))
            }
            RedisCommand::Info { section: _ } => {
                let role = match self.config.role {
                    crate::config::ServerRole::Master => "master",
                    crate::config::ServerRole::Slave => "slave",
                };
                let info = format!(
                    "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                    role, self.config.master_replid, self.config.master_repl_offset
                );
                Ok(Value::BulkString(info.into()))
            }
            RedisCommand::ReplConf { subcommand, args } => {
                if subcommand.to_uppercase() == "ACK" {
                    if let Some(offset_str) = args.get(0) {
                        if let Ok(offset) = offset_str.parse::<i64>() {
                            // Update replica offset
                            if let Some(replica) = self.replicas.iter_mut().find(|r| r.id == client_id) {
                                replica.offset = offset;
                                
                                // Check pending waits
                                let mut completed_indices = Vec::new();
                                for (i, wait_opt) in self.pending_waits.iter().enumerate() {
                                    if let Some(wait) = wait_opt {
                                        let synced_count = self.replicas.iter()
                                            .filter(|r| r.offset >= wait.target_offset)
                                            .count();
                                        
                                        if synced_count >= wait.num_replicas {
                                            completed_indices.push(i);
                                        }
                                    }
                                }
                                
                                for i in completed_indices {
                                    self.complete_wait(i);
                                }
                            }
                        }
                    }
                }
                Ok(Value::SimpleString(Bytes::from("OK")))
            }
            RedisCommand::PSync { replication_id: _, offset: _ } => {
                // Register replica if channel provided
                if let Some(tx) = resp_tx {
                    self.replicas.push(Replica {
                        id: client_id,
                        tx,
                        offset: 0,
                    });
                }
                
                let response = format!("FULLRESYNC {} {}", self.config.master_replid, self.config.master_repl_offset);
                // Send FULLRESYNC first
                // let _ = req.response_tx.send(Ok(Value::SimpleString(Bytes::from(response))));
                
                // Then send RDB file
                // Empty RDB file in hex
                let empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
		        let empty_rdb = match hex::decode(empty_rdb_hex) {
		            Ok(bytes) => bytes,
		            Err(e) => {
		                eprintln!("Failed to decode built-in empty RDB: {}", e);
		                Vec::new()
		            }
		        };
                
                Ok(Value::Multiple(vec![
                    Value::SimpleString(Bytes::from(response)),
                    Value::RdbFile(empty_rdb)
                ]))
            }
            RedisCommand::Error { message } => {
                Ok(Value::SimpleString(Bytes::from(format!("ERR {}", message))))
            }
            RedisCommand::Wait { .. } => {
                // Should never reach here since Wait is handled separately
                Err(anyhow::Error::msg("WAIT should be handled in handle_command"))
            }
            RedisCommand::Multi | RedisCommand::Exec | RedisCommand::Discard => {
                // Should never reach here as they are handled in handle_command
                Err(anyhow::Error::msg("Transaction commands should be handled in handle_command"))
            }
            RedisCommand::None => {
                Ok(Value::Null)
            }
        }
    }
    
    async fn propagate_command(&mut self, cmd: Value) {
        // Update master offset
        let bytes = cmd.clone().serialize_bytes();
        self.replication_offset += bytes.len() as i64;
        
        for replica in &self.replicas {
            let _ = replica.tx.send(cmd.clone()).await;
        }
    }
    
    async fn check_blocked_list_clients(&mut self, key: &str) {
        let mut i = 0;
        while i < self.waiting_list_clients.len() {
            let (keys, _) = &self.waiting_list_clients[i];
            if keys.contains(&key.to_string()) {
                // Found a client waiting for this key
                let (_, tx) = self.waiting_list_clients.remove(i);
                
                // Try to pop from the key that triggered the wakeup
                match self.db.lpop(key, None) {
                    Ok(Some(values)) => {
                        let response = Value::Array(vec![
                            Value::BulkString(Bytes::from(key.to_string())),
                            Value::BulkString(Bytes::from(String::from_utf8_lossy(&values[0]).to_string()))
                        ]);
                        let _ = tx.send(Ok(response));
                        // We successfully unblocked one client with this element.
                        return; 
                    }
                    _ => {
                        // Should not happen if we just pushed.
                    }
                }
            } else {
                i += 1;
            }
        }
    }
}
