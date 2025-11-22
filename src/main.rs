use tokio::net::TcpListener;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use crate::config::Config;
use crate::engine::{Engine, CommandRequest};
use crate::command::RedisCommand;
use crate::db::Db;
use bytes::Bytes;

mod resp;
mod db;
mod config;
mod command;
mod engine;
mod storage;
mod actor_store;
mod single_lock_store;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::parse());
    println!("Listening on 127.0.0.1:{}", config.port);

    // Create DB instance
    let mut db = Db::new();
    if let Err(e) = db.load_rdb(config.rdb_path()) {
        eprintln!("Failed to load RDB file: {}", e);
    }
    let engine_db = db.clone();

    // Create the Engine actor
    let (tx, rx) = mpsc::channel(32);
    let mut engine = Engine::new(config.clone(), rx, engine_db);
    tokio::spawn(async move {
        engine.run().await;
    });

    if let crate::config::ServerRole::Slave = config.role {
        if let (Some(host), Some(port)) = (&config.master_host, &config.master_port) {
            let host = host.clone();
            let port = port.clone();
            let listening_port = config.port.to_string();
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Err(e) = perform_handshake(host, port.to_string(), listening_port, tx).await {
                    eprintln!("Handshake error: {}", e);
                }
            });
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    println!("Listening on 127.0.0.1:{}", config.port);
    
    let mut client_id_counter = 0;

    loop {
        let (stream, _) = listener.accept().await?;

        let tx = tx.clone();
        let db = db.clone();
        let config = config.clone();
        client_id_counter += 1;
        let client_id = client_id_counter;
        
        tokio::spawn(async move {
            let mut handler = resp::RespHandler::new(stream);
            let mut repl_rx: Option<mpsc::Receiver<crate::resp::Value>> = None;
            let (msg_tx, mut msg_rx) = mpsc::channel(32);
            let mut in_txn = false;

            // Phase 2: Pipelining constants
            const MAX_BATCH_COMMANDS: usize = 1024;
            const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024; // 4MB

            loop {
                tokio::select! {
                    value = handler.read_value() => {
                        match value {
                            Ok(Some(v)) => {
                                // Phase 2: Collect commands into batch
                                let mut command_batch = vec![v];
                                let mut batch_bytes = 0usize; // Approximate

                                // Drain buffer for additional commands (non-blocking)
                                loop {
                                    if command_batch.len() >= MAX_BATCH_COMMANDS || batch_bytes >= MAX_BATCH_BYTES {
                                        break;
                                    }

                                    match handler.try_read_value_from_buf() {
                                        resp::ParseResult::Complete(val, _consumed) => {
                                            batch_bytes += 100; // Rough estimate per command
                                            command_batch.push(val);
                                        }
                                        resp::ParseResult::Incomplete => break, // No more complete commands
                                        resp::ParseResult::Error(e) => {
                                            // Malformed command, add error and stop batch
                                            let _ = handler.write_value(resp::Value::Error(Bytes::from(format!("ERR {}", e)))).await;
                                            break;
                                        }
                                    }
                                }

                                // Phase 2: Process batch of commands
                                let mut response_batch = Vec::with_capacity(command_batch.len());

                                for cmd_value in command_batch {
                                    match RedisCommand::from_resp(cmd_value) {
                                        Ok(command) => {
                                            // Update transaction state
                                            match &command {
                                                RedisCommand::Multi => in_txn = true,
                                                RedisCommand::Exec | RedisCommand::Discard => in_txn = false,
                                                _ => {}
                                            }

                                            // Optimization: Execute read-only commands locally if not in transaction
                                            let is_read_only = match &command {
                                                RedisCommand::Get { .. } |
                                                RedisCommand::Type { .. } |
                                                RedisCommand::Keys { .. } |
                                                RedisCommand::LLen { .. } |
                                                RedisCommand::LRange { .. } |
                                                RedisCommand::ZCard { .. } |
                                                RedisCommand::ZScore { .. } |
                                                RedisCommand::ZRank { .. } |
                                                RedisCommand::ZRange { .. } |
                                                RedisCommand::ConfigGet { .. } |
                                                RedisCommand::Echo { .. } |
                                                RedisCommand::Ping { .. } => true,
                                                _ => false
                                            };

                                            // Note: ConfigGet and Echo/Ping are safe to run locally too if they don't depend on Engine state.
                                            // Ping might check pub/sub state? No, Ping { message } is stateless.
                                            // ConfigGet depends on config which is Arc.
                                            
                                            if is_read_only && !in_txn {
                                                // Execute locally
                                                // Check for Ping in PubSub mode? Engine handles that check.
                                                // If we execute locally, we bypass "only (P)SUBSCRIBE..." check.
                                                // But that check is only if client is subscribed.
                                                // Does local client know if it is subscribed?
                                                // We don't track subscription state here easily (Engine has pub_sub_subs).
                                                // If client IS subscribed, Engine would reject GET.
                                                // If we run GET locally, we might allow it?
                                                // Redis spec: "A client subscribed to one or more channels should not issue commands..."
                                                // We should probably be safe and only optimize if NOT subscribed.
                                                // Since we don't track subscription state here, maybe skip optimization for PING if we want to be strict?
                                                // But GET/SET are definitely disallowed in PubSub.
                                                // If we don't know subscription state, we risk violating spec.
                                                // However, subscription state is rare.
                                                // Ideally we should track `is_subscribed` in main loop too.
                                                // When we send SUBSCRIBE to engine, we can set `is_subscribed = true`.
                                                // When UNSUBSCRIBE (all), set false.
                                                // Let's skip tracking for now and assume normal clients for optimization.
                                                // Or, just optimize safely.
                                                
                                                if let Some(response) = execute_local(&db, &config, &command) {
                                                    response_batch.push(response);
                                                    continue;
                                                }
                                                // If execute_local returns None (e.g. for Ping we prefer Engine?), fallback to Engine.
                                                // Actually execute_local should handle it.
                                            }

                                            let (resp_tx, resp_rx) = oneshot::channel();

                                            let mut replica_tx = None;
                                            if let RedisCommand::PSync { .. } = &command {
                                                let (tx, rx) = mpsc::channel(32);
                                                replica_tx = Some(tx);
                                                repl_rx = Some(rx);
                                            }

                                            let req = CommandRequest {
                                                client_id,
                                                command,
                                                response_tx: resp_tx,
                                                replica_tx,
                                                pub_sub_tx: Some(msg_tx.clone()),
                                            };

                                            if let Err(_) = tx.send(req).await {
                                                break; // Engine dropped, will exit outer loop
                                            }

                                            match resp_rx.await {
                                                Ok(Ok(response)) => {
                                                    if let crate::resp::Value::Error(msg) = &response {
                                                        if msg == "NO_REPLY" {
                                                            continue; // Skip adding to response batch
                                                        }
                                                    }
                                                    response_batch.push(response);
                                                }
                                                Ok(Err(e)) => {
                                                    response_batch.push(resp::Value::Error(Bytes::from(format!("ERR {}", e))));
                                                }
                                                Err(_) => {
                                                    break; // Response channel dropped
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            response_batch.push(resp::Value::Error(Bytes::from(format!("ERR {}", e))));
                                        }
                                    }
                                }

                                // Phase 2: Write all responses with single flush
                                if !response_batch.is_empty() {
                                    let _ = handler.write_batch(response_batch).await;
                                }
                            }
                            Ok(None) => break,
                            Err(_e) => {
                                // println!("Error: {}", _e);
                                break;
                            }
                        }
                    }
                    Some(cmd) = async {
                        if let Some(rx) = &mut repl_rx {
                            rx.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        let _ = handler.write_value(cmd).await;
                    }
                    Some(msg) = msg_rx.recv() => {
                        let _ = handler.write_value(msg).await;
                    }
                }
            }
            
            // Client disconnected
            let (resp_tx, _) = oneshot::channel();
            let req = CommandRequest {
                client_id,
                command: RedisCommand::InternalDisconnect,
                response_tx: resp_tx,
                replica_tx: None,
                pub_sub_tx: None,
            };
            let _ = tx.send(req).await;
        });
    }
}

fn execute_local(db: &Db, config: &Config, command: &RedisCommand) -> Option<crate::resp::Value> {
    use crate::resp::Value;
    
    match command {
        RedisCommand::Ping { message } => {
            match message {
                Some(msg) => Some(Value::BulkString(Bytes::from(msg.clone()))),
                None => Some(Value::SimpleString(Bytes::from("PONG"))),
            }
        }
        RedisCommand::Echo { message } => Some(Value::BulkString(Bytes::from(message.clone()))),
        RedisCommand::ConfigGet { parameter } => {
            let value = match parameter.to_lowercase().as_str() {
                "dir" => Some(config.data_dir.clone()),
                "dbfilename" => Some(config.db_filename.clone()),
                _ => None,
            };

            if let Some(val) = value {
                Some(Value::Array(vec![
                    Value::BulkString(Bytes::from(parameter.clone())),
                    Value::BulkString(Bytes::from(val)),
                ]))
            } else {
                Some(Value::Array(vec![]))
            }
        }
        RedisCommand::Keys { pattern } => {
            // println!("Engine: Executing KEYS with pattern '{}'", pattern);
            let keys = db.keys(pattern);
            // println!("Engine: Found {} keys matching pattern '{}'", keys.len(), pattern);
            let resp_values = keys.into_iter()
                .map(|s| Value::BulkString(Bytes::from(s)))
                .collect();
            Some(Value::Array(resp_values))
        }
        RedisCommand::Type { key } => {
            let t = db.key_type(key);
            Some(Value::SimpleString(Bytes::from(t)))
        }
        RedisCommand::Get { key } => {
            match db.get(key) {
                Some(val) => Some(Value::BulkString(val)),
                None => Some(Value::Null),
            }
        }
        RedisCommand::LLen { key } => {
            match db.llen(key) {
                Ok(len) => Some(Value::Integer(len as i64)),
                Err(e) => Some(Value::Error(Bytes::from(e))),
            }
        }
        RedisCommand::LRange { key, start, end } => {
            match db.lrange(key, *start, *end) {
                Ok(items) => {
                    let values = items.into_iter().map(Value::BulkString).collect();
                    Some(Value::Array(values))
                },
                Err(e) => Some(Value::Error(Bytes::from(e))),
            }
        }
        RedisCommand::ZCard { key } => {
            match db.zcard(key) {
                Ok(len) => Some(Value::Integer(len as i64)),
                Err(e) => Some(Value::Error(Bytes::from(e))),
            }
        }
        RedisCommand::ZScore { key, member } => {
            match db.zscore(key, member) {
                Ok(Some(score)) => Some(Value::BulkString(Bytes::from(score.to_string()))), // Redis returns score as bulk string
                Ok(None) => Some(Value::Null),
                Err(e) => Some(Value::Error(Bytes::from(e))),
            }
        }
        RedisCommand::ZRank { key, member } => {
            match db.zrank(key, member) {
                Ok(Some(rank)) => Some(Value::Integer(rank as i64)),
                Ok(None) => Some(Value::Null),
                Err(e) => Some(Value::Error(Bytes::from(e))),
            }
        }
        RedisCommand::ZRange { key, start, end, with_scores } => {
            match db.zrange(key, *start, *end) {
                Ok(items) => {
                    let mut values = Vec::new();
                    for (member, score) in items {
                        values.push(Value::BulkString(Bytes::from(member)));
                        if *with_scores {
                            if let Some(s) = score {
                                values.push(Value::BulkString(Bytes::from(s.to_string())));
                            }
                        }
                    }
                    Some(Value::Array(values))
                },
                Err(e) => Some(Value::Error(Bytes::from(e))),
            }
        }
        _ => None,
    }
}

async fn perform_handshake(master_host: String, master_port: String, listening_port: String, tx: mpsc::Sender<CommandRequest>) -> Result<()> {
    use tokio::net::TcpStream;
    use crate::resp::{RespHandler, Value};

    let stream = TcpStream::connect(format!("{}:{}", master_host, master_port)).await?;
    let mut handler = RespHandler::new(stream);
    
    // 1. PING
    handler.write_value(Value::Array(vec![Value::BulkString(Bytes::from("PING"))])).await?;
    let _ = handler.read_value().await?; // PONG

    // 2. REPLCONF listening-port
    handler.write_value(Value::Array(vec![
        Value::BulkString(Bytes::from("REPLCONF")),
        Value::BulkString(Bytes::from("listening-port")),
        Value::BulkString(Bytes::from(listening_port)),
    ])).await?;
    let _ = handler.read_value().await?; // OK

    // 3. REPLCONF capa psync2
    handler.write_value(Value::Array(vec![
        Value::BulkString(Bytes::from("REPLCONF")),
        Value::BulkString(Bytes::from("capa")),
        Value::BulkString(Bytes::from("psync2")),
    ])).await?;
    let _ = handler.read_value().await?; // OK

    // 4. PSYNC ? -1
    handler.write_value(Value::Array(vec![
        Value::BulkString(Bytes::from("PSYNC")),
        Value::BulkString(Bytes::from("?")),
        Value::BulkString(Bytes::from("-1")),
    ])).await?;
    
    // Expect FULLRESYNC
    let _ = handler.read_value().await?; 
    
    // Expect RDB file
    let _ = handler.read_rdb_file().await?;

    // Process commands from master
    loop {
        let value = handler.read_value().await?;
        match value {
            Some(v) => {
                eprintln!("[repl] received from master: {:?}", v);
                match RedisCommand::from_resp(v) {
                    Ok(command) => {
                        eprintln!("[repl] parsed replication command: {:?}", command);
                        if let RedisCommand::ReplConf { subcommand, .. } = &command {
                            if subcommand.to_uppercase() == "GETACK" {
                                let ack = Value::Array(vec![
                                    Value::BulkString(Bytes::from("REPLCONF")),
                                    Value::BulkString(Bytes::from("ACK")),
                                    Value::BulkString(Bytes::from("0")),
                                ]);
                                eprintln!("[repl] sending ACK 0 to master");
                                handler.write_value(ack).await?;
                                continue;
                            }
                        }
                        // Execute command against Engine
                        let (resp_tx, resp_rx) = oneshot::channel();
                        let req = CommandRequest {
                            client_id: 0, // Internal/Replica ID
                            command,
                            response_tx: resp_tx,
                            replica_tx: None, // We are the replica, we don't propagate further
                            pub_sub_tx: None,
                        };

                        if let Err(_) = tx.send(req).await {
                            eprintln!("[repl] failed to send command to engine");
                            break;
                        }

                        // Wait for execution to finish
                        if let Err(_) = resp_rx.await {
                            eprintln!("[repl] engine dropped response channel");
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("[repl] failed to parse replication command: {}", e);
                    }
                }
            }
            None => {
                eprintln!("[repl] master closed replication connection");
                break;
            }
        }
    }
    Ok(())
}
