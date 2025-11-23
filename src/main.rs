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
    let port = config.port;
    println!("Listening on 127.0.0.1:{}", port);

    // 1. Determine number of shards (from config)
    let num_shards = config.num_shards;
    println!("Starting Mikkadb with {} shards (Share-Nothing Architecture)", num_shards);

    // 2. Spawn Engines (one per shard)
    let mut channels = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        channels.push(mpsc::channel(32));
    }

    let shard_txs: Vec<mpsc::Sender<CommandRequest>> = channels.iter().map(|(tx, _)| tx.clone()).collect();
    
    for (shard_id, (_, rx)) in channels.into_iter().enumerate() {
        // Construct peers list: all txs except mine
        let mut peers = Vec::new();
        for (peer_id, peer_tx) in shard_txs.iter().enumerate() {
            if peer_id != shard_id {
                peers.push(peer_tx.clone());
            }
        }

        let mut db = Db::new();
        // Load RDB only for shard 0
        if shard_id == 0 {
             if let Err(e) = db.load_rdb(config.rdb_path()) {
                eprintln!("Failed to load RDB file: {}", e);
            }
        }

        let config_clone = config.clone();
        tokio::spawn(async move {
            let mut engine = Engine::new(shard_id, config_clone, rx, db, peers);
            engine.run().await;
        });
    }
    
    // Share shard channels with connection handlers
    let shard_channels = Arc::new(shard_txs);

    if let crate::config::ServerRole::Slave = config.role {
        if let (Some(host), Some(port)) = (&config.master_host, &config.master_port) {
            let host = host.clone();
            let port = port.clone();
            let listening_port = config.port.to_string();
            let tx = shard_channels[0].clone(); // Handshake logic needs update for sharding, use shard 0 for now
            tokio::spawn(async move {
                if let Err(e) = perform_handshake(host, port.to_string(), listening_port, tx).await {
                    eprintln!("Handshake error: {}", e);
                }
            });
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    
    let mut client_id_counter = 0;

    loop {
        let (stream, _) = listener.accept().await?;
        let shard_channels = shard_channels.clone();
        
        client_id_counter += 1;
        let client_id = client_id_counter;
        
        tokio::spawn(async move {
            let mut handler = resp::RespHandler::new(stream);
            let mut repl_rx: Option<mpsc::Receiver<crate::resp::Value>> = None;
            let (msg_tx, mut msg_rx) = mpsc::channel(32);

            // Pipelining constants
            const MAX_BATCH_COMMANDS: usize = 1024;
            const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024; // 4MB

            loop {
                tokio::select! {
                    value = handler.read_value() => {
                        match value {
                            Ok(Some(v)) => {
                                // Collect commands into batch
                                let mut command_batch = vec![v];
                                let mut batch_bytes = 0usize;

                                loop {
                                    if command_batch.len() >= MAX_BATCH_COMMANDS || batch_bytes >= MAX_BATCH_BYTES {
                                        break;
                                    }

                                    match handler.try_read_value_from_buf() {
                                        resp::ParseResult::Complete(val, _consumed) => {
                                            batch_bytes += 100;
                                            command_batch.push(val);
                                        }
                                        resp::ParseResult::Incomplete => break,
                                        resp::ParseResult::Error(e) => {
                                            let _ = handler.write_value(resp::Value::Error(Bytes::from(format!("ERR {}", e)))).await;
                                            break;
                                        }
                                    }
                                }

                                // Process batch
                                let batch_size = command_batch.len();
                                let mut response_batch = Vec::with_capacity(batch_size);

                                for cmd_value in command_batch {
                                    match RedisCommand::from_resp(cmd_value) {
                                        Ok(command) => {
                                            let (resp_tx, resp_rx) = oneshot::channel();

                                            // Sharding Logic - Connection Based
                                            let shard_idx = (client_id as usize) % num_shards;
                                            let tx = &shard_channels[shard_idx];

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
                                                from_replica: false,
                                            };

                                            if let Err(_) = tx.send(req).await {
                                                // Engine dropped
                                                response_batch.push(resp::Value::Error(Bytes::from("ERR Engine shutdown")));
                                                break; 
                                            }

                                            match resp_rx.await {
                                                Ok(Ok(response)) => {
                                                    if let crate::resp::Value::Error(msg) = &response {
                                                        if msg == "NO_REPLY" {
                                                            continue; 
                                                        }
                                                    }
                                                    response_batch.push(response);
                                                }
                                                Ok(Err(e)) => {
                                                    response_batch.push(resp::Value::Error(Bytes::from(format!("ERR {}", e))));
                                                }
                                                Err(_) => {
                                                    break; 
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            response_batch.push(resp::Value::Error(Bytes::from(format!("ERR {}", e))));
                                        }
                                    }
                                }

                                if !response_batch.is_empty() {
                                    let _ = handler.write_batch(response_batch).await;
                                }

                                // Notify handler about batch size for adaptive buffering
                                handler.notify_batch_processed(batch_size);
                            }
                            Ok(None) => break,
                            Err(_e) => {
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
            
            // Client disconnected - Notify all shards?
            // Ideally we track which shards have state for this client.
            // For simplicity, broadcast disconnect to all shards.
            for tx in shard_channels.iter() {
                let (resp_tx, _) = oneshot::channel();
                let req = CommandRequest {
                    client_id,
                    command: RedisCommand::InternalDisconnect,
                    response_tx: resp_tx,
                    replica_tx: None,
                    pub_sub_tx: None,
                    from_replica: false,
                };
                let _ = tx.send(req).await;
            }
        });
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

    let mut offset = 0;

    // Process commands from master
    loop {
        let value = handler.read_value().await?;
        match value {
            Some(v) => {
                let len = v.clone().serialize_bytes().len();
                
                eprintln!("[repl] received from master: {:?}", v);
                match RedisCommand::from_resp(v) {
                    Ok(command) => {
                        eprintln!("[repl] parsed replication command: {:?}", command);
                        if let RedisCommand::ReplConf { subcommand, .. } = &command {
                            if subcommand.to_uppercase() == "GETACK" {
                                let ack = Value::Array(vec![
                                    Value::BulkString(Bytes::from("REPLCONF")),
                                    Value::BulkString(Bytes::from("ACK")),
                                    Value::BulkString(Bytes::from(offset.to_string())),
                                ]);
                                eprintln!("[repl] sending ACK {} to master", offset);
                                handler.write_value(ack).await?;
                                offset += len; // Update offset after ACK
                                continue;
                            }
                        }
                        
                        offset += len; // Update offset for non-GETACK commands
                        
                        // Execute command against Engine
                        let (resp_tx, resp_rx) = oneshot::channel();
                        let req = CommandRequest {
                            client_id: 0, // Internal/Replica ID
                            command,
                            response_tx: resp_tx,
                            replica_tx: None, // We are the replica, we don't propagate further
                            pub_sub_tx: None,
                            from_replica: false,
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
