use tokio::net::TcpListener;
use anyhow::Result;
use std::sync::Arc;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;
use socket2::{Socket, Domain, Type, Protocol};
use std::net::SocketAddr;
use crate::config::Config;
use crate::engine::{Engine, CommandRequest, EngineRequest};
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

fn main() -> Result<()> {
    let config = Arc::new(Config::parse());
    let port = config.port;
    let num_shards = config.num_shards;
    
    println!("Starting Mikkadb with {} shards (Thread-Per-Shard Architecture)", num_shards);
    println!("Listening on 127.0.0.1:{}", port);

    // Create replication channels for cross-shard writes
    let mut channels: Vec<(mpsc::Sender<EngineRequest>, mpsc::Receiver<EngineRequest>)> = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        channels.push(mpsc::channel(256));
    }

    let shard_txs: Vec<mpsc::Sender<EngineRequest>> = channels.iter().map(|(tx, _)| tx.clone()).collect();
    
    // Spawn dedicated OS thread per shard
    let mut handles = Vec::with_capacity(num_shards);
    
    for (shard_id, (_, rx)) in channels.into_iter().enumerate() {
        // Construct peers list: all txs except mine (for write replication)
        let mut peers = Vec::new();
        for (peer_id, peer_tx) in shard_txs.iter().enumerate() {
            if peer_id != shard_id {
                peers.push(peer_tx.clone());
            }
        }
        
        let config_clone = config.clone();
        
        let handle = std::thread::Builder::new()
            .name(format!("shard-{}", shard_id))
            .spawn(move || {
                // Create single-threaded tokio runtime for this shard
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create shard runtime");
                
                // Run the shard's event loop
                rt.block_on(async {
                    let local = LocalSet::new();
                    local.run_until(shard_main(shard_id, config_clone, rx, peers)).await;
                });
            })
            .expect("Failed to spawn shard thread");
        
        handles.push(handle);
    }
    
    // Main thread waits for all shard threads
    for handle in handles {
        let _ = handle.join();
    }
    
    Ok(())
}

/// Main event loop for a shard thread
/// Owns: TcpListener (SO_REUSEPORT), Engine, connection handlers
async fn shard_main(
    shard_id: usize,
    config: Arc<Config>,
    rx: mpsc::Receiver<EngineRequest>,
    peers: Vec<mpsc::Sender<EngineRequest>>,
) {
    let port = config.port;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    
    // Create SO_REUSEPORT listener
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
        .expect("Failed to create socket");
    
    socket.set_reuse_port(true).expect("Failed to set SO_REUSEPORT");
    socket.set_reuse_address(true).expect("Failed to set SO_REUSEADDR");
    socket.set_nonblocking(true).expect("Failed to set non-blocking");
    socket.bind(&addr.into()).expect("Failed to bind socket");
    socket.listen(1024).expect("Failed to listen");
    
    let std_listener: std::net::TcpListener = socket.into();
    let listener = TcpListener::from_std(std_listener).expect("Failed to create TcpListener");
    
    println!("[shard-{}] Listening on {} with SO_REUSEPORT", shard_id, addr);
    
    // Initialize database (load RDB only for shard 0)
    let mut db = Db::new();
    if shard_id == 0 {
        if let Err(e) = db.load_rdb(config.rdb_path()) {
            eprintln!("[shard-{}] Failed to load RDB file: {}", shard_id, e);
        }
    }
    
    // Create engine - wrapped in Rc<RefCell> for thread-local access
    let engine = Rc::new(RefCell::new(Engine::new(shard_id, config, rx, db, peers)));
    
    // Client ID counter for this shard
    let client_id_counter = Rc::new(RefCell::new(0u64));
    
    loop {
        // Accept new connection
        match listener.accept().await {
            Ok((stream, _addr)) => {
                // Disable Nagle's algorithm
                let _ = stream.set_nodelay(true);
                
                // Generate client ID
                let mut counter = client_id_counter.borrow_mut();
                *counter += 1;
                let client_id = *counter;
                drop(counter);
                
                // Spawn connection handler on this thread's local set
                let engine = engine.clone();
                tokio::task::spawn_local(async move {
                    handle_connection(client_id, stream, engine).await;
                });
            }
            Err(e) => {
                eprintln!("[shard-{}] Accept error: {}", shard_id, e);
            }
        }
        
        // Process any pending replication from peers
        {
            let mut engine = engine.borrow_mut();
            while engine.process_replication().await {}
            engine.process_timeouts();
        }
    }
}

/// Handle a single client connection (runs on shard thread via spawn_local)
/// Uses direct engine access - zero channel overhead
async fn handle_connection(
    client_id: u64,
    stream: tokio::net::TcpStream,
    engine: Rc<RefCell<Engine>>,
) {
    let mut handler = resp::RespHandler::new(stream);
    let (msg_tx, mut msg_rx) = mpsc::channel::<resp::Value>(256);
    
    // Pipelining constants
    const MAX_BATCH_COMMANDS: usize = 1024;
    const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024; // 4MB
    
    loop {
        tokio::select! {
            biased;
            
            // Handle incoming pub/sub messages
            Some(msg) = msg_rx.recv() => {
                if handler.write_value(msg).await.is_err() {
                    break;
                }
            }
            
            // Handle incoming commands from client
            value = handler.read_value() => {
                match value {
                    Ok(Some(v)) => {
                        // Collect commands into batch (opportunistic batching)
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
                        
                        // Parse and execute batch
                        let batch_size = command_batch.len();
                        let mut commands = Vec::with_capacity(batch_size);
                        let mut parse_errors: Vec<(usize, String)> = Vec::new();
                        
                        for (idx, cmd_value) in command_batch.into_iter().enumerate() {
                            match RedisCommand::from_resp(cmd_value) {
                                Ok(command) => commands.push(command),
                                Err(e) => parse_errors.push((idx, e.to_string())),
                            }
                        }
                        
                        // Execute directly (zero channel overhead)
                        let responses = {
                            let mut engine = engine.borrow_mut();
                            engine.execute_batch_direct(client_id, commands, Some(msg_tx.clone())).await
                        };
                        
                        // Build response batch
                        let mut response_batch: Vec<resp::Value> = responses.into_iter()
                            .map(|r| match r {
                                Ok(v) => v,
                                Err(e) => resp::Value::Error(Bytes::from(format!("ERR {}", e))),
                            })
                            .collect();
                        
                        // Insert parse errors at correct positions
                        for (idx, err_msg) in parse_errors {
                            if idx <= response_batch.len() {
                                response_batch.insert(idx, resp::Value::Error(Bytes::from(format!("ERR {}", err_msg))));
                            }
                        }
                        
                        // Send responses
                        if !response_batch.is_empty() {
                            if handler.write_batch(response_batch).await.is_err() {
                                break;
                            }
                        }
                        
                        // Process any pending replication/timeouts
                        {
                            let mut engine = engine.borrow_mut();
                            while engine.process_replication().await {}
                            engine.process_timeouts();
                        }
                        
                        handler.notify_batch_processed(batch_size);
                    }
                    Ok(None) => break, // Connection closed
                    Err(_) => break,   // Error
                }
            }
        }
    }
    
    // Client disconnected - cleanup
    {
        let mut engine = engine.borrow_mut();
        engine.handle_disconnect(client_id);
    }
}


async fn perform_handshake(master_host: String, master_port: String, listening_port: String, tx: mpsc::Sender<EngineRequest>) -> Result<()> {
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

                        if let Err(_) = tx.send(EngineRequest::Single(req)).await {
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
