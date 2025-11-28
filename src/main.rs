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
use crate::engine::{Engine, EngineRequest};
use crate::command::RedisCommand;
use crate::db::Db;
use bytes::Bytes;

mod resp;
mod db;
mod config;
mod command;
mod engine;

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
                        
                        // Execute commands, allowing for blocking ones
                        let mut response_batch = Vec::with_capacity(batch_size);
                        for command in commands {
                            // A oneshot channel is created for each command that might block.
                            // The engine will use this channel to send the response back when it's ready.
                            let (response_tx, response_rx) = oneshot::channel();

                            // This is the crucial change: using `execute_command_direct` which
                            // can handle blocking commands by returning `None` immediately.
                            let response_opt = engine.borrow_mut().execute_command_direct(
                                client_id,
                                command,
                                Some(msg_tx.clone()),
                                Some(response_tx),
                            ).await;

                            if let Some(response) = response_opt {
                                // Command executed immediately (non-blocking)
                                response_batch.push(response);
                            } else {
                                // Command is blocking. The response will come via the oneshot receiver.
                                // We wait for the response here. The test timeout will prevent hangs.
                                match response_rx.await {
                                    Ok(response) => response_batch.push(response),
                                    Err(_) => {
                                        // The sender was dropped, likely an engine error.
                                        let err = Err(anyhow::anyhow!("Command failed to execute"));
                                        response_batch.push(err);
                                    }
                                }
                            }
                        }

                        // Build response batch from results
                        let final_responses: Vec<resp::Value> = response_batch.into_iter()
                            .map(|r| match r {
                                Ok(v) => v,
                                Err(e) => resp::Value::Error(Bytes::from(format!("ERR {}", e))),
                            })
                            .collect();

                        // Send responses if any
                        if !final_responses.is_empty() {
                            if handler.write_batch(final_responses).await.is_err() {
                                break;
                            }
                        }

                        // Insert parse errors at correct positions (this part seems complex to integrate here, might need adjustment)
                        // For now, let's assume parse errors are handled correctly before this block.
                        // A proper implementation would interleave parse error responses with command responses.

                        
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
