use std::io::Read;
use std::sync::{Arc, Mutex, atomic::Ordering};
use std::thread;
use std::time::Duration;
use std::collections::VecDeque;
use crate::redis::{Redis, RedisCommand};
use crate::redis::core::RedisResponse;
use crate::resp::parse_resp;
use crate::redis::replication::TcpStreamTrait;
use crate::redis::xread_handler::{XReadHandler, XReadRequest};

// ClientHandler should ideally be an actor.
#[derive(Clone)]
pub struct ClientHandler {
    client: Arc<Mutex<Box<dyn TcpStreamTrait>>>,
    redis: Arc<Mutex<Redis>>,
    in_transaction: Arc<Mutex<bool>>,
    queued_commands: Arc<Mutex<VecDeque<RedisCommand>>>,
    ready: Arc<Mutex<bool>>,
    shutdown: Arc<Mutex<bool>>,
    is_redis_connection: bool,
}

impl ClientHandler {
    pub fn new<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        Self::new_with_connection_type(client, redis, false)
    }

    pub fn new_redis_handler<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        Self::new_with_connection_type(client, redis, true)
    }

    fn new_with_connection_type<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>, is_redis_connection: bool) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(Box::new(client) as Box<dyn TcpStreamTrait>)),
            redis,
            in_transaction: Arc::new(Mutex::new(false)),
            queued_commands: Arc::new(Mutex::new(VecDeque::new())),
            shutdown: Arc::new(Mutex::new(false)),
            ready: Arc::new(Mutex::new(false)),
            is_redis_connection,
        }
    }

    #[allow(dead_code)]
    pub fn is_ready(&self) -> bool {
        *self.ready.lock().unwrap()
    }

    #[allow(dead_code)]
    pub fn shutdown(&self) {
        #[cfg(debug_assertions)]
        println!("[ClientHandler::shutdown] Setting shutdown flag");
        *self.shutdown.lock().unwrap() = true;
    }

    pub fn execute_command(&mut self, command: &RedisCommand) -> RedisResponse {
        let raw_response = match &command {
            RedisCommand::XRead { keys, ids, block, count } => {
                #[cfg(debug_assertions)]
                println!("[ClientHandler::execute_command] Handling XREAD command");
                
                let request = XReadRequest {
                    keys: keys.iter().map(|s| s.to_string()).collect(),
                    ids: ids.iter().map(|s| s.to_string()).collect(),
                    block: *block,
                    count: *count,
                };
                
                let mut handler = XReadHandler::new(Arc::clone(&self.redis), request);
                match handler.run_loop() {
                    Ok(results) => {
                        if results.is_empty() {
                            RedisResponse::Error("-1".to_string())
                        } else {
                            let mut streams = Vec::new();
                            for (stream_key, entries) in results {
                                let mut stream_entries = Vec::new();
                                for entry in entries {
                                    let mut entry_data = Vec::new();
                                    entry_data.push(RedisResponse::BulkString(entry.id));
                                    
                                    let mut fields = Vec::new();
                                    for (field, value) in entry.fields {
                                        fields.push(RedisResponse::BulkString(field));
                                        fields.push(RedisResponse::BulkString(value));
                                    }
                                    entry_data.push(RedisResponse::Array(fields));
                                    stream_entries.push(RedisResponse::Array(entry_data));
                                }
                                
                                let mut stream_data = Vec::new();
                                stream_data.push(RedisResponse::BulkString(stream_key));
                                stream_data.push(RedisResponse::Array(stream_entries));
                                streams.push(RedisResponse::Array(stream_data));
                            }
                            RedisResponse::Array(streams)
                        }
                    },
                    Err(e) => RedisResponse::Error(e)
                }
            },
            RedisCommand::Multi => {
                let mut in_transaction = self.in_transaction.lock().unwrap();
                if *in_transaction {
                    RedisResponse::Error("MULTI calls can not be nested".to_string())
                } else {
                    *in_transaction = true;
                    RedisResponse::Ok("OK".to_string())
                }
            },
            RedisCommand::Exec => {
                let mut in_transaction = self.in_transaction.lock().unwrap();
                if !*in_transaction {
                    RedisResponse::Error("EXEC without MULTI".to_string())
                } else {
                    *in_transaction = false;
                    let mut responses = Vec::new();
                    let mut queued_commands = self.queued_commands.lock().unwrap();
                    let mut client = self.client.lock().unwrap();
                    
                    while let Some(cmd) = queued_commands.pop_front() {
                        let result = self.redis.lock()
                            .expect("failed to lock redis")
                            .execute_command(&cmd, Some(&mut client));
                        match result {
                            RedisResponse::Ok(response) => responses.push(RedisResponse::Ok(response)),
                            RedisResponse::Error(e) => {
                                println!("{}", e.trim());
                                responses.push(RedisResponse::Error(e))
                            },
                            RedisResponse::Retry => continue,
                            RedisResponse::Array(items) => responses.push(RedisResponse::Array(items)),
                            RedisResponse::BulkString(s) => responses.push(RedisResponse::BulkString(s)),
                            RedisResponse::Integer(i) => responses.push(RedisResponse::Integer(i)),
                            RedisResponse::SimpleString(s) => responses.push(RedisResponse::SimpleString(s)),
                            RedisResponse::NullBulkString => responses.push(RedisResponse::NullBulkString),
                        }
                    }

                    RedisResponse::Array(responses)
                }
            },
            RedisCommand::Discard => {
                let mut in_transaction = self.in_transaction.lock().unwrap();
                if !*in_transaction {
                    RedisResponse::Error("DISCARD without MULTI".to_string())
                } else {
                    *in_transaction = false;
                    self.queued_commands.lock().unwrap().clear();
                    RedisResponse::Ok("OK".to_string())
                }
            },
            _ => {
                if *self.in_transaction.lock().unwrap() {
                    let owned_command = command.clone();
                    self.queued_commands.lock().unwrap().push_back(owned_command);
                    RedisResponse::Ok("QUEUED".to_string())
                } else {
                    // defer to redis.execute_command()
                    let mut redis_guard = self.redis.lock().unwrap();
                    let mut client_guard = self.client.lock().unwrap();
                    let response = redis_guard.execute_command(command, Some(&mut *client_guard));
                    match response {
                        RedisResponse::Ok(r) => RedisResponse::Ok(r),
                        RedisResponse::Error(e) => RedisResponse::Error(e),
                        RedisResponse::Retry => RedisResponse::Retry,
                        RedisResponse::Array(items) => RedisResponse::Array(items),
                        RedisResponse::BulkString(s) => RedisResponse::BulkString(s),
                        RedisResponse::Integer(i) => RedisResponse::Integer(i),
                        RedisResponse::SimpleString(s) => RedisResponse::SimpleString(s),
                        RedisResponse::NullBulkString => RedisResponse::NullBulkString,
                    }
                }
            }
        };
        raw_response
    }

    pub fn start(&mut self) -> std::thread::JoinHandle<()> {
        // Clone self to move into the thread
        let mut handler = self.clone();
        
        thread::spawn(move || {
            #[cfg(debug_assertions)]
            println!("[CLIENT] Starting new client handler");

            let mut buffer = Vec::new();
            let mut read_buffer = [0; 1024];

            loop {

                // after entering the first loop, before blocking on anything, set the ready flag
                *handler.ready.lock().unwrap() = true;

                if *handler.shutdown.lock().unwrap() {
                    break;
                }

                // Read from client with minimal lock scope
                let read_result = {
                    #[cfg(debug_assertions)]
                    println!("[CLIENT] Attempting to acquire client lock for read");
                    let mut client = handler.client.lock().unwrap();
                    #[cfg(debug_assertions)]
                    println!("[CLIENT] Acquired client lock for read");
                    let result = client.read(&mut read_buffer);
                    #[cfg(debug_assertions)]
                    println!("[CLIENT] Released client lock after read");
                    result
                };
                
                #[cfg(debug_assertions)]
                println!("[CLIENT] Read result: {:?}", read_result);

                match read_result {
                    Ok(0) => {
                        // Connection closed
                        break;
                    }
                    Ok(n) => {
                        println!("[CLIENT] Received {} bytes", n);
                        
                        buffer.extend_from_slice(&read_buffer[..n]);
                        
                        // Process any complete commands in buffer
                        let commands = parse_resp(&buffer, buffer.len());
                        if !commands.is_empty() {
                            buffer.clear();

                            println!("[CLIENT] Processing {} commands", commands.len());

                            // Handle multiple commands differently for Redis connections
                            if handler.is_redis_connection && commands.len() > 1 {
                                let mut responses = Vec::new();
                                
                                for command in commands {
                                    println!("[CLIENT] Executing command: {:?}", command);

                                    let response = if let RedisCommand::Wait { numreplicas, timeout, elapsed: _ } = command {
                                        let start = std::time::Instant::now();
                                        let mut sent_getack = false;
                                        #[cfg(debug_assertions)]
                                        println!("[CLIENT] Attempting to acquire redis lock for command");
                                        let mut resp = handler.execute_command(&command);
                                        #[cfg(debug_assertions)]
                                        println!("[CLIENT] Released redis lock after command");
                                        
                                        while let RedisResponse::Retry = resp {
                                            thread::sleep(Duration::from_millis(50));
                                            let total_elapsed = start.elapsed().as_millis() as i64;
                                            let updated_command = RedisCommand::Wait {
                                                numreplicas,
                                                timeout,
                                                elapsed: total_elapsed,
                                            };
                                            
                                            if !sent_getack {
                                                println!("[CLIENT] Attempting to acquire redis lock for getack");
                                                if let Ok(redis) = handler.redis.lock() {
                                                    println!("[CLIENT] Acquired redis lock for getack");
                                                    let _ = redis.replication.send_getack_to_replicas();
                                                    sent_getack = true;
                                                }
                                                println!("[CLIENT] Released redis lock after getack");
                                            }
                                            
                                            resp = handler.execute_command(&updated_command);
                                        }
                                        resp
                                    } else {
                                        println!("[CLIENT] Attempting to acquire redis lock for command");
                                        let resp = handler.execute_command(&command);
                                        println!("[CLIENT] Released redis lock after command");
                                        resp
                                    };

                                    println!("[CLIENT] Got response: {}", response.format().replace("\r\n", "\\r\\n"));

                                    let formatted = response.format();
                                    if !formatted.is_empty() {
                                        responses.push(formatted);
                                    }
                                }

                                // Format all responses as a single array
                                if !responses.is_empty() {
                                    let mut batch_response = format!("*{}\r\n", responses.len());
                                    for resp in responses {
                                        batch_response.push_str(&resp);
                                    }
                                    
                                    #[cfg(debug_assertions)]
                                    println!("[CLIENT] Writing batch response: {}", batch_response.replace("\r\n", "\\r\\n"));
                                    
                                    let mut client = handler.client.lock().unwrap();
                                    client.write_all(batch_response.as_bytes()).unwrap();
                                    client.flush().unwrap();
                                }

                                // Only count bytes after processing commands
                                if handler.is_redis_connection {
                                    if let Ok(redis) = handler.redis.lock() {
                                        if redis.config.replicaof_host.is_some() {
                                            redis.bytes_processed.fetch_add(n as u64, Ordering::SeqCst);
                                            #[cfg(debug_assertions)]
                                            println!("[CLIENT] Added {} bytes, total now: {}", 
                                                n,
                                                redis.bytes_processed.load(Ordering::SeqCst));
                                        }
                                    }
                                }
                            } else {
                                // Handle single commands or non-Redis connections
                                for command in commands {
                                    println!("[CLIENT] Executing command: {:?}", command);

                                    let response = if let RedisCommand::Wait { numreplicas, timeout, elapsed: _ } = command {
                                        let start = std::time::Instant::now();
                                        let mut sent_getack = false;
                                        println!("[CLIENT] Attempting to acquire redis lock for command");
                                        let mut resp = handler.execute_command(&command);
                                        println!("[CLIENT] Released redis lock after command");
                                        
                                        while let RedisResponse::Retry = resp {
                                            thread::sleep(Duration::from_millis(50));
                                            let total_elapsed = start.elapsed().as_millis() as i64;
                                            let updated_command = RedisCommand::Wait {
                                                numreplicas,
                                                timeout,
                                                elapsed: total_elapsed,
                                            };
                                            
                                            if !sent_getack {
                                                println!("[CLIENT] Attempting to acquire redis lock for getack");
                                                if let Ok(redis) = handler.redis.lock() {
                                                    println!("[CLIENT] Acquired redis lock for getack");
                                                    let _ = redis.replication.send_getack_to_replicas();
                                                    sent_getack = true;
                                                }
                                                println!("[CLIENT] Released redis lock after getack");
                                            }
                                            
                                            resp = handler.execute_command(&updated_command);
                                        }
                                        resp
                                    } else {
                                        println!("[CLIENT] Attempting to acquire redis lock for command");
                                        let resp = handler.execute_command(&command);
                                        println!("[CLIENT] Released redis lock after command");
                                        resp
                                    };

                                    println!("[CLIENT] Got response: {}", response.format().replace("\r\n", "\\r\\n"));

                                    let formatted = response.format();
                                    if !formatted.is_empty() {
                                        #[cfg(debug_assertions)]
                                        println!("[CLIENT] Writing response: {}", formatted.replace("\r\n", "\\r\\n"));
                                        
                                        let mut client = handler.client.lock().unwrap();
                                        client.write_all(formatted.as_bytes()).unwrap();
                                        client.flush().unwrap();
                                    }
                                }
                            }

                            // Only count bytes after processing commands
                            if handler.is_redis_connection {
                                if let Ok(redis) = handler.redis.lock() {
                                    if redis.config.replicaof_host.is_some() {
                                        redis.bytes_processed.fetch_add(n as u64, Ordering::SeqCst);
                                        #[cfg(debug_assertions)]
                                        println!("[CLIENT] Added {} bytes, total now: {}", 
                                            n,
                                            redis.bytes_processed.load(Ordering::SeqCst));
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            eprintln!("[CLIENT] Error reading from client: {}", e);
                            break;
                        }
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                }
            }
        })
    }
}
