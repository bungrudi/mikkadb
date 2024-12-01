use std::io::Read;
use std::sync::{Arc, Mutex, atomic::Ordering};
use std::thread;
use std::time::Duration;
use std::collections::VecDeque;
use crate::redis::{Redis, RedisCommand};
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
            is_redis_connection,
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&self) {
        #[cfg(debug_assertions)]
        println!("[ClientHandler::shutdown] Setting shutdown flag");
        *self.shutdown.lock().unwrap() = true;
    }

    fn format_response(&self, response: &str) -> String {
        if response.is_empty() {
            // Empty response means handler already wrote to stream directly
            return response.to_string();
        }

        // For Redis CLI connections, we should return the response as-is
        if !self.is_redis_connection {
            return response.to_string();
        }

        // Redis-to-Redis communication formatting
        if response.starts_with('+') || response.starts_with('-') || response.starts_with(':') {
            // Convert simple string/error/integer to array with bulk string
            let content = response[1..].trim_end_matches("\r\n");
            format!("*1\r\n${}\r\n{}\r\n", content.len(), content)
        } else if response.starts_with('$') {
            // Convert bulk string to array with bulk string
            format!("*1\r\n{}", response)
        } else if response.starts_with('*') {
            // Already in array format
            response.to_string()
        } else {
            // Non-empty response without RESP prefix
            format!("*1\r\n${}\r\n{}\r\n", response.len(), response)
        }
    }

    pub fn execute_command(&mut self, command: &RedisCommand) -> String {
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
                            "*-1\r\n".to_string()
                        } else {
                            let mut response = format!("*{}\r\n", results.len());
                            for (stream_key, entries) in results {
                                if !entries.is_empty() {
                                    response.push_str(&format!("*2\r\n${}\r\n{}\r\n*{}\r\n", 
                                        stream_key.len(), stream_key, entries.len()));
                                    
                                    for entry in entries {
                                        let field_count = entry.fields.len() * 2;
                                        response.push_str(&format!("*2\r\n${}\r\n{}\r\n*{}\r\n", 
                                            entry.id.len(), entry.id, field_count));
                                        
                                        for (field, value) in entry.fields {
                                            response.push_str(&format!("${}\r\n{}\r\n${}\r\n{}\r\n",
                                                field.len(), field, value.len(), value));
                                        }
                                    }
                                }
                            }
                            response
                        }
                    },
                    Err(e) => format!("-ERR {}\r\n", e)
                }
            },
            RedisCommand::Multi => {
                let mut in_transaction = self.in_transaction.lock().unwrap();
                if *in_transaction {
                    let error = "-ERR MULTI calls can not be nested\r\n".to_string();
                    println!("{}", error.trim());
                    error
                } else {
                    *in_transaction = true;
                    "+OK\r\n".to_string()
                }
            },
            RedisCommand::Exec => {
                let mut in_transaction = self.in_transaction.lock().unwrap();
                if !*in_transaction {
                    let error = "-ERR EXEC without MULTI\r\n".to_string();
                    println!("{}", error.trim());
                    error
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
                            Ok(response) => responses.push(response),
                            Err(_e) => {
                                println!("{}", _e.trim());
                                responses.push(_e)
                            },
                        }
                    }

                    let mut result = format!("*{}\r\n", responses.len());
                    for response in responses {
                        result.push_str(&response);
                    }
                    result
                }
            },
            RedisCommand::Discard => {
                let mut in_transaction = self.in_transaction.lock().unwrap();
                if !*in_transaction {
                    let error = "-ERR DISCARD without MULTI\r\n".to_string();
                    println!("{}", error.trim());
                    error
                } else {
                    *in_transaction = false;
                    self.queued_commands.lock().unwrap().clear();
                    "+OK\r\n".to_string()
                }
            },
            _ => {
                if *self.in_transaction.lock().unwrap() {
                    let owned_command = command.clone();
                    self.queued_commands.lock().unwrap().push_back(owned_command);
                    "+QUEUED\r\n".to_string()
                } else {
                    // defer to redis.execute_command()
                    match self.redis.lock().unwrap().execute_command(command, Some(&mut *self.client.lock().unwrap())) {
                        Ok(response) => response,
                        Err(e) => e,
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
                if *handler.shutdown.lock().unwrap() {
                    break;
                }

                // Read from client with minimal lock scope
                let read_result = {
                    let mut client = handler.client.lock().unwrap();
                    client.read(&mut read_buffer)
                };

                match read_result {
                    Ok(0) => {
                        // Connection closed
                        break;
                    }
                    Ok(n) => {
                        #[cfg(debug_assertions)]
                        println!("[CLIENT] Received {} bytes", n);
                        
                        buffer.extend_from_slice(&read_buffer[..n]);
                        
                        // Process any complete commands in buffer
                        let commands = parse_resp(&buffer, buffer.len());
                        if !commands.is_empty() {
                            buffer.clear();

                            #[cfg(debug_assertions)]
                            println!("[CLIENT] Processing {} commands", commands.len());

                            // Handle multiple commands differently for Redis connections
                            if handler.is_redis_connection && commands.len() > 1 {
                                let mut responses = Vec::new();
                                
                                for command in commands {
                                    #[cfg(debug_assertions)]
                                    println!("[CLIENT] Executing command: {:?}", command);

                                    let response = if let RedisCommand::Wait { numreplicas, timeout, elapsed: _ } = command {
                                        let start = std::time::Instant::now();
                                        let mut sent_getack = false;
                                        let mut resp = handler.execute_command(&command);
                                
                                        while resp == "WAIT_RETRY" {
                                            thread::sleep(Duration::from_millis(50));
                                            let total_elapsed = start.elapsed().as_millis() as i64;
                                            let updated_command = RedisCommand::Wait {
                                                numreplicas,
                                                timeout,
                                                elapsed: total_elapsed,
                                            };
                                            
                                            if !sent_getack {
                                                if let Ok(redis) = handler.redis.lock() {
                                                    let _ = redis.replication.send_getack_to_replicas();
                                                    sent_getack = true;
                                                }
                                            }
                                            
                                            resp = handler.execute_command(&updated_command);
                                        }
                                        resp
                                    } else {
                                        handler.execute_command(&command)
                                    };

                                    #[cfg(debug_assertions)]
                                    println!("[CLIENT] Got response: {}", response.replace("\r\n", "\\r\\n"));

                                    if !response.is_empty() {
                                        responses.push(response);
                                    }
                                }

                                // Format all responses as a single array
                                if !responses.is_empty() {
                                    let mut batch_response = format!("*{}\r\n", responses.len());
                                    for resp in responses {
                                        if resp.starts_with('*') {
                                            batch_response.push_str(&resp[1..]);
                                        } else if resp.starts_with('+') || resp.starts_with('-') || resp.starts_with(':') {
                                            let content = &resp[1..].trim_end_matches("\r\n");
                                            batch_response.push_str(&format!("${}\r\n{}\r\n", content.len(), content));
                                        } else if resp.starts_with('$') {
                                            batch_response.push_str(&resp);
                                        } else {
                                            batch_response.push_str(&format!("${}\r\n{}\r\n", resp.len(), resp));
                                        }
                                    }
                                    let mut client = handler.client.lock().unwrap();
                                    client.write_all(batch_response.as_bytes()).unwrap();
                                    client.flush().unwrap();
                                }
                            } else {
                                // Handle single commands or non-Redis connections
                                for command in commands {
                                    #[cfg(debug_assertions)]
                                    println!("[CLIENT] Executing command: {:?}", command);

                                    let response = if let RedisCommand::Wait { numreplicas, timeout, elapsed: _ } = command {
                                        let start = std::time::Instant::now();
                                        let mut sent_getack = false;
                                        let mut resp = handler.execute_command(&command);
                                        
                                        while resp == "WAIT_RETRY" {
                                            thread::sleep(Duration::from_millis(50));
                                            let total_elapsed = start.elapsed().as_millis() as i64;
                                            let updated_command = RedisCommand::Wait {
                                                numreplicas,
                                                timeout,
                                                elapsed: total_elapsed,
                                            };
                                            
                                            if !sent_getack {
                                                if let Ok(redis) = handler.redis.lock() {
                                                    let _ = redis.replication.send_getack_to_replicas();
                                                    sent_getack = true;
                                                }
                                            }
                                            
                                            resp = handler.execute_command(&updated_command);
                                        }
                                        resp
                                    } else {
                                        handler.execute_command(&command)
                                    };

                                    #[cfg(debug_assertions)]
                                    println!("[CLIENT] Got response: {}", response.replace("\r\n", "\\r\\n"));

                                    // Only send response if it's not empty and not already handled
                                    if !response.is_empty() {
                                        let formatted_response = handler.format_response(&response);
                                        if !formatted_response.is_empty() {
                                            let mut client = handler.client.lock().unwrap();
                                            client.write_all(formatted_response.as_bytes()).unwrap();
                                            client.flush().unwrap();
                                        }
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
