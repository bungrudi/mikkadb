use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use crate::redis::{Redis, RedisCommand};
use crate::resp::parse_resp;
use crate::redis::replication::TcpStreamTrait;
use crate::redis::xread_handler::{XReadHandler, XReadRequest};

// ClientHandler should ideally be an actor.
pub struct ClientHandler {
    client: Arc<Mutex<Box<dyn TcpStreamTrait>>>,
    redis: Arc<Mutex<Redis>>,
    master: bool, // is this client a master?
    in_transaction: Arc<Mutex<bool>>,
    queued_commands: Arc<Mutex<VecDeque<RedisCommand>>>,
}

impl ClientHandler {
    pub fn new<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(Box::new(client) as Box<dyn TcpStreamTrait>)),
            redis,
            master: false,
            in_transaction: Arc::new(Mutex::new(false)),
            queued_commands: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn new_master<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(Box::new(client) as Box<dyn TcpStreamTrait>)),
            redis,
            master: true,
            in_transaction: Arc::new(Mutex::new(false)),
            queued_commands: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn execute_command(&mut self, command: &RedisCommand) -> String {
        match &command {
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
                    
                    // Execute all queued commands
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

                    // Format response array
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
                    // Clear all queued commands
                    self.queued_commands.lock().unwrap().clear();
                    "+OK\r\n".to_string()
                }
            },
            RedisCommand::XRead { keys, ids, block, count } => {
                #[cfg(debug_assertions)]
                println!("\n[ClientHandler::execute_command] Handling XREAD command:");
                #[cfg(debug_assertions)]
                println!("  - Keys: {:?}", keys);
                #[cfg(debug_assertions)]
                println!("  - IDs: {:?}", ids);
                #[cfg(debug_assertions)]
                println!("  - Block: {:?}", block);
                #[cfg(debug_assertions)]
                println!("  - Count: {:?}", count);

                let request = XReadRequest {
                    keys: keys.iter().map(|s| s.to_string()).collect(),
                    ids: ids.iter().map(|s| s.to_string()).collect(),
                    block: *block,
                    count: *count,
                };
                
                #[cfg(debug_assertions)]
                println!("[ClientHandler::execute_command] Created request:");
                #[cfg(debug_assertions)]
                println!("  - Keys (after clone): {:?}", request.keys);
                #[cfg(debug_assertions)]
                println!("  - IDs (after clone): {:?}", request.ids);
                
                let mut handler = XReadHandler::new(Arc::clone(&self.redis), request);
                match handler.run_loop() {
                    Ok(results) => {
                        #[cfg(debug_assertions)]
                        println!("[ClientHandler::execute_command] Got {} streams with entries", results.len());
                        if results.is_empty() {
                            #[cfg(debug_assertions)]
                            println!("[ClientHandler::execute_command] Returning nil response for empty read");
                            "*-1\r\n".to_string()  // Redis nil response for empty read (both blocking and non-blocking)
                        } else {
                            let mut response = String::new();
                            
                            // Format: *<num_streams>\r\n
                            response.push_str(&format!("*{}\r\n", results.len()));
                            #[cfg(debug_assertions)]
                            println!("[ClientHandler::execute_command] Response header: *{}", results.len());
                            #[cfg(debug_assertions)]
                            println!("[ClientHandler::execute_command] Results to process: {:?}", results);
                            
                            // For each stream: *2\r\n$<len>\r\n<stream>\r\n*<entries>\r\n
                            for (stream_name, entries) in results {
                                #[cfg(debug_assertions)]
                                println!("[ClientHandler::execute_command] Processing stream '{}' with {} entries", stream_name, entries.len());
                                // Array of 2 elements (stream name and entries array)
                                response.push_str("*2\r\n");
                                
                                // Stream name
                                response.push_str(&format!("${}\r\n{}\r\n", stream_name.len(), stream_name));
                                
                                // Array of entries
                                response.push_str(&format!("*{}\r\n", entries.len()));
                                
                                // Each entry is an array of 2 elements (ID and fields array)
                                for entry in entries {
                                    #[cfg(debug_assertions)]
                                    println!("[ClientHandler::execute_command] Processing entry {} with {} fields", entry.id, entry.fields.len());
                                    response.push_str("*2\r\n");  // Array of 2 elements (ID and fields)
                                    
                                    // ID
                                    response.push_str(&format!("${}\r\n{}\r\n", entry.id.len(), entry.id));
                                    
                                    // Fields array - each field is a key-value pair
                                    let field_count = entry.fields.len() * 2;  // Each field has key and value
                                    response.push_str(&format!("*{}\r\n", field_count));
                                    
                                    for (key, value) in entry.fields {
                                        #[cfg(debug_assertions)]
                                        println!("[ClientHandler::execute_command] Field: {} = {}", key, value);
                                        // Field key
                                        response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                                        // Field value
                                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                                    }
                                }
                            }
                            #[cfg(debug_assertions)]
                            println!("[ClientHandler::execute_command] Final response: {:?}", response);
                            response
                        }
                    },
                    Err(_e) => {
                        #[cfg(debug_assertions)]
                        println!("[ClientHandler::execute_command] Error: {}", _e);
                        format!("-ERR {}\r\n", _e)
                    }
                }
            },
            _ => {
                let in_transaction = self.in_transaction.lock().unwrap();
                if *in_transaction {
                    // Queue command and return QUEUED
                    let owned_command = match command {
                        RedisCommand::Set { key, value, ttl, original_resp } => RedisCommand::Set {
                            key: key.to_string(),
                            value: value.to_string(),
                            ttl: *ttl,
                            original_resp: original_resp.clone(),
                        },
                        RedisCommand::Get { key } => RedisCommand::Get {
                            key: key.to_string(),
                        },
                        RedisCommand::Incr { key } => RedisCommand::Incr {
                            key: key.to_string(),
                        },
                        _ => {
                            let error = "-ERR Command not supported in transaction\r\n".to_string();
                            println!("{}", error.trim());
                            return error;
                        }
                    };
                    self.queued_commands.lock().unwrap().push_back(owned_command);
                    "+QUEUED\r\n".to_string()
                } else {
                    let mut client = self.client.lock().unwrap();
                    match self.redis.lock().unwrap().execute_command(command, Some(&mut client)) {
                        Ok(response) => response,
                        Err(_e) => {
                            if _e == "XREAD_RETRY" {
                                "XREAD_RETRY".to_string()
                            } else {
                                println!("{}", _e.trim());
                                _e
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn start(&mut self) {
        #[cfg(debug_assertions)]
        println!("\n[ClientHandler::start] Starting client handler");
        let mut buffer: [u8;2048] = [0; 2048];
        let client = Arc::clone(&self.client);
        let redis = Arc::clone(&self.redis);
        let master = self.master;
        let in_transaction = Arc::clone(&self.in_transaction);
        let queued_commands = Arc::clone(&self.queued_commands);

        thread::spawn(move || {
            let mut handler = ClientHandler {
                client: client.clone(),
                redis: redis.clone(),
                master,
                in_transaction,
                queued_commands,
            };

            let _addr = {
                let client = client.lock().unwrap();
                client.peer_addr().unwrap()
            };

            loop {
                let bytes_read = {
                    let mut client = client.lock().unwrap();
                    match client.read(&mut buffer) {
                        Ok(n) => n,
                        Err(_e) => {
                            #[cfg(debug_assertions)]
                            println!("[ClientHandler::start] Error reading from client: {}", _e);
                            break;
                        }
                    }
                };

                if bytes_read == 0 {
                    #[cfg(debug_assertions)]
                    println!("[ClientHandler::start] Client disconnected (0 bytes read)");
                    break;
                }

                #[cfg(debug_assertions)]
                println!("[ClientHandler::start] Read {} bytes from client", bytes_read);
                #[cfg(debug_assertions)]
                println!("[ClientHandler::start] Raw input: {}", String::from_utf8_lossy(&buffer[..bytes_read]));

                let commands = parse_resp(&buffer, bytes_read);
                #[cfg(debug_assertions)]
                println!("[ClientHandler::start] Parsed {} commands", commands.len());

                for command in commands {
                    if matches!(command, RedisCommand::None) {
                        #[cfg(debug_assertions)]
                        println!("[ClientHandler::start] Skipping None command");
                        break;
                    }

                    let mut command = command;
                    let mut should_retry = true;
                    let start_time = Instant::now();

                    while should_retry {
                        #[cfg(debug_assertions)]
                        println!("[ClientHandler::start] Executing command: {:?}", command);
                        let response = handler.execute_command(&command);
                        #[cfg(debug_assertions)]
                        println!("[ClientHandler::start] Got response: {:?}", response);

                        // Handle empty response (content sent directly)
                        if response.is_empty() {
                            #[cfg(debug_assertions)]
                            println!("[ClientHandler::start] Empty response, continuing");
                            should_retry = false;
                            continue;
                        }

                        // Handle retry cases
                        if response == "XREAD_RETRY" {
                            #[cfg(debug_assertions)]
                            println!("DEBUG: Got XREAD_RETRY response");

                            let timeout = if let RedisCommand::XRead { block, .. } = &command {
                                block.unwrap_or(0)
                            } else {
                                0
                            };

                            let elapsed = start_time.elapsed().as_millis() as u64;

                            if timeout > 0 && elapsed >= timeout {
                                #[cfg(debug_assertions)]
                                println!("DEBUG: XREAD timeout expired, sending null response");

                                let mut client = client.lock().unwrap();
                                let _ = client.write(b"$-1\r\n");
                                let _ = client.flush();
                                should_retry = false;
                            } else {
                                should_retry = true;
                            }
                        } else if response == "WAIT_RETRY" {
                            if let RedisCommand::Wait { numreplicas, timeout, elapsed: _ } = command {
                                let new_elapsed = start_time.elapsed().as_millis() as i64;
                                if new_elapsed >= timeout {
                                    let redis = redis.lock().unwrap();
                                    let up_to_date_replicas = redis.replication.count_up_to_date_replicas();
                                    let mut client = client.lock().unwrap();
                                    let _ = client.write(format!(":{}\r\n", up_to_date_replicas).as_bytes());
                                    let _ = client.flush();
                                    should_retry = false;
                                } else {
                                    should_retry = true;
                                    command = RedisCommand::Wait { numreplicas, timeout, elapsed: new_elapsed };
                                }
                            } else {
                                should_retry = false;
                            }
                        } else {
                            // Handle normal response
                            if !response.is_empty() {
                                let mut client = client.lock().unwrap();
                                #[cfg(debug_assertions)]
                                println!("[ClientHandler::start] Writing response to client");
                                let _ = client.write(response.as_bytes());
                                let _ = client.flush();
                            }
                            should_retry = false;
                        }
                        if should_retry {
                            thread::sleep(Duration::from_millis(10));
                        }
                    }
                }
            }
        });        
    }
}
