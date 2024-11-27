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
    shutdown: Arc<Mutex<bool>>,
}

impl ClientHandler {
    pub fn new<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(Box::new(client) as Box<dyn TcpStreamTrait>)),
            redis,
            master: false,
            in_transaction: Arc::new(Mutex::new(false)),
            queued_commands: Arc::new(Mutex::new(VecDeque::new())),
            shutdown: Arc::new(Mutex::new(false)),
        }
    }

    pub fn new_master<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(Box::new(client) as Box<dyn TcpStreamTrait>)),
            redis,
            master: true,
            in_transaction: Arc::new(Mutex::new(false)),
            queued_commands: Arc::new(Mutex::new(VecDeque::new())),
            shutdown: Arc::new(Mutex::new(false)),
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&self) {
        #[cfg(debug_assertions)]
        println!("[ClientHandler::shutdown] Setting shutdown flag");
        *self.shutdown.lock().unwrap() = true;
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
            _ => {
                if *self.in_transaction.lock().unwrap() {
                    // Queue command and return QUEUED
                    let owned_command = match command {
                        RedisCommand::Set { key, value, ttl, original_resp } => RedisCommand::Set {
                            key: key.to_string(),
                            value: value.to_string(),
                            ttl: *ttl,
                            original_resp: original_resp.to_string(),
                        },
                        RedisCommand::Get { key } => RedisCommand::Get {
                            key: key.to_string(),
                        },
                        RedisCommand::Info { subcommand } => RedisCommand::Info {
                            subcommand: subcommand.to_string(),
                        },
                        RedisCommand::Type { key } => RedisCommand::Type {
                            key: key.to_string(),
                        },
                        RedisCommand::Config { subcommand, parameter } => RedisCommand::Config {
                            subcommand: subcommand.to_string(),
                            parameter: parameter.to_string(),
                        },
                        RedisCommand::Keys { pattern } => RedisCommand::Keys {
                            pattern: pattern.to_string(),
                        },
                        RedisCommand::Ping => RedisCommand::Ping,
                        RedisCommand::Echo { data } => RedisCommand::Echo {
                            data: data.to_string(),
                        },
                        RedisCommand::Incr { key } => RedisCommand::Incr {
                            key: key.to_string(),
                        },
                        RedisCommand::LPush { key, value } => RedisCommand::LPush {
                            key: key.to_string(),
                            value: value.to_string(),
                        },
                        RedisCommand::RPush { key, value } => RedisCommand::RPush {
                            key: key.to_string(),
                            value: value.to_string(),
                        },
                        RedisCommand::LRange { key, start, stop } => RedisCommand::LRange {
                            key: key.to_string(),
                            start: *start,
                            stop: *stop,
                        },
                        RedisCommand::XAdd { key, id, fields, original_resp } => RedisCommand::XAdd {
                            key: key.to_string(),
                            id: id.to_string(),
                            fields: fields.clone(),
                            original_resp: original_resp.to_string(),
                        },
                        RedisCommand::XRange { key, start, end } => RedisCommand::XRange {
                            key: key.to_string(),
                            start: start.to_string(),
                            end: end.to_string(),
                        },
                        RedisCommand::XRead { keys, ids, block, count } => RedisCommand::XRead {
                            keys: keys.clone(),
                            ids: ids.clone(),
                            block: *block,
                            count: *count,
                        },
                        RedisCommand::Multi => RedisCommand::Multi,
                        RedisCommand::Exec => RedisCommand::Exec,
                        RedisCommand::Discard => RedisCommand::Discard,
                        RedisCommand::Error { message } => RedisCommand::Error {
                            message: message.to_string(),
                        },
                        _ => RedisCommand::Error {
                            message: "Command not supported in transaction".to_string(),
                        },
                    };
                    self.queued_commands.lock().unwrap().push_back(owned_command);
                    "+QUEUED\r\n".to_string()
                } else {
                    self.execute_command(&command)
                }
            }
        }
    }

    pub fn start(&mut self) -> std::thread::JoinHandle<()> {
        let client = Arc::clone(&self.client);
        let redis = Arc::clone(&self.redis);
        let shutdown = Arc::clone(&self.shutdown);
        let in_transaction = Arc::clone(&self.in_transaction);
        let queued_commands = Arc::clone(&self.queued_commands);

        thread::spawn(move || {
            #[cfg(debug_assertions)]
            println!("[CLIENT] Starting new client handler");

            let mut buffer = Vec::new();
            let mut read_buffer = [0; 1024];

            loop {
                if *shutdown.lock().unwrap() {
                    break;
                }

                // Read from client
                let n = {
                    let mut client = client.lock().unwrap();
                    match client.read(&mut read_buffer) {
                        Ok(0) => {
                            // Connection closed
                            break;
                        }
                        Ok(n) => {
                            #[cfg(debug_assertions)]
                            println!("[CLIENT] Received {} bytes", n);
                            buffer.extend_from_slice(&read_buffer[..n]);
                            n
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
                };

                // Parse commands from buffer
                let commands = parse_resp(&buffer, buffer.len());
                if !commands.is_empty() {
                    buffer.clear();

                    #[cfg(debug_assertions)]
                    println!("[CLIENT] Processing {} commands", commands.len());

                    for command in commands {
                        #[cfg(debug_assertions)]
                        println!("[CLIENT] Executing command: {:?}", command);

                        let mut response = String::new();
                        match command {
                            RedisCommand::Multi => {
                                let mut in_transaction = in_transaction.lock().unwrap();
                                if *in_transaction {
                                    response = "-ERR MULTI calls can not be nested\r\n".to_string();
                                } else {
                                    *in_transaction = true;
                                    response = "+OK\r\n".to_string();
                                }
                            },
                            RedisCommand::Exec => {
                                let mut in_transaction = in_transaction.lock().unwrap();
                                if !*in_transaction {
                                    response = "-ERR EXEC without MULTI\r\n".to_string();
                                } else {
                                    *in_transaction = false;
                                    let mut responses = Vec::new();
                                    let mut queued_commands = queued_commands.lock().unwrap();
                                    
                                    // Execute all queued commands
                                    while let Some(cmd) = queued_commands.pop_front() {
                                        let result = redis.lock().unwrap().execute_command(&cmd, Some(&mut *client.lock().unwrap()));
                                        match result {
                                            Ok(resp) => responses.push(resp),
                                            Err(e) => responses.push(e),
                                        }
                                    }

                                    // Format response array
                                    response = format!("*{}\r\n", responses.len());
                                    for resp in responses {
                                        response.push_str(&resp);
                                    }
                                }
                            },
                            RedisCommand::Discard => {
                                let mut in_transaction = in_transaction.lock().unwrap();
                                if !*in_transaction {
                                    response = "-ERR DISCARD without MULTI\r\n".to_string();
                                } else {
                                    *in_transaction = false;
                                    queued_commands.lock().unwrap().clear();
                                    response = "+OK\r\n".to_string();
                                }
                            },
                            _ => {
                                if *in_transaction.lock().unwrap() {
                                    // Queue command and return QUEUED
                                    queued_commands.lock().unwrap().push_back(command);
                                    response = "+QUEUED\r\n".to_string();
                                } else {
                                    // Handle XREAD command separately since it has its own retry logic
                                    if let RedisCommand::XRead { keys, ids, block, count } = &command {
                                        #[cfg(debug_assertions)]
                                        println!("[ClientHandler::start] Handling XREAD command");
                                        
                                        let request = XReadRequest {
                                            keys: keys.iter().map(|s| s.to_string()).collect(),
                                            ids: ids.iter().map(|s| s.to_string()).collect(),
                                            block: *block,
                                            count: *count,
                                        };
                                        
                                        let mut handler = XReadHandler::new(Arc::clone(&redis), request);
                                        #[cfg(debug_assertions)]
                                        println!("[ClientHandler::start] Running XReadHandler...");
                                        match handler.run_loop() {
                                            Ok(results) => {
                                                #[cfg(debug_assertions)]
                                                println!("[ClientHandler::start] Got results with {} streams", results.len());
                                                let mut client = client.lock().unwrap();
                                                
                                                if results.is_empty() {
                                                    // Return nil response for empty results
                                                    #[cfg(debug_assertions)]
                                                    println!("[ClientHandler::start] Empty result, sending nil response");
                                                    client.write_all(b"*-1\r\n").unwrap();
                                                    client.flush().unwrap();
                                                    #[cfg(debug_assertions)]
                                                    println!("[ClientHandler::start] Nil response written and flushed");
                                                } else {
                                                    // Format non-empty response
                                                    let mut response = format!("*{}\r\n", results.len());
                                                    for (stream_key, entries) in results {
                                                        // Only include streams with entries
                                                        if !entries.is_empty() {
                                                            response.push_str(&format!("*2\r\n${}\r\n{}\r\n*{}\r\n", 
                                                                stream_key.len(), stream_key, entries.len()));
                                                            
                                                            for entry in entries {
                                                                // Format: *2\r\n$[id_len]\r\n[id]\r\n*[field_count*2]\r\n
                                                                let field_count = entry.fields.len() * 2; // Each field has key and value
                                                                response.push_str(&format!("*2\r\n${}\r\n{}\r\n*{}\r\n", 
                                                                    entry.id.len(), entry.id, field_count));
                                                                
                                                                for (field, value) in entry.fields {
                                                                    response.push_str(&format!("${}\r\n{}\r\n${}\r\n{}\r\n",
                                                                        field.len(), field, value.len(), value));
                                                                }
                                                            }
                                                        }
                                                    }
                                                    #[cfg(debug_assertions)]
                                                    println!("[ClientHandler::start] Sending response: {}", response);
                                                    client.write_all(response.as_bytes()).unwrap();
                                                    client.flush().unwrap();
                                                    #[cfg(debug_assertions)]
                                                    println!("[ClientHandler::start] Non-empty response written and flushed");
                                                }
                                            },
                                            Err(e) => {
                                                #[cfg(debug_assertions)]
                                                println!("[ClientHandler::start] Got error: {:?}", e);
                                                let mut client = client.lock().unwrap();
                                                client.write_all(format!("-ERR {}\r\n", e).as_bytes()).unwrap();
                                                client.flush().unwrap();
                                                #[cfg(debug_assertions)]
                                                println!("[ClientHandler::start] Error response written and flushed");
                                            }
                                        }
                                        continue;
                                    }
                                    
                                    match redis.lock().unwrap().execute_command(&command, Some(&mut *client.lock().unwrap())) {
                                        Ok(resp) => response = resp,
                                        Err(e) => response = e,
                                    }
                                }
                            }
                        }

                        #[cfg(debug_assertions)]
                        println!("[CLIENT] Got response: {}", response.replace("\r\n", "\\r\\n"));

                        if !response.is_empty() {
                            let mut client = client.lock().unwrap();
                            client.write_all(response.as_bytes()).unwrap();
                            client.flush().unwrap();
                        }
                    }
                }
            }
        })
    }
}
