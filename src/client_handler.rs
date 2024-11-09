use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use crate::redis::{Redis, RedisCommand};
use crate::resp::parse_resp;
use crate::redis::replication::TcpStreamTrait;

// ClientHandler should ideally be an actor.
pub struct ClientHandler {
    client: Arc<Mutex<Box<dyn TcpStreamTrait>>>,
    redis: Arc<Mutex<Redis>>,
    master: bool // is this client a master?
}

impl ClientHandler {
    pub fn new<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(Box::new(client) as Box<dyn TcpStreamTrait>)),
            redis,
            master: false
        }
    }

    pub fn new_master<T: TcpStreamTrait + 'static>(client: T, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(Box::new(client) as Box<dyn TcpStreamTrait>)),
            redis,
            master: true
        }
    }

    pub fn start(&mut self) {
        let mut buffer: [u8;2048] = [0; 2048];
        let client = Arc::clone(&self.client);
        let redis = Arc::clone(&self.redis);
        let master = self.master;
        thread::spawn(move || {
            let mut client = client.lock().unwrap();
            let addr = client.peer_addr().unwrap();
            'LOOP_READ_NETWORK: while let Ok(bytes_read) = client.read(&mut buffer) {
                // TODO disconnection hook.. this is handy when we want to do cleanup i.e. in a replica setup
                println!("read {} bytes", bytes_read);
                if bytes_read == 0 {
                    break 'LOOP_READ_NETWORK;
                }
                // TODO refactor this to use a buffer pool.
                let commands = parse_resp(&buffer, bytes_read);

                // increment bytes read..
                // TODO: only for write commands
                redis.lock().expect("failed to lock redis").incr_bytes_processed(bytes_read as u64);

                // iterate over commands
                'LOOP_REDIS_CMD: for command in commands {
                    if matches!(command, RedisCommand::None) {
                        break 'LOOP_REDIS_CMD;
                    }
                    println!("DEBUG: Starting to process command: {:?}", command);
                    // TODO use the same buffer to write the response.
                    // actually is there benefit in re-using the buffer?
                    let mut should_retry = true;
                    let mut wait_params = None;
                    let start_time = Instant::now();
                    let mut command = command;
                    let mut original_xread_command = None;

                    // Store the original XREAD command if this is an XREAD
                    if let RedisCommand::XRead { keys, ids, block } = &command {
                        println!("DEBUG: Storing original XREAD command - keys: {:?}, ids: {:?}, block: {:?}", keys, ids, block);
                        original_xread_command = Some((keys.clone(), ids.clone(), *block));
                    }

                    let mut retry_count = 0;

                    while should_retry {
                        println!("DEBUG: Executing command (retry #{}) - Current command: {:?}", retry_count, command);
                        let result = {
                            // Scope the Redis lock to ensure it's released after the execute_command
                            redis.lock().expect("failed to lock redis").execute_command(&command, Some(&mut client))
                        };

                        match result {
                            Ok(response) => {
                                println!("DEBUG: Command successful, response: {}", response);
                                if !response.is_empty() && !master {
                                    println!("DEBUG: Writing response to client");
                                    let _ = client.write(response.as_bytes());
                                }
                                if master {
                                    println!("DEBUG: Master mode, not writing response");
                                }
                                should_retry = false;
                            },
                            Err(error) if error.starts_with(crate::redis::WAIT_RETRY_PREFIX) => {
                                println!("DEBUG: WAIT retry requested: {}", error);
                                let parts: Vec<&str> = error.split_whitespace().collect();
                                
                                let numreplicas = parts[1].parse::<i64>().unwrap();
                                let timeout = parts[2].parse::<i64>().unwrap();
                                wait_params = Some((numreplicas, timeout));
                                should_retry = true;

                                println!("DEBUG: WAIT retry params - numreplicas: {}, timeout: {}", numreplicas, timeout);
                                
                                // Sleep without holding the lock
                                thread::sleep(Duration::from_millis(300));
                            },
                            Err(error) if error.starts_with(&format!("-{}", crate::redis::XREAD_RETRY_PREFIX)) => {
                                println!("DEBUG: XREAD retry requested: {}", error);
                                let parts: Vec<&str> = error.split_whitespace().collect();
                                let timeout = parts[1].parse::<u64>().unwrap();
                                
                                let elapsed = start_time.elapsed().as_millis() as u64;
                                println!("DEBUG: XREAD - timeout: {}, elapsed: {}ms", timeout, elapsed);
                                
                                if elapsed >= timeout {
                                    println!("DEBUG: XREAD timeout expired, sending null string");
                                    if !master {
                                        let _ = client.write(b"*0\r\n");
                                    }
                                    should_retry = false;
                                } else {
                                    println!("DEBUG: XREAD continuing to wait for data");
                                    thread::sleep(Duration::from_millis(50));
                                    should_retry = true;
                                }
                            },
                            Err(error) => {
                                println!("DEBUG: Command error: {}", error);
                                if !master {
                                    let _ = client.write(error.as_bytes());
                                }
                                should_retry = false;
                            }
                        }

                        // Handle retries for both WAIT and XREAD
                        if should_retry {
                            retry_count += 1;
                            println!("DEBUG: Preparing for retry #{}", retry_count);
                            
                            // Handle WAIT command retry
                            if let Some((numreplicas, original_timeout)) = wait_params {
                                println!("DEBUG: Preparing WAIT command retry");
                                if retry_count == 1 {
                                    // Scope the Redis lock
                                    {
                                        let redis = redis.lock().unwrap();
                                        println!("DEBUG: Setting GETACK flag");
                                        redis.replication.set_enqueue_getack(true);
                                    }
                                }

                                let elapsed = start_time.elapsed().as_millis() as i64;
                                command = RedisCommand::Wait { numreplicas, timeout: original_timeout, elapsed: elapsed };
                                println!("DEBUG: Updated WAIT command with elapsed time: {}", elapsed);
                            }
                            
                            // Re-create XREAD command for retry
                            if let Some((keys, ids, block)) = &original_xread_command {
                                println!("DEBUG: Re-creating XREAD command for retry");
                                println!("DEBUG: Using keys: {:?}, ids: {:?}, block: {:?}", keys, ids, block);
                                command = RedisCommand::XRead {
                                    keys: keys.clone(),
                                    ids: ids.clone(),
                                    block: *block,
                                };
                            }
                        } else {
                            println!("DEBUG: Command completed, no retry needed");
                        }
                    }
                    println!("DEBUG: Command processing complete");
                }
            }
            println!("DEBUG: Closing connection {}", addr);
        });
    }
}
