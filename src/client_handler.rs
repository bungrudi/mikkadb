use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use crate::redis::{Redis, RedisCommand};
use crate::resp::parse_resp;

// ClientHandler should ideally be an actor.
pub struct ClientHandler {
    client: Arc<Mutex<TcpStream>>,
    redis: Arc<Mutex<Redis>>,
    master: bool // is this client a master?
}

impl ClientHandler {

    pub fn new (client: TcpStream, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(client)),
            redis,
            master: false
        }
    }

    pub fn new_master (client: TcpStream, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(client)),
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
                redis.lock().expect("failed to lock redis").incr_bytes_processed(bytes_read as u64);

                // iterate over commands
                'LOOP_REDIS_CMD: for command in commands {
                    if matches!(command, RedisCommand::None) {
                        break 'LOOP_REDIS_CMD;
                    }
                    println!("command: {:?}", command);
                    // TODO use the same buffer to write the response.
                    // actually is there benefit in re-using the buffer?
                    let mut retry_wait = true;
                    let mut wait_params = None;
                    let start_time = Instant::now();
                    let mut command = command;

                    let mut retry_count = 0;

                    while retry_wait {
                        match redis.lock().expect("failed to lock redis").execute_command(&command, Some(&mut client)) {
                            Ok(response) => {
                                println!("response: {}", response);
                                if !response.is_empty() && !master {
                                    let _ = client.write(response.as_bytes());
                                }
                                if master {
                                    println!("master, not writing to client");
                                }
                                retry_wait = false;
                            },
                            Err(error) if error.starts_with("WAIT_RETRY") => {
                                println!("encountered WAIT_RETRY: {}", error);
                                let parts: Vec<&str> = error.split_whitespace().collect();
                                
                                let numreplicas = parts[1].parse::<i64>().unwrap();
                                let timeout = parts[2].parse::<i64>().unwrap();
                                wait_params = Some((numreplicas, timeout));

                                println!("WAIT_RETRY: numreplicas: {}, timeout: {}", numreplicas, timeout);
                            },
                            Err(error) => {
                                eprintln!("error: {}", error);
                                if !master {
                                    let _ = client.write(error.as_bytes());
                                }
                                retry_wait = false;
                            }
                        }

                        // If we're retrying a WAIT command, update the command with reduced timeout
                        if retry_wait {
                            println!("retrying WAIT command");
                            if retry_count == 0 {
                                println!("enqueuing GETACK");
                                redis.lock().unwrap().enque_getack();
                            }
                            retry_count += 1;
                            if let Some((numreplicas, original_timeout)) = wait_params {

                                thread::sleep(Duration::from_millis(10));
                                let elapsed = start_time.elapsed().as_millis() as i64;
                                // let new_timeout = std::cmp::max(0, original_timeout - elapsed);
                                command = RedisCommand::Wait { numreplicas, timeout: original_timeout, elapsed: elapsed };
                                
                                println!("retrying WAIT command with timeout: {} elapsed: {}", original_timeout, elapsed);
                            }
                        }
                    }
                    println!("after write");
                }

            }
            println!("closing connection {}", addr);
        });
    }
}
