mod commands;
mod config;
mod replica;
mod storage;
mod replication;

use std::path::Path;
use std::fs::File;
use std::io::{Read, BufReader, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use base64;
use base64::Engine;
use base64::engine::general_purpose;
use std::sync::{Arc, Mutex};

pub use config::RedisConfig;
pub use commands::RedisCommand;
pub use storage::Storage;
pub use replication::ReplicationManager;
use std::net::TcpStream;
use std::thread;

pub struct Redis {
    config: RedisConfig,
    storage: Storage,
    bytes_processed: AtomicU64, // bytes processed by the server. important for a replica    
    pub replication: ReplicationManager,
}

impl Redis {
    pub fn new(config: RedisConfig) -> Self {
        Redis {
            config,
            storage: Storage::new(),
            bytes_processed: AtomicU64::new(0),
            replication: ReplicationManager::new(),
        }
    }

    pub fn set(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        self.storage.set(key, value, ttl);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.storage.get(key)
    }

    pub fn enqueue_for_replication(&mut self, command: &str) {
        self.replication.enqueue_for_replication(command);
    }

    pub fn update_replica_offset(&mut self, replica_key: &str, offset: u64) {
        self.replication.update_replica_offset(replica_key, offset);
    }

    pub fn get_bytes_processed(&self) -> u64 {
        self.bytes_processed.load(Ordering::SeqCst)
    }

    pub fn incr_bytes_processed(&mut self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.storage.keys(pattern)
    }

    pub fn execute_command(&mut self, command: &RedisCommand, client: Option<&mut TcpStream>) -> Result<String, String> {
        match command {
            RedisCommand::Ping  => {
                Ok("+PONG\r\n".to_string())
            },
            RedisCommand::Echo { data} => {
                Ok(format!("${}\r\n{}\r\n", data.len(), data))
            },
            RedisCommand::Get { key} => {
                match self.get(key) {
                    Some(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                    None => Ok("$-1\r\n".to_string()),
                }
            },
            RedisCommand::Set { key, value, ttl, original_resp } => {
                self.set(key, value, *ttl);
                self.enqueue_for_replication(original_resp);
                Ok("+OK\r\n".to_string())
            },
            RedisCommand::Info { subcommand} => {
                match subcommand.as_str() {
                    "replication" => {
                        let ret:String =
                            if self.config.replicaof_host.is_some() {
                                format!("role:slave\r\nmaster_replid:{}\r\nmaster_repl_offset:0\r\nmaster_host:{}\r\nmaster_port:{}",
                                        gen_replid(),
                                        self.config.replicaof_host.as_ref().unwrap(),
                                        self.config.replicaof_port.as_ref().unwrap())
                            } else {
                                format!("role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:0\r\nconnected_slaves:0",
                                        gen_replid())
                            };
                        Ok(format!("${}\r\n{}\r\n", ret.len(), ret))
                    },
                    _ => Err("ERR Unknown INFO subcommand".to_string()),
                }
            },
            RedisCommand::Replconf {
                subcommand,params
            } => {
                match subcommand.to_lowercase().as_str() {
                    "listening-port" => {
                        if let Some(port) = params.get(0) {
                            if let Some(client) = client {
                                let peer = client.peer_addr().unwrap();
                                let replica_host = peer.ip().to_string();
                                let real_port = peer.port();
                                println!("replica_host: {} replica_port: {}", replica_host, port);

                                // let replica = Replica {
                                //     host: replica_host.clone(),
                                //     port: port.to_string(),
                                //     stream: Arc::new(Mutex::new(client.try_clone().unwrap())),
                                //     offset: AtomicU64::new(0),
                                // };
                                // let replica_key = format!("{}:{}", replica_host, real_port);
                                self.replication.add_replica(replica_host, real_port.to_string(), client.try_clone().unwrap());
                                return Ok("+OK\r\n".to_string());
                            }
                        }
                        Err("-cannot establish replica connection\r\n".to_string())
                    },
                    "capa" => {
                        // TODO: Implement the actual logic for these subcommands
                        Ok("+OK\r\n".to_string())
                    },
                    "ack" => {
                        if let Some(offset_str) = params.get(0) {
                            if let Ok(offset) = offset_str.parse::<u64>() {
                                if let Some(client) = client {
                                    let addr = client.peer_addr().unwrap();
                                    let replica_key = format!("{}:{}", addr.ip(), addr.port());
                                    self.update_replica_offset(&replica_key, offset);
                                    return Ok("".to_string());
                                }
                            }
                        }
                        Err("-ERR Invalid ACK format\r\n".to_string())
                    }
                    _ => Err(format!("ERR Unknown REPLCONF subcommand: {}\r\n", subcommand)),
                }
            },
            RedisCommand::ReplconfGetack => {
                match client {
                    Some(client) => {
                        // hacky, temporary way. we need to omit the "REPLCONF GETACK" and we assume it is 37 bytes.
                        let bytes_processed = self.get_bytes_processed() - 37;
                        
                        let num_digits = bytes_processed.to_string().len();
                        let response = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", num_digits, bytes_processed);
                        let _ = client.write(response.as_bytes());
                        let _ = client.flush();
                    },
                    None => {
                        return Err("ERR No stream client to send REPLCONF GETACK\r\n".to_string());
                    }
                }
                Ok("".to_string())
            },
            RedisCommand::Psync {
                replica_id, offset
            } => {
                if *offset == -1 && *replica_id == "?" {
                    if let Some(client) = client {
                        let _ = client.write(format!("+FULLRESYNC {} {}\r\n", gen_replid(), 0).as_bytes());

                        let rdb_file_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
                        // Decode the base64 string into a byte array
                        let rdb_file = general_purpose::STANDARD.decode(rdb_file_base64).unwrap();
                        let length = rdb_file.len();
                        let _ = client.write(format!("${}\r\n", length).as_bytes());
                        let _ = client.write(rdb_file.as_slice());
                        let _ = client.flush();
                    }

                    Ok("".to_string())
                } else {
                    Err("ERR Unknown PSYNC subcommand".to_string())
                }
            },
            RedisCommand::Wait { numreplicas, timeout , elapsed} => {
                println!("executing WAIT command");
                let current_offset = self.replication.get_replication_offset();
                let up_to_date_replicas = self.replication.count_up_to_date_replicas();

                println!("up_to_date: {} target ack: {} current offset {}", 
                    up_to_date_replicas, 
                    numreplicas, 
                    current_offset);

                if up_to_date_replicas >= *numreplicas as usize {
                    Ok(format!(":{}\r\n", up_to_date_replicas))
                } else {
                    if *elapsed >= *timeout {
                        println!("timeout elapsed, returning up_to_date_replicas: {} target ack: {}", up_to_date_replicas, numreplicas);
                        Ok(format!(":{}\r\n", up_to_date_replicas))
                    } else {
                        // Return a special error to indicate that we need to retry 
                        Err(format!("WAIT_RETRY {} {}", numreplicas, timeout))
                    }
                }               
                
            },
            RedisCommand::Config { subcommand, parameter} => {
                match *subcommand {
                    "GET" => {
                        match *parameter {
                            "dir" => {
                                // Return the current directory
                                let dir = self.config.dir.clone();
                                Ok(format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", dir.len(), dir))
                            },
                            "dbfilename" => {
                                // Return the current DB filename
                                let dbfilename = self.config.dbfilename.clone();
                                Ok(format!("*2\r\n$9\r\ndbfilename\r\n${}\r\n{}\r\n", dbfilename.len(), dbfilename))
                            },
                            _ => Err(format!("-ERR unknown config parameter '{}'\r\n", parameter)),
                        }
                    },
                    _ => Err(format!("-ERR unknown subcommand '{}'\r\n", subcommand)),
                }
            },
            RedisCommand::Error { message } => {
                Err(format!("ERR {}", message))
            },
            RedisCommand::Keys { pattern } => {
                let keys = self.keys(pattern);
                let mut response = format!("*{}\r\n", keys.len());
                for key in keys {
                    response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                }
                Ok(response)
            },
            _ => Err("ERR Unknown command".to_string()),
        }
    }

    pub fn parse_rdb_file(&mut self, dir: &str, dbfilename: &str) -> std::io::Result<()> {
        let path = Path::new(dir).join(dbfilename);
        println!("Attempting to open RDB file: {:?}", path);
        if !path.exists() {
            println!("RDB file does not exist");
            return Ok(());
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        println!("RDB file size: {} bytes", buffer.len());

        if buffer.len() < 9 || &buffer[0..5] != b"REDIS" {
            println!("Invalid RDB file format");
            return Ok(());
        }

        let mut pos = 9; // Skip the header

        while pos < buffer.len() {
            println!("Parsing at position: {}", pos);
            match buffer[pos] {
                0xFA => {
                    // Auxiliary field, skip it
                    println!("Skipping auxiliary field");
                    pos += 1;
                    let (_, new_pos) = self.parse_string(&buffer, pos)?;
                    pos = new_pos;
                    let (_, new_pos) = self.parse_string(&buffer, pos)?;
                    pos = new_pos;
                }
                0xFE => {
                    println!("Database selector found");
                    // Database selector
                    pos += 1;
                    // Skip database number and hash table sizes
                    while buffer[pos] != 0xFB && pos < buffer.len() {
                        pos += 1;
                    }
                    pos += 3;
                }
                0xFF => {
                    println!("End of file marker found");
                    // End of file
                    break;
                }
                _ => {
                    println!("Parsing key-value pair");
                    // Key-value pair
                    match self.parse_key_value(&buffer, pos) {
                        Ok((key, value, new_pos)) => {
                            println!("Parsed key: {}", key);
                            self.storage.set(&key, &value, None);
                            pos = new_pos;
                        }
                        Err(e) => {
                            println!("Error parsing key-value pair: {:?}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        println!("RDB file parsing completed");
        Ok(())
    }

    fn parse_key_value(&self, buffer: &[u8], mut pos: usize) -> std::io::Result<(String, String, usize)> {
        // Skip expire info if present
        if buffer[pos] == 0xFD || buffer[pos] == 0xFC {
            pos += if buffer[pos] == 0xFD { 5 } else { 9 };
        }

        // Skip value type
        pos += 1;

        // Parse key
        let (key, new_pos) = self.parse_string(buffer, pos)?;
        pos = new_pos;

        // Parse value
        let (value, new_pos) = self.parse_string(buffer, pos)?;
        pos = new_pos;

        Ok((key, value, pos))
    }

    fn parse_string(&self, buffer: &[u8], mut pos: usize) -> std::io::Result<(String, usize)> {
        let len = match buffer[pos] >> 6 {
            0 => {
                let len = (buffer[pos] & 0x3F) as usize;
                pos += 1;
                len
            }
            1 => {
                let len = (((buffer[pos] & 0x3F) as usize) << 8) | buffer[pos + 1] as usize;
                pos += 2;
                len
            }
            2 => {
                let len = ((buffer[pos + 1] as usize) << 24)
                    | ((buffer[pos + 2] as usize) << 16)
                    | ((buffer[pos + 3] as usize) << 8)
                    | (buffer[pos + 4] as usize);
                pos += 5;
                len
            }
            3 => {
                // Special encoding
                match buffer[pos] & 0x3F {
                    0 => {
                        // 8 bit integer
                        let value = buffer[pos + 1] as i8;
                        pos += 2;
                        return Ok((value.to_string(), pos));
                    }
                    1 => {
                        // 16 bit integer
                        let value = i16::from_le_bytes([buffer[pos + 1], buffer[pos + 2]]);
                        pos += 3;
                        return Ok((value.to_string(), pos));
                    }
                    2 => {
                        // 32 bit integer
                        let value = i32::from_le_bytes([buffer[pos + 1], buffer[pos + 2], buffer[pos + 3], buffer[pos + 4]]);
                        pos += 5;
                        return Ok((value.to_string(), pos));
                    }
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Unsupported string encoding",
                        ));
                    }
                }
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid string encoding",
                ));
            }
        };

        if pos + len > buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "String length exceeds buffer size",
            ));
        }

        let s = String::from_utf8_lossy(&buffer[pos..pos + len]).to_string();
        pos += len;

        Ok((s, pos))
    }
}

// Helper function to generate a replica ID
fn gen_replid() -> String {
    // todo: generate a random replid
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}

#[inline]
fn connect_to_server(host: &str, port: &str) -> std::io::Result<TcpStream> {
    let replicaof_addr = format!("{}:{}", host, port);
    TcpStream::connect(replicaof_addr)
}

#[inline]
fn send_command(stream: &mut std::net::TcpStream, command: &str) -> std::io::Result<usize> {
    stream.write(command.as_bytes())
}

#[inline]
fn read_response(stream: &mut TcpStream, buffer: &mut [u8; 512]) -> std::io::Result<String> {
    let bytes_read = stream.read(buffer)?;
    Ok(String::from_utf8_lossy(&buffer[0..bytes_read]).to_string()) // TODO ready the RDB part
}

#[inline]
fn read_until_end_of_rdb(stream: &mut TcpStream, buffer: &mut [u8; 512]) {
    // peek until we get '$' and then read the length of the 'fullresync'+RDB file.
    let mut count = 0;
    'LOOP_PEEK: while let Ok(peek_size) = stream.peek(buffer) {
        thread::sleep(std::time::Duration::from_millis(200));
        println!("peek_size: {}", peek_size);
        println!("peeked: {}", String::from_utf8_lossy(&buffer[0..peek_size]));
        if count > 10 {
            println!("count exceeded");
            break;
        }
        if peek_size == 0 {
            count += 1;
            continue;
        }

        // find the patter '$xx\r\n' where xx is numeric in the buffer, then read from 0 to current pos+88
        let mut found = false;
        let mut ret_len;
        for i in 0..peek_size {
            if buffer[i] == b'$' {
                ret_len = i;
                let mut length = 0;
                let mut j = i + 1;
                while j < peek_size {
                    if buffer[j] == b'\r' {
                        found = true;
                        break;
                    }
                    length = length * 10 + (buffer[j] - b'0') as usize;
                    j += 1;
                }
                if found {
                    println!("found RDB length: {}", length);
                    let mut rdb_buffer = vec![0; ret_len + length + 5]; // 2 would be \r\n
                    let _ = stream.read_exact(&mut rdb_buffer);
                    let rdb_file = String::from_utf8_lossy(&rdb_buffer);
                    println!("rdb_file: {}", rdb_file);
                    break 'LOOP_PEEK;
                }
            }
        }
    }
}

/// This function is called when the Redis server is configured as a replica. It performs the following steps:
/// 1. Connects to the master server
/// 2. Sends a PING command to verify the connection
/// 3. Configures the replica with REPLCONF commands
/// 4. Initiates replication with the PSYNC command
///
/// # Arguments
///
/// * `config` - A mutable reference to the RedisConfig struct containing server configuration
/// * `redis` - An Arc<Mutex<Redis>> representing the shared Redis state
#[inline]
pub fn init_replica(config: &mut RedisConfig, redis: Arc<Mutex<Redis>>) {
    let config = config.clone();
    thread::sleep(std::time::Duration::from_millis(500));
    if let Some(replicaof_host) = &config.replicaof_host {
        if let Some(replicaof_port) = &config.replicaof_port {
            match connect_to_server(replicaof_host, replicaof_port) {
                Ok(mut stream) => {
                    let replconf_command = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n", config.port.len(), config.port);
                    let commands: Vec<(&str, Box<dyn Fn(&str, &mut TcpStream) -> bool>)> = vec![
                        ("*1\r\n$4\r\nPING\r\n", Box::new(|response: &str, _| response == "+PONG\r\n")),
                        (&replconf_command, Box::new(|response: &str, _| response == "+OK\r\n")),
                        ("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", Box::new(|response: &str, _| response == "+OK\r\n")),

                    ];

                    let mut buffer = [0; 512];
                    for (command, validate) in commands {
                        println!("sending command: {}", command);
                        if send_command(&mut stream, command).is_err() {
                            eprintln!("error executing command: {}", command);
                            std::process::exit(1);
                        }
                        println!("reading response..");
                        let response = read_response(&mut stream, &mut buffer).unwrap();
                        println!("response: {}", response);
                        if !validate(&response, &mut stream) {
                            eprintln!("unexpected response: {}", response);
                            std::process::exit(1);
                        }
                    }
                    // send psync, and then hand over to the event loop.
                    if send_command(&mut stream, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").is_err() {
                        eprintln!("error executing PSYNC command");
                        std::process::exit(1);
                    }
                    println!("psync sent");
                    // Read fullresync and RDB file, and no more.
                    // leave the rest to the event loop.

                    read_until_end_of_rdb(&mut stream, &mut buffer);

                    // let mut redis = redis.lock().unwrap();
                    // redis.master_stream = Some(Arc::new(Mutex::new(stream)));
                    let mut client_handler = crate::client_handler::ClientHandler::new_master(stream, redis.clone());
                    client_handler.start();
                    println!("replicaof host and port is valid");
                }
                Err(e) => {
                    eprintln!("error connecting to replicaof host and port: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}