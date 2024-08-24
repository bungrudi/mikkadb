use std::path::Path;
use std::fs::File;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::thread;
use std::io::{Read, Write, BufReader};
use std::net::TcpStream;
use base64;
use base64::Engine;
use base64::engine::general_purpose;
use std::sync::atomic::{AtomicU64, Ordering};

fn gen_replid() -> String {
    // let mut rng = rand::thread_rng();
    // std::iter::repeat(())
    //     .map(|()| {
    //         let num = rng.gen_range(0..62);
    //         match num {
    //             0..=9 => (num + 48) as u8 as char, // 0-9
    //             10..=35 => (num + 87) as u8 as char, // a-z
    //             _ => (num + 29) as u8 as char, // A-Z
    //         }
    //     })
    //     .take(40)
    //     .collect()
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}
#[derive(Debug)]
pub enum RedisCommand<'a> {
    None,
    Echo { data: &'a str },
    Ping,
    Set { key: &'a str, value: &'a str, ttl: Option<usize>, original_resp: String },
    Get { key: &'a str },
    Info { subcommand: String },
    Replconf { subcommand: &'a str, params: Vec<&'a str> },
    ReplconfGetack,
    Psync { replica_id: &'a str, offset: i8 },
    Wait { numreplicas: i64, timeout: i64, elapsed: i64 },
    Config { subcommand: &'a str, parameter: &'a str },
    Error { message: String },
    Keys { pattern: String },
}

impl RedisCommand<'_> {
    const PING : &'static str = "PING";
    const ECHO : &'static str = "ECHO";
    const SET : &'static str = "SET";
    const GET : &'static str = "GET";
    const INFO : &'static str = "INFO";
    const REPLCONF : &'static str = "REPLCONF";
    const PSYNC : &'static str = "PSYNC";
    const WAIT: &'static str = "WAIT";
    const CONFIG: &'static str = "CONFIG";
    const KEYS: &'static str = "KEYS";

    /// Create command from the data received from the client.
    /// It should check if the parameters are complete, otherwise return None.
    /// For example Set requires 2 parameters, key and value. When this method is called for
    /// a Set command, the first time it will return true to indicate that it expects another.
    pub fn data<'a>(command: &'a str, params: [&'a str;5], original_resp: &'a str) -> Option<RedisCommand<'a>> {
        match command {
            command if command.eq_ignore_ascii_case(Self::PING) => Some(RedisCommand::Ping),
            command if command.eq_ignore_ascii_case(Self::ECHO) => {
                if params[0] == "" {
                    None
                } else {
                    Some(RedisCommand::Echo { data: params[0] })
                }
            },
            command if command.eq_ignore_ascii_case(Self::SET) => {
                let key = params[0];
                let value = params[1];
                if key == "" || value == "" {
                    None
                } else {
                    let ttl = match params[2].eq_ignore_ascii_case("EX") {
                        true => match params[3].parse::<usize>() {
                            Ok(value) => Some(value * 1000),
                            Err(_) => None,
                        },
                        false => match params[2].eq_ignore_ascii_case("PX") {
                            true => match params[3].parse::<usize>() {
                                Ok(value) => Some(value),
                                Err(_) => None,
                            },
                            false => None,
                        },
                    };
                    // TODO original_resp.to_string() is not efficient.. find a way to avoid it.
                    Some(RedisCommand::Set { key, value, ttl, original_resp: original_resp.to_string() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::GET) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::Get { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::INFO) => {
                if params[0].is_empty() {
                    None
                } else {
                    Some(RedisCommand::Info { subcommand: params[0].to_string() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::REPLCONF) => {
                let subcommand = params[0];
                let params = params[1..].iter().filter(|&&p| !p.is_empty()).cloned().collect();
                if subcommand == "" {
                    Some(RedisCommand::Error { message: "ERR Wrong number of arguments for 'replconf' command".to_string() })
                } else {
                    // check if ReplConf GetAck
                    if subcommand.eq_ignore_ascii_case("getack") {
                        Some(RedisCommand::ReplconfGetack)
                    } else {
                        Some(RedisCommand::Replconf { subcommand, params })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::PSYNC) => {
                let replica_id = params[0];
                match params[1].parse::<i8>() {
                    Ok(offset) => Some(RedisCommand::Psync { replica_id, offset }),
                    Err(_) => Some(RedisCommand::Error { message: "ERR Invalid offset".to_string() }),
                }
            },
            command if command.eq_ignore_ascii_case(Self::WAIT) => {
                let numreplicas = params[0].parse::<i64>().unwrap();
                let timeout = params[1].parse::<i64>().unwrap();
                Some(RedisCommand::Wait { numreplicas, timeout, elapsed: 0 })
            },
            command if command.eq_ignore_ascii_case(Self::CONFIG) => {
                let subcommand = params[0];
                let parameter = params[1];
                if subcommand == "" || parameter == "" {
                    None
                } else {
                    Some(RedisCommand::Config { subcommand, parameter })
                }
            },
            command if command.eq_ignore_ascii_case(Self::KEYS) => {
                if params[0].is_empty() {
                    None
                } else {
                    Some(RedisCommand::Keys { pattern: params[0].to_string() })
                }
            },
            _ => Some(RedisCommand::Error { message: format!("Unknown command: {}", command) }),
        }
    }
}

struct ValueWrapper {
    value: String,
    expiration: Option<u128>,
}

#[derive(Debug)]
#[derive(Clone)]
pub struct RedisConfig {
    pub addr: String,
    pub port: String,
    pub replicaof_host: Option<String>,
    pub replicaof_port: Option<String>,
    pub dir: String,
    pub dbfilename: String,
}

impl RedisConfig {
    pub fn new() -> Self {
        RedisConfig {
            addr: "0.0.0.0".to_string(),
            port: "6379".to_string(),
            replicaof_host: None,
            replicaof_port: None,
            dir: ".".to_string(),
            dbfilename: "dump.rdb".to_string(),
        }
    }
}

pub struct Replica {
    #[allow(dead_code)]
    pub(crate) host: String,
    #[allow(dead_code)]
    pub(crate) port: String,
    pub(crate) stream: Arc<Mutex<TcpStream>>,
    offset: AtomicU64,
    
}
pub struct Redis {
    config: RedisConfig,
    data: Mutex<HashMap<String, ValueWrapper>>,
    pub(crate) replicas: Mutex<HashMap<String, Replica>>,
    pub(crate) replica_queue: Mutex<VecDeque<String>>,
    bytes_processed: AtomicU64, // total bytes processed for a replica
    replication_offset: AtomicU64, // total bytes propagated to replicas (if master)
    pub(crate) enqueue_getack: bool,
    // pub(crate) master_stream: Option<Arc<Mutex<TcpStream>>>,
}
impl Redis {

    pub fn new(config:RedisConfig) -> Self {
        Redis {
            config,
            data: Mutex::new(HashMap::new()),
            replicas: Mutex::new(HashMap::new()),
            replica_queue: Mutex::new(VecDeque::new()),
            bytes_processed: AtomicU64::new(0),
            replication_offset: AtomicU64::new(0),
            enqueue_getack: false,
        }
    }

    #[allow(dead_code)]
    fn new_default() -> Self {
        Redis {
            config: RedisConfig::new(),
            data: Mutex::new(HashMap::new()),
            replicas: Mutex::new(HashMap::new()),
            replica_queue: Mutex::new(VecDeque::new()),
            bytes_processed: AtomicU64::new(0),
            replication_offset: AtomicU64::new(0),
            enqueue_getack: false,
        }
    }

    pub fn set(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        let expiration =
            match ttl {
                Some(ttl) => {
                    if ttl == 0 {
                        None
                    } else {
                        Some(SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() + ttl as u128)
                    }
                },
                None => None,
            };
        let value_with_ttl = ValueWrapper { value: value.to_string(), expiration: expiration };
        self.data.lock().unwrap().insert(key.to_string(), value_with_ttl);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut binding = self.data.lock().unwrap();
        let value_wrapper = binding.get(key);
        match value_wrapper {
            Some(value_with_ttl) => {
                match value_with_ttl.expiration {
                    Some(expiration) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        if now > expiration {
                            binding.remove(key);
                            None
                        } else {
                            Some(value_with_ttl.value.clone())
                        }
                    },
                    None => Some(value_with_ttl.value.clone()),
                }
            },
            None => None,
        }
    }

    // TODO client should be a TcpStream not an option.. find how to mock it in tests.
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

                                let replica = Replica {
                                    host: replica_host.clone(),
                                    port: port.to_string(),
                                    stream: Arc::new(Mutex::new(client.try_clone().unwrap())),
                                    offset: AtomicU64::new(0),
                                };
                                let replica_key = format!("{}:{}", replica_host, real_port);
                                self.replicas.lock().unwrap().insert(replica_key, replica);
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
                let current_offset = self.replication_offset.load(Ordering::SeqCst);
                let up_to_date_replicas = self.count_up_to_date_replicas(self.replication_offset.load(Ordering::SeqCst));

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

    pub fn enqueue_for_replication(&mut self, command: &str) {
        let mut replica_queue = self.replica_queue.lock().unwrap();
        replica_queue.push_back(command.to_string());
        self.increment_replication_offset(command.len() as u64);
    }

    pub fn get_bytes_processed(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }

    pub fn incr_bytes_processed(&self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn increment_replication_offset(&self, increment: u64) {
        self.replication_offset.fetch_add(increment, Ordering::SeqCst);
    }

    pub fn update_replica_offset(&self, replica_key: &str, offset: u64) {
        let replicas = self.replicas.lock().unwrap();
        if let Some(replica) = replicas.get(replica_key) {
            replica.offset.fetch_max(offset, Ordering::SeqCst);
        } else {
            eprintln!("replica not found: {}", replica_key);          
        }

    }

    fn count_up_to_date_replicas(&self, current_offset: u64) -> usize {
        let replicas = self.replicas.lock().unwrap();
        let mut count = 0;
        for replica in replicas.values() {
            if replica.offset.load(Ordering::SeqCst) >= current_offset {
                count += 1;
            }
        }
        count
    }

    pub fn enque_getack(&mut self) {
        self.enqueue_getack = true;
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
                            self.set(&key, &value, None);
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

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        if pattern == "*" {
            self.data.lock().unwrap().keys()
                .filter(|k| !k.starts_with("redis-"))
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }
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
pub fn init_replica(config: &mut RedisConfig, redis: Arc<Mutex<Redis>>) {
    // as part of startup sequence..
    // if there is a replicaof host and port then this instance is a replica.

    // for now let's just code it here.
    // connect to remote host and port, send PING as RESP.
    // (a) sends PING command - wait for +PONG
    // (b) sends REPLCONF listening-port <PORT> - wait for +OK
    // (c) sends REPLCONF capa eof capa psync2 - wait for +OK
    // (d) sends PSYNC ? -1 - wait for +fullresync
    // the following sequence in separate thread..
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let db = Redis::new_default();
        assert!(db.data.lock().unwrap().is_empty());
    }

    #[test]
    fn test_set() {
        let mut db = Redis::new_default();
        db.set("key1", "value1", None);
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_get() {
        let mut db = Redis::new_default();
        db.set("key1", "value1", None);
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_set_ttl_and_get() {
        let mut db = Redis::new_default();
        db.set("key1", "value1", Some(1000));
        assert_eq!(db.get("key1"), Some("value1".to_string()));
        // sleep for 1 second
        std::thread::sleep(std::time::Duration::from_millis(1500));
        assert_eq!(db.get("key1"), None);
    }

    #[test]
    fn test_get_nonexistent() {
        let db = Redis::new_default();
        assert_eq!(db.get("key1"), None);
    }

    /// Test create RedisCommand::Set with TTL in milliseconds.
    /// SET key value PX 1000
    #[test]
    fn test_set_ttl_px() {
        let command = RedisCommand::data("SET", ["key", "value", "PX", "1000", ""], "SET key value PX 1000\r\n");
        match command {
            Some(RedisCommand::Set { key, value, ttl, original_resp }) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(ttl, Some(1000));
                assert_eq!(original_resp, "SET key value PX 1000\r\n".to_string());
            },
            _ => panic!("Expected RedisCommand::Set"),
        }

    }

    /// Test create RedisCommand::Set with TTL in seconds.
    /// SET key value EX 1
    #[test]
    fn test_set_ttl_ex() {
        let command = RedisCommand::data("SET", ["key", "value", "EX", "1", ""], "SET key value EX 1\r\n");
        match command {
            Some(RedisCommand::Set { key, value, ttl, original_resp }) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(ttl, Some(1000));
                assert_eq!(original_resp, "SET key value EX 1\r\n".to_string());
            },
            _ => panic!("Expected RedisCommand::Set"),
        }
    }

    #[test]
    fn test_replconf() {
        let mut db = Redis::new_default();
        let replconf_command = RedisCommand::Replconf {
            subcommand: "listening-port",
            params: vec!["6379"]
        };
        // TODO mock TcpStream...
        let _ = db.execute_command(&replconf_command, None);
        // TODO proper assert after we can mock TcpStream properly
        // assert_eq!(result, Ok("+OK\r\n".to_string()));

        let replconf_command = RedisCommand::Replconf {
            subcommand: "capa",
            params: vec!["psync2"]
        };
        let result = db.execute_command(&replconf_command, None);
        assert_eq!(result, Ok("+OK\r\n".to_string()));

        let replconf_command = RedisCommand::Replconf {
            subcommand: "unknown",
            params: vec!["param1", "param2"]
        };
        let result = db.execute_command(&replconf_command, None);
        assert_eq!(result, Err("ERR Unknown REPLCONF subcommand: unknown\r\n".to_string()));
    }

    #[test]
    fn test_psync_fullresync() {
        let mut db = Redis::new_default();
        let psync_command = RedisCommand::Psync {
            replica_id: "?",
            offset: -1,
        };
        let result = db.execute_command(&psync_command, None);
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.eq("")); // empty response, we write directly to the client.
    }

    // use super::*;
    // use std::io::Cursor;
    // use std::net::TcpStream;
    // use std::os::unix::io::FromRawFd;
    //
    // #[test]
    // fn test_read_until_end_of_rdb() {
    //     // Prepare the data to be read
    //     // "$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"
    //     let data = "+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011       redis-ver7.2.0\r\nredis-bits@ctimeeused-mem°aof-basen;Z*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    //     let cursor = Cursor::new(data);
    //
    //     // Create a TcpStream from the cursor
    //     let stream = unsafe { TcpStream::from_raw_fd(cursor.into_raw_fd()) };
    //
    //     // Create a buffer to read into
    //     let mut buffer = [0; 512];
    //
    //     // Create a Redis instance
    //     let redis = Redis::new_default();
    //
    //     // Call the function
    //     let rdb_file = redis.read_until_end_of_rdb(&mut stream, &mut buffer).unwrap();
    //
    //     // Check the RDB file
    //     assert!(rdb_file.starts_with("REDIS0011"));
    //     assert!(rdb_file.contains("redis-bits"));
    //     assert!(rdb_file.ends_with("*\r\n"));
    // }
}