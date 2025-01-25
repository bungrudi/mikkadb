use std::sync::atomic::{AtomicU64, Ordering};
use std::io::Write;
use base64::engine::general_purpose;
use base64::Engine;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::thread;
use std::collections::{HashMap, BTreeMap};

use crate::redis::config::RedisConfig;
use crate::redis::storage::Storage;
use crate::redis::replication::{ReplicationManager, TcpStreamTrait};
use crate::redis::commands::RedisCommand;
use crate::redis::utils::gen_replid;
use crate::redis::rdb::RdbParser;

#[derive(Debug)]
pub enum RedisResponse {
    Ok(String),
    Retry,
    Error(String),
    Array(Vec<RedisResponse>),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    SimpleString(String),
}

impl RedisResponse {
    pub fn format(&self) -> String {
        match self {
            RedisResponse::Ok(s) => format!("+{}\r\n", s),
            RedisResponse::Error(e) => format!("-{}\r\n", e),
            RedisResponse::SimpleString(s) => format!("+{}\r\n", s),
            RedisResponse::Integer(i) => format!(":{}\r\n", i),
            RedisResponse::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            RedisResponse::NullBulkString => "$-1\r\n".to_string(),
            RedisResponse::Array(arr) => {
                let mut result = format!("*{}\r\n", arr.len());
                for item in arr {
                    result.push_str(&item.format());
                }
                result
            },
            RedisResponse::Retry => String::new(),
        }
    }
}

pub struct Redis {
    pub config: RedisConfig,
    pub storage: Storage,
    pub bytes_processed: AtomicU64, // bytes processed by the server. important for a replica    
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

    #[allow(dead_code)]
    pub fn new_with_replication(replication: ReplicationManager) -> Self {
        Redis {
            config: RedisConfig::default(),
            storage: Storage::new(),
            bytes_processed: AtomicU64::new(0),
            replication,
        }
    }

    pub fn set(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        self.storage.set(key, value, ttl);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.storage.get(key)
    }

    pub fn xadd(&mut self, key: &str, id: &str, fields: HashMap<String, String>) -> Result<String, String> {
        self.storage.xadd(key, id, fields).map_err(|e| e.into_owned())
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

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.storage.keys(pattern)
    }

    pub fn parse_rdb_file(&mut self) -> std::io::Result<()> {
        let path = Path::new(&self.config.dir).join(&self.config.dbfilename);
        let key_value_pairs = RdbParser::parse(&path)?;
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        for (key, value, expiry) in key_value_pairs {
            let ttl = expiry.map(|exp| {
                let exp_secs = exp / 1000; // Convert milliseconds to seconds
                if exp_secs > now {
                    (exp_secs - now) as usize
                } else {
                    0
                }
            });
            self.set(&key, &value, ttl);
        }

        Ok(())
    }

    pub fn execute_command(&mut self, command: &RedisCommand, client: Option<&mut Box<dyn TcpStreamTrait>>) -> RedisResponse {
        match command {
            RedisCommand::None => {
                RedisResponse::Error("Unknown command".to_string())
            },
            RedisCommand::Multi => RedisResponse::Ok("OK".to_string()),
            RedisCommand::Exec => RedisResponse::Ok("*0".to_string()),
            RedisCommand::Discard => RedisResponse::Ok("OK".to_string()),
            RedisCommand::Ping => {
                if self.config.replicaof_host.is_some() {
                    // We're a replica, respond with REPLCONF ACK
                    if let Some(client) = client {
                        let bytes_processed = self.get_bytes_processed();
                        let num_digits = bytes_processed.to_string().len();
                        let response = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", 
                            num_digits, bytes_processed);
                        let _ = client.write(response.as_bytes());
                        let _ = client.flush();
                        RedisResponse::Ok("".to_string())  // Return empty string to indicate response was sent directly
                    } else {
                        RedisResponse::Error("No stream client to send REPLCONF ACK".to_string())
                    }
                } else {
                    // We're not a replica, respond with normal PONG
                    RedisResponse::Ok("PONG".to_string())
                }
            },
            RedisCommand::Echo { data } => RedisResponse::BulkString(data.clone()),
            RedisCommand::Get { key } => {
                match self.get(key) {
                    Some(value) => RedisResponse::BulkString(value),
                    None => RedisResponse::BulkString("".to_string()),
                }
            },
            RedisCommand::Set { key, value, ttl, original_resp } => {
                self.set(key, value, *ttl);
                self.enqueue_for_replication(original_resp);
                RedisResponse::Ok("OK".to_string())
            },
            RedisCommand::Type { key } => {
                let type_str = self.storage.get_type(key).into_owned();
                RedisResponse::BulkString(type_str)
            },
            RedisCommand::Incr { key } => {
                match self.storage.incr(key) {
                    Ok(value) => RedisResponse::Integer(value),
                    Err(e) => RedisResponse::Error(format!("{}", e)),
                }
            },
            RedisCommand::LPush { key, value } => {
                match self.storage.lpush(key, value) {
                    Ok(len) => RedisResponse::Integer(len),
                    Err(e) => RedisResponse::Error(format!("{}", e)),
                }
            },
            RedisCommand::RPush { key, value } => {
                match self.storage.rpush(key, value) {
                    Ok(len) => RedisResponse::Integer(len),
                    Err(e) => RedisResponse::Error(format!("{}", e)),
                }
            },
            RedisCommand::LPop { key } => {
                match self.storage.lpop(key) {
                    Some(value) => RedisResponse::BulkString(value),
                    None => RedisResponse::NullBulkString,
                }
            },
            RedisCommand::RPop { key } => {
                match self.storage.rpop(key) {
                    Some(value) => RedisResponse::BulkString(value),
                    None => RedisResponse::NullBulkString,
                }
            },
            RedisCommand::LLen { key } => {
                let len = self.storage.llen(key);
                RedisResponse::Integer(len as i64)
            },
            RedisCommand::LRange { key, start, stop } => {
                let values = self.storage.lrange(key, *start, *stop);
                if values.is_empty() {
                    RedisResponse::Array(vec![])
                } else {
                    RedisResponse::Array(
                        values.iter()
                            .map(|value| RedisResponse::BulkString(value.to_string()))
                            .collect()
                    )
                }
            },
            RedisCommand::LTrim { key, start, stop } => {
                match self.storage.ltrim(key, *start, *stop) {
                    Ok(_) => RedisResponse::Ok("OK".to_string()),
                    Err(e) => RedisResponse::Error(e),
                }
            },
            RedisCommand::LIndex { key, index } => {
                match self.storage.lindex(key, *index) {
                    Some(value) => RedisResponse::BulkString(value),
                    None => RedisResponse::NullBulkString,
                }
            },
            RedisCommand::LPos { key, element, count } => {
                match self.storage.lpos(key, element, count.map(|c| c as usize)) {
                    Some(positions) if positions.is_empty() => RedisResponse::Integer(-1),
                    Some(positions) => {
                        if count.is_some() {
                            RedisResponse::Array(positions.iter().map(|&pos| RedisResponse::Integer(pos as i64)).collect())
                        } else {
                            RedisResponse::Integer(positions[0] as i64)
                        }
                    },
                    None => RedisResponse::Integer(-1)
                }
            },
            RedisCommand::LInsert { key, before, pivot, element } => {
                match self.storage.linsert(key, *before, pivot, element) {
                    Some(len) => RedisResponse::Integer(len as i64),
                    None => RedisResponse::Integer(-1),
                }
            },
            RedisCommand::LSet { key, index, element } => {
                match self.storage.lset(key, *index, element) {
                    Ok(_) => RedisResponse::SimpleString("OK".to_string()),
                    Err(e) => RedisResponse::Error(e),
                }
            },
            RedisCommand::XAdd { key, id, fields, original_resp } => {
                match self.xadd(key, id, fields.clone()) {
                    Ok(entry_id) => {
                        self.enqueue_for_replication(original_resp);
                        RedisResponse::BulkString(entry_id)
                    },
                    Err(e) => RedisResponse::Error(format!("{}", e)),
                }
            },
            RedisCommand::XRange { key, start, end } => {
                match self.storage.xrange(key, start, end) {
                    Ok(entries) => {
                        let mut response = Vec::new();
                        for entry in entries {
                            // Format each entry as an array containing the ID and field-value pairs
                            let mut entry_response = Vec::new();
                            entry_response.push(RedisResponse::BulkString(entry.id.clone()));
                            // Convert HashMap to BTreeMap to ensure consistent ordering
                            let ordered_fields: BTreeMap<_, _> = entry.fields.into_iter().collect();
                            for (key, value) in ordered_fields {
                                entry_response.push(RedisResponse::BulkString(key));
                                entry_response.push(RedisResponse::BulkString(value));
                            }
                            response.push(RedisResponse::Array(entry_response));
                        }
                        RedisResponse::Array(response)
                    },
                    Err(e) => RedisResponse::Error(format!("{}", e)),
                }
            },
            RedisCommand::XRead { .. } => {
                // XREAD is handled by XReadHandler
                RedisResponse::Error("ERR XREAD command is handled by XReadHandler".to_string())
            },
            RedisCommand::Info { subcommand } => {
                let mut info = String::new();
                
                // Add replication info
                info.push_str("# Replication\n");
                if self.config.replicaof_host.is_some() {
                    info.push_str("role:slave\n");
                    info.push_str(&format!("master_replid:{}\n", gen_replid()));
                    info.push_str(&format!("master_repl_offset:0\n"));
                    info.push_str(&format!("master_host:{}\n", self.config.replicaof_host.as_ref().unwrap()));
                    info.push_str(&format!("master_port:{}\n", self.config.replicaof_port.as_ref().unwrap()));
                } else {
                    info.push_str("role:master\n");
                    info.push_str(&format!("master_replid:{}\n", gen_replid()));
                    info.push_str(&format!("master_repl_offset:0\n"));
                    info.push_str(&format!("connected_slaves:0\n"));
                }
                
                RedisResponse::BulkString(info)
            },
            RedisCommand::Replconf { subcommand, params } => {
                match subcommand.to_lowercase().as_str() {
                    "listening-port" => {
                        if let Some(_port) = params.get(0) {
                            if let Some(client) = client {
                                let peer = client.peer_addr().unwrap();
                                let replica_host = peer.ip().to_string();
                                let real_port = peer.port();
                                #[cfg(debug_assertions)]
                                println!("replica_host: {} replica_port: {}", replica_host, _port);

                                self.replication.add_replica(replica_host, real_port.to_string(), client.try_clone().unwrap());
                                return RedisResponse::Ok("OK".to_string());
                            }
                        }
                        RedisResponse::Error("Cannot establish replica connection".to_string())
                    },
                    "capa" => {
                        // TODO: Implement the actual logic for these subcommands
                        RedisResponse::Ok("OK".to_string())
                    },
                    "ack" => {
                        if let Some(offset_str) = params.get(0) {
                            if let Ok(offset) = offset_str.parse::<u64>() {
                                if let Some(client) = client {
                                    let addr = client.peer_addr().unwrap();
                                    let replica_key = format!("{}:{}", addr.ip(), addr.port());
                                    self.update_replica_offset(&replica_key, offset);
                                    return RedisResponse::Ok("OK".to_string());
                                }
                            }
                        }
                        RedisResponse::Error("Invalid ACK format".to_string())
                    }
                    _ => RedisResponse::Error(format!("Unknown REPLCONF subcommand: {}", subcommand)),
                }
            },
            RedisCommand::ReplconfGetack => {
                if let Some(client) = client {
                    let bytes_processed = self.get_bytes_processed();
                    // hacky, temporary way. we need to omit the "REPLCONF GETACK" and we assume it is 37 bytes.
                    let response = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", 
                        bytes_processed.to_string().len(), 
                        bytes_processed);
                    let _ = client.write(response.as_bytes());
                    let _ = client.flush();
                    // Reset counter after reporting
                    self.bytes_processed.store(0, Ordering::SeqCst);
                    RedisResponse::Ok("".to_string())  // Return empty string to indicate response was sent directly
                } else {
                    RedisResponse::Error("No stream client to send REPLCONF GETACK".to_string())
                }
            },
            RedisCommand::Psync { replica_id, offset } => {
                if *offset == -1 && *replica_id == "?" {
                    if let Some(client) = client {
                        let _ = client.write(format!("+FULLRESYNC {} {}", gen_replid(), 0).as_bytes());

                        let rdb_file_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
                        // Decode the base64 string into a byte array
                        let rdb_file = general_purpose::STANDARD.decode(rdb_file_base64).unwrap();
                        let length = rdb_file.len();
                        let _ = client.write(format!("${}\r\n", length).as_bytes());
                        let _ = client.write(rdb_file.as_slice());
                        let _ = client.flush();
                    }

                    RedisResponse::Ok("".to_string())  // Return empty string to indicate response was sent directly
                } else {
                    RedisResponse::Error("Unknown PSYNC subcommand".to_string())
                }
            },
            RedisCommand::Wait { numreplicas, timeout, elapsed } => {
                #[cfg(debug_assertions)]
                println!("[WAIT] Starting WAIT command execution: target_replicas={}, timeout={}ms, elapsed={}ms", numreplicas, timeout, elapsed);
                
                // Send initial GETACK to replicas to get their current offsets
                #[cfg(debug_assertions)]
                println!("[WAIT] Sending GETACK to replicas");
                
                let mut elapsed_time = *elapsed as i64;
                let start = std::time::Instant::now();

                while elapsed_time < *timeout as i64 {
                    // Send GETACK to replicas
                    if let Err(_) = self.replication.send_getack_to_replicas() {
                        #[cfg(debug_assertions)]
                        println!("[WAIT] Failed to send GETACK to replicas");
                        return RedisResponse::Integer(0);
                    }

                    // Give replicas a chance to respond
                    thread::sleep(Duration::from_millis(10));

                    // Check if we have enough replicas
                    #[cfg(debug_assertions)]
                    println!("[WAIT] Checking replica acknowledgments");
                    
                    let acks = self.replication.count_up_to_date_replicas();
                    if acks >= *numreplicas as usize {
                        #[cfg(debug_assertions)]
                        println!("[WAIT] Found {} up-to-date replicas (target: {})", acks, numreplicas);
                        return RedisResponse::Integer(acks as i64);
                    }

                    // Update elapsed time
                    elapsed_time = start.elapsed().as_millis() as i64;

                    #[cfg(debug_assertions)]
                    println!("[WAIT] Not enough replicas acknowledged (got {}, need {}). Elapsed: {}ms", acks, numreplicas, elapsed_time);
                }

                // If we've exceeded the timeout, return 0
                #[cfg(debug_assertions)]
                println!("[WAIT] Command timed out after {}ms", elapsed_time);
                RedisResponse::Integer(0)
            },
            RedisCommand::Config { subcommand, parameter } => {
                match subcommand.as_str() {
                    "GET" => {
                        match parameter.as_str() {
                            "dir" => {
                                // Return the current directory
                                let dir = self.config.dir.clone();
                                RedisResponse::BulkString(dir)
                            },
                            "dbfilename" => {
                                // Return the current DB filename
                                let dbfilename = self.config.dbfilename.clone();
                                RedisResponse::BulkString(dbfilename)
                            },
                            _ => RedisResponse::Error(format!("Unknown config parameter '{}'", parameter)),
                        }
                    },
                    _ => RedisResponse::Error(format!("Unknown CONFIG subcommand '{}'", subcommand)),
                }
            },
            RedisCommand::Keys { pattern } => {
                let keys = self.keys(pattern);
                let mut response = Vec::new();
                for key in keys {
                    response.push(RedisResponse::BulkString(key));
                }
                RedisResponse::Array(response)
            },
            RedisCommand::FlushDB => {
                self.storage.flushdb();
                RedisResponse::Ok("OK".to_string())
            },
            RedisCommand::Error { message } => {
                RedisResponse::Error(message.clone())
            },
        }
    }
}
