use std::sync::atomic::{AtomicU64, Ordering};
use std::io::Write;
use base64::engine::general_purpose;
use base64::Engine;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, BTreeMap};

use crate::redis::config::RedisConfig;
use crate::redis::storage::Storage;
use crate::redis::replication::{ReplicationManager, TcpStreamTrait};
use crate::redis::commands::RedisCommand;
use crate::redis::utils::gen_replid;
use crate::redis::rdb::RdbParser;

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

    pub fn execute_command(&mut self, command: &RedisCommand, client: Option<&mut Box<dyn TcpStreamTrait>>) -> Result<String, String> {
        match command {
            RedisCommand::None => {
                Err("-ERR Unknown command\r\n".to_string())
            },
            RedisCommand::Multi => Ok("+OK\r\n".to_string()),
            RedisCommand::Exec => Ok("*0\r\n".to_string()),
            RedisCommand::Discard => Ok("+OK\r\n".to_string()),
            RedisCommand::Ping => Ok("+PONG\r\n".to_string()),
            RedisCommand::Echo { data } => Ok(format!("${}\r\n{}\r\n", data.len(), data)),
            RedisCommand::Get { key } => {
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
            RedisCommand::Type { key } => {
                let type_str = self.storage.get_type(key).into_owned();
                Ok(format!("+{}\r\n", type_str))
            },
            RedisCommand::Incr { key } => {
                match self.storage.incr(key) {
                    Ok(value) => Ok(format!(":{}\r\n", value)),
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::LPush { key, value } => {
                match self.storage.lpush(key, value) {
                    Ok(len) => Ok(format!(":{}\r\n", len)),
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::RPush { key, value } => {
                match self.storage.rpush(key, value) {
                    Ok(len) => Ok(format!(":{}\r\n", len)),
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::LPop { key } => {
                match self.storage.lpop(key) {
                    Some(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                    None => Ok("$-1\r\n".to_string()),
                }
            },
            RedisCommand::RPop { key } => {
                match self.storage.rpop(key) {
                    Some(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                    None => Ok("$-1\r\n".to_string()),
                }
            },
            RedisCommand::LLen { key } => {
                let len = self.storage.llen(key);
                Ok(format!(":{}\r\n", len))
            },
            RedisCommand::LRange { key, start, stop } => {
                let values = self.storage.lrange(key, *start, *stop);
                let mut response = format!("*{}\r\n", values.len());
                for value in values {
                    response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                }
                Ok(response)
            },
            RedisCommand::LTrim { key, start, stop } => {
                match self.storage.ltrim(key, *start, *stop) {
                    Ok(_) => Ok("+OK\r\n".to_string()),
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::LPos { key, element, count } => {
                match self.storage.lpos(key, element, *count) {
                    Ok(response) => Ok(response),
                    Err(err) => Err(err),
                }
            },
            RedisCommand::LInsert { key, before, pivot, element } => {
                match self.storage.linsert(key, *before, pivot, element) {
                    Ok(len) => Ok(format!(":{}\r\n", len)),
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::LSet { key, index, element } => {
                match self.storage.lset(key, *index, element) {
                    Ok(_) => Ok("+OK\r\n".to_string()),
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::LIndex { key, index } => {
                match self.storage.lindex(key, *index) {
                    Some(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                    None => Ok("$-1\r\n".to_string()),
                }
            },
            RedisCommand::XAdd { key, id, fields, original_resp } => {
                match self.xadd(key, id, fields.clone()) {
                    Ok(entry_id) => {
                        self.enqueue_for_replication(original_resp);
                        Ok(format!("${}\r\n{}\r\n", entry_id.len(), entry_id))
                    },
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::XRange { key, start, end } => {
                match self.storage.xrange(key, start, end) {
                    Ok(entries) => {
                        let mut response = format!("*{}\r\n", entries.len());
                        for entry in entries {
                            // Format each entry as an array containing the ID and field-value pairs
                            response.push_str("*2\r\n"); // Entry array has 2 elements: ID and fields array
                            response.push_str(&format!("${}\r\n{}\r\n", entry.id.len(), entry.id)); // ID

                            // Convert HashMap to BTreeMap to ensure consistent ordering
                            let ordered_fields: BTreeMap<_, _> = entry.fields.into_iter().collect();

                            // Format field-value pairs as an array
                            let field_count = ordered_fields.len() * 2; // Each field has a key and value
                            response.push_str(&format!("*{}\r\n", field_count));
                            for (key, value) in ordered_fields {
                                response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                                response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                            }
                        }
                        if response.contains("*") {
                            Ok(response)
                        } else {
                            Ok("$-1\r\n".to_string()) // Return null bulk string for empty results
                        }
                    },
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
            },
            RedisCommand::XRead { .. } => {
                // XREAD is handled by XReadHandler
                Err("ERR XREAD command is handled by XReadHandler".to_string())
            },
            RedisCommand::Info { subcommand } => {
                match subcommand.as_str() {
                    "replication" => {
                        let ret = if self.config.replicaof_host.is_some() {
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
                    _ => Err(format!("-ERR Unknown INFO subcommand: {}\r\n", subcommand)),
                }
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
                                return Ok("+OK\r\n".to_string());
                            }
                        }
                        Err("-ERR Cannot establish replica connection\r\n".to_string())
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
                    _ => Err(format!("-ERR Unknown REPLCONF subcommand: {}\r\n", subcommand)),
                }
            },
            RedisCommand::ReplconfGetack => {
                if let Some(client) = client {
                    let bytes_processed = self.get_bytes_processed();
                    // hacky, temporary way. we need to omit the "REPLCONF GETACK" and we assume it is 37 bytes.
                    let ack_bytes = if bytes_processed >= 37 {
                        bytes_processed - 37
                    } else {
                        0
                    };
                    let num_digits = ack_bytes.to_string().len();
                    let response = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", num_digits, ack_bytes);
                    let _ = client.write(response.as_bytes());
                    let _ = client.flush();
                    Ok("".to_string())  // Return empty string to indicate response was sent directly
                } else {
                    Err("-ERR No stream client to send REPLCONF GETACK\r\n".to_string())
                }
            },
            RedisCommand::Psync { replica_id, offset } => {
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

                    Ok("".to_string())  // Return empty string to indicate response was sent directly
                } else {
                    Err("-ERR Unknown PSYNC subcommand\r\n".to_string())
                }
            },
            RedisCommand::Wait { numreplicas, timeout, elapsed } => {
                #[cfg(debug_assertions)]
                println!("executing WAIT command");

                // First, ensure all pending commands are sent to replicas
                let sent_commands = self.replication.send_pending_commands();
                
                // Check current state
                let up_to_date_replicas = self.replication.count_up_to_date_replicas();
                
                if up_to_date_replicas >= *numreplicas as usize {
                    Ok(format!(":{}\r\n", up_to_date_replicas))
                } else if *elapsed >= *timeout {
                    #[cfg(debug_assertions)]
                    println!("timeout elapsed returning up_to_date_replicas: {} target ack: {}", up_to_date_replicas, numreplicas);
                    Ok(format!(":{}\r\n", up_to_date_replicas))
                } else {
                    // Need to retry - commands sent but not enough ACKs yet
                    Err("WAIT_RETRY".to_string())
                }
            },
            RedisCommand::Config { subcommand, parameter } => {
                match subcommand.as_str() {
                    "GET" => {
                        match parameter.as_str() {
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
                            _ => Err(format!("-ERR Unknown config parameter '{}'\r\n", parameter)),
                        }
                    },
                    _ => Err(format!("-ERR Unknown CONFIG subcommand '{}'\r\n", subcommand)),
                }
            },
            RedisCommand::Keys { pattern } => {
                let keys = self.keys(pattern);
                let mut response = format!("*{}\r\n", keys.len());
                for key in keys {
                    response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                }
                Ok(response)
            },
            RedisCommand::FlushDB => {
                self.storage.flushdb();
                Ok("+OK\r\n".to_string())
            },
            RedisCommand::Error { message } => {
                Err(format!("-{}\r\n", message))
            },
        }
    }
}
