use std::sync::atomic::{AtomicU64, Ordering};
use std::net::TcpStream;
use std::io::Write;
use base64::engine::general_purpose;
use base64::Engine;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

use crate::redis::config::RedisConfig;
use crate::redis::storage::Storage;
use crate::redis::replication::ReplicationManager;
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

    pub fn set(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        println!("DEBUG: Setting key '{}' with value '{}' and TTL {:?}", key, value, ttl);
        self.storage.set(key, value, ttl);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let result = self.storage.get(key);
        println!("DEBUG: Getting key '{}'. Result: {:?}", key, result);
        result
    }

    pub fn xadd(&mut self, key: &str, id: &str, fields: HashMap<String, String>) -> Result<String, String> {
        self.storage.xadd(key, id, fields)
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

    pub fn execute_command(&mut self, command: &RedisCommand, client: Option<&mut TcpStream>) -> Result<String, String> {
        match command {
            RedisCommand::Ping => {
                Ok("+PONG\r\n".to_string())
            },
            RedisCommand::Echo { data } => {
                Ok(format!("${}\r\n{}\r\n", data.len(), data))
            },
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
                let type_str = self.storage.get_type(key);
                Ok(format!("+{}\r\n", type_str))
            },
            RedisCommand::XAdd { key, id, fields, original_resp } => {
                match self.xadd(key, id, fields.clone()) {
                    Ok(entry_id) => {
                        self.enqueue_for_replication(original_resp);
                        Ok(format!("+{}\r\n", entry_id))
                    },
                    Err(e) => Err(format!("-{}\r\n", e)),
                }
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
                        if let Some(port) = params.get(0) {
                            if let Some(client) = client {
                                let peer = client.peer_addr().unwrap();
                                let replica_host = peer.ip().to_string();
                                let real_port = peer.port();
                                println!("replica_host: {} replica_port: {}", replica_host, port);

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
                        return Err("-ERR No stream client to send REPLCONF GETACK\r\n".to_string());
                    }
                }
                Ok("".to_string())
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

                    Ok("".to_string())
                } else {
                    Err("-ERR Unknown PSYNC subcommand\r\n".to_string())
                }
            },
            RedisCommand::Wait { numreplicas, timeout, elapsed } => {
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
            RedisCommand::Config { subcommand, parameter } => {
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
            RedisCommand::Error { message } => {
                Err(format!("-{}\r\n", message))
            },
            RedisCommand::None => {
                Err("-ERR Unknown command\r\n".to_string())
            },
        }
    }
}
