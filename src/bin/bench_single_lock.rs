use tokio::net::TcpListener;
use anyhow::Result;
use std::sync::Arc;
use std::path::PathBuf;
use mikkadb_rust::single_lock_store::SingleLockStore;
use mikkadb_rust::storage::KeyValueStore;
use mikkadb_rust::resp::{RespHandler, Value};
use mikkadb_rust::db::Db;
use bytes::Bytes;
use socket2::SockRef;
use std::env;

#[derive(Clone, Debug)]
struct Config {
    dir: String,
    dbfilename: String,
    port: u16,
    replicaof: Option<(String, u16)>, // (host, port) of master if this is a replica
}

impl Config {
    fn rdb_path(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(&self.dbfilename)
    }

    fn is_replica(&self) -> bool {
        self.replicaof.is_some()
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            dir: "/tmp".to_string(),
            dbfilename: "dump.rdb".to_string(),
            port: 6379,
            replicaof: None,
        }
    }
}

fn parse_args() -> Config {
    let args: Vec<String> = env::args().collect();
    let mut config = Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" => {
                if i + 1 < args.len() {
                    config.dir = args[i + 1].clone();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--dbfilename" => {
                if i + 1 < args.len() {
                    config.dbfilename = args[i + 1].clone();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--port" => {
                if i + 1 < args.len() {
                    if let Ok(port) = args[i + 1].parse::<u16>() {
                        config.port = port;
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--replicaof" => {
                if i + 1 < args.len() {
                    // Parse "host port" format
                    let parts: Vec<&str> = args[i + 1].split_whitespace().collect();
                    if parts.len() == 2 {
                        if let Ok(port) = parts[1].parse::<u16>() {
                            config.replicaof = Some((parts[0].to_string(), port));
                        }
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    config
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(parse_args());
    println!("SingleLockStore benchmark server starting on 127.0.0.1:{}", config.port);
    println!("Config: dir={}, dbfilename={}, port={}", config.dir, config.dbfilename, config.port);

    // Load RDB file if it exists
    let mut db = Db::new();
    let rdb_path = config.rdb_path();
    if let Err(e) = db.load_rdb(&rdb_path) {
        eprintln!("Failed to load RDB file: {}", e);
    }

    // Create the SingleLockStore from the loaded database
    let store = Arc::new(SingleLockStore::from_db(db));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    println!("Listening on 127.0.0.1:{}", config.port);

    loop {
        let (stream, _) = listener.accept().await?;

        // Phase 4: TCP_NODELAY optimization
        // Disable Nagle's algorithm for low-latency Redis workloads
        // Eliminates 40ms delay for small packets
        if let Err(e) = SockRef::from(&stream).set_nodelay(true) {
            eprintln!("Failed to set TCP_NODELAY: {}", e);
            continue;
        }

        let store = store.clone();
        let config = config.clone();

        tokio::spawn(async move {
            let mut handler = RespHandler::new(stream);

            // Transaction state per connection
            let mut in_transaction = false;
            let mut transaction_queue: Vec<Value> = Vec::new();

            // Phase 2: Pipelining constants
            const MAX_BATCH_COMMANDS: usize = 1024;
            const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024; // 4MB

            loop {
                match handler.read_value().await {
                    Ok(Some(v)) => {
                        // Phase 2: Collect commands into batch
                        let mut command_batch = vec![v];
                        let mut batch_bytes = 0usize;

                        // Drain buffer for additional commands (non-blocking)
                        loop {
                            if command_batch.len() >= MAX_BATCH_COMMANDS || batch_bytes >= MAX_BATCH_BYTES {
                                break;
                            }

                            match handler.try_read_value_from_buf() {
                                mikkadb_rust::resp::ParseResult::Complete(val, _consumed) => {
                                    batch_bytes += 100; // Rough estimate per command
                                    command_batch.push(val);
                                }
                                mikkadb_rust::resp::ParseResult::Incomplete => break,
                                mikkadb_rust::resp::ParseResult::Error(e) => {
                                    let _ = handler.write_value(Value::Error(Bytes::from(format!("ERR {}", e)))).await;
                                    break;
                                }
                            }
                        }

                        // Phase 2: Process batch of commands
                        let mut response_batch = Vec::with_capacity(command_batch.len());

                        for cmd_value in command_batch {
                            // Check if this is a transaction control command
                            let is_transaction_cmd = matches!(
                                get_command_name(&cmd_value).as_deref(),
                                Some("MULTI") | Some("EXEC") | Some("DISCARD")
                            );

                            if is_transaction_cmd {
                                // Handle transaction control commands
                                let cmd_name = get_command_name(&cmd_value).unwrap_or_default();
                                match cmd_name.as_str() {
                                    "MULTI" => {
                                        if in_transaction {
                                            response_batch.push(Value::Error(Bytes::from("ERR MULTI calls can not be nested")));
                                        } else {
                                            in_transaction = true;
                                            transaction_queue.clear();
                                            response_batch.push(Value::SimpleString(Bytes::from("OK")));
                                        }
                                    }
                                    "EXEC" => {
                                        if !in_transaction {
                                            response_batch.push(Value::Error(Bytes::from("ERR EXEC without MULTI")));
                                        } else {
                                            // Execute all queued commands
                                            let mut exec_results = Vec::new();
                                            for queued_cmd in &transaction_queue {
                                                let result = match handle_command(&store, &config, queued_cmd.clone()).await {
                                                    Ok(resp) => resp,
                                                    Err(e) => Value::Error(Bytes::from(format!("ERR {}", e))),
                                                };
                                                exec_results.push(result);
                                            }
                                            response_batch.push(Value::Array(exec_results));
                                            in_transaction = false;
                                            transaction_queue.clear();
                                        }
                                    }
                                    "DISCARD" => {
                                        if !in_transaction {
                                            response_batch.push(Value::Error(Bytes::from("ERR DISCARD without MULTI")));
                                        } else {
                                            in_transaction = false;
                                            transaction_queue.clear();
                                            response_batch.push(Value::SimpleString(Bytes::from("OK")));
                                        }
                                    }
                                    _ => {}
                                }
                            } else if in_transaction {
                                // Queue command during transaction
                                transaction_queue.push(cmd_value);
                                response_batch.push(Value::SimpleString(Bytes::from("QUEUED")));
                            } else {
                                // Normal command execution outside transaction
                                let response = match handle_command(&store, &config, cmd_value).await {
                                    Ok(resp) => resp,
                                    Err(e) => Value::Error(Bytes::from(format!("ERR {}", e))),
                                };
                                response_batch.push(response);
                            }
                        }

                        // Phase 2: Write all responses with single flush
                        if !response_batch.is_empty() {
                            if let Err(_) = handler.write_batch(response_batch).await {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        });
    }
}

// Helper function to extract command name from Value
fn get_command_name(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) if !items.is_empty() => {
            items[0].to_uppercase_string().ok()
        }
        _ => None,
    }
}

async fn handle_command(store: &Arc<SingleLockStore>, config: &Arc<Config>, value: Value) -> Result<Value> {
    let items = match value {
        Value::Array(items) => items,
        _ => return Ok(Value::Error(Bytes::from("ERR Invalid command format"))),
    };

    if items.is_empty() {
        return Ok(Value::Error(Bytes::from("ERR Empty command")));
    }

    let command = match items[0].to_uppercase_string() {
        Ok(cmd) => cmd,
        Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid command"))),
    };

    match command.as_str() {
        "PING" => {
            if items.len() > 1 {
                Ok(items[1].clone())
            } else {
                Ok(Value::SimpleString(Bytes::from("PONG")))
            }
        }
        "ECHO" => {
            if items.len() < 2 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'echo' command")));
            }
            match &items[1] {
                Value::BulkString(_) => Ok(items[1].clone()),
                _ => Ok(Value::Error(Bytes::from("ERR Invalid argument for ECHO"))),
            }
        }
        "GET" => {
            if items.len() < 2 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'get' command")));
            }
            let key = match items[1].to_string() {
                Ok(k) => k,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid key"))),
            };

            match store.get(&key).await {
                Ok(Some(bytes)) => Ok(Value::BulkString(bytes)),
                Ok(None) => Ok(Value::Null),
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "SET" => {
            if items.len() < 3 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'set' command")));
            }
            let key = match items[1].to_string() {
                Ok(k) => k,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid key"))),
            };
            let value = match &items[2] {
                Value::BulkString(b) => b.clone(),
                _ => return Ok(Value::Error(Bytes::from("ERR Invalid value"))),
            };

            // Parse PX option if present
            let px = if items.len() >= 5 {
                let option = match items[3].to_uppercase_string() {
                    Ok(opt) => opt,
                    Err(_) => String::new(),
                };
                if option == "PX" {
                    match items[4].to_string() {
                        Ok(s) => s.parse::<u64>().ok(),
                        Err(_) => None,
                    }
                } else {
                    None
                }
            } else {
                None
            };

            match store.set(key, value, px).await {
                Ok(_) => Ok(Value::SimpleString(Bytes::from("OK"))),
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "LPUSH" => {
            if items.len() < 3 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'lpush' command")));
            }
            let key = match items[1].to_string() {
                Ok(k) => k,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid key"))),
            };
            let values: Vec<Bytes> = items[2..]
                .iter()
                .filter_map(|v| match v {
                    Value::BulkString(b) => Some(b.clone()),
                    _ => None,
                })
                .collect();

            match store.lpush(key, values).await {
                Ok(len) => Ok(Value::Integer(len as i64)),
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "RPUSH" => {
            if items.len() < 3 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'rpush' command")));
            }
            let key = match items[1].to_string() {
                Ok(k) => k,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid key"))),
            };
            let values: Vec<Bytes> = items[2..]
                .iter()
                .filter_map(|v| match v {
                    Value::BulkString(b) => Some(b.clone()),
                    _ => None,
                })
                .collect();

            match store.rpush(key, values).await {
                Ok(len) => Ok(Value::Integer(len as i64)),
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "LRANGE" => {
            if items.len() < 4 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'lrange' command")));
            }
            let key = match items[1].to_string() {
                Ok(k) => k,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid key"))),
            };
            let start = match items[2].to_string() {
                Ok(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid start index"))),
                },
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid start index"))),
            };
            let stop = match items[3].to_string() {
                Ok(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid stop index"))),
                },
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid stop index"))),
            };

            match store.lrange(&key, start, stop).await {
                Ok(values) => {
                    let resp_values: Vec<Value> = values
                        .into_iter()
                        .map(|b| Value::BulkString(b))
                        .collect();
                    Ok(Value::Array(resp_values))
                }
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "INCR" => {
            if items.len() < 2 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'incr' command")));
            }
            let key = match items[1].to_string() {
                Ok(k) => k,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid key"))),
            };

            match store.incr(&key).await {
                Ok(n) => Ok(Value::Integer(n)),
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "LLEN" => {
            if items.len() < 2 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'llen' command")));
            }
            let key = match items[1].to_string() {
                Ok(k) => k,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid key"))),
            };

            match store.llen(&key).await {
                Ok(len) => Ok(Value::Integer(len)),
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "INFO" => {
            let section = if items.len() > 1 {
                match items[1].to_string() {
                    Ok(s) => s,
                    Err(_) => "default".to_string(),
                }
            } else {
                "default".to_string()
            };

            // Return INFO response based on whether this is a replica or master
            let info_response = match section.to_lowercase().as_str() {
                "replication" => {
                    if config.is_replica() {
                        "# Replication\nrole:slave\n"
                    } else {
                        "# Replication\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n"
                    }
                }
                _ => {
                    if config.is_replica() {
                        "# Server\nredis_version:7.0.0\n# Replication\nrole:slave\n"
                    } else {
                        "# Server\nredis_version:7.0.0\n# Replication\nrole:master\n"
                    }
                }
            };

            Ok(Value::BulkString(Bytes::from(info_response)))
        }
        "KEYS" => {
            if items.len() < 2 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'keys' command")));
            }
            let pattern = match items[1].to_string() {
                Ok(p) => p,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid pattern"))),
            };

            match store.keys(&pattern).await {
                Ok(keys) => {
                    let resp_values: Vec<Value> = keys
                        .into_iter()
                        .map(|k| Value::BulkString(Bytes::from(k)))
                        .collect();
                    Ok(Value::Array(resp_values))
                }
                Err(e) => Ok(Value::Error(Bytes::from(format!("ERR {}", e)))),
            }
        }
        "CONFIG" => {
            if items.len() < 3 {
                return Ok(Value::Error(Bytes::from("ERR wrong number of arguments for 'config' command")));
            }
            let subcommand = match items[1].to_uppercase_string() {
                Ok(s) => s,
                Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid subcommand"))),
            };

            match subcommand.as_str() {
                "GET" => {
                    let param = match items[2].to_string() {
                        Ok(p) => p,
                        Err(_) => return Ok(Value::Error(Bytes::from("ERR Invalid parameter"))),
                    };

                    match param.to_lowercase().as_str() {
                        "dir" => {
                            Ok(Value::Array(vec![
                                Value::BulkString(Bytes::from("dir")),
                                Value::BulkString(Bytes::from(config.dir.clone())),
                            ]))
                        }
                        "dbfilename" => {
                            Ok(Value::Array(vec![
                                Value::BulkString(Bytes::from("dbfilename")),
                                Value::BulkString(Bytes::from(config.dbfilename.clone())),
                            ]))
                        }
                        _ => {
                            // Unknown parameter - return empty array
                            Ok(Value::Array(vec![]))
                        }
                    }
                }
                _ => Ok(Value::Error(Bytes::from("ERR Unsupported CONFIG subcommand"))),
            }
        }
        _ => Ok(Value::Error(Bytes::from(format!("ERR unknown command '{}'", command)))),
    }
}
