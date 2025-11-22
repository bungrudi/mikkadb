use tokio::net::TcpListener;
use anyhow::Result;
use std::sync::Arc;
use mikkadb_rust::single_lock_store::SingleLockStore;
use mikkadb_rust::storage::KeyValueStore;
use mikkadb_rust::resp::{RespHandler, Value};
use bytes::Bytes;
use socket2::SockRef;

#[tokio::main]
async fn main() -> Result<()> {
    let port = 6379;
    println!("SingleLockStore benchmark server starting on 127.0.0.1:{}", port);

    // Create the SingleLockStore
    let store = Arc::new(SingleLockStore::new());

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!("Listening on 127.0.0.1:{}", port);

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

        tokio::spawn(async move {
            let mut handler = RespHandler::new(stream);

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
                                    let _ = handler.write_value(Value::Error(format!("ERR {}", e))).await;
                                    break;
                                }
                            }
                        }

                        // Phase 2: Process batch of commands
                        let mut response_batch = Vec::with_capacity(command_batch.len());

                        for cmd_value in command_batch {
                            let response = match handle_command(&store, cmd_value).await {
                                Ok(resp) => resp,
                                Err(e) => Value::Error(format!("ERR {}", e)),
                            };
                            response_batch.push(response);
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

async fn handle_command(store: &Arc<SingleLockStore>, value: Value) -> Result<Value> {
    let items = match value {
        Value::Array(items) => items,
        _ => return Ok(Value::Error("ERR Invalid command format".to_string())),
    };

    if items.is_empty() {
        return Ok(Value::Error("ERR Empty command".to_string()));
    }

    let command = match &items[0] {
        Value::BulkString(s) => s.to_uppercase(),
        _ => return Ok(Value::Error("ERR Invalid command".to_string())),
    };

    match command.as_str() {
        "PING" => {
            if items.len() > 1 {
                Ok(items[1].clone())
            } else {
                Ok(Value::SimpleString("PONG".to_string()))
            }
        }
        "ECHO" => {
            if items.len() < 2 {
                return Ok(Value::Error("ERR wrong number of arguments for 'echo' command".to_string()));
            }
            match &items[1] {
                Value::BulkString(s) => Ok(Value::BulkString(s.clone())),
                _ => Ok(Value::Error("ERR Invalid argument for ECHO".to_string())),
            }
        }
        "GET" => {
            if items.len() < 2 {
                return Ok(Value::Error("ERR wrong number of arguments for 'get' command".to_string()));
            }
            let key = match &items[1] {
                Value::BulkString(s) => s,
                _ => return Ok(Value::Error("ERR Invalid key".to_string())),
            };

            match store.get(key).await {
                Ok(Some(bytes)) => {
                    let s = String::from_utf8_lossy(&bytes).to_string();
                    Ok(Value::BulkString(s))
                }
                Ok(None) => Ok(Value::Null),
                Err(e) => Ok(Value::Error(format!("ERR {}", e))),
            }
        }
        "SET" => {
            if items.len() < 3 {
                return Ok(Value::Error("ERR wrong number of arguments for 'set' command".to_string()));
            }
            let key = match &items[1] {
                Value::BulkString(s) => s.clone(),
                _ => return Ok(Value::Error("ERR Invalid key".to_string())),
            };
            let value = match &items[2] {
                Value::BulkString(s) => s.clone(),
                _ => return Ok(Value::Error("ERR Invalid value".to_string())),
            };

            // Parse PX option if present
            let px = if items.len() >= 5 {
                let option = match &items[3] {
                    Value::BulkString(s) => s.to_uppercase(),
                    _ => String::new(),
                };
                if option == "PX" {
                    match &items[4] {
                        Value::BulkString(s) => s.parse::<u64>().ok(),
                        _ => None,
                    }
                } else {
                    None
                }
            } else {
                None
            };

            match store.set(key, Bytes::from(value), px).await {
                Ok(_) => Ok(Value::SimpleString("OK".to_string())),
                Err(e) => Ok(Value::Error(format!("ERR {}", e))),
            }
        }
        "LPUSH" => {
            if items.len() < 3 {
                return Ok(Value::Error("ERR wrong number of arguments for 'lpush' command".to_string()));
            }
            let key = match &items[1] {
                Value::BulkString(s) => s.clone(),
                _ => return Ok(Value::Error("ERR Invalid key".to_string())),
            };
            let values: Vec<Bytes> = items[2..]
                .iter()
                .filter_map(|v| match v {
                    Value::BulkString(s) => Some(Bytes::from(s.clone())),
                    _ => None,
                })
                .collect();

            match store.lpush(key, values).await {
                Ok(len) => Ok(Value::Integer(len as i64)),
                Err(e) => Ok(Value::Error(format!("ERR {}", e))),
            }
        }
        "RPUSH" => {
            if items.len() < 3 {
                return Ok(Value::Error("ERR wrong number of arguments for 'rpush' command".to_string()));
            }
            let key = match &items[1] {
                Value::BulkString(s) => s.clone(),
                _ => return Ok(Value::Error("ERR Invalid key".to_string())),
            };
            let values: Vec<Bytes> = items[2..]
                .iter()
                .filter_map(|v| match v {
                    Value::BulkString(s) => Some(Bytes::from(s.clone())),
                    _ => None,
                })
                .collect();

            match store.rpush(key, values).await {
                Ok(len) => Ok(Value::Integer(len as i64)),
                Err(e) => Ok(Value::Error(format!("ERR {}", e))),
            }
        }
        "LRANGE" => {
            if items.len() < 4 {
                return Ok(Value::Error("ERR wrong number of arguments for 'lrange' command".to_string()));
            }
            let key = match &items[1] {
                Value::BulkString(s) => s,
                _ => return Ok(Value::Error("ERR Invalid key".to_string())),
            };
            let start = match &items[2] {
                Value::BulkString(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return Ok(Value::Error("ERR Invalid start index".to_string())),
                },
                _ => return Ok(Value::Error("ERR Invalid start index".to_string())),
            };
            let stop = match &items[3] {
                Value::BulkString(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return Ok(Value::Error("ERR Invalid stop index".to_string())),
                },
                _ => return Ok(Value::Error("ERR Invalid stop index".to_string())),
            };

            match store.lrange(key, start, stop).await {
                Ok(values) => {
                    let resp_values: Vec<Value> = values
                        .into_iter()
                        .map(|b| Value::BulkString(String::from_utf8_lossy(&b).to_string()))
                        .collect();
                    Ok(Value::Array(resp_values))
                }
                Err(e) => Ok(Value::Error(format!("ERR {}", e))),
            }
        }
        "INCR" => {
            if items.len() < 2 {
                return Ok(Value::Error("ERR wrong number of arguments for 'incr' command".to_string()));
            }
            let key = match &items[1] {
                Value::BulkString(s) => s,
                _ => return Ok(Value::Error("ERR Invalid key".to_string())),
            };

            match store.incr(key).await {
                Ok(n) => Ok(Value::Integer(n)),
                Err(e) => Ok(Value::Error(format!("ERR {}", e))),
            }
        }
        "LLEN" => {
            if items.len() < 2 {
                return Ok(Value::Error("ERR wrong number of arguments for 'llen' command".to_string()));
            }
            let key = match &items[1] {
                Value::BulkString(s) => s,
                _ => return Ok(Value::Error("ERR Invalid key".to_string())),
            };

            match store.llen(key).await {
                Ok(len) => Ok(Value::Integer(len)),
                Err(e) => Ok(Value::Error(format!("ERR {}", e))),
            }
        }
        _ => Ok(Value::Error(format!("ERR unknown command '{}'", command))),
    }
}
