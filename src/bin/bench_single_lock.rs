use tokio::net::TcpListener;
use anyhow::Result;
use std::sync::Arc;
use mikkadb_rust::single_lock_store::SingleLockStore;
use mikkadb_rust::storage::KeyValueStore;
use mikkadb_rust::resp::{RespHandler, Value};
use bytes::Bytes;

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
        let store = store.clone();

        tokio::spawn(async move {
            let mut handler = RespHandler::new(stream);

            loop {
                match handler.read_value().await {
                    Ok(Some(v)) => {
                        let response = match handle_command(&store, v).await {
                            Ok(resp) => resp,
                            Err(e) => Value::Error(format!("ERR {}", e)),
                        };

                        if let Err(_) = handler.write_value(response).await {
                            break;
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
        _ => Ok(Value::Error(format!("ERR unknown command '{}'", command))),
    }
}
