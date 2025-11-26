//! ActorStore: KeyValueStore implementation wrapping the existing Engine actor
//!
//! This serves as a baseline for performance comparison against SingleLockStore.
//! It allows us to benchmark the Actor model using the same trait interface.

use crate::command::RedisCommand;
use crate::engine::CommandRequest;
use crate::resp::Value;
use crate::storage::{KeyValueStore, StoreError};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

/// Wrapper around the existing Engine actor that implements KeyValueStore
pub struct ActorStore {
    tx: mpsc::Sender<CommandRequest>,
    client_id: u64,
}

impl ActorStore {
    /// Create a new ActorStore wrapping an Engine actor channel
    pub fn new(tx: mpsc::Sender<CommandRequest>, client_id: u64) -> Self {
        ActorStore { tx, client_id }
    }

    /// Helper to execute a command and get the response
    async fn execute_command(&self, command: RedisCommand) -> Result<Value, StoreError> {
        let (response_tx, response_rx) = oneshot::channel();

        let req = CommandRequest {
            client_id: self.client_id,
            command,
            response_tx,
            replica_tx: None,
            pub_sub_tx: None,
            from_replica: false,
        };

        self.tx
            .send(req)
            .await
            .map_err(|e| StoreError::Internal(format!("Failed to send command: {}", e)))?;

        response_rx
            .await
            .map_err(|e| StoreError::Internal(format!("Failed to receive response: {}", e)))?
            .map_err(|e| StoreError::Internal(format!("Command execution error: {}", e)))
    }
}

#[async_trait]
impl KeyValueStore for ActorStore {
    // ============================================================================
    // String Operations
    // ============================================================================

    async fn get(&self, key: &str) -> Result<Option<Bytes>, StoreError> {
        let value = self
            .execute_command(RedisCommand::Get {
                key: Bytes::from(key.to_string()),
            })
            .await?;

        match value {
            Value::BulkString(s) => Ok(Some(Bytes::from(s))),
            Value::Null => Ok(None),
            _ => Err(StoreError::Internal(format!(
                "Unexpected response type for GET: {:?}",
                value
            ))),
        }
    }

    async fn set(&self, key: String, value: Bytes, px: Option<u64>) -> Result<(), StoreError> {
        let result = self
            .execute_command(RedisCommand::Set {
                key: Bytes::from(key),
                value,
                px,
            })
            .await?;

        match result {
            Value::SimpleString(_) => Ok(()),
            Value::Error(e) => Err(StoreError::Internal(String::from_utf8_lossy(&e).to_string())),
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for SET: {:?}",
                result
            ))),
        }
    }

    async fn incr(&self, key: &str) -> Result<i64, StoreError> {
        let value = self
            .execute_command(RedisCommand::Incr {
                key: Bytes::from(key.to_string()),
            })
            .await?;

        match value {
            Value::Integer(n) => Ok(n),
            Value::Error(ref e) => {
                let err_str = String::from_utf8_lossy(e).to_string();
                if err_str.contains("not an integer") {
                    Err(StoreError::InvalidOperation(err_str))
                } else {
                    Err(StoreError::Internal(err_str))
                }
            }
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for INCR: {:?}",
                value
            ))),
        }
    }

    async fn decr(&self, _key: &str) -> Result<i64, StoreError> {
        // DECR not implemented in current system, simulate with INCR logic
        // For baseline comparison, we can just return an error
        Err(StoreError::InvalidOperation(
            "DECR not implemented in Actor model".to_string(),
        ))
    }

    async fn exists(&self, _keys: &[String]) -> Result<i64, StoreError> {
        // EXISTS not directly implemented, would need to add to RedisCommand
        // For baseline, return error
        Err(StoreError::InvalidOperation(
            "EXISTS not implemented in Actor model".to_string(),
        ))
    }

    async fn del(&self, _keys: &[String]) -> Result<i64, StoreError> {
        // DEL not directly implemented, would need to add to RedisCommand
        // For baseline, return error
        Err(StoreError::InvalidOperation(
            "DEL not implemented in Actor model".to_string(),
        ))
    }

    // ============================================================================
    // List Operations
    // ============================================================================

    async fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError> {
        let string_values: Vec<String> = values
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).to_string())
            .collect();

        let result = self
            .execute_command(RedisCommand::LPush {
                key,
                values: string_values,
            })
            .await?;

        match result {
            Value::Integer(n) => Ok(n as usize),
            Value::Error(ref e) => {
                let err_str = String::from_utf8_lossy(e).to_string();
                if err_str.contains("WRONGTYPE") {
                    Err(StoreError::WrongType {
                        expected: "list",
                        actual: "unknown",
                    })
                } else {
                    Err(StoreError::Internal(err_str))
                }
            }
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for LPUSH: {:?}",
                result
            ))),
        }
    }

    async fn rpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError> {
        let string_values: Vec<String> = values
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).to_string())
            .collect();

        let result = self
            .execute_command(RedisCommand::RPush {
                key,
                values: string_values,
            })
            .await?;

        match result {
            Value::Integer(n) => Ok(n as usize),
            Value::Error(ref e) => {
                let err_str = String::from_utf8_lossy(e).to_string();
                if err_str.contains("WRONGTYPE") {
                    Err(StoreError::WrongType {
                        expected: "list",
                        actual: "unknown",
                    })
                } else {
                    Err(StoreError::Internal(err_str))
                }
            }
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for RPUSH: {:?}",
                result
            ))),
        }
    }

    async fn lpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError> {
        let result = self
            .execute_command(RedisCommand::LPop {
                key: key.to_string(),
                count,
            })
            .await?;

        match result {
            Value::Null => Ok(None),
            Value::BulkString(s) => Ok(Some(vec![Bytes::from(s)])),
            Value::Array(items) => {
                let bytes_vec: Vec<Bytes> = items
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::BulkString(s) => Some(Bytes::from(s)),
                        _ => None,
                    })
                    .collect();
                if bytes_vec.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(bytes_vec))
                }
            }
            Value::Error(ref e) => {
                let err_str = String::from_utf8_lossy(e).to_string();
                if err_str.contains("WRONGTYPE") {
                    Err(StoreError::WrongType {
                        expected: "list",
                        actual: "unknown",
                    })
                } else {
                    Err(StoreError::Internal(err_str))
                }
            }
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for LPOP: {:?}",
                result
            ))),
        }
    }

    async fn rpop(&self, _key: &str, _count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError> {
        // RPOP not implemented in current system
        Err(StoreError::InvalidOperation(
            "RPOP not implemented in Actor model".to_string(),
        ))
    }

    async fn llen(&self, key: &str) -> Result<i64, StoreError> {
        let result = self
            .execute_command(RedisCommand::LLen {
                key: key.to_string(),
            })
            .await?;

        match result {
            Value::Integer(n) => Ok(n),
            Value::Error(ref e) => {
                let err_str = String::from_utf8_lossy(e).to_string();
                if err_str.contains("WRONGTYPE") {
                    Err(StoreError::WrongType {
                        expected: "list",
                        actual: "unknown",
                    })
                } else {
                    Err(StoreError::Internal(err_str))
                }
            }
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for LLEN: {:?}",
                result
            ))),
        }
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, StoreError> {
        let result = self
            .execute_command(RedisCommand::LRange {
                key: key.to_string(),
                start,
                end: stop,
            })
            .await?;

        match result {
            Value::Array(items) => {
                let bytes_vec: Vec<Bytes> = items
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::BulkString(s) => Some(Bytes::from(s)),
                        _ => None,
                    })
                    .collect();
                Ok(bytes_vec)
            }
            Value::Error(ref e) => {
                let err_str = String::from_utf8_lossy(e).to_string();
                if err_str.contains("WRONGTYPE") {
                    Err(StoreError::WrongType {
                        expected: "list",
                        actual: "unknown",
                    })
                } else {
                    Err(StoreError::Internal(err_str))
                }
            }
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for LRANGE: {:?}",
                result
            ))),
        }
    }

    // ============================================================================
    // Hash Operations (Not implemented in current system)
    // ============================================================================

    async fn hset(
        &self,
        _key: String,
        _field: String,
        _value: Bytes,
    ) -> Result<bool, StoreError> {
        Err(StoreError::InvalidOperation(
            "HSET not implemented in Actor model".to_string(),
        ))
    }

    async fn hget(&self, _key: &str, _field: &str) -> Result<Option<Bytes>, StoreError> {
        Err(StoreError::InvalidOperation(
            "HGET not implemented in Actor model".to_string(),
        ))
    }

    async fn hgetall(&self, _key: &str) -> Result<Vec<(Bytes, Bytes)>, StoreError> {
        Err(StoreError::InvalidOperation(
            "HGETALL not implemented in Actor model".to_string(),
        ))
    }

    async fn hdel(&self, _key: &str, _fields: &[String]) -> Result<i64, StoreError> {
        Err(StoreError::InvalidOperation(
            "HDEL not implemented in Actor model".to_string(),
        ))
    }

    async fn hexists(&self, _key: &str, _field: &str) -> Result<bool, StoreError> {
        Err(StoreError::InvalidOperation(
            "HEXISTS not implemented in Actor model".to_string(),
        ))
    }

    async fn hlen(&self, _key: &str) -> Result<i64, StoreError> {
        Err(StoreError::InvalidOperation(
            "HLEN not implemented in Actor model".to_string(),
        ))
    }

    // ============================================================================
    // Generic Operations
    // ============================================================================

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, StoreError> {
        let result = self
            .execute_command(RedisCommand::Keys {
                pattern: pattern.to_string(),
            })
            .await?;

        match result {
            Value::Array(items) => {
                let keys: Vec<String> = items
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::BulkString(s) => Some(String::from_utf8_lossy(&s).to_string()),
                        _ => None,
                    })
                    .collect();
                Ok(keys)
            }
            Value::Error(e) => Err(StoreError::Internal(String::from_utf8_lossy(&e).to_string())),
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for KEYS: {:?}",
                result
            ))),
        }
    }

    async fn key_type(&self, key: &str) -> Result<Option<String>, StoreError> {
        let result = self
            .execute_command(RedisCommand::Type {
                key: key.to_string(),
            })
            .await?;

        match result {
            Value::SimpleString(s) => {
                let s_str = String::from_utf8_lossy(&s).to_string();
                if s_str == "none" {
                    Ok(None)
                } else {
                    Ok(Some(s_str))
                }
            }
            Value::Error(e) => Err(StoreError::Internal(String::from_utf8_lossy(&e).to_string())),
            _ => Err(StoreError::Internal(format!(
                "Unexpected response for TYPE: {:?}",
                result
            ))),
        }
    }
}
