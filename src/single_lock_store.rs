//! SingleLockStore: KeyValueStore implementation using Arc<RwLock<Db>>
//!
//! This is the core shared-state implementation that enables concurrent reads
//! while maintaining correctness. Multiple readers can access the database
//! simultaneously, while writers get exclusive access.

use crate::db::Db;
use crate::storage::{KeyValueStore, StoreError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Storage implementation using a single RwLock for concurrent read access
pub struct SingleLockStore {
    db: Arc<RwLock<Db>>,
}

impl SingleLockStore {
    /// Create a new SingleLockStore with an empty database
    pub fn new() -> Self {
        SingleLockStore {
            db: Arc::new(RwLock::new(Db::new())),
        }
    }

    /// Create a SingleLockStore from an existing Db instance
    pub fn from_db(db: Db) -> Self {
        SingleLockStore {
            db: Arc::new(RwLock::new(db)),
        }
    }
}

#[async_trait]
impl KeyValueStore for SingleLockStore {
    // ============================================================================
    // String Operations
    // ============================================================================

    async fn get(&self, key: &str) -> Result<Option<Bytes>, StoreError> {
        // get() now uses &self with lazy expiration
        // Use read lock for concurrent access!
        let db = self.db.read().await;
        Ok(db.get(key))
    }

    async fn set(&self, key: String, value: Bytes, px: Option<u64>) -> Result<(), StoreError> {
        let mut db = self.db.write().await;
        db.set(key, value, px);
        Ok(())
    }

    async fn incr(&self, key: &str) -> Result<i64, StoreError> {
        let mut db = self.db.write().await;

        // Get current value
        let current = match db.get(key) {
            Some(bytes) => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<i64>().map_err(|_| {
                    StoreError::InvalidOperation(
                        "ERR value is not an integer or out of range".to_string(),
                    )
                })?
            }
            None => 0,
        };

        // Increment and store
        let new_val = current + 1;
        db.set(
            key.to_string(),
            Bytes::from(new_val.to_string()),
            None,
        );

        Ok(new_val)
    }

    async fn decr(&self, key: &str) -> Result<i64, StoreError> {
        let mut db = self.db.write().await;

        // Get current value
        let current = match db.get(key) {
            Some(bytes) => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<i64>().map_err(|_| {
                    StoreError::InvalidOperation(
                        "ERR value is not an integer or out of range".to_string(),
                    )
                })?
            }
            None => 0,
        };

        // Decrement and store
        let new_val = current - 1;
        db.set(
            key.to_string(),
            Bytes::from(new_val.to_string()),
            None,
        );

        Ok(new_val)
    }

    async fn exists(&self, keys: &[String]) -> Result<i64, StoreError> {
        // key_type() now uses &self - use read lock!
        let db = self.db.read().await;
        let mut count = 0;

        for key in keys {
            // Check if key exists in any form
            if db.key_type(key) != "none" {
                count += 1;
            }
        }

        Ok(count)
    }

    async fn del(&self, _keys: &[String]) -> Result<i64, StoreError> {
        // DEL not implemented in current Db
        // Would need to add remove() method to Db
        Err(StoreError::InvalidOperation(
            "DEL not implemented in Db yet".to_string(),
        ))
    }

    // ============================================================================
    // List Operations
    // ============================================================================

    async fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError> {
        let mut db = self.db.write().await;

        db.lpush(key, values).map_err(|e| {
            if e.contains("WRONGTYPE") {
                StoreError::WrongType {
                    expected: "list",
                    actual: "unknown",
                }
            } else {
                StoreError::Internal(e)
            }
        })
    }

    async fn rpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError> {
        let mut db = self.db.write().await;

        db.rpush(key, values).map_err(|e| {
            if e.contains("WRONGTYPE") {
                StoreError::WrongType {
                    expected: "list",
                    actual: "unknown",
                }
            } else {
                StoreError::Internal(e)
            }
        })
    }

    async fn lpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError> {
        let mut db = self.db.write().await;

        db.lpop(key, count).map_err(|e| {
            if e.contains("WRONGTYPE") {
                StoreError::WrongType {
                    expected: "list",
                    actual: "unknown",
                }
            } else if e.contains("out of range") {
                StoreError::InvalidOperation(e)
            } else {
                StoreError::Internal(e)
            }
        })
    }

    async fn rpop(&self, _key: &str, _count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError> {
        // RPOP not implemented in Db
        Err(StoreError::InvalidOperation(
            "RPOP not implemented in Db yet".to_string(),
        ))
    }

    async fn llen(&self, key: &str) -> Result<i64, StoreError> {
        let db = self.db.read().await;

        db.llen(key)
            .map(|len| len as i64)
            .map_err(|e| {
                if e.contains("WRONGTYPE") {
                    StoreError::WrongType {
                        expected: "list",
                        actual: "unknown",
                    }
                } else {
                    StoreError::Internal(e)
                }
            })
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, StoreError> {
        let db = self.db.read().await;

        db.lrange(key, start, stop).map_err(|e| {
            if e.contains("WRONGTYPE") {
                StoreError::WrongType {
                    expected: "list",
                    actual: "unknown",
                }
            } else {
                StoreError::Internal(e)
            }
        })
    }

    // ============================================================================
    // Hash Operations (Not implemented in current Db)
    // ============================================================================

    async fn hset(
        &self,
        _key: String,
        _field: String,
        _value: Bytes,
    ) -> Result<bool, StoreError> {
        Err(StoreError::InvalidOperation(
            "HSET not implemented in Db yet".to_string(),
        ))
    }

    async fn hget(&self, _key: &str, _field: &str) -> Result<Option<Bytes>, StoreError> {
        Err(StoreError::InvalidOperation(
            "HGET not implemented in Db yet".to_string(),
        ))
    }

    async fn hgetall(&self, _key: &str) -> Result<Vec<(Bytes, Bytes)>, StoreError> {
        Err(StoreError::InvalidOperation(
            "HGETALL not implemented in Db yet".to_string(),
        ))
    }

    async fn hdel(&self, _key: &str, _fields: &[String]) -> Result<i64, StoreError> {
        Err(StoreError::InvalidOperation(
            "HDEL not implemented in Db yet".to_string(),
        ))
    }

    async fn hexists(&self, _key: &str, _field: &str) -> Result<bool, StoreError> {
        Err(StoreError::InvalidOperation(
            "HEXISTS not implemented in Db yet".to_string(),
        ))
    }

    async fn hlen(&self, _key: &str) -> Result<i64, StoreError> {
        Err(StoreError::InvalidOperation(
            "HLEN not implemented in Db yet".to_string(),
        ))
    }

    // ============================================================================
    // Generic Operations
    // ============================================================================

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, StoreError> {
        // keys() now uses &self - use read lock!
        let db = self.db.read().await;
        Ok(db.keys(pattern))
    }

    async fn key_type(&self, key: &str) -> Result<Option<String>, StoreError> {
        // key_type() now uses &self - use read lock!
        let db = self.db.read().await;
        let type_str = db.key_type(key);
        if type_str == "none" {
            Ok(None)
        } else {
            Ok(Some(type_str))
        }
    }
}
