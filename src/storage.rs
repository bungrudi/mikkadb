//! Storage abstraction layer for MikkaDB
//!
//! This module defines the core storage traits and types that enable
//! different backend implementations (Actor model, RwLock, DashMap, etc.)
//! to be swapped transparently.

use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;

/// Errors that can occur during storage operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// The key exists but holds a value of the wrong type
    WrongType {
        expected: &'static str,
        actual: &'static str,
    },
    /// The key does not exist
    KeyNotFound,
    /// Invalid operation (e.g., INCR on non-integer)
    InvalidOperation(String),
    /// Internal storage error
    Internal(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::WrongType { expected, actual } => {
                write!(
                    f,
                    "WRONGTYPE Operation against a key holding the wrong kind of value. Expected: {}, Actual: {}",
                    expected, actual
                )
            }
            StoreError::KeyNotFound => write!(f, "Key not found"),
            StoreError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            StoreError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for StoreError {}

/// Core storage abstraction that all backends must implement.
///
/// All methods take `&self` (not `&mut self`) to enable concurrent access
/// via interior mutability patterns (RwLock, DashMap, etc.).
///
/// Phase 1 focuses on core types: String, List, Hash
#[async_trait]
pub trait KeyValueStore: Send + Sync {
    // ============================================================================
    // String Operations
    // ============================================================================

    /// Get the value of a key
    async fn get(&self, key: &str) -> Result<Option<Bytes>, StoreError>;

    /// Set the value of a key with optional expiration in milliseconds
    async fn set(&self, key: String, value: Bytes, px: Option<u64>) -> Result<(), StoreError>;

    /// Increment the integer value of a key by 1
    async fn incr(&self, key: &str) -> Result<i64, StoreError>;

    /// Decrement the integer value of a key by 1
    async fn decr(&self, key: &str) -> Result<i64, StoreError>;

    /// Check if a key exists
    async fn exists(&self, keys: &[String]) -> Result<i64, StoreError>;

    /// Delete one or more keys
    async fn del(&self, keys: &[String]) -> Result<i64, StoreError>;

    // ============================================================================
    // List Operations
    // ============================================================================

    /// Push one or more values to the head of a list
    async fn lpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError>;

    /// Push one or more values to the tail of a list
    async fn rpush(&self, key: String, values: Vec<Bytes>) -> Result<usize, StoreError>;

    /// Pop one or more elements from the head of a list
    async fn lpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError>;

    /// Pop one or more elements from the tail of a list
    async fn rpop(&self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, StoreError>;

    /// Get the length of a list
    async fn llen(&self, key: &str) -> Result<i64, StoreError>;

    /// Get a range of elements from a list
    async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, StoreError>;

    // ============================================================================
    // Hash Operations
    // ============================================================================

    /// Set the value of a hash field
    async fn hset(
        &self,
        key: String,
        field: String,
        value: Bytes,
    ) -> Result<bool, StoreError>;

    /// Get the value of a hash field
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, StoreError>;

    /// Get all fields and values in a hash
    async fn hgetall(&self, key: &str) -> Result<Vec<(Bytes, Bytes)>, StoreError>;

    /// Delete one or more hash fields
    async fn hdel(&self, key: &str, fields: &[String]) -> Result<i64, StoreError>;

    /// Check if a hash field exists
    async fn hexists(&self, key: &str, field: &str) -> Result<bool, StoreError>;

    /// Get the number of fields in a hash
    async fn hlen(&self, key: &str) -> Result<i64, StoreError>;

    // ============================================================================
    // Generic Operations
    // ============================================================================

    /// Get all keys matching a pattern (supports * wildcard)
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, StoreError>;

    /// Get the type of a key
    async fn key_type(&self, key: &str) -> Result<Option<String>, StoreError>;
}
