use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use redis_starter_rust::redis::{
    core::Redis,
    storage::Storage,
    config::RedisConfig,
};

// Stream ID Generation Tests
#[test]
fn test_xadd_auto_sequence_zero_time() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // When time part is 0, first sequence should be 1
    let redis_guard = redis.lock().unwrap();
    let result = redis_guard.storage.xadd("mystream", "0-*", fields.clone());
    assert_eq!(result.unwrap(), "0-1");
}

#[test]
fn test_xadd_auto_sequence_new_time() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // For a new time part, sequence should start at 0
    let redis_guard = redis.lock().unwrap();
    let result = redis_guard.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result.unwrap(), "5-0");
}

#[test]
fn test_xadd_auto_sequence_increment() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // First entry with time part 5
    let redis_guard = redis.lock().unwrap();
    let result1 = redis_guard.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result1.unwrap(), "5-0");

    // Second entry with same time part should increment sequence
    fields.insert("bar".to_string(), "baz".to_string());
    let result2 = redis_guard.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result2.unwrap(), "5-1");
}

#[test]
fn test_xadd_auto_sequence_multiple_time_parts() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // Add entries with different time parts
    let redis_guard = redis.lock().unwrap();
    let result1 = redis_guard.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result1.unwrap(), "5-0");

    let result2 = redis_guard.storage.xadd("mystream", "6-*", fields.clone());
    assert_eq!(result2.unwrap(), "6-0");

    // Going back to time part 5 should fail since it's less than 6
    let result3 = redis_guard.storage.xadd("mystream", "5-*", fields.clone());
    assert!(result3.is_err());
}

#[test]
fn test_xadd_auto_generate_full_id() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    let redis_guard = redis.lock().unwrap();
    let result = redis_guard.storage.xadd("mystream", "*", fields.clone());
    assert!(result.is_ok());
    
    let id = result.unwrap();
    let parts: Vec<&str> = id.split('-').collect();
    assert_eq!(parts.len(), 2);
    
    // Time part should be current time
    let time_part = parts[0].parse::<u64>().unwrap();
    let current_time = Storage::get_current_time_ms();
    assert!(time_part > 0 && time_part <= current_time);
    
    // Sequence should start at 0
    assert_eq!(parts[1], "0");
}

#[test]
fn test_xrange_basic() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let redis_guard = redis.lock().unwrap();
    
    // Add test entries
    let mut fields1 = HashMap::new();
    fields1.insert("temperature".to_string(), "36".to_string());
    fields1.insert("humidity".to_string(), "95".to_string());
    
    let mut fields2 = HashMap::new();
    fields2.insert("temperature".to_string(), "37".to_string());
    fields2.insert("humidity".to_string(), "94".to_string());

    // Add entries
    let _ = redis_guard.storage.xadd("mystream", "1526985054069-0", fields1);
    let _ = redis_guard.storage.xadd("mystream", "1526985054079-0", fields2);

    // Test XRANGE
    let result = redis_guard.storage.xrange("mystream", "1526985054069-0", "1526985054079-0").unwrap();
    assert_eq!(result.len(), 2);
    
    // Verify first entry
    assert_eq!(result[0].id, "1526985054069-0");
    assert_eq!(result[0].fields.get("temperature").unwrap(), "36");
    assert_eq!(result[0].fields.get("humidity").unwrap(), "95");
    
    // Verify second entry
    assert_eq!(result[1].id, "1526985054079-0");
    assert_eq!(result[1].fields.get("temperature").unwrap(), "37");
    assert_eq!(result[1].fields.get("humidity").unwrap(), "94");
}

#[test]
fn test_xrange_with_minus() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let redis_guard = redis.lock().unwrap();
    
    // Add test entries
    let entries = vec![
        ("1000-0", "1"),
        ("2000-0", "2"),
        ("3000-0", "3"),
    ];

    for (id, value) in entries {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), value.to_string());
        let _ = redis_guard.storage.xadd("stream", id, fields);
    }

    // Query from beginning to specific ID
    let result = redis_guard.storage.xrange("stream", "-", "2000-0").unwrap();
    assert_eq!(result.len(), 2);
    
    // Verify entries
    assert_eq!(result[0].id, "1000-0");
    assert_eq!(result[0].fields.get("value").unwrap(), "1");
    assert_eq!(result[1].id, "2000-0");
    assert_eq!(result[1].fields.get("value").unwrap(), "2");
}

#[test]
fn test_xrange_with_plus() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let redis_guard = redis.lock().unwrap();
    
    // Add test entries
    let entries = vec![
        ("1000-0", "1"),
        ("2000-0", "2"),
        ("3000-0", "3"),
    ];

    for (id, value) in entries {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), value.to_string());
        let _ = redis_guard.storage.xadd("stream", id, fields);
    }

    // Query from specific ID to end
    let result = redis_guard.storage.xrange("stream", "2000-0", "+").unwrap();
    assert_eq!(result.len(), 2);
    
    // Verify entries
    assert_eq!(result[0].id, "2000-0");
    assert_eq!(result[0].fields.get("value").unwrap(), "2");
    assert_eq!(result[1].id, "3000-0");
    assert_eq!(result[1].fields.get("value").unwrap(), "3");
}

#[test]
fn test_xadd_explicit_id() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("sensor".to_string(), "1".to_string());
    
    let redis_guard = redis.lock().unwrap();
    
    // Add with explicit ID
    let result = redis_guard.storage.xadd("mystream", "1526919030474-55", fields.clone());
    assert_eq!(result.unwrap(), "1526919030474-55");
    
    // Verify can't add lower ID
    let result = redis_guard.storage.xadd("mystream", "1526919030474-54", fields.clone());
    assert!(result.is_err());
    
    // Can add higher sequence number
    let result = redis_guard.storage.xadd("mystream", "1526919030474-56", fields.clone());
    assert_eq!(result.unwrap(), "1526919030474-56");
}

#[test]
#[should_panic]
fn test_xadd_invalid_id_format() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    let redis_guard = redis.lock().unwrap();
    
    // Test invalid ID format
    let _ = redis_guard.storage.xadd("mystream", "invalid-id", fields.clone());
}

#[test]
#[should_panic]
fn test_xadd_invalid_timestamp() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    let redis_guard = redis.lock().unwrap();
    
    // Test invalid millisecond timestamp
    let _ = redis_guard.storage.xadd("mystream", "xyz-0", fields.clone());
}

#[test]
#[should_panic]
fn test_xadd_invalid_sequence() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    let redis_guard = redis.lock().unwrap();
    
    // Test invalid sequence number
    let _ = redis_guard.storage.xadd("mystream", "1526919030474-xyz", fields.clone());
}

#[test]
fn test_xrange_nonexistent_stream() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let redis_guard = redis.lock().unwrap();
    
    // Test range query on non-existent stream
    let result = redis_guard.storage.xrange("nonexistent", "-", "+").unwrap_or(Vec::new());
    assert_eq!(result.len(), 0);
}