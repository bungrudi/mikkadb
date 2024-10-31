use std::collections::{HashMap, BTreeMap};

use redis_starter_rust::redis::{
    core::Redis,
    commands::RedisCommand,
    replication::ReplicationManager,
};

#[cfg(test)]
fn test_redis() -> Redis {
    Redis::new_with_replication(ReplicationManager::new())
}

#[cfg(test)]
fn test_command<'a>(command: &'a str, params: &[&'a str], original_resp: &'a str) -> RedisCommand<'a> {
    let mut fixed_params = [""; 5];
    for (i, &param) in params.iter().enumerate() {
        if i < 5 {
            fixed_params[i] = param;
        }
    }
    RedisCommand::data(command, fixed_params, original_resp).unwrap()
}

#[test]
fn test_xadd_auto_sequence_zero_time() {
    let redis = test_redis();
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // When time part is 0, first sequence should be 1
    let result = redis.storage.xadd("mystream", "0-*", fields.clone());
    assert_eq!(result.unwrap(), "0-1");
}

#[test]
fn test_xadd_auto_sequence_new_time() {
    let redis = test_redis();
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // For a new time part, sequence should start at 0
    let result = redis.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result.unwrap(), "5-0");
}

#[test]
fn test_xadd_auto_sequence_increment() {
    let redis = test_redis();
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // First entry with time part 5
    let result1 = redis.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result1.unwrap(), "5-0");

    // Second entry with same time part should increment sequence
    fields.insert("bar".to_string(), "baz".to_string());
    let result2 = redis.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result2.unwrap(), "5-1");
}

#[test]
fn test_xadd_auto_sequence_multiple_time_parts() {
    let redis = test_redis();
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // Add entries with different time parts
    let result1 = redis.storage.xadd("mystream", "5-*", fields.clone());
    assert_eq!(result1.unwrap(), "5-0");

    let result2 = redis.storage.xadd("mystream", "6-*", fields.clone());
    assert_eq!(result2.unwrap(), "6-0");

    // Going back to time part 5 should fail since it's less than 6
    let result3 = redis.storage.xadd("mystream", "5-*", fields.clone());
    assert!(result3.is_err());
}

#[test]
fn test_xadd_auto_sequence_error_cases() {
    let redis = test_redis();
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    // Add first entry
    let _ = redis.storage.xadd("mystream", "5-0", fields.clone());

    // Trying to add entry with same ID should fail
    let result = redis.storage.xadd("mystream", "5-0", fields.clone());
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    );

    // Trying to add entry with lower time part should fail
    let result = redis.storage.xadd("mystream", "4-0", fields.clone());
    assert!(result.is_err());
}

#[test]
fn test_xadd_auto_generate_full_id() {
    let redis = test_redis();
    let mut fields = HashMap::new();
    fields.insert("foo".to_string(), "bar".to_string());
    
    let result = redis.storage.xadd("mystream", "*", fields.clone());
    assert!(result.is_ok());
    
    let id = result.unwrap();
    let parts: Vec<&str> = id.split('-').collect();
    assert_eq!(parts.len(), 2);
    
    // Time part should be current time
    let time_part = parts[0].parse::<u64>().unwrap();
    let current_time = redis_starter_rust::redis::storage::Storage::get_current_time_ms();
    assert!(time_part > 0 && time_part <= current_time);
    
    // Sequence should start at 0
    assert_eq!(parts[1], "0");
}

#[test]
fn test_xrange_basic() {
    let redis = test_redis();
    
    // Add test entries
    let mut fields1 = HashMap::new();
    fields1.insert("temperature".to_string(), "36".to_string());
    fields1.insert("humidity".to_string(), "95".to_string());
    
    let mut fields2 = HashMap::new();
    fields2.insert("temperature".to_string(), "37".to_string());
    fields2.insert("humidity".to_string(), "94".to_string());

    // Add entries
    let _ = redis.storage.xadd("mystream", "1526985054069-0", fields1);
    let _ = redis.storage.xadd("mystream", "1526985054079-0", fields2);

    // Test XRANGE
    let result = redis.storage.xrange("mystream", "1526985054069-0", "1526985054079-0").unwrap();
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
fn test_xrange_empty_stream() {
    let redis = test_redis();
    
    // Test XRANGE on non-existent stream
    let result = redis.storage.xrange("nonexistent", "0-0", "9999999999999-0").unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_xrange_wrong_type() {
    let redis = test_redis();
    
    // Set a string value
    redis.storage.set("string_key", "some_value", None);

    // Try XRANGE on string key
    let result = redis.storage.xrange("string_key", "0-0", "9999999999999-0");
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "ERR WRONGTYPE Operation against a key holding the wrong kind of value"
    );
}

#[test]
fn test_xrange_partial_range() {
    let redis = test_redis();
    
    // Add test entries with different timestamps
    let entries = vec![
        ("1000-0", "1"),
        ("2000-0", "2"),
        ("3000-0", "3"),
        ("4000-0", "4"),
    ];

    for (id, value) in entries {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), value.to_string());
        let _ = redis.storage.xadd("stream", id, fields);
    }

    // Query middle range
    let result = redis.storage.xrange("stream", "2000-0", "3000-0").unwrap();
    assert_eq!(result.len(), 2);
    
    // Verify entries
    assert_eq!(result[0].id, "2000-0");
    assert_eq!(result[0].fields.get("value").unwrap(), "2");
    assert_eq!(result[1].id, "3000-0");
    assert_eq!(result[1].fields.get("value").unwrap(), "3");
}

#[test]
fn test_xrange_resp_format() {
    let mut redis = test_redis();
    
    // Add test entries with ordered fields
    let mut fields1 = BTreeMap::new();
    fields1.insert("humidity".to_string(), "95".to_string());
    fields1.insert("temperature".to_string(), "36".to_string());
    
    let mut fields2 = BTreeMap::new();
    fields2.insert("humidity".to_string(), "94".to_string());
    fields2.insert("temperature".to_string(), "37".to_string());

    // Add entries
    let _ = redis.storage.xadd("mystream", "1526985054069-0", fields1.into_iter().collect());
    let _ = redis.storage.xadd("mystream", "1526985054079-0", fields2.into_iter().collect());

    // Test XRANGE with RESP formatting
    let xrange = test_command(
        "XRANGE",
        &["mystream", "1526985054069-0", "1526985054079-0"],
        ""
    );

    let result = redis.execute_command(&xrange, None).unwrap();
    
    let expected = "*2\r\n\
                   *2\r\n\
                   $15\r\n1526985054069-0\r\n\
                   *4\r\n\
                   $8\r\nhumidity\r\n\
                   $2\r\n95\r\n\
                   $11\r\ntemperature\r\n\
                   $2\r\n36\r\n\
                   *2\r\n\
                   $15\r\n1526985054079-0\r\n\
                   *4\r\n\
                   $8\r\nhumidity\r\n\
                   $2\r\n94\r\n\
                   $11\r\ntemperature\r\n\
                   $2\r\n37\r\n";

    assert_eq!(result, expected);
}
