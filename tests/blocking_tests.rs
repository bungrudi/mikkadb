use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use redis_starter_rust::redis::commands::RedisCommand;
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::redis::config::RedisConfig;

#[test]
fn test_xread_blocking_timeout() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let redis_clone = Arc::clone(&redis);

    // Start a thread that will add data after a delay
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let mut fields = std::collections::HashMap::new();
        fields.insert("field1".to_string(), "value1".to_string());
        
        let mut redis = redis_clone.lock().unwrap();
        redis.xadd("mystream", "*", fields).unwrap();
    });

    // Execute blocking XREAD with timeout
    let mut redis = redis.lock().unwrap();
    let command = RedisCommand::XRead {
        keys: vec!["mystream"],
        ids: vec!["0-0"],
        block: Some(1000), // 1 second timeout
    };

    let result = redis.execute_command(&command, None).unwrap();
    assert!(result.contains("value1")); // Verify we got the data
}

#[test]
fn test_xread_blocking_timeout_expired() {
    let mut redis = Redis::new(RedisConfig::default());
    
    // Execute blocking XREAD with a short timeout
    let command = RedisCommand::XRead {
        keys: vec!["mystream"],
        ids: vec!["0-0"],
        block: Some(100), // 100ms timeout
    };

    let result = redis.execute_command(&command, None).unwrap();
    assert_eq!(result, "*0\r\n"); // Should return empty array when timeout expires
}

#[test]
fn test_xread_non_blocking_empty() {
    let mut redis = Redis::new(RedisConfig::default());
    
    // Execute non-blocking XREAD
    let command = RedisCommand::XRead {
        keys: vec!["mystream"],
        ids: vec!["0-0"],
        block: None,
    };

    let result = redis.execute_command(&command, None).unwrap();
    assert_eq!(result, "*0\r\n"); // Should return empty array immediately
}
