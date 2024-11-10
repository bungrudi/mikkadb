use std::collections::{HashMap, BTreeMap};
use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};

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
fn test_incr_numeric_value() {
    let mut redis = test_redis();
    
    // Set initial value
    redis.storage.set("counter", "41", None);

    // Test INCR command
    let incr = test_command("INCR", &["counter"], "");
    let result = redis.execute_command(&incr, None).unwrap();
    assert_eq!(result, ":42\r\n");

    // Verify the value was actually incremented
    let get = test_command("GET", &["counter"], "");
    let result = redis.execute_command(&get, None).unwrap();
    assert_eq!(result, "$2\r\n42\r\n");
}

#[test]
fn test_incr_non_numeric() {
    let mut redis = test_redis();
    
    // Set non-numeric value
    redis.storage.set("key", "abc", None);

    // Test INCR command
    let incr = test_command("INCR", &["key"], "");
    let result = redis.execute_command(&incr, None);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "-ERR value is not an integer or out of range\r\n");
}

#[test]
fn test_incr_non_existent() {
    let mut redis = test_redis();
    
    // Test INCR command on non-existent key
    let incr = test_command("INCR", &["nonexistent"], "");
    let result = redis.execute_command(&incr, None);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "-ERR key does not exist\r\n");
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
fn test_xread_dollar_id() {
    let redis = test_redis();
    
    // Add initial entries
    let mut fields1 = HashMap::new();
    fields1.insert("sensor".to_string(), "1".to_string());
    fields1.insert("value".to_string(), "100".to_string());
    let _ = redis.storage.xadd("mystream", "1000-0", fields1);

    let mut fields2 = HashMap::new();
    fields2.insert("sensor".to_string(), "2".to_string());
    fields2.insert("value".to_string(), "200".to_string());
    let _ = redis.storage.xadd("mystream", "2000-0", fields2);

    // First XREAD with $ should return no entries since we want only new ones
    let result = redis.storage.xread(&["mystream"], &["$"], None).unwrap();
    assert!(result.is_empty(), "Expected no entries for initial $ read");

    // Add a new entry after the $ read
    let mut fields3 = HashMap::new();
    fields3.insert("sensor".to_string(), "3".to_string());
    fields3.insert("value".to_string(), "300".to_string());
    let _ = redis.storage.xadd("mystream", "3000-0", fields3);

    // Second XREAD with $ should return only the new entry
    let result = redis.storage.xread(&["mystream"], &["$"], None).unwrap();
    assert_eq!(result.len(), 1, "Expected one stream in results");
    
    let (stream_name, entries) = &result[0];
    assert_eq!(stream_name, "mystream");
    assert_eq!(entries.len(), 1, "Expected one entry");
    
    let entry = &entries[0];
    assert_eq!(entry.id, "3000-0");
    assert_eq!(entry.fields.get("sensor").unwrap(), "3");
    assert_eq!(entry.fields.get("value").unwrap(), "300");
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
fn test_xrange_with_minus() {
    let redis = test_redis();
    
    // Add test entries
    let entries = vec![
        ("1000-0", "1"),
        ("2000-0", "2"),
        ("3000-0", "3"),
    ];

    for (id, value) in entries {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), value.to_string());
        let _ = redis.storage.xadd("stream", id, fields);
    }

    // Query from beginning to specific ID
    let result = redis.storage.xrange("stream", "-", "2000-0").unwrap();
    assert_eq!(result.len(), 2);
    
    // Verify entries
    assert_eq!(result[0].id, "1000-0");
    assert_eq!(result[0].fields.get("value").unwrap(), "1");
    assert_eq!(result[1].id, "2000-0");
    assert_eq!(result[1].fields.get("value").unwrap(), "2");
}

#[test]
fn test_xrange_with_plus() {
    let redis = test_redis();
    
    // Add test entries
    let entries = vec![
        ("1000-0", "1"),
        ("2000-0", "2"),
        ("3000-0", "3"),
    ];

    for (id, value) in entries {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), value.to_string());
        let _ = redis.storage.xadd("stream", id, fields);
    }

    // Query from specific ID to end
    let result = redis.storage.xrange("stream", "2000-0", "+").unwrap();
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

#[test]
fn test_xread_basic() {
    let mut redis = test_redis();
    
    // Add test entry
    let mut fields = BTreeMap::new();
    fields.insert("temperature".to_string(), "96".to_string());
    let _ = redis.storage.xadd("stream_key", "1-1", fields.into_iter().collect());

    // Test XREAD command
    let xread = test_command(
        "XREAD",
        &["streams", "stream_key", "1-0"],
        ""
    );

    let result = redis.execute_command(&xread, None).unwrap();
    
    let expected = "*1\r\n\
                   *2\r\n\
                   $10\r\nstream_key\r\n\
                   *1\r\n\
                   *2\r\n\
                   $3\r\n1-1\r\n\
                   *2\r\n\
                   $11\r\ntemperature\r\n\
                   $2\r\n96\r\n";

    assert_eq!(result, expected);
}

#[test]
fn test_xread_exclusive() {
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

    // Test XREAD - should only return entries after the given ID
    let xread = test_command(
        "XREAD",
        &["streams", "mystream", "1526985054069-0"],
        ""
    );

    let result = redis.execute_command(&xread, None).unwrap();
    
    let expected = "*1\r\n\
                   *2\r\n\
                   $8\r\nmystream\r\n\
                   *1\r\n\
                   *2\r\n\
                   $15\r\n1526985054079-0\r\n\
                   *4\r\n\
                   $8\r\nhumidity\r\n\
                   $2\r\n94\r\n\
                   $11\r\ntemperature\r\n\
                   $2\r\n37\r\n";

    assert_eq!(result, expected);
}

#[test]
fn test_xread_empty_stream() {
    let mut redis = test_redis();
    
    // Test XREAD on non-existent stream
    let xread = test_command(
        "XREAD",
        &["streams", "nonexistent", "1-0"],
        ""
    );

    let result = redis.execute_command(&xread, None).unwrap();
    assert_eq!(result, "$-1\r\n");
}

#[test]
fn test_xread_wrong_type() {
    let mut redis = test_redis();
    
    // Set a string value
    redis.storage.set("string_key", "some_value", None);

    // Try XREAD on string key
    let xread = test_command(
        "XREAD",
        &["streams", "string_key", "1-0"],
        ""
    );

    let result = redis.execute_command(&xread, None);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );
}

#[test]
fn test_xread_invalid_id() {
    let mut redis = test_redis();
    
    // Try XREAD with invalid ID format
    let xread = test_command(
        "XREAD",
        &["streams", "mystream", "invalid-id"],
        ""
    );

    let result = redis.execute_command(&xread, None);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "-ERR Invalid milliseconds in stream ID\r\n"
    );
}

#[test]
fn test_xread_missing_parameters() {
    let mut redis = test_redis();
    
    // Try XREAD without required parameters
    let xread = test_command(
        "XREAD",
        &["streams"],
        ""
    );

    let result = redis.execute_command(&xread, None);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "-ERR wrong number of arguments for 'xread' command\r\n"
    );
}

#[test]
fn test_xread_multiple_streams() {
    let mut redis = test_redis();
    
    // Add entries to two different streams
    let mut fields1 = BTreeMap::new();
    fields1.insert("temperature".to_string(), "95".to_string());
    let _ = redis.storage.xadd("stream_key", "0-1", fields1.into_iter().collect());

    let mut fields2 = BTreeMap::new();
    fields2.insert("humidity".to_string(), "97".to_string());
    let _ = redis.storage.xadd("other_stream_key", "0-2", fields2.into_iter().collect());

    // Test XREAD with multiple streams
    let xread = test_command(
        "XREAD",
        &["streams", "stream_key", "other_stream_key", "0-0", "0-1"],
        ""
    );

    let result = redis.execute_command(&xread, None).unwrap();
    
    let expected = "*2\r\n\
                   *2\r\n\
                   $10\r\nstream_key\r\n\
                   *1\r\n\
                   *2\r\n\
                   $3\r\n0-1\r\n\
                   *2\r\n\
                   $11\r\ntemperature\r\n\
                   $2\r\n95\r\n\
                   *2\r\n\
                   $16\r\nother_stream_key\r\n\
                   *1\r\n\
                   *2\r\n\
                   $3\r\n0-2\r\n\
                   *2\r\n\
                   $8\r\nhumidity\r\n\
                   $2\r\n97\r\n";

    assert_eq!(result, expected);
}

#[test]
fn test_xread_block_zero() {
    let redis = Arc::new(Mutex::new(test_redis()));
    
    // Create a thread that will add an entry after a delay
    let redis_clone = Arc::clone(&redis);
    let handle = thread::spawn(move || {
        // Wait a bit before adding the entry
        thread::sleep(Duration::from_millis(500));
        
        // Add test entry
        let mut fields = BTreeMap::new();
        fields.insert("temperature".to_string(), "95".to_string());
        let redis = redis_clone.lock().unwrap();
        redis.storage.xadd("stream_key", "0-2", fields.into_iter().collect()).unwrap();
    });

    // Test XREAD command with BLOCK 0
    let xread = test_command(
        "XREAD",
        &["BLOCK", "0", "STREAMS", "stream_key", "0-0"],
        ""
    );

    // Keep retrying until we get a result
    let mut result = redis.lock().unwrap().execute_command(&xread, None);
    while let Err(ref e) = result {
        if e.starts_with("-XREAD_RETRY") {
            thread::sleep(Duration::from_millis(50));
            result = redis.lock().unwrap().execute_command(&xread, None);
        } else {
            panic!("Unexpected error: {}", e);
        }
    }

    let result = result.unwrap();
    
    // Wait for the thread to complete
    handle.join().unwrap();
    
    let expected = "*1\r\n\
                   *2\r\n\
                   $10\r\nstream_key\r\n\
                   *1\r\n\
                   *2\r\n\
                   $3\r\n0-2\r\n\
                   *2\r\n\
                   $11\r\ntemperature\r\n\
                   $2\r\n95\r\n";

    assert_eq!(result, expected);
}
