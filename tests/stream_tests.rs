use std::collections::{HashMap, BTreeMap};
use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};

use redis_starter_rust::redis::{
    core::Redis,
    commands::RedisCommand,
    replication::ReplicationManager,
    xread_handler::{XReadHandler, XReadRequest},
    storage::StreamEntry,
};

#[cfg(test)]
fn test_redis() -> Redis {
    Redis::new_with_replication(ReplicationManager::new())
}

#[cfg(test)]
fn test_command(command: &str, params: &[&str], original_resp: &str) -> RedisCommand {
    // Create array with default strings
    let mut fixed_params = [
        String::new(),
        String::new(),
        String::new(), 
        String::new(),
        String::new()
    ];

    // Copy parameters into the array
    for (i, &param) in params.iter().enumerate() {
        if i < 5 {
            fixed_params[i] = param.to_string();
        }
    }

    RedisCommand::data(command.to_string(), &fixed_params, original_resp.to_string()).unwrap()
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
    assert_eq!(result.unwrap_err(), "ERR The ID specified in XADD is equal or smaller than the target stream top item");

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
    assert_eq!(result.unwrap_err(), "ERR WRONGTYPE Operation against a key holding the wrong kind of value");
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
    let result = redis.storage.xread_with_options(&["mystream"], &["$"], None, None).unwrap();
    assert!(result.is_empty(), "Expected no entries for initial $ read");

    // Add a new entry after the $ read
    let mut fields3 = HashMap::new();
    fields3.insert("sensor".to_string(), "3".to_string());
    fields3.insert("value".to_string(), "300".to_string());
    let _ = redis.storage.xadd("mystream", "3000-0", fields3);

    // Second XREAD with $ should return only the new entry
    let result = redis.storage.xread_with_options(&["mystream"], &["$"], None, None).unwrap();
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
    let redis = test_redis();
    
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
    let redis = test_redis();
    
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
    let redis = test_redis();
    
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
    let redis = test_redis();
    
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
    assert_eq!(result.unwrap_err(), "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
}

#[test]
fn test_xread_invalid_id() {
    let redis = test_redis();
    
    // Try XREAD with invalid ID format
    let xread = test_command(
        "XREAD",
        &["streams", "mystream", "invalid-id"],
        ""
    );

    let result = redis.execute_command(&xread, None);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "-ERR Invalid milliseconds in stream ID\r\n");
}

#[test]
fn test_xread_missing_parameters() {
    let redis = test_redis();
    
    // Try XREAD without required parameters
    let xread = test_command(
        "XREAD",
        &["streams"],
        ""
    );

    let result = redis.execute_command(&xread, None);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "-ERR wrong number of arguments for 'xread' command\r\n");
}

#[test]
fn test_xread_multiple_streams() {
    let redis = test_redis();
    
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
fn test_incr_nonexistent_key() {
    let redis = test_redis();
    
    // Test INCR command on non-existent key
    let incr = test_command("INCR", &["nonexistent"], "");
    let result = redis.execute_command(&incr, None).unwrap();
    assert_eq!(result, ":1\r\n");
    
    // Verify the key was created with value "1"
    let get = test_command("GET", &["nonexistent"], "");
    let result = redis.execute_command(&get, None).unwrap();
    assert_eq!(result, "$1\r\n1\r\n");
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
        if e.starts_with("XREAD_RETRY") {
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

#[test]
fn test_xread_with_count() {
    let redis = test_redis();

    // Add test entries
    let mut fields1 = HashMap::new();
    fields1.insert("sensor".to_string(), "1".to_string());
    fields1.insert("value".to_string(), "100".to_string());
    let _ = redis.storage.xadd("mystream", "1000-0", fields1);

    let mut fields2 = HashMap::new();
    fields2.insert("sensor".to_string(), "2".to_string());
    fields2.insert("value".to_string(), "200".to_string());
    let _ = redis.storage.xadd("mystream", "2000-0", fields2);

    // Test XREAD with COUNT parameter
    let result = redis.storage.xread_with_options(&["mystream"], &["0"], None, Some(1)).unwrap();
    assert_eq!(result.len(), 1, "Expected one stream in results");

    let (stream_name, entries) = &result[0];
    assert_eq!(stream_name, "mystream");
    assert_eq!(entries.len(), 1, "COUNT parameter should limit to 1 entry");

    let entry = &entries[0];
    assert_eq!(entry.id, "1000-0");
    assert_eq!(entry.fields.get("sensor").unwrap(), "1");
    assert_eq!(entry.fields.get("value").unwrap(), "100");
}

#[test]
fn test_xread_with_count_and_block() {
    let mut redis = test_redis();

    // Add test entries
    let mut fields1 = HashMap::new();
    fields1.insert("name".to_string(), "test1".to_string());
    let _ = redis.storage.xadd("mystream", "1000-0", fields1);

    let mut fields2 = HashMap::new();
    fields2.insert("name".to_string(), "test2".to_string());
    let _ = redis.storage.xadd("mystream", "2000-0", fields2);

    // Test XREAD command with both COUNT and BLOCK parameters
    let xread = test_command(
        "XREAD",
        &["COUNT", "1", "BLOCK", "0", "STREAMS", "mystream", "0-0"],
        ""
    );

    let result = redis.execute_command(&xread, None).unwrap();

    // Verify we get exactly one entry
    assert!(result.contains("*1\r\n")); // One stream
    assert!(result.contains("$8\r\nmystream\r\n")); // Stream name
    assert!(result.contains("1000-0")); // First entry ID
    assert!(result.contains("test1")); // First entry value
    assert!(!result.contains("2000-0")); // Should not contain second entry
    assert!(!result.contains("test2")); // Should not contain second entry value
}

#[test]
fn test_xread_count_parameter_parsing() {
    let mut redis = test_redis();
    
    // Add test entries
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), "first".to_string());
    let _ = redis.storage.xadd("mystream", "1000-0", fields.clone());

    fields.insert("name".to_string(), "second".to_string());
    let _ = redis.storage.xadd("mystream", "2000-0", fields.clone());

    fields.insert("name".to_string(), "third".to_string());
    let _ = redis.storage.xadd("mystream", "3000-0", fields.clone());

    // Test XREAD with COUNT before STREAMS (proper Redis spec)
    let xread = test_command("XREAD", &["COUNT", "2", "STREAMS", "mystream", "0"], "");
    let result = redis.execute_command(&xread, None).unwrap();
    
    // Should return only first 2 entries
    let entry_count = count_xread_entries(&result);
    assert_eq!(entry_count, 2);
}

#[test]
fn test_xread_multiple_streams() {
    let mut redis = test_redis();
    
    // Add entries to first stream
    let mut fields1 = HashMap::new();
    fields1.insert("name".to_string(), "alice".to_string());
    let _ = redis.storage.xadd("stream1", "1000-0", fields1.clone());
    
    fields1.insert("name".to_string(), "bob".to_string());
    let _ = redis.storage.xadd("stream1", "2000-0", fields1.clone());

    // Add entries to second stream
    let mut fields2 = HashMap::new();
    fields2.insert("temp".to_string(), "25".to_string());
    let _ = redis.storage.xadd("stream2", "1500-0", fields2.clone());
    
    fields2.insert("temp".to_string(), "30".to_string());
    let _ = redis.storage.xadd("stream2", "2500-0", fields2.clone());

    // Test XREAD with multiple streams
    let xread = test_command("XREAD", &["STREAMS", "stream1", "stream2", "0", "0"], "");
    let result = redis.execute_command(&xread, None).unwrap();
    
    // Verify response format and content
    assert!(result.contains("stream1"));
    assert!(result.contains("stream2"));
    assert!(result.contains("alice"));
    assert!(result.contains("bob"));
    assert!(result.contains("25"));
    assert!(result.contains("30"));
}

#[test]
fn test_xread_multiple_streams_with_different_ids() {
    let mut redis = test_redis();
    
    // Add entries to first stream
    let mut fields1 = HashMap::new();
    fields1.insert("name".to_string(), "alice".to_string());
    let _ = redis.storage.xadd("stream1", "1000-0", fields1.clone());
    
    fields1.insert("name".to_string(), "bob".to_string());
    let _ = redis.storage.xadd("stream1", "2000-0", fields1.clone());

    // Add entries to second stream
    let mut fields2 = HashMap::new();
    fields2.insert("temp".to_string(), "25".to_string());
    let _ = redis.storage.xadd("stream2", "1500-0", fields2.clone());
    
    fields2.insert("temp".to_string(), "30".to_string());
    let _ = redis.storage.xadd("stream2", "2500-0", fields2.clone());

    // Test XREAD with different IDs for each stream
    let xread = test_command("XREAD", &["STREAMS", "stream1", "stream2", "1500-0", "2000-0"], "");
    let result = redis.execute_command(&xread, None).unwrap();
    
    // Should only return entries after specified IDs
    assert!(!result.contains("alice")); // Before 1500-0
    assert!(result.contains("bob")); // After 1500-0
    assert!(!result.contains("25")); // Before 2000-0
    assert!(result.contains("30")); // After 2000-0
}

// Helper function to parse the RESP response and count the entries
fn count_xread_entries(resp: &str) -> usize {
    let mut lines = resp.lines();
    let mut entry_count = 0;

    // Skip the initial array size and stream info
    if lines.next() == Some("*1") {
        lines.next(); // *2
        lines.next(); // $<len>
        lines.next(); // stream name
        if let Some(num_entries_line) = lines.next() {
            if num_entries_line.starts_with('*') {
                // Parse the number of entries
                let num_entries = num_entries_line[1..].parse::<usize>().unwrap_or(0);
                entry_count = num_entries;
            }
        }
    }

    entry_count
}

// XREAD Parser Tests
mod xread_parser_tests {
    use super::*;
    use redis_starter_rust::redis::xread_parser::parse_xread;

    #[test]
    fn test_parse_basic_xread() {
        // Basic XREAD STREAMS key id
        let params = vec![
            "STREAMS".to_string(),
            "mystream".to_string(),
            "0-0".to_string(),
        ];
        let result = parse_xread(&params).unwrap();
        assert_eq!(result.keys, vec!["mystream"]);
        assert_eq!(result.ids, vec!["0-0"]);
        assert_eq!(result.block, None);
        assert_eq!(result.count, None);
    }

    #[test]
    fn test_parse_xread_with_count() {
        // COUNT must come before STREAMS
        let params = vec![
            "COUNT".to_string(),
            "2".to_string(),
            "STREAMS".to_string(),
            "mystream".to_string(),
            "0-0".to_string(),
        ];
        let result = parse_xread(&params).unwrap();
        assert_eq!(result.keys, vec!["mystream"]);
        assert_eq!(result.ids, vec!["0-0"]);
        assert_eq!(result.count, Some(2));
    }

    #[test]
    fn test_parse_xread_with_block() {
        // BLOCK must come before STREAMS
        let params = vec![
            "BLOCK".to_string(),
            "0".to_string(),
            "STREAMS".to_string(),
            "mystream".to_string(),
            "0-0".to_string(),
        ];
        let result = parse_xread(&params).unwrap();
        assert_eq!(result.keys, vec!["mystream"]);
        assert_eq!(result.ids, vec!["0-0"]);
        assert_eq!(result.block, Some(0));
    }

    #[test]
    fn test_parse_xread_with_block_and_count() {
        // Both BLOCK and COUNT must come before STREAMS
        let params = vec![
            "COUNT".to_string(),
            "5".to_string(),
            "BLOCK".to_string(),
            "1000".to_string(),
            "STREAMS".to_string(),
            "mystream".to_string(),
            "0-0".to_string(),
        ];
        let result = parse_xread(&params).unwrap();
        assert_eq!(result.keys, vec!["mystream"]);
        assert_eq!(result.ids, vec!["0-0"]);
        assert_eq!(result.block, Some(1000));
        assert_eq!(result.count, Some(5));
    }

    #[test]
    fn test_parse_xread_with_special_ids() {
        // Test with $ (special ID meaning "latest ID")
        let params = vec![
            "STREAMS".to_string(),
            "mystream".to_string(),
            "$".to_string(),
        ];
        let result = parse_xread(&params).unwrap();
        assert_eq!(result.ids, vec!["$"]);

        // Test with standard ID format (ms-seq)
        let params = vec![
            "STREAMS".to_string(),
            "mystream".to_string(),
            "1000-0".to_string(),
        ];
        let result = parse_xread(&params).unwrap();
        assert_eq!(result.ids, vec!["1000-0"]);
    }

    #[test]
    fn test_parse_xread_multiple_streams() {
        // Multiple streams must have matching number of IDs
        let params = vec![
            "STREAMS".to_string(),
            "stream1".to_string(),
            "stream2".to_string(),
            "1000-0".to_string(),
            "2000-0".to_string(),
        ];
        let result = parse_xread(&params).unwrap();
        assert_eq!(result.keys, vec!["stream1", "stream2"]);
        assert_eq!(result.ids, vec!["1000-0", "2000-0"]);
    }

    #[test]
    fn test_parse_xread_error_cases() {
        // Missing STREAMS keyword
        let params = vec!["mystream".to_string(), "0-0".to_string()];
        let result = parse_xread(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "-ERR Missing 'STREAMS' keyword\r\n");

        // Invalid BLOCK value
        let params = vec![
            "BLOCK".to_string(),
            "invalid".to_string(),
            "STREAMS".to_string(),
            "mystream".to_string(),
            "0-0".to_string(),
        ];
        let result = parse_xread(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "-ERR syntax error in BLOCK parameter\r\n");

        // Invalid COUNT value
        let params = vec![
            "COUNT".to_string(),
            "invalid".to_string(),
            "STREAMS".to_string(),
            "mystream".to_string(),
            "0-0".to_string(),
        ];
        let result = parse_xread(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "-ERR syntax error in COUNT parameter\r\n");

        // Mismatched number of streams and IDs
        let params = vec![
            "STREAMS".to_string(),
            "stream1".to_string(),
            "stream2".to_string(),
            "1000-0".to_string(),
        ];
        let result = parse_xread(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "-ERR wrong number of arguments for 'xread' command\r\n");
    }
}

fn setup_test_stream() -> Arc<Mutex<Redis>> {
    let redis = Arc::new(Mutex::new(Redis::new()));
    let mut fields = HashMap::new();
    fields.insert("temperature".to_string(), "25".to_string());
    fields.insert("humidity".to_string(), "65".to_string());
    
    {
        let mut redis_guard = redis.lock().unwrap();
        redis_guard.xadd("sensor:1", "1000-0", fields.clone()).unwrap();
        
        fields.insert("temperature".to_string(), "26".to_string());
        redis_guard.xadd("sensor:1", "2000-0", fields.clone()).unwrap();
        
        fields.insert("temperature".to_string(), "27".to_string());
        redis_guard.xadd("sensor:1", "3000-0", fields).unwrap();
    }
    
    redis
}

#[test]
fn test_xread_basic() {
    let redis = setup_test_stream();
    
    let request = XReadRequest {
        keys: vec!["sensor:1".to_string()],
        ids: vec!["0-0".to_string()],
        block: None,
        count: None,
    };
    
    let handler = XReadHandler::new(redis, request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 1);
    let (stream_name, entries) = &results[0];
    assert_eq!(stream_name, "sensor:1");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].id, "1000-0");
    assert_eq!(entries[0].fields["temperature"], "25");
    assert_eq!(entries[0].fields["humidity"], "65");
}

#[test]
fn test_xread_with_specific_id() {
    let redis = setup_test_stream();
    
    let request = XReadRequest {
        keys: vec!["sensor:1".to_string()],
        ids: vec!["2000-0".to_string()],
        block: None,
        count: None,
    };
    
    let handler = XReadHandler::new(redis, request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, "3000-0");
}

#[test]
fn test_xread_with_count() {
    let redis = setup_test_stream();
    
    let request = XReadRequest {
        keys: vec!["sensor:1".to_string()],
        ids: vec!["0-0".to_string()],
        block: None,
        count: Some(2),
    };
    
    let handler = XReadHandler::new(redis, request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, "1000-0");
    assert_eq!(entries[1].id, "2000-0");
}

#[test]
fn test_xread_with_dollar() {
    let redis = setup_test_stream();
    
    let request = XReadRequest {
        keys: vec!["sensor:1".to_string()],
        ids: vec!["$".to_string()],
        block: None,
        count: None,
    };
    
    let handler = XReadHandler::new(redis.clone(), request);
    let results = handler.run_loop().unwrap();
    
    // Should be empty since $ means "new entries only"
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 0);
    
    // Add a new entry and try again
    {
        let mut redis_guard = redis.lock().unwrap();
        let mut fields = HashMap::new();
        fields.insert("temperature".to_string(), "28".to_string());
        redis_guard.xadd("sensor:1", "4000-0", fields).unwrap();
    }
    
    let request = XReadRequest {
        keys: vec!["sensor:1".to_string()],
        ids: vec!["$".to_string()],
        block: None,
        count: None,
    };
    
    let handler = XReadHandler::new(redis, request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 0); // Still 0 because $ points to the latest ID
}

#[test]
fn test_xread_blocking() {
    let redis = setup_test_stream();
    
    let request = XReadRequest {
        keys: vec!["sensor:1".to_string()],
        ids: vec!["3000-0".to_string()],
        block: Some(1000), // 1 second timeout
        count: None,
    };
    
    let handler = XReadHandler::new(redis.clone(), request);
    let results = handler.run_loop().unwrap();
    
    // Initially no results as we're reading after the last ID
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 0);
}

#[test]
fn test_stream_basic_operations() {
    let storage = Arc::new(Mutex::new(Storage::new()));
    
    // Test XADD
    {
        let mut storage_guard = storage.lock().unwrap();
        let mut fields = HashMap::new();
        fields.insert("sensor".to_string(), "1".to_string());
        fields.insert("temperature".to_string(), "25".to_string());
        
        let id = storage_guard.xadd("mystream", "1000-0", fields).unwrap();
        assert_eq!(id, "1000-0");
    }
    
    // Test XREAD
    let request = XReadRequest {
        keys: vec!["mystream".to_string()],
        ids: vec!["0-0".to_string()],
        block: None,
        count: None,
    };
    
    let handler = XReadHandler::new(storage.clone(), request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 1);
    let (stream_name, entries) = &results[0];
    assert_eq!(stream_name, "mystream");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, "1000-0");
    assert_eq!(entries[0].fields.get("sensor").unwrap(), "1");
    assert_eq!(entries[0].fields.get("temperature").unwrap(), "25");
}

#[test]
fn test_stream_multiple_entries() {
    let storage = Arc::new(Mutex::new(Storage::new()));
    
    // Add multiple entries
    {
        let mut storage_guard = storage.lock().unwrap();
        let mut fields = HashMap::new();
        
        // Entry 1
        fields.insert("sensor".to_string(), "1".to_string());
        fields.insert("temperature".to_string(), "25".to_string());
        storage_guard.xadd("mystream", "1000-0", fields.clone()).unwrap();
        
        // Entry 2
        fields.insert("temperature".to_string(), "26".to_string());
        storage_guard.xadd("mystream", "1001-0", fields.clone()).unwrap();
        
        // Entry 3
        fields.insert("temperature".to_string(), "27".to_string());
        storage_guard.xadd("mystream", "1002-0", fields).unwrap();
    }
    
    // Test XREAD with COUNT
    let request = XReadRequest {
        keys: vec!["mystream".to_string()],
        ids: vec!["0-0".to_string()],
        block: None,
        count: Some(2),
    };
    
    let handler = XReadHandler::new(storage.clone(), request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, "1000-0");
    assert_eq!(entries[1].id, "1001-0");
}

#[test]
fn test_stream_blocking_read() {
    let storage = Arc::new(Mutex::new(Storage::new()));
    let storage_clone = storage.clone();
    
    // Start a blocking read in a separate thread
    let handle = thread::spawn(move || {
        let request = XReadRequest {
            keys: vec!["mystream".to_string()],
            ids: vec!["$".to_string()],
            block: Some(1000), // 1 second timeout
            count: None,
        };
        
        let handler = XReadHandler::new(storage_clone, request);
        handler.run_loop()
    });
    
    // Wait a bit then add an entry
    thread::sleep(Duration::from_millis(100));
    {
        let mut storage_guard = storage.lock().unwrap();
        let mut fields = HashMap::new();
        fields.insert("sensor".to_string(), "1".to_string());
        fields.insert("temperature".to_string(), "25".to_string());
        storage_guard.xadd("mystream", "1000-0", fields).unwrap();
    }
    
    // Get results from blocking read
    let results = handle.join().unwrap().unwrap();
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, "1000-0");
}

#[test]
fn test_stream_multiple_keys() {
    let storage = Arc::new(Mutex::new(Storage::new()));
    
    // Add entries to multiple streams
    {
        let mut storage_guard = storage.lock().unwrap();
        let mut fields = HashMap::new();
        
        // Stream 1
        fields.insert("sensor".to_string(), "1".to_string());
        fields.insert("temperature".to_string(), "25".to_string());
        storage_guard.xadd("stream1", "1000-0", fields.clone()).unwrap();
        
        // Stream 2
        fields.insert("sensor".to_string(), "2".to_string());
        fields.insert("temperature".to_string(), "26".to_string());
        storage_guard.xadd("stream2", "1001-0", fields).unwrap();
    }
    
    // Test XREAD from multiple streams
    let request = XReadRequest {
        keys: vec!["stream1".to_string(), "stream2".to_string()],
        ids: vec!["0-0".to_string(), "0-0".to_string()],
        block: None,
        count: None,
    };
    
    let handler = XReadHandler::new(storage, request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 2);
    
    // Check stream1
    let (stream1_name, stream1_entries) = &results[0];
    assert_eq!(stream1_name, "stream1");
    assert_eq!(stream1_entries.len(), 1);
    assert_eq!(stream1_entries[0].id, "1000-0");
    assert_eq!(stream1_entries[0].fields.get("sensor").unwrap(), "1");
    
    // Check stream2
    let (stream2_name, stream2_entries) = &results[1];
    assert_eq!(stream2_name, "stream2");
    assert_eq!(stream2_entries.len(), 1);
    assert_eq!(stream2_entries[0].id, "1001-0");
    assert_eq!(stream2_entries[0].fields.get("sensor").unwrap(), "2");
}
