use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::redis::config::RedisConfig;
use redis_starter_rust::client_handler::ClientHandler;
use crate::utils::mock_tcp_stream::MockTcpStream;

mod utils;

#[test]
fn test_xread_blocking_with_new_data() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let redis_clone = Arc::clone(&redis);

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());

    // Start the client handler in a separate thread
    client_handler.start();

    // Write XREAD command with BLOCK option
    let xread_command = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Start a thread that will add data after a delay
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let mut fields = std::collections::HashMap::new();
        fields.insert("field1".to_string(), "value1".to_string());
        
        let mut redis = redis_clone.lock().unwrap();
        redis.xadd("mystream", "*", fields).unwrap();
    });

    // Wait for response
    thread::sleep(Duration::from_millis(300));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    
    // Response should be in format:
    // *1\r\n                     (one stream)
    // *2\r\n                     (stream name and entries)
    // $8\r\nmystream\r\n         (stream name)
    // *1\r\n                     (one entry)
    // *2\r\n                     (ID and fields)
    // $<len>\r\n<ID>\r\n        (entry ID)
    // *2\r\n                     (field-value pair)
    // $6\r\nfield1\r\n          (field name)
    // $6\r\nvalue1\r\n          (field value)
    assert!(response.starts_with("*1\r\n"), "Response should start with array of one stream");
    assert!(response.contains("mystream"), "Response should contain stream name");
    assert!(response.contains("field1"), "Response should contain field name");
    assert!(response.contains("value1"), "Response should contain field value");
}

#[test]
fn test_xread_blocking_timeout() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());

    // Start the client handler
    client_handler.start();

    // Write XREAD command with BLOCK option and short timeout
    let xread_command = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Wait for timeout
    thread::sleep(Duration::from_millis(200));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    assert_eq!(response, "*0\r\n", "Should return empty array when timeout expires");
}

#[test]
fn test_xread_non_blocking_empty() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());

    // Start the client handler
    client_handler.start();

    // Write non-blocking XREAD command
    let xread_command = "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Wait a bit for processing
    thread::sleep(Duration::from_millis(50));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    assert_eq!(response, "*0\r\n", "Should return empty array for non-blocking empty read");
}

#[test]
fn test_xread_blocking_multiple_streams() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let redis_clone = Arc::clone(&redis);

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());

    // Start the client handler
    client_handler.start();

    // Write XREAD command with multiple streams
    let xread_command = "*8\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$7\r\nstream1\r\n$7\r\nstream2\r\n$1\r\n$\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Start a thread that will add data after a delay
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let mut fields = std::collections::HashMap::new();
        
        let mut redis = redis_clone.lock().unwrap();
        
        // Add to first stream
        fields.insert("field1".to_string(), "value1".to_string());
        redis.xadd("stream1", "*", fields.clone()).unwrap();
        
        // Add to second stream
        fields.insert("field1".to_string(), "value2".to_string());
        redis.xadd("stream2", "*", fields).unwrap();
    });

    // Wait for response
    thread::sleep(Duration::from_millis(300));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    
    // Response should be in format:
    // *2\r\n                     (two streams)
    // *2\r\n                     (stream1 name and entries)
    // $7\r\nstream1\r\n         (stream1 name)
    // *1\r\n                     (one entry)
    // *2\r\n                     (ID and fields)
    // $<len>\r\n<ID>\r\n        (entry ID)
    // *2\r\n                     (field-value pair)
    // $6\r\nfield1\r\n          (field name)
    // $6\r\nvalue1\r\n          (field value)
    // *2\r\n                     (stream2 name and entries)
    // $7\r\nstream2\r\n         (stream2 name)
    // Similar format for stream2...
    assert!(response.starts_with("*2\r\n"), "Response should start with array of two streams");
    assert!(response.contains("stream1"), "Response should contain stream1");
    assert!(response.contains("stream2"), "Response should contain stream2");
    assert!(response.contains("value1"), "Response should contain value1");
    assert!(response.contains("value2"), "Response should contain value2");
}
