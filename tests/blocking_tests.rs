use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::redis::config::RedisConfig;
use redis_starter_rust::client_handler::ClientHandler;
use crate::utils::mock_tcp_stream::MockTcpStream;

mod utils;

#[test]
fn test_xread_blocking_timeout() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let redis_clone = Arc::clone(&redis);

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());

    // Start the client handler in a separate thread
    client_handler.start();

    // Start a thread that will add data after a delay
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let mut fields = std::collections::HashMap::new();
        fields.insert("field1".to_string(), "value1".to_string());
        
        let mut redis = redis_clone.lock().unwrap();
        redis.xadd("mystream", "*", fields).unwrap();
    });

    // Write XREAD command to the stream
    let xread_command = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Wait for response
    thread::sleep(Duration::from_millis(200));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    assert!(response.contains("value1")); // Verify we got the data
}

#[test]
fn test_xread_blocking_timeout_expired() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());

    // Start the client handler
    client_handler.start();

    // Write XREAD command to the stream
    let xread_command = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Wait for timeout
    thread::sleep(Duration::from_millis(150));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    assert_eq!(response, "*0\r\n"); // Should return empty array when timeout expires
}

#[test]
fn test_xread_non_blocking_empty() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());

    // Start the client handler
    client_handler.start();

    // Write non-blocking XREAD command to the stream
    let xread_command = "*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Wait a bit for processing
    thread::sleep(Duration::from_millis(50));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    assert_eq!(response, "*0\r\n"); // Should return empty array immediately
}
