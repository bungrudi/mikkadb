use std::sync::{Arc, Mutex};
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::redis::config::RedisConfig;
use redis_starter_rust::client_handler::ClientHandler;
use crate::utils::mock_tcp_stream::MockTcpStream;

mod utils;

#[test]
fn test_multi_exec_basic() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mock_stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(mock_stream.clone(), Arc::clone(&redis));
    
    // Start client handler
    let handle = client_handler.start();

    // Start transaction
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$5\r\nMULTI\r\n");
    assert!(mock_stream.wait_for_write("+OK\r\n", 1000));
    mock_stream.clear_written_data();

    // Queue SET command
    mock_stream.read_data.lock().unwrap().extend(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\n41\r\n");
    assert!(mock_stream.wait_for_write("+QUEUED\r\n", 1000));
    mock_stream.clear_written_data();

    // Queue INCR command
    mock_stream.read_data.lock().unwrap().extend(b"*2\r\n$4\r\nINCR\r\n$3\r\nfoo\r\n");
    assert!(mock_stream.wait_for_write("+QUEUED\r\n", 1000));
    mock_stream.clear_written_data();

    // Execute transaction
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$4\r\nEXEC\r\n");
    assert!(mock_stream.wait_for_write("*2\r\n", 1000));
    assert!(mock_stream.wait_for_write("+OK\r\n", 1000));
    assert!(mock_stream.wait_for_write(":42\r\n", 1000));

    // Verify final value
    let value = redis.lock().unwrap().get("foo").unwrap();
    assert_eq!(value, "42");

    // Cleanup
    mock_stream.shutdown();
}

#[test]
fn test_nested_multi() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mock_stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(mock_stream.clone(), Arc::clone(&redis));
    
    // Start client handler
    let handle = client_handler.start();

    // Start first transaction
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$5\r\nMULTI\r\n");
    assert!(mock_stream.wait_for_write("+OK\r\n", 1000));
    mock_stream.clear_written_data();

    // Try to start nested transaction
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$5\r\nMULTI\r\n");
    assert!(mock_stream.wait_for_write("-ERR MULTI calls can not be nested\r\n", 1000));

    // Cleanup
    mock_stream.shutdown();
}

#[test]
fn test_exec_without_multi() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mock_stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(mock_stream.clone(), Arc::clone(&redis));
    
    // Start client handler
    let handle = client_handler.start();

    // Try EXEC without MULTI
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$4\r\nEXEC\r\n");
    assert!(mock_stream.wait_for_write("-ERR EXEC without MULTI\r\n", 1000));

    // Cleanup
    mock_stream.shutdown();
}

#[test]
fn test_discard_transaction() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mock_stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(mock_stream.clone(), Arc::clone(&redis));
    
    // Start client handler
    let handle = client_handler.start();

    // Start transaction
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$5\r\nMULTI\r\n");
    assert!(mock_stream.wait_for_write("+OK\r\n", 1000));
    mock_stream.clear_written_data();

    // Queue SET command
    mock_stream.read_data.lock().unwrap().extend(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\n41\r\n");
    assert!(mock_stream.wait_for_write("+QUEUED\r\n", 1000));
    mock_stream.clear_written_data();

    // Discard transaction
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$7\r\nDISCARD\r\n");
    assert!(mock_stream.wait_for_write("+OK\r\n", 1000));

    // Verify key was not set
    assert!(redis.lock().unwrap().get("foo").is_none());

    // Cleanup
    mock_stream.shutdown();
}

#[test]
fn test_discard_without_multi() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mock_stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(mock_stream.clone(), Arc::clone(&redis));
    
    // Start client handler
    let handle = client_handler.start();

    // Try DISCARD without MULTI
    mock_stream.read_data.lock().unwrap().extend(b"*1\r\n$7\r\nDISCARD\r\n");
    assert!(mock_stream.wait_for_write("-ERR DISCARD without MULTI\r\n", 1000));

    // Cleanup
    mock_stream.shutdown();
}
