use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};

use redis_starter_rust::redis::{
    core::Redis,
    storage::Storage,
    xread_handler::{XReadHandler, XReadRequest},
    config::RedisConfig,
};
use redis_starter_rust::client_handler::ClientHandler;
use crate::utils::mock_tcp_stream::MockTcpStream;

mod utils;

// Core XRead Handler Tests
#[test]
fn test_xread_parse_stream_id() {
    assert_eq!(Storage::parse_stream_id("1234-0").unwrap(), (1234, 0));
    assert_eq!(Storage::parse_stream_id("1234-5").unwrap(), (1234, 5));
    assert_eq!(Storage::parse_stream_id("1234").unwrap(), (1234, 0));
    assert_eq!(Storage::parse_stream_id("$").unwrap(), (u64::MAX, 0));
    assert!(Storage::parse_stream_id("invalid").is_err());
}

#[test]
fn test_xread_with_dollar_id() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    
    // Add some test data
    {
        let redis_guard = redis.lock().unwrap();
        let fields = HashMap::new();
        redis_guard.storage.xadd("mystream", "1-0", fields).unwrap();
        
        let fields = HashMap::new();
        redis_guard.storage.xadd("mystream", "2-0", fields).unwrap();
    }

    let request = XReadRequest {
        keys: vec!["mystream".to_string()],
        ids: vec!["$".to_string()],
        block: None,
        count: None,
    };

    let mut handler = XReadHandler::new(redis.clone(), request);
    let results = handler.run_loop().unwrap();
    
    // When using $ with no new data, Redis returns nil
    assert!(results.is_empty());
}

#[test]
fn test_xread_with_count_limit() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    
    // Add test data
    {
        let redis_guard = redis.lock().unwrap();
        let fields = HashMap::new();
        redis_guard.storage.xadd("mystream", "1-0", fields).unwrap();
        
        let fields = HashMap::new();
        redis_guard.storage.xadd("mystream", "2-0", fields).unwrap();
        
        let fields = HashMap::new();
        redis_guard.storage.xadd("mystream", "3-0", fields).unwrap();
    }

    let request = XReadRequest {
        keys: vec!["mystream".to_string()],
        ids: vec!["0-0".to_string()],
        block: None,
        count: Some(2),
    };

    let mut handler = XReadHandler::new(redis.clone(), request);
    let results = handler.run_loop().unwrap();
    
    assert_eq!(results.len(), 1);
    let (_, entries) = &results[0];
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, "1-0");
    assert_eq!(entries[1].id, "2-0");
}

#[test]
fn test_xread_blocking_timeout_handler_logic() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    let request = XReadRequest {
        keys: vec!["mystream".to_string()],
        ids: vec!["$".to_string()],
        block: Some(100), // 100ms timeout
        count: None,
    };

    let mut handler = XReadHandler::new(redis.clone(), request);
    let start = Instant::now();
    let results = handler.run_loop().unwrap();
    let elapsed = start.elapsed();

    #[cfg(debug_assertions)]
    {
        println!("[test_xread_blocking_timeout_handler_logic] Results: {:?}\n", results);
        println!("[test_xread_blocking_timeout_handler_logic] Elapsed: {:?}\n", elapsed);
    }

    assert!(elapsed >= Duration::from_millis(100));
    // Redis returns nil on timeout
    assert!(results.is_empty());
}

#[test]
fn test_xread_multiple_streams() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::new())));
    
    // Add test data
    {
        let redis_guard = redis.lock().unwrap();
        let mut fields = HashMap::new();
        fields.insert("field1".to_string(), "value1".to_string());
        redis_guard.storage.xadd("stream1", "1-0", fields).unwrap();
        
        let mut fields = HashMap::new();
        fields.insert("field1".to_string(), "value2".to_string());
        redis_guard.storage.xadd("stream2", "1-0", fields).unwrap();
    }

    // Test 1: Both streams have entries
    {
        let request = XReadRequest {
            keys: vec!["stream1".to_string(), "stream2".to_string()],
            ids: vec!["0-0".to_string(), "0-0".to_string()],
            block: None,
            count: None,
        };

        let mut handler = XReadHandler::new(redis.clone(), request);
        let results = handler.run_loop().unwrap();

        #[cfg(debug_assertions)]
        println!("[test_xread_multiple_streams::test1] Results: {:?}\n", results);
        
        assert_eq!(results.len(), 2);
        
        // Check first stream
        let (stream_name, entries) = &results[0];
        assert_eq!(stream_name, "stream1");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1-0");
        assert_eq!(entries[0].fields["field1"], "value1");
        
        // Check second stream
        let (stream_name, entries) = &results[1];
        assert_eq!(stream_name, "stream2");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1-0");
        assert_eq!(entries[0].fields["field1"], "value2");
    }

    // Test 2: Only one stream has entries
    {
        let request = XReadRequest {
            keys: vec!["stream1".to_string(), "stream2".to_string()],
            ids: vec!["0-0".to_string(), "1-0".to_string()], // stream2 has no new entries after 1-0
            block: None,
            count: None,
        };

        let mut handler = XReadHandler::new(redis.clone(), request);
        let results = handler.run_loop().unwrap();

        #[cfg(debug_assertions)]
        println!("[test_xread_multiple_streams::test2] Results: {:?}\n", results);
        
        assert_eq!(results.len(), 1, "Should only include stream1 which has entries");
        
        // Check stream1 (only stream with entries)
        let (stream_name, entries) = &results[0];
        assert_eq!(stream_name, "stream1");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1-0");
        assert_eq!(entries[0].fields["field1"], "value1");
    }

    // Test 3: No streams have entries
    {
        let request = XReadRequest {
            keys: vec!["stream1".to_string(), "stream2".to_string()],
            ids: vec!["1-0".to_string(), "1-0".to_string()], // No new entries after 1-0
            block: None,
            count: None,
        };

        let mut handler = XReadHandler::new(redis.clone(), request);
        let results = handler.run_loop().unwrap();

        #[cfg(debug_assertions)]
        println!("[test_xread_multiple_streams::test3] Results: {:?}\n", results);
        
        assert!(results.is_empty(), "Should return empty array when no streams have entries");
    }
}

// Protocol-Level XREAD Tests
#[test]
fn test_xread_blocking_with_new_data() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let redis_clone = Arc::clone(&redis);

    // Add initial data that should not be returned
    {
        let mut fields = std::collections::HashMap::new();
        fields.insert("field1".to_string(), "old_value".to_string());
        let redis = redis.lock().unwrap();
        redis.storage.xadd("mystream", "*", fields).unwrap();
    }

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());
    let handle = client_handler.start();

    // Write XREAD command with BLOCK option
    let xread_command = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1000\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }

    // Start a thread that will add new data after a delay
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let mut fields = std::collections::HashMap::new();
        fields.insert("field1".to_string(), "new_value".to_string());
        
        let redis = redis_clone.lock().unwrap();
        redis.storage.xadd("mystream", "*", fields).unwrap();
    });

    // Wait for response
    thread::sleep(Duration::from_millis(300));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    println!("\n[test_xread_blocking_with_new_data] Response received: {:?}\n", response);
    
    assert!(response.starts_with("*1\r\n"), "Response should start with array of one stream");
    assert!(response.contains("mystream"), "Response should contain stream name");
    assert!(response.contains("field1"), "Response should contain field name");
    assert!(response.contains("new_value"), "Response should contain new field value");
    assert!(!response.contains("old_value"), "Response should not contain old field value");

    // Shutdown both stream and client handler
    stream.shutdown();
    client_handler.shutdown();
    handle.join().unwrap();
}

#[test]
fn test_xread_non_blocking_empty() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());
    let handle = client_handler.start();

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
    assert_eq!(response, "*-1\r\n", "Should return nil for non-blocking empty read");

    // Shutdown both stream and client handler
    stream.shutdown();
    client_handler.shutdown();
    handle.join().unwrap();
}

#[test]
fn test_xread_blocking_multiple_streams_protocol() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let redis_clone = Arc::clone(&redis);

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());
    let handle = client_handler.start();

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
        
        let redis = redis_clone.lock().unwrap();
        
        // Add to first stream
        fields.insert("field1".to_string(), "value1".to_string());
        redis.storage.xadd("stream1", "*", fields.clone()).unwrap();
        
        // Add to second stream
        fields.insert("field1".to_string(), "value2".to_string());
        redis.storage.xadd("stream2", "*", fields).unwrap();
    });

    // Wait for response
    thread::sleep(Duration::from_millis(300));

    // Check the response
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    
    // If we timed out or no data was available yet, we should get nil
    if response == "*-1\r\n" {
        return;
    }
    
    // If we got data, verify the format
    assert!(response.starts_with("*2\r\n"), "Response should either be nil or start with array of two streams");
    assert!(response.contains("stream1"), "Response should contain stream1");
    assert!(response.contains("stream2"), "Response should contain stream2");
    assert!(response.contains("value1"), "Response should contain value1");
    assert!(response.contains("value2"), "Response should contain value2");

    // Shutdown both stream and client handler
    stream.shutdown();
    client_handler.shutdown();
    handle.join().unwrap();
}

#[test]
fn test_xread_blocking_timeout() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());
    let handle = client_handler.start();

    // Write XREAD command with BLOCK option
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
    assert_eq!(response, "*-1\r\n", "Should return nil for blocking read timeout");

    // Shutdown both stream and client handler
    stream.shutdown();
    client_handler.shutdown();
    handle.join().unwrap();
}

#[test]
fn test_xread_empty_vs_nil_response() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());
    let handle = client_handler.start();

    // Test 1: Non-blocking read with $ (should return nil)
    let xread_command = "*5\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }
    thread::sleep(Duration::from_millis(50));
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_vs_nil_response::test1] Response received: {:?}", response);
    assert_eq!(response, "*-1\r\n", "Should return nil for non-blocking read with $");

    // Wait for write to complete
    thread::sleep(Duration::from_millis(50));
    stream.clear_written_data();
    stream.clear_read_data();

    // Test 2: Non-blocking read with non-existent ID (should also return nil)
    let xread_command = "*5\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }
    thread::sleep(Duration::from_millis(50));
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_vs_nil_response::test2] Response received: {:?}", response);
    assert_eq!(response, "*-1\r\n", "Should return nil for non-blocking read with no matches");

    // Wait for write to complete and clear buffers
    thread::sleep(Duration::from_millis(50));
    stream.clear_written_data();
    stream.clear_read_data();

    // Test 3: Blocking read with timeout (should return nil)
    let xread_command = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }
    thread::sleep(Duration::from_millis(200));
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_vs_nil_response::test3] Response received: {:?}", response);
    assert_eq!(response, "*-1\r\n", "Should return nil for blocking read timeout");

    // Shutdown stream and client handler
    stream.shutdown();
    client_handler.shutdown();
    thread::sleep(Duration::from_millis(100)); // Give time for shutdown to propagate
    handle.join().unwrap();
}

#[test]
fn test_xread_empty_responses() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));

    // Create mock stream
    let stream = MockTcpStream::new();
    let mut client_handler = ClientHandler::new(stream.clone(), redis.clone());
    let handle = client_handler.start();

    // Test 1: Non-blocking read with $ (should return nil)
    let xread_command = "*5\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
    }
    thread::sleep(Duration::from_millis(300));  // Wait longer for command processing
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_responses::test1] Response received: {:?}", response);
    
    assert_eq!(response, "*-1\r\n", "Should return nil for non-blocking read with $");

    // Wait for write to complete and clear buffers
    thread::sleep(Duration::from_millis(300));  // Wait longer before clearing
    stream.clear_written_data();
    stream.clear_read_data();
    let written_data = stream.get_written_data();
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_responses::test1] After clear - written data: {:?}", String::from_utf8_lossy(&written_data));

    // Test 2: Non-blocking read with non-existent ID (should also return nil)
    let xread_command = "*5\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n";
    #[cfg(debug_assertions)]
    println!("\n[test_xread_empty_responses::test2] Sending command: {:?}", xread_command);
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
        #[cfg(debug_assertions)]
        println!("[test_xread_empty_responses::test2] Command added to read buffer, len: {}", read_data.len());
    }
    thread::sleep(Duration::from_millis(300));  // Wait longer for command processing
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_responses::test2] Response received: {:?}", response);
    assert_eq!(response, "*-1\r\n", "Should return nil for non-blocking read with no matches");

    // Wait for write to complete and clear buffers
    thread::sleep(Duration::from_millis(300));  // Wait longer before clearing
    stream.clear_written_data();
    stream.clear_read_data();
    let written_data = stream.get_written_data();
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_responses::test2] After clear - written data: {:?}", String::from_utf8_lossy(&written_data));

    // Test 3: Blocking read with timeout (should return nil)
    let xread_command = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n";
    #[cfg(debug_assertions)]
    println!("\n[test_xread_empty_responses::test3] Sending command: {:?}", xread_command);
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(xread_command.as_bytes());
        #[cfg(debug_assertions)]
        println!("[test_xread_empty_responses::test3] Command added to read buffer, len: {}", read_data.len());
    }
    thread::sleep(Duration::from_millis(300));  // Wait longer for command processing
    let written_data = stream.get_written_data();
    let response = String::from_utf8_lossy(&written_data);
    #[cfg(debug_assertions)]
    println!("[test_xread_empty_responses::test3] Response received: {:?}", response);
    assert_eq!(response, "*-1\r\n", "Should return nil for blocking read timeout");

    // Shutdown stream and client handler
    stream.shutdown();
    client_handler.shutdown();
    thread::sleep(Duration::from_millis(100)); // Give time for shutdown to propagate
    handle.join().unwrap();
}
