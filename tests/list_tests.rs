use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::io::{Write, Read};
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::redis::config::RedisConfig;
use redis_starter_rust::client_handler::ClientHandler;

mod utils;
use utils::mock_tcp_stream::MockTcpStream;

fn read_response(stream: &mut MockTcpStream) -> String {
    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer).unwrap();
    String::from_utf8_lossy(&buffer[..bytes_read]).into_owned()
}

#[test]
fn test_basic_list_push_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Test LPUSH
    {
        client.write_all(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n").unwrap();
    }
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    // Test RPUSH
    {
        client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$4\r\nlast\r\n").unwrap();
    }
    let response = read_response(&mut client);
    println!("RPUSH response: {:?}", response);
    assert!(client.wait_for_pattern(":2\r\n", 1000));

    // Test LRANGE
    {
        client.write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n").unwrap();
    }
    assert!(client.wait_for_pattern("*2\r\n$5\r\nfirst\r\n$4\r\nlast\r\n", 1000));
}

#[test]
fn test_list_pop_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    client.write_all(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$6\r\nsecond\r\n").unwrap();
    assert!(client.wait_for_pattern(":2\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthird\r\n").unwrap();
    assert!(client.wait_for_pattern(":3\r\n", 1000));

    // Test LPOP
    client.write_all(b"*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n").unwrap();
    assert!(client.wait_for_pattern("$5\r\nfirst\r\n", 1000));

    // Test RPOP
    client.write_all(b"*2\r\n$4\r\nRPOP\r\n$6\r\nmylist\r\n").unwrap();
    assert!(client.wait_for_pattern("$5\r\nthird\r\n", 1000));

    // Verify length
    client.write_all(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));
}

#[test]
fn test_empty_list_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Test LPOP on empty list
    client.write_all(b"*2\r\n$4\r\nLPOP\r\n$9\r\nemptylist\r\n").unwrap();
    assert!(client.wait_for_pattern("$-1\r\n", 1000));

    // Test RPOP on empty list
    client.write_all(b"*2\r\n$4\r\nRPOP\r\n$9\r\nemptylist\r\n").unwrap();
    assert!(client.wait_for_pattern("$-1\r\n", 1000));

    // Test LLEN on empty list
    client.write_all(b"*2\r\n$4\r\nLLEN\r\n$9\r\nemptylist\r\n").unwrap();
    assert!(client.wait_for_pattern(":0\r\n", 1000));

    // Test LRANGE on empty list
    client.write_all(b"*4\r\n$6\r\nLRANGE\r\n$9\r\nemptylist\r\n$1\r\n0\r\n$2\r\n-1\r\n").unwrap();
    assert!(client.wait_for_pattern("*0\r\n", 1000));
}

#[test]
fn test_list_range_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n").unwrap();
    assert!(client.wait_for_pattern(":2\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n").unwrap();
    assert!(client.wait_for_pattern(":3\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$4\r\nfour\r\n").unwrap();
    assert!(client.wait_for_pattern(":4\r\n", 1000));

    // Test LRANGE with positive indices
    client.write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n2\r\n").unwrap();
    assert!(client.wait_for_pattern("*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", 1000));

    // Test LRANGE with negative indices
    client.write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$2\r\n-2\r\n$2\r\n-1\r\n").unwrap();
    assert!(client.wait_for_pattern("*2\r\n$5\r\nthree\r\n$4\r\nfour\r\n", 1000));
}

#[test]
fn test_list_trim_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n").unwrap();
    assert!(client.wait_for_pattern(":2\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n").unwrap();
    assert!(client.wait_for_pattern(":3\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$4\r\nfour\r\n").unwrap();
    assert!(client.wait_for_pattern(":4\r\n", 1000));

    // Test LTRIM
    client.write_all(b"*4\r\n$5\r\nLTRIM\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n2\r\n").unwrap();
    assert!(client.wait_for_pattern("+OK\r\n", 1000));

    // Verify the list after trim
    client.write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n").unwrap();
    assert!(client.wait_for_pattern("*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", 1000));
}

#[test]
fn test_list_position_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n").unwrap();
    assert!(client.wait_for_pattern(":2\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n").unwrap();
    assert!(client.wait_for_pattern(":3\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n").unwrap();
    assert!(client.wait_for_pattern(":4\r\n", 1000));

    // Test LPOS with simple case
    client.write_all(b"*3\r\n$4\r\nLPOS\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    // Test LPOS with count=2
    client.write_all(b"*5\r\n$4\r\nLPOS\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n").unwrap();
    assert!(client.wait_for_pattern("*2\r\n:1\r\n:2\r\n", 1000));

    // Test LPOS with non-existent element
    client.write_all(b"*3\r\n$4\r\nLPOS\r\n$6\r\nmylist\r\n$4\r\nfive\r\n").unwrap();
    assert!(client.wait_for_pattern(":-1\r\n", 1000));
}

#[test]
fn test_list_insert_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n").unwrap();
    assert!(client.wait_for_pattern(":2\r\n", 1000));

    // Test LINSERT BEFORE
    client.write_all(b"*5\r\n$7\r\nLINSERT\r\n$6\r\nmylist\r\n$6\r\nBEFORE\r\n$5\r\nthree\r\n$3\r\ntwo\r\n").unwrap();
    assert!(client.wait_for_pattern(":3\r\n", 1000));

    // Verify list after insert
    client.write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n").unwrap();
    assert!(client.wait_for_pattern("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", 1000));

    // Test LINSERT AFTER
    client.write_all(b"*5\r\n$7\r\nLINSERT\r\n$6\r\nmylist\r\n$5\r\nAFTER\r\n$5\r\nthree\r\n$4\r\nfour\r\n").unwrap();
    assert!(client.wait_for_pattern(":4\r\n", 1000));

    // Verify list after insert
    client.write_all(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n").unwrap();
    assert!(client.wait_for_pattern("*4\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nfour\r\n", 1000));
}

#[test]
fn test_list_set_and_get_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let (mut client, server) = MockTcpStream::new_pair();
    let mut handler = ClientHandler::new(server, Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n").unwrap();
    assert!(client.wait_for_pattern(":1\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n").unwrap();
    assert!(client.wait_for_pattern(":2\r\n", 1000));

    client.write_all(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n").unwrap();
    assert!(client.wait_for_pattern(":3\r\n", 1000));

    // Test LSET
    client.write_all(b"*4\r\n$4\r\nLSET\r\n$6\r\nmylist\r\n$1\r\n1\r\n$8\r\nmodified\r\n").unwrap();
    assert!(client.wait_for_pattern("+OK\r\n", 1000));

    // Test LINDEX
    client.write_all(b"*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$1\r\n1\r\n").unwrap();
    assert!(client.wait_for_pattern("$8\r\nmodified\r\n", 1000));

    // Test LINDEX with negative index
    client.write_all(b"*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$2\r\n-1\r\n").unwrap();
    assert!(client.wait_for_pattern("$5\r\nthree\r\n", 1000));

    // Test LINDEX out of range
    client.write_all(b"*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$1\r\n5\r\n").unwrap();
    assert!(client.wait_for_pattern("$-1\r\n", 1000));
}
