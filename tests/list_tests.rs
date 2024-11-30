use std::sync::{Arc, Mutex};
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::redis::config::RedisConfig;
use redis_starter_rust::client_handler::ClientHandler;

mod utils;
use utils::mock_tcp_stream::MockTcpStream;

#[test]
fn test_basic_list_push_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Test LPUSH
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));

    // Test RPUSH
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$4\r\nlast\r\n");
    }
    assert!(stream.wait_for_write(":2\r\n", 1000));

    // Test LRANGE to verify order
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
    }
    assert!(stream.wait_for_write("*2\r\n$5\r\nfirst\r\n$4\r\nlast\r\n", 1000));

    stream.shutdown(&mut handler, handle);
}

#[test]
fn test_list_pop_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$6\r\nsecond\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthird\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));
    assert!(stream.wait_for_write(":2\r\n", 1000));
    assert!(stream.wait_for_write(":3\r\n", 1000));

    // Test LPOP
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");
    }
    assert!(stream.wait_for_write("$5\r\nfirst\r\n", 1000));

    // Test RPOP
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*2\r\n$4\r\nRPOP\r\n$6\r\nmylist\r\n");
    }
    assert!(stream.wait_for_write("$5\r\nthird\r\n", 1000));

    // Verify length
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));

    stream.shutdown(&mut handler, handle);
}

#[test]
fn test_empty_list_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Test LPOP on empty list
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*2\r\n$4\r\nLPOP\r\n$9\r\nemptylist\r\n");
    }
    assert!(stream.wait_for_write("$-1\r\n", 1000));

    // Test RPOP on empty list
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*2\r\n$4\r\nRPOP\r\n$9\r\nemptylist\r\n");
    }
    assert!(stream.wait_for_write("$-1\r\n", 1000));

    // Test LLEN on empty list
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*2\r\n$4\r\nLLEN\r\n$9\r\nemptylist\r\n");
    }
    assert!(stream.wait_for_write(":0\r\n", 1000));

    stream.shutdown(&mut handler, handle);
}

#[test]
fn test_list_range_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$4\r\nfour\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));
    assert!(stream.wait_for_write(":2\r\n", 1000));
    assert!(stream.wait_for_write(":3\r\n", 1000));
    assert!(stream.wait_for_write(":4\r\n", 1000));

    // Test LRANGE with positive indices
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n2\r\n");
    }
    assert!(stream.wait_for_write("*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", 1000));

    // Test LRANGE with negative indices
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$2\r\n-2\r\n$2\r\n-1\r\n");
    }
    assert!(stream.wait_for_write("*2\r\n$5\r\nthree\r\n$4\r\nfour\r\n", 1000));

    stream.shutdown(&mut handler, handle);
}

#[test]
fn test_list_trim_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$4\r\nfour\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));
    assert!(stream.wait_for_write(":2\r\n", 1000));
    assert!(stream.wait_for_write(":3\r\n", 1000));
    assert!(stream.wait_for_write(":4\r\n", 1000));

    // Test LTRIM
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$5\r\nLTRIM\r\n$6\r\nmylist\r\n$1\r\n1\r\n$1\r\n2\r\n");
    }
    assert!(stream.wait_for_write("+OK\r\n", 1000));

    // Verify result with LRANGE
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
    }
    assert!(stream.wait_for_write("*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", 1000));

    // Verify length
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
    }
    assert!(stream.wait_for_write(":2\r\n", 1000));
    
    // Small delay to ensure response is fully written
    std::thread::sleep(std::time::Duration::from_millis(50));
    
    stream.shutdown(&mut handler, handle);
}

#[test]
fn test_list_position_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));
    assert!(stream.wait_for_write(":2\r\n", 1000));
    assert!(stream.wait_for_write(":3\r\n", 1000));
    assert!(stream.wait_for_write(":4\r\n", 1000));

    // Test LPOS for single occurrence
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$4\r\nLPOS\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));

    // Test LPOS with COUNT
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$4\r\nLPOS\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n$1\r\n2\r\n");
    }
    assert!(stream.wait_for_write("*2\r\n:1\r\n:2\r\n", 1000));

    stream.shutdown(&mut handler, handle);
}

#[test]
fn test_list_insert_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));
    assert!(stream.wait_for_write(":2\r\n", 1000));

    // Test LINSERT
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*5\r\n$7\r\nLINSERT\r\n$6\r\nmylist\r\n$6\r\nBEFORE\r\n$5\r\nthree\r\n$3\r\ntwo\r\n");
    }
    assert!(stream.wait_for_write(":3\r\n", 1000));

    // Verify result with LRANGE
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
    }
    assert!(stream.wait_for_write("*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n", 1000));

    stream.shutdown(&mut handler, handle);
}

#[test]
fn test_list_set_and_get_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let stream = MockTcpStream::new();
    let mut handler = ClientHandler::new(stream.clone(), Arc::clone(&redis));
    let handle = handler.start();

    // Setup test data
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\ntwo\r\n");
        read_data.extend_from_slice(b"*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nthree\r\n");
    }
    assert!(stream.wait_for_write(":1\r\n", 1000));
    assert!(stream.wait_for_write(":2\r\n", 1000));
    assert!(stream.wait_for_write(":3\r\n", 1000));

    // Test LSET
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$4\r\nLSET\r\n$6\r\nmylist\r\n$1\r\n1\r\n$8\r\nmodified\r\n");
    }
    assert!(stream.wait_for_write("+OK\r\n", 1000));

    // Test LINDEX
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$1\r\n1\r\n");
    }
    assert!(stream.wait_for_write("$8\r\nmodified\r\n", 1000));

    // Verify final state with LRANGE
    {
        let mut read_data = stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n");
    }
    assert!(stream.wait_for_write("*3\r\n$3\r\none\r\n$8\r\nmodified\r\n$5\r\nthree\r\n", 1000));

    stream.shutdown(&mut handler, handle);
}
