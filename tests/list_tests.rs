use std::sync::{Arc, Mutex};
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::redis::config::RedisConfig;
use redis_starter_rust::redis::commands::RedisCommand;
use redis_starter_rust::client_handler::ClientHandler;

// Mock TCP stream for testing
struct MockTcpStream;

impl std::io::Read for MockTcpStream {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl std::io::Write for MockTcpStream {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Ok(0)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl redis_starter_rust::redis::replication::TcpStreamTrait for MockTcpStream {
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080))
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn redis_starter_rust::redis::replication::TcpStreamTrait>> {
        Ok(Box::new(MockTcpStream))
    }
}

#[test]
fn test_basic_list_push_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Test LPUSH
    let command = RedisCommand::LPush { key: "mylist", value: "first" };
    let result = handler.execute_command(&command);
    assert_eq!(result, ":1\r\n"); // List length should be 1

    // Test RPUSH
    let command = RedisCommand::RPush { key: "mylist", value: "last" };
    let result = handler.execute_command(&command);
    assert_eq!(result, ":2\r\n"); // List length should be 2

    // Test LRANGE to verify order
    let command = RedisCommand::LRange { key: "mylist", start: 0, stop: -1 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "*2\r\n$5\r\nfirst\r\n$4\r\nlast\r\n");
}

#[test]
fn test_list_pop_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Setup test data
    let command = RedisCommand::LPush { key: "mylist", value: "first" };
    handler.execute_command(&command);
    let command = RedisCommand::RPush { key: "mylist", value: "second" };
    handler.execute_command(&command);
    let command = RedisCommand::RPush { key: "mylist", value: "third" };
    handler.execute_command(&command);

    // Test LPOP
    let command = RedisCommand::LPop { key: "mylist" };
    let result = handler.execute_command(&command);
    assert_eq!(result, "$5\r\nfirst\r\n");

    // Test RPOP
    let command = RedisCommand::RPop { key: "mylist" };
    let result = handler.execute_command(&command);
    assert_eq!(result, "$5\r\nthird\r\n");

    // Verify length
    let command = RedisCommand::LLen { key: "mylist" };
    let result = handler.execute_command(&command);
    assert_eq!(result, ":1\r\n");
}

#[test]
fn test_empty_list_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Test LPOP on empty list
    let command = RedisCommand::LPop { key: "emptylist" };
    let result = handler.execute_command(&command);
    assert_eq!(result, "$-1\r\n");

    // Test RPOP on empty list
    let command = RedisCommand::RPop { key: "emptylist" };
    let result = handler.execute_command(&command);
    assert_eq!(result, "$-1\r\n");

    // Test LLEN on empty list
    let command = RedisCommand::LLen { key: "emptylist" };
    let result = handler.execute_command(&command);
    assert_eq!(result, ":0\r\n");
}

#[test]
fn test_list_range_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Setup test data
    let commands = vec![
        RedisCommand::RPush { key: "mylist", value: "one" },
        RedisCommand::RPush { key: "mylist", value: "two" },
        RedisCommand::RPush { key: "mylist", value: "three" },
        RedisCommand::RPush { key: "mylist", value: "four" },
    ];
    for command in commands {
        handler.execute_command(&command);
    }

    // Test LRANGE with positive indices
    let command = RedisCommand::LRange { key: "mylist", start: 1, stop: 2 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n");

    // Test LRANGE with negative indices
    let command = RedisCommand::LRange { key: "mylist", start: -2, stop: -1 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "*2\r\n$5\r\nthree\r\n$4\r\nfour\r\n");
}

#[test]
fn test_list_trim_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Setup test data
    let commands = vec![
        RedisCommand::RPush { key: "mylist", value: "one" },
        RedisCommand::RPush { key: "mylist", value: "two" },
        RedisCommand::RPush { key: "mylist", value: "three" },
        RedisCommand::RPush { key: "mylist", value: "four" },
    ];
    for command in commands {
        handler.execute_command(&command);
    }

    // Test LTRIM
    let command = RedisCommand::LTrim { key: "mylist", start: 1, stop: 2 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "+OK\r\n");

    // Verify result with LRANGE
    let command = RedisCommand::LRange { key: "mylist", start: 0, stop: -1 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n");

    // Verify length
    let command = RedisCommand::LLen { key: "mylist" };
    let result = handler.execute_command(&command);
    assert_eq!(result, ":2\r\n");
}

#[test]
fn test_list_position_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Setup test data
    let commands = vec![
        RedisCommand::RPush { key: "mylist", value: "one" },
        RedisCommand::RPush { key: "mylist", value: "two" },
        RedisCommand::RPush { key: "mylist", value: "two" },
        RedisCommand::RPush { key: "mylist", value: "three" },
    ];
    for command in commands {
        handler.execute_command(&command);
    }

    // Test LPOS for single occurrence
    let command = RedisCommand::LPos { key: "mylist", element: "two", count: None };
    let result = handler.execute_command(&command);
    assert_eq!(result, ":1\r\n");

    // Test LPOS with COUNT
    let command = RedisCommand::LPos { key: "mylist", element: "two", count: Some(2) };
    let result = handler.execute_command(&command);
    assert_eq!(result, "*2\r\n:1\r\n:2\r\n");
}

#[test]
fn test_list_insert_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Setup test data
    let commands = vec![
        RedisCommand::RPush { key: "mylist", value: "one" },
        RedisCommand::RPush { key: "mylist", value: "three" },
    ];
    for command in commands {
        handler.execute_command(&command);
    }

    // Test LINSERT
    let command = RedisCommand::LInsert { key: "mylist", before: true, pivot: "three", element: "two" };
    let result = handler.execute_command(&command);
    assert_eq!(result, ":3\r\n");

    // Verify result with LRANGE
    let command = RedisCommand::LRange { key: "mylist", start: 0, stop: -1 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n");
}

#[test]
fn test_list_set_and_get_operations() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Setup test data
    let commands = vec![
        RedisCommand::RPush { key: "mylist", value: "one" },
        RedisCommand::RPush { key: "mylist", value: "two" },
        RedisCommand::RPush { key: "mylist", value: "three" },
    ];
    for command in commands {
        handler.execute_command(&command);
    }

    // Test LSET
    let command = RedisCommand::LSet { key: "mylist", index: 1, element: "modified" };
    let result = handler.execute_command(&command);
    assert_eq!(result, "+OK\r\n");

    // Test LINDEX
    let command = RedisCommand::LIndex { key: "mylist", index: 1 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "$8\r\nmodified\r\n");

    // Verify final state with LRANGE
    let command = RedisCommand::LRange { key: "mylist", start: 0, stop: -1 };
    let result = handler.execute_command(&command);
    assert_eq!(result, "*3\r\n$3\r\none\r\n$8\r\nmodified\r\n$5\r\nthree\r\n");
}
