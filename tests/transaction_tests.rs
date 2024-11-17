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
fn test_multi_exec_basic() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Start transaction
    let response = handler.execute_command(&RedisCommand::Multi);
    assert_eq!(response, "+OK\r\n");

    // Queue SET command
    let response = handler.execute_command(&RedisCommand::Set {
        key: "foo".to_string(),
        value: "41".to_string(),
        ttl: None,
        original_resp: "SET foo 41".to_string(),
    });
    assert_eq!(response, "+QUEUED\r\n");

    // Queue INCR command
    let response = handler.execute_command(&RedisCommand::Incr { key: "foo".to_string() });
    assert_eq!(response, "+QUEUED\r\n");

    // Execute transaction
    let response = handler.execute_command(&RedisCommand::Exec);
    assert!(response.starts_with("*2\r\n")); // Array of 2 responses
    assert!(response.contains("+OK\r\n")); // SET response
    assert!(response.contains(":42\r\n")); // INCR response

    // Verify final value
    let value = redis.lock().unwrap().get("foo").unwrap();
    assert_eq!(value, "42");
}

#[test]
fn test_nested_multi() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Start first transaction
    let response = handler.execute_command(&RedisCommand::Multi);
    assert_eq!(response, "+OK\r\n");

    // Try to start nested transaction
    let response = handler.execute_command(&RedisCommand::Multi);
    assert_eq!(response, "-ERR MULTI calls can not be nested\r\n");
}

#[test]
fn test_exec_without_multi() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Try EXEC without MULTI
    let response = handler.execute_command(&RedisCommand::Exec);
    assert_eq!(response, "-ERR EXEC without MULTI\r\n");
}

#[test]
fn test_unsupported_command_in_transaction() {
    let redis = Arc::new(Mutex::new(Redis::new(RedisConfig::default())));
    let mut handler = ClientHandler::new(MockTcpStream, Arc::clone(&redis));

    // Start transaction
    let response = handler.execute_command(&RedisCommand::Multi);
    assert_eq!(response, "+OK\r\n");

    // Try unsupported command
    let response = handler.execute_command(&RedisCommand::Ping);
    assert_eq!(response, "-ERR Command not supported in transaction\r\n");
}
