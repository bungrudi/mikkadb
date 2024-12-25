use std::sync::{Arc, Mutex};
use std::io::{Write, Read};
use redis_starter_rust::redis::{Redis, RedisConfig};
use redis_starter_rust::client_handler::ClientHandler;
mod utils;
use utils::mock_tcp_stream::MockTcpStream;

fn send_command(stream: &mut MockTcpStream, command: &str) -> std::io::Result<()> {
    #[cfg(debug_assertions)]
    println!("[TEST] Sending command: {}", command.trim());
    stream.write_all(command.as_bytes())?;
    Ok(())
}

fn read_response(stream: &mut MockTcpStream) -> std::io::Result<String> {
    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer)?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]).into_owned();
    #[cfg(debug_assertions)]
    println!("[TEST] Received response: {}", response.trim());
    Ok(response)
}

fn wait_for_response(stream: &mut MockTcpStream, expected: &str, timeout_ms: u64) -> std::io::Result<String> {
    if !stream.wait_for_pattern(expected, timeout_ms) {
        return Ok(String::new());
    }
    read_response(stream)
}

#[test]
fn test_concurrent_set_get() -> std::io::Result<()> {
    #[cfg(debug_assertions)]
    println!("[TEST] Starting test_concurrent_set_get");
    let config = RedisConfig {
        port: "6379".to_string(),
        addr: "127.0.0.1".to_string(),
        replicaof_host: None,
        replicaof_port: None,
        dir: "./".to_string(),
        dbfilename: "dump.rdb".to_string(),
    };
    let redis = Arc::new(Mutex::new(Redis::new(config)));

    // Create client-server pairs
    let (mut client1, server1) = MockTcpStream::new_pair();
    let (mut client2, server2) = MockTcpStream::new_pair();
    let (mut client3, server3) = MockTcpStream::new_pair();

    // Start client handlers with server ends
    let redis_clone1 = redis.clone();
    let mut client_handler1 = ClientHandler::new(server1, redis_clone1);
    let handle1 = client_handler1.start();

    let redis_clone2 = redis.clone();
    let mut client_handler2 = ClientHandler::new(server2, redis_clone2);
    let handle2 = client_handler2.start();

    let redis_clone3 = redis.clone();
    let mut client_handler3 = ClientHandler::new(server3, redis_clone3);
    let handle3 = client_handler3.start();

    // Wait for handlers to be ready
    while !client_handler1.is_ready() || !client_handler2.is_ready() || !client_handler3.is_ready() {
        #[cfg(debug_assertions)]
        println!("[TEST] Waiting for client handlers to be ready");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Client 1 operations
    send_command(&mut client1, "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")?;
    let response1 = wait_for_response(&mut client1, "+OK\r\n", 1000)?;
    assert_eq!(response1, "+OK\r\n");

    send_command(&mut client1, "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n")?;
    let response1 = wait_for_response(&mut client1, "$6\r\nvalue1\r\n", 1000)?;
    assert_eq!(response1, "$6\r\nvalue1\r\n");

    // Client 2 operations
    send_command(&mut client2, "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")?;
    let response2 = wait_for_response(&mut client2, "+OK\r\n", 1000)?;
    assert_eq!(response2, "+OK\r\n");

    send_command(&mut client2, "*2\r\n$3\r\nGET\r\n$4\r\nkey2\r\n")?;
    let response2 = wait_for_response(&mut client2, "$6\r\nvalue2\r\n", 1000)?;
    assert_eq!(response2, "$6\r\nvalue2\r\n");

    // Client 3 operations
    send_command(&mut client3, "*3\r\n$3\r\nSET\r\n$4\r\nkey3\r\n$6\r\nvalue3\r\n")?;
    let response3 = wait_for_response(&mut client3, "+OK\r\n", 1000)?;
    assert_eq!(response3, "+OK\r\n");

    send_command(&mut client3, "*2\r\n$3\r\nGET\r\n$4\r\nkey3\r\n")?;
    let response3 = wait_for_response(&mut client3, "$6\r\nvalue3\r\n", 1000)?;
    assert_eq!(response3, "$6\r\nvalue3\r\n");

    // Shutdown gracefully
    #[cfg(debug_assertions)]
    println!("[TEST] Shutting down client handlers");
    
    // First shutdown client handlers
    client_handler1.shutdown();
    client_handler2.shutdown();
    client_handler3.shutdown();
    
    // Wait for handlers to process shutdown
    std::thread::sleep(std::time::Duration::from_millis(100));
    
    // Then shutdown streams
    client1.shutdown();
    client2.shutdown();
    client3.shutdown();
    
    // Wait for threads to finish
    let _ = handle1.join();
    let _ = handle2.join();
    let _ = handle3.join();

    Ok(())
}
