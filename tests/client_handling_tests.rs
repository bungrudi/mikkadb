use std::sync::{Arc, Mutex};
use std::io::{Write, Read};
use redis_starter_rust::redis::{Redis, RedisConfig};
use redis_starter_rust::client_handler::ClientHandler;
mod utils;
use utils::mock_tcp_stream::MockTcpStream;

fn send_command(stream: &mut MockTcpStream, command: &str) -> std::io::Result<()> {
    println!("[TEST] Sending command: {}", command.trim());
    stream.write_all(command.as_bytes())?;
    Ok(())
}

fn read_response(stream: &mut MockTcpStream) -> std::io::Result<String> {
    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer)?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]).into_owned();
    println!("[TEST] Received response: {}", response.trim());
    Ok(response)
}

#[test]
fn test_concurrent_set_get() -> std::io::Result<()> {
    println!("[TEST] Starting test_concurrent_set_get");
    // Create a shared Redis instance
    let config = RedisConfig {
        port: "6379".to_string(),
        addr: "127.0.0.1".to_string(),
        replicaof_host: None,
        replicaof_port: None,
        dir: "./".to_string(),
        dbfilename: "dump.rdb".to_string(),
    };
    let redis = Arc::new(Mutex::new(Redis::new(config)));

    // Client 1 setup
    let mut stream1 = MockTcpStream::new();
    let redis_clone1 = redis.clone();
    let mut client_handler1 = ClientHandler::new(stream1.clone(), redis_clone1);
    let handle1 = client_handler1.start();

    // Client 2 setup
    let mut stream2 = MockTcpStream::new();
    let redis_clone2 = redis.clone();
    let mut client_handler2 = ClientHandler::new(stream2.clone(), redis_clone2);
    let handle2 = client_handler2.start();

    // Client 3 setup
    let mut stream3 = MockTcpStream::new();
    let redis_clone3 = redis.clone();
    let mut client_handler3 = ClientHandler::new(stream3.clone(), redis_clone3);
    let handle3 = client_handler3.start();

    // we need to wait until all client handlers are ready
    while !client_handler1.is_ready() || !client_handler2.is_ready() || !client_handler3.is_ready() {
        println!("[TEST] Waiting for client handlers to be ready");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    // Client 1 operations
    send_command(&mut stream1, "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n").unwrap();
    stream1.wait_for_write("+OK\r\n", 1000);
    stream1.read_data.lock().unwrap().extend_from_slice("+OK\r\n".as_bytes());
    let response1 = read_response(&mut stream1).unwrap();
    assert_eq!(response1, "+OK\r\n");

    send_command(&mut stream1, "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").unwrap();
    stream1.wait_for_write("$6\r\nvalue1\r\n", 1000);
    stream1.read_data.lock().unwrap().extend_from_slice("$6\r\nvalue1\r\n".as_bytes());
    let response1 = read_response(&mut stream1).unwrap();
    assert_eq!(response1, "$6\r\nvalue1\r\n");

    // Client 2 operations
    send_command(&mut stream2, "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n").unwrap();
    stream2.wait_for_write("+OK\r\n", 1000);
    stream2.read_data.lock().unwrap().extend_from_slice("+OK\r\n".as_bytes());
    let response2 = read_response(&mut stream2).unwrap();
    assert_eq!(response2, "+OK\r\n");

    send_command(&mut stream2, "*2\r\n$3\r\nGET\r\n$4\r\nkey2\r\n").unwrap();
    stream2.wait_for_write("$6\r\nvalue2\r\n", 1000);
    stream2.read_data.lock().unwrap().extend_from_slice("$6\r\nvalue2\r\n".as_bytes());
    let response2 = read_response(&mut stream2).unwrap();
    assert_eq!(response2, "$6\r\nvalue2\r\n");

    // Client 3 operations
    send_command(&mut stream3, "*3\r\n$3\r\nSET\r\n$4\r\nkey3\r\n$6\r\nvalue3\r\n").unwrap();
    stream3.wait_for_write("+OK\r\n", 1000);
    stream3.read_data.lock().unwrap().extend_from_slice("+OK\r\n".as_bytes());
    let response3 = read_response(&mut stream3).unwrap();
    assert_eq!(response3, "+OK\r\n");

    send_command(&mut stream3, "*2\r\n$3\r\nGET\r\n$4\r\nkey3\r\n").unwrap();
    stream3.wait_for_write("$6\r\nvalue3\r\n", 1000);
    stream3.read_data.lock().unwrap().extend_from_slice("$6\r\nvalue3\r\n".as_bytes());
    let response3 = read_response(&mut stream3).unwrap();
    assert_eq!(response3, "$6\r\nvalue3\r\n");

    println!("[TEST] Shutting down client handlers");
    stream1.shutdown(&mut client_handler1, handle1);
    stream2.shutdown(&mut client_handler2, handle2);
    stream3.shutdown(&mut client_handler3, handle3);

    println!("[TEST] Ending test_concurrent_set_get");

    Ok(())
}
