use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::io::Write;
use redis_starter_rust::redis::replication::ReplicationManager;
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::client_handler::ClientHandler;
use crate::utils::mock_tcp_stream::MockTcpStream;

mod utils;

#[test]
fn given_replication_manager_when_command_enqueued_then_sent_to_replica() {
    // Arrange
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for replica connection
    let (replica_stream, mut _replica_server) = MockTcpStream::new_pair();
    manager.add_replica("localhost".to_string(), "6379".to_string(), Box::new(replica_stream));

    // Act
    let command = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    manager.enqueue_for_replication(command);
    manager.send_pending_commands();

    // Wait for the command to be sent
    thread::sleep(Duration::from_millis(100));

    // Assert - check what was received by the replica server
    let read_data = _replica_server.read_data.lock().unwrap().clone();
    assert_eq!(read_data, command.as_bytes().to_vec());
}

#[test]
fn given_replication_manager_when_multiple_commands_enqueued_then_all_sent_to_replica() {
    // Arrange
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for replica connection
    let (replica_stream, mut _replica_server) = MockTcpStream::new_pair();
    manager.add_replica("localhost".to_string(), "6379".to_string(), Box::new(replica_stream));

    // Act
    let commands = vec![
        "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n",
        "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
    ];

    for command in commands.iter() {
        manager.enqueue_for_replication(command);
    }
    manager.send_pending_commands();

    // Wait for the commands to be sent
    thread::sleep(Duration::from_millis(100));

    // Assert - check what was received by the replica server
    let read_data = _replica_server.read_data.lock().unwrap().clone();
    let expected_data: Vec<u8> = commands.join("").as_bytes().to_vec();
    assert_eq!(read_data, expected_data);
}

#[test]
fn test_wait_command_with_ack() {
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for master and replica
    let (mut master_stream, master_server) = MockTcpStream::new_pair();
    let (replica_stream, mut _replica_server) = MockTcpStream::new_pair();
    manager.add_replica("127.0.0.1".to_string(), "8080".to_string(), Box::new(replica_stream.clone()));

    // Create a Redis instance with this ReplicationManager
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Start replication sync
    ReplicationManager::start_replication_sync(redis.clone());

    // Create client handler
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up client connection");
    
    let mut client_handler = ClientHandler::new(master_server, redis.clone());
    let _handle = client_handler.start();

    // Create replica handler to process ACKs
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica handler");
    
    let mut replica_handler = ClientHandler::new(replica_stream, redis.clone());
    let _replica_handle = replica_handler.start();

    // Send SET command from client
    #[cfg(debug_assertions)]
    println!("[TEST] Sending SET command from client");
    let set_command = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
    master_stream.write_all(set_command).unwrap();

    // Wait for SET response
    assert!(master_stream.wait_for_pattern("+OK\r\n", 1000), "Missing SET response");

    // Send WAIT command from client
    #[cfg(debug_assertions)]
    println!("[TEST] Sending WAIT command from client");
    let wait_command = b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n";
    master_stream.write_all(wait_command).unwrap();

    // Wait for GETACK to be sent to replica
    thread::sleep(Duration::from_millis(50));

    // Calculate expected offset based on command length
    let expected_offset = set_command.len() as u64;
    let ack_command = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", 
        expected_offset.to_string().len(), expected_offset);
    _replica_server.write_all(ack_command.as_bytes()).unwrap();

    // Wait for WAIT command response
    assert!(master_stream.wait_for_pattern(":1\r\n", 1000), "Missing WAIT response");

    master_stream.shutdown();
}

#[test]
fn test_wait_command_timeout() {
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for master and replica
    let (mut master_stream, master_server) = MockTcpStream::new_pair();
    let (replica_stream, _replica_server) = MockTcpStream::new_pair();
    manager.add_replica("127.0.0.1".to_string(), "8080".to_string(), Box::new(replica_stream.clone()));

    // Create a Redis instance with this ReplicationManager
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Start replication sync
    ReplicationManager::start_replication_sync(redis.clone());

    // Create client handler
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up client connection");
    
    let mut client_handler = ClientHandler::new(master_server, redis.clone());
    let _handle = client_handler.start();

    // Create replica handler to process ACKs
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica handler");
    
    let mut replica_handler = ClientHandler::new(replica_stream, redis.clone());
    let _replica_handle = replica_handler.start();

    // Send SET command
    let set_command = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
    master_stream.write_all(set_command).unwrap();

    // Wait for SET response
    assert!(master_stream.wait_for_pattern("+OK\r\n", 1000), "Missing SET response");

    // Send WAIT command with short timeout
    let wait_command = b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$2\r\n50\r\n";
    master_stream.write_all(wait_command).unwrap();

    // Wait for the timeout to elapse
    thread::sleep(Duration::from_millis(100));

    // The client handler will retry internally until timeout elapses
    assert!(master_stream.wait_for_pattern(":0\r\n", 1000), "Missing WAIT timeout response");

    master_stream.shutdown();
}

#[test]
fn test_wait_command_no_replicas() {
    let mut manager = ReplicationManager::new();
    let (mut master_stream, master_server) = MockTcpStream::new_pair();
    
    // Create a Redis instance with this ReplicationManager (no replicas added)
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Create client handler
    let mut client_handler = ClientHandler::new(master_server, redis.clone());
    let _handle = client_handler.start();

    // Send SET command
    let set_command = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
    master_stream.write_all(set_command).unwrap();

    // Wait for SET response
    assert!(master_stream.wait_for_pattern("+OK\r\n", 1000), "Missing SET response");

    // Send WAIT command - should return immediately with 0
    let wait_command = b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n";
    master_stream.write_all(wait_command).unwrap();
    master_stream.flush().unwrap();
    thread::sleep(Duration::from_millis(600)); // Wait longer than timeout


    // Should return immediately with 0 replicas
    assert!(master_stream.wait_for_pattern(":0\r\n", 1000), "Missing WAIT response");

    master_stream.shutdown();
}
