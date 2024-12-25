use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::io::{Read, Write};
use redis_starter_rust::redis::replication::ReplicationManager;
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::client_handler::ClientHandler;
use crate::utils::mock_tcp_stream::MockTcpStream;

mod utils;

#[test]
fn given_replication_manager_when_command_enqueued_then_sent_to_replica() {
    let mut manager = ReplicationManager::new();
    let (mock_stream, _server) = MockTcpStream::new_pair();
    
    // Add a replica with the mock stream
    manager.add_replica("localhost".to_string(), "6379".to_string(), Box::new(mock_stream.clone()));

    // Enqueue a command
    let command = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    manager.enqueue_for_replication(command);

    // Create a Redis instance with this ReplicationManager
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Start the replication sync
    ReplicationManager::start_replication_sync(redis.clone());

    // Give some time for the sync to occur
    thread::sleep(Duration::from_millis(100));

    // Check that the command was sent to the replica
    let mut received_data = Vec::new();
    let mut mock_stream_clone = mock_stream.clone();
    mock_stream_clone.read_to_end(&mut received_data).unwrap();
    assert_eq!(received_data, command.as_bytes());
}

#[test]
fn given_replication_manager_when_multiple_commands_enqueued_then_all_sent_to_replica() {
    let mut manager = ReplicationManager::new();
    let (mock_stream, _server) = MockTcpStream::new_pair();
    
    // Add a replica with the mock stream
    manager.add_replica("localhost".to_string(), "6379".to_string(), Box::new(mock_stream.clone()));

    // Enqueue multiple commands
    let commands = vec![
        "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        "*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n",
        "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
    ];

    for command in &commands {
        manager.enqueue_for_replication(command);
    }

    // Create a Redis instance with this ReplicationManager
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Start the replication sync
    ReplicationManager::start_replication_sync(redis.clone());

    // Give some time for the sync to occur
    thread::sleep(Duration::from_millis(100));

    // Check that all commands were sent to the replica
    let mut received_data = Vec::new();
    let mut mock_stream_clone = mock_stream.clone();
    mock_stream_clone.read_to_end(&mut received_data).unwrap();
    let expected_data: Vec<u8> = commands.join("").into_bytes();
    assert_eq!(received_data, expected_data);
}

#[test]
fn test_wait_command_with_ack() {
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for client and replica
    let (mut client_stream, server_stream) = MockTcpStream::new_pair();
    let (mut replica_stream, replica_server_stream) = MockTcpStream::new_pair();
    
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica connection");
    
    // Add a replica with its own stream - use 127.0.0.1 to match the MockTcpStream's peer_addr
    manager.add_replica("127.0.0.1".to_string(), "8080".to_string(), Box::new(replica_stream.clone()));

    // Create a Redis instance with this ReplicationManager
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Start replication sync
    ReplicationManager::start_replication_sync(redis.clone());

    // Create client handler
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up client connection");
    
    let mut client_handler = ClientHandler::new(server_stream, redis.clone());
    let handle = client_handler.start();

    // Create replica handler to process ACKs
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica handler");
    
    let mut replica_handler = ClientHandler::new(replica_server_stream, redis.clone());
    let _replica_handle = replica_handler.start();

    // Send SET command from client
    #[cfg(debug_assertions)]
    println!("[TEST] Sending SET command from client");
    let set_command = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
    client_stream.write_all(set_command).unwrap();

    // Wait for SET response
    assert!(client_stream.wait_for_pattern("+OK\r\n", 1000), "Missing SET response");

    // Send WAIT command from client
    #[cfg(debug_assertions)]
    println!("[TEST] Sending WAIT command from client");
    let wait_command = b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n";
    client_stream.write_all(wait_command).unwrap();

    // Wait for GETACK to be sent to replica
    thread::sleep(Duration::from_millis(50));

    // Simulate replica receiving GETACK and responding with ACK
    #[cfg(debug_assertions)]
    println!("[TEST] Simulating replica ACK response");
    let ack_command = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n31\r\n";
    replica_stream.write_all(ack_command).unwrap();

    // Wait for WAIT command response
    assert!(client_stream.wait_for_pattern(":1\r\n", 1000), "Missing WAIT response");

    client_stream.shutdown(&mut client_handler, handle);
}

#[test]
fn test_wait_command_timeout() {
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for client and replica
    let (mut client_stream, server_stream) = MockTcpStream::new_pair();
    let (replica_stream, replica_server_stream) = MockTcpStream::new_pair();
    
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica connection");
    
    // Add a replica with its own stream - use 127.0.0.1 to match the MockTcpStream's peer_addr
    manager.add_replica("127.0.0.1".to_string(), "8080".to_string(), Box::new(replica_stream));

    // Create a Redis instance with this ReplicationManager
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Start replication sync
    ReplicationManager::start_replication_sync(redis.clone());

    // Create client handler
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up client connection");
    
    let mut client_handler = ClientHandler::new(server_stream, redis.clone());
    let handle = client_handler.start();

    // Create replica handler to process ACKs
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica handler");
    
    let mut replica_handler = ClientHandler::new(replica_server_stream, redis.clone());
    let _replica_handle = replica_handler.start();

    // Send SET command
    let set_command = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
    client_stream.write_all(set_command).unwrap();

    // Wait for SET response
    assert!(client_stream.wait_for_pattern("+OK\r\n", 1000), "Missing SET response");

    // Send WAIT command with short timeout
    let wait_command = b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$2\r\n50\r\n";
    client_stream.write_all(wait_command).unwrap();

    // The client handler will retry internally until timeout elapses
    assert!(client_stream.wait_for_pattern(":0\r\n", 1000), "Missing WAIT timeout response");

    client_stream.shutdown(&mut client_handler, handle);
}
