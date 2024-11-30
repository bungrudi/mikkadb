use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use redis_starter_rust::redis::replication::ReplicationManager;
use redis_starter_rust::redis::core::Redis;
use redis_starter_rust::client_handler::ClientHandler;
use crate::utils::mock_tcp_stream::MockTcpStream;

mod utils;

#[test]
fn given_replication_manager_when_command_enqueued_then_sent_to_replica() {
    let mut manager = ReplicationManager::new();
    let mock_stream = MockTcpStream::new();
    
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
    let written_data = mock_stream.get_written_data();
    assert_eq!(written_data, command.as_bytes());
}

#[test]
fn given_replication_manager_when_multiple_commands_enqueued_then_all_sent_to_replica() {
    let mut manager = ReplicationManager::new();
    let mock_stream = MockTcpStream::new();
    
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
    let written_data = mock_stream.get_written_data();
    let expected_data: Vec<u8> = commands.join("").into_bytes();
    assert_eq!(written_data, expected_data);
}

#[test]
fn given_replication_manager_when_getack_set_then_getack_sent_to_replica() {
    let mut manager = ReplicationManager::new();
    let mock_stream = MockTcpStream::new();
    
    // Add a replica with the mock stream
    manager.add_replica("localhost".to_string(), "6379".to_string(), Box::new(mock_stream.clone()));

    // Set GETACK flag
    manager.set_enqueue_getack(true);

    // Create a Redis instance with this ReplicationManager
    let redis = Arc::new(Mutex::new(Redis::new_with_replication(manager)));

    // Start the replication sync
    ReplicationManager::start_replication_sync(redis.clone());

    // Give some time for the sync to occur
    thread::sleep(Duration::from_millis(100));

    // Check that GETACK was sent to the replica
    let written_data = mock_stream.get_written_data();
    let expected_data = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    assert_eq!(written_data, expected_data);
}

#[test]
fn test_wait_command_with_ack() {
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for client and replica
    let client_stream = MockTcpStream::new();
    let replica_stream = MockTcpStream::new();
    
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
    
    let mut client_handler = ClientHandler::new(client_stream.clone(), redis.clone());
    let handle = client_handler.start();

    // Create replica handler to process ACKs
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica handler");
    
    let mut replica_handler = ClientHandler::new(replica_stream.clone(), redis.clone());
    let _replica_handle = replica_handler.start();

    // Send SET command from client
    #[cfg(debug_assertions)]
    println!("[TEST] Sending SET command from client");
    {
        let mut read_data = client_stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n");
    }

    // Wait longer for SET to be processed and replicated
    thread::sleep(Duration::from_millis(100));

    // Send WAIT command from client
    #[cfg(debug_assertions)]
    println!("[TEST] Sending WAIT command from client");
    {
        let mut read_data = client_stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n");
    }

    // Wait for GETACK to be sent to replica
    thread::sleep(Duration::from_millis(50));

    // Simulate replica receiving GETACK and responding with ACK
    #[cfg(debug_assertions)]
    println!("[TEST] Simulating replica ACK response");
    {
        let mut read_data = replica_stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n31\r\n");
    }

    // Wait for ACK to be processed with longer timeout
    thread::sleep(Duration::from_millis(100));

    // More robust polling with exponential backoff
    let start = std::time::Instant::now();
    let timeout = Duration::from_millis(5000);
    let mut wait_time = Duration::from_millis(50);
    let mut replica_synced = false;

    while start.elapsed() < timeout {
        let current_offset = {
            let redis = redis.lock().unwrap();
            let replicas = redis.replication.get_replicas();
            replicas.get("127.0.0.1:8080")
                .map(|r| r.offset.load(Ordering::SeqCst))
        };
        
        if let Some(offset) = current_offset {
            if offset == 31 {
                replica_synced = true;
                break;
            }
        }
        thread::sleep(wait_time);
        wait_time = std::cmp::min(wait_time * 2, Duration::from_millis(200));
    }

    assert!(replica_synced, "Timeout waiting for replica to sync");

    // Wait for WAIT command response with timeout
    assert!(client_stream.wait_for_write(":1\r\n", 1000), 
        "Timeout waiting for WAIT command response");

    // Verify complete responses
    let written_data = String::from_utf8(client_stream.get_written_data()).unwrap();
    assert!(written_data.contains("+OK\r\n"), "Missing SET response"); 
    assert!(written_data.contains(":1\r\n"), "Missing WAIT response");

    client_stream.shutdown(&mut client_handler, handle);
}

#[test]
fn test_wait_command_timeout() {
    let mut manager = ReplicationManager::new();
    
    // Create separate streams for client and replica
    let client_stream = MockTcpStream::new();
    let replica_stream = MockTcpStream::new();
    
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
    
    let mut client_handler = ClientHandler::new(client_stream.clone(), redis.clone());
    let handle = client_handler.start();

    // Create replica handler to process ACKs
    #[cfg(debug_assertions)]
    println!("[TEST] Setting up replica handler");
    
    let mut replica_handler = ClientHandler::new(replica_stream.clone(), redis.clone());
    let _replica_handle = replica_handler.start();

    // Send SET command
    {
        let mut read_data = client_stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n");
    }
    thread::sleep(Duration::from_millis(50));

    // Send WAIT command with short timeout
    {
        let mut read_data = client_stream.read_data.lock().unwrap();
        read_data.extend_from_slice(b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$2\r\n50\r\n");
    }
    
    // Wait for both the SET and WAIT responses (200ms to ensure both writes complete)
    // thread::sleep(Duration::from_millis(200));
    
    client_stream.wait_for_write(":0\r\n", 1000);

    // Verify responses
    let final_data = String::from_utf8(client_stream.get_written_data()).unwrap();
    assert!(final_data.contains("+OK\r\n"), "Missing SET response"); // SET response
    assert!(final_data.contains(":0\r\n"), "Missing WAIT timeout response"); // WAIT response showing 0 replicas acknowledged due to timeout

    client_stream.shutdown(&mut client_handler, handle);
}
