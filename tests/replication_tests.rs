use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use redis_starter_rust::redis::replication::ReplicationManager;
use redis_starter_rust::redis::core::Redis;
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
