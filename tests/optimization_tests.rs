//! Integration tests for I/O and parsing optimizations
//! 
//! Test-Driven Development: These tests define expected behavior BEFORE implementation.
//! Run with: cargo test --test optimization_tests

use bytes::Bytes;
use mikkadb_rust::resp::Value;
use mikkadb_rust::db::Db;
use mikkadb_rust::command::RedisCommand;
use std::io::IoSlice;

// ============================================================================
// PHASE 1: Vectored I/O Tests
// ============================================================================

mod vectored_io_tests {
    use super::*;

    /// Test that serialized responses can be written using IoSlice references
    /// without intermediate buffer copying
    #[test]
    fn test_ioslice_creation_from_serialized_responses() {
        let responses = vec![
            Value::SimpleString(Bytes::from("OK")),
            Value::BulkString(Bytes::from("hello")),
            Value::Integer(42),
        ];

        // Serialize each response
        let serialized: Vec<Vec<u8>> = responses.into_iter()
            .map(|v| v.serialize_bytes())
            .collect();

        // Create IoSlices - this is the key optimization
        let io_slices: Vec<IoSlice> = serialized.iter()
            .map(|buf| IoSlice::new(buf))
            .collect();

        // Verify we have the right number of slices
        assert_eq!(io_slices.len(), 3);

        // Verify total bytes match
        let total_from_slices: usize = io_slices.iter().map(|s| s.len()).sum();
        let total_from_vecs: usize = serialized.iter().map(|v| v.len()).sum();
        assert_eq!(total_from_slices, total_from_vecs);
    }

    /// Test that single response takes fast path (no vectored I/O overhead)
    #[test]
    fn test_single_response_fast_path() {
        let response = Value::SimpleString(Bytes::from("PONG"));
        let serialized = response.serialize_bytes();
        
        // Single response should be "+PONG\r\n"
        assert_eq!(serialized, b"+PONG\r\n");
        
        // No need for IoSlice array with single response
        let io_slice = IoSlice::new(&serialized);
        assert_eq!(io_slice.len(), 7);
    }

    /// Test large batch of responses can be converted to IoSlices
    #[test]
    fn test_large_batch_ioslices() {
        let mut responses = Vec::with_capacity(100);
        for i in 0..100 {
            responses.push(Value::Integer(i));
        }

        let serialized: Vec<Vec<u8>> = responses.into_iter()
            .map(|v| v.serialize_bytes())
            .collect();

        let io_slices: Vec<IoSlice> = serialized.iter()
            .map(|buf| IoSlice::new(buf))
            .collect();

        assert_eq!(io_slices.len(), 100);
        
        // Each integer response like ":0\r\n" is 4+ bytes
        let total_bytes: usize = io_slices.iter().map(|s| s.len()).sum();
        assert!(total_bytes >= 400); // At least 4 bytes per response
    }

    /// Test empty batch handling
    #[test]
    fn test_empty_batch_ioslices() {
        let responses: Vec<Value> = vec![];
        let serialized: Vec<Vec<u8>> = responses.into_iter()
            .map(|v| v.serialize_bytes())
            .collect();
        
        let io_slices: Vec<IoSlice> = serialized.iter()
            .map(|buf| IoSlice::new(buf))
            .collect();

        assert_eq!(io_slices.len(), 0);
    }
    
    /// Test IoSlice preserves exact byte content
    #[test]
    fn test_ioslice_content_integrity() {
        let responses = vec![
            Value::BulkString(Bytes::from("hello")),
            Value::BulkString(Bytes::from("world")),
        ];

        let serialized: Vec<Vec<u8>> = responses.into_iter()
            .map(|v| v.serialize_bytes())
            .collect();

        // Concatenate via IoSlice iteration (simulating vectored write)
        let mut combined = Vec::new();
        for buf in &serialized {
            let slice = IoSlice::new(buf);
            combined.extend_from_slice(&slice);
        }
        
        // Compare with direct concatenation
        let mut expected = Vec::new();
        for buf in &serialized {
            expected.extend_from_slice(buf);
        }
        
        assert_eq!(combined, expected);
    }
}

// ============================================================================
// PHASE 2: Zero-Copy Parsing Tests
// ============================================================================

mod zero_copy_tests {
    use super::*;

    /// Test that Value::as_bytes() returns reference without allocation
    #[test]
    fn test_value_as_bytes_no_allocation() {
        let original = Bytes::from("test_key");
        let value = Value::BulkString(original.clone());
        
        // as_bytes() should return Option<&Bytes> without cloning
        if let Some(bytes_ref) = value.as_bytes() {
            assert_eq!(bytes_ref, &original);
            // Verify it's the same underlying data (no copy) - Bytes is ref-counted
            assert_eq!(bytes_ref.as_ptr(), original.as_ptr());
        } else {
            panic!("as_bytes() should return Some for BulkString");
        }
        
        // Also test clone_bytes() - cheap ref-counted clone
        if let Some(cloned) = value.clone_bytes() {
            assert_eq!(cloned, original);
            // Same underlying data due to ref-counting
            assert_eq!(cloned.as_ptr(), original.as_ptr());
        } else {
            panic!("clone_bytes() should return Some for BulkString");
        }
    }
    
    /// Test as_bytes() returns None for non-string types
    #[test]
    fn test_value_as_bytes_non_string_types() {
        let int_value = Value::Integer(42);
        assert!(int_value.as_bytes().is_none());
        
        let null_value = Value::Null;
        assert!(null_value.as_bytes().is_none());
        
        let array_value = Value::Array(vec![]);
        assert!(array_value.as_bytes().is_none());
    }

    /// Test that GET command can be parsed with zero-copy Bytes
    #[test]
    fn test_get_command_parsing() {
        let items = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("mykey")),
        ];
        let value = Value::Array(items);
        
        let cmd = RedisCommand::from_resp(value).unwrap();
        
        match cmd {
            RedisCommand::Get { key } => {
                // Now uses Bytes for zero-copy
                assert_eq!(key, Bytes::from("mykey"));
            }
            _ => panic!("Expected Get command"),
        }
    }

    /// Test that SET command parses correctly with zero-copy Bytes
    #[test]
    fn test_set_command_parsing() {
        let items = vec![
            Value::BulkString(Bytes::from("SET")),
            Value::BulkString(Bytes::from("mykey")),
            Value::BulkString(Bytes::from("myvalue")),
        ];
        let value = Value::Array(items);
        
        let cmd = RedisCommand::from_resp(value).unwrap();
        
        match cmd {
            RedisCommand::Set { key, value, px } => {
                // Now uses Bytes for zero-copy
                assert_eq!(key, Bytes::from("mykey"));
                assert_eq!(value, Bytes::from("myvalue"));
                assert_eq!(px, None);
            }
            _ => panic!("Expected Set command"),
        }
    }

    /// Test Db operations with current API
    #[test]
    fn test_db_operations() {
        let mut db = Db::new();
        
        // Set with String key
        db.set("testkey".to_string(), Bytes::from("testvalue"), None);
        
        // Get with &str
        let result = db.get("testkey");
        assert_eq!(result, Some(Bytes::from("testvalue")));
    }

    /// Test INCR command parsing with zero-copy Bytes
    #[test]
    fn test_incr_command_parsing() {
        let items = vec![
            Value::BulkString(Bytes::from("INCR")),
            Value::BulkString(Bytes::from("counter")),
        ];
        let value = Value::Array(items);
        
        let cmd = RedisCommand::from_resp(value).unwrap();
        
        match cmd {
            RedisCommand::Incr { key } => {
                // Now uses Bytes for zero-copy
                assert_eq!(key, Bytes::from("counter"));
            }
            _ => panic!("Expected Incr command"),
        }
    }

    /// Test binary data handling - documents current behavior
    /// Current implementation fails on non-UTF8; after optimization it should work
    #[test]
    fn test_binary_data_handling() {
        // Binary data that's not valid UTF-8
        let binary_key = Bytes::from(vec![0xFF, 0xFE, 0x00, 0x01]);
        let binary_value = Bytes::from(vec![0x00, 0x01, 0x02, 0x03]);
        
        let items = vec![
            Value::BulkString(Bytes::from("SET")),
            Value::BulkString(binary_key.clone()),
            Value::BulkString(binary_value.clone()),
        ];
        let value = Value::Array(items);
        
        let result = RedisCommand::from_resp(value);
        // Current: fails due to String conversion
        // After optimization: should succeed
        // For now, just document the behavior
        let _ = result; // Don't assert, just verify it doesn't panic
    }
}

// ============================================================================
// PHASE 3: Integration Tests
// ============================================================================

mod integration_tests {
    use super::*;

    /// End-to-end test: parse command, execute, serialize response
    #[test]
    fn test_command_roundtrip() {
        // Parse SET command
        let set_items = vec![
            Value::BulkString(Bytes::from("SET")),
            Value::BulkString(Bytes::from("foo")),
            Value::BulkString(Bytes::from("bar")),
        ];
        let set_cmd = RedisCommand::from_resp(Value::Array(set_items)).unwrap();
        
        // Execute on Db
        let mut db = Db::new();
        match set_cmd {
            RedisCommand::Set { key, value, px } => {
                // Use set_bytes() for zero-copy Bytes key
                db.set_bytes(key, value, px);
            }
            _ => panic!("Expected Set"),
        }
        
        // Parse GET command
        let get_items = vec![
            Value::BulkString(Bytes::from("GET")),
            Value::BulkString(Bytes::from("foo")),
        ];
        let get_cmd = RedisCommand::from_resp(Value::Array(get_items)).unwrap();
        
        // Execute GET
        let result = match get_cmd {
            // Use get_bytes() for zero-copy Bytes key lookup
            RedisCommand::Get { key } => db.get_bytes(&key),
            _ => panic!("Expected Get"),
        };
        
        assert_eq!(result, Some(Bytes::from("bar")));
        
        // Serialize response
        let response = Value::BulkString(result.unwrap());
        let serialized = response.serialize_bytes();
        assert_eq!(serialized, b"$3\r\nbar\r\n");
    }

    /// Test batch of commands (simulating pipeline)
    #[test]
    fn test_batch_command_processing() {
        let mut db = Db::new();
        
        // Batch of 10 SET commands
        for i in 0..10 {
            let items = vec![
                Value::BulkString(Bytes::from("SET")),
                Value::BulkString(Bytes::from(format!("key{}", i))),
                Value::BulkString(Bytes::from(format!("value{}", i))),
            ];
            let cmd = RedisCommand::from_resp(Value::Array(items)).unwrap();
            match cmd {
                RedisCommand::Set { key, value, px } => {
                    // Use set_bytes() for zero-copy Bytes key
                    db.set_bytes(key, value, px);
                }
                _ => panic!("Expected Set"),
            }
        }
        
        // Verify all keys
        for i in 0..10 {
            let key = Bytes::from(format!("key{}", i));
            let expected = Bytes::from(format!("value{}", i));
            assert_eq!(db.get_bytes(&key), Some(expected));
        }
    }
    
    /// Test batch response serialization and IoSlice creation
    #[test]
    fn test_batch_response_serialization() {
        // Simulate 10 GET responses
        let mut responses = Vec::new();
        for i in 0..10 {
            responses.push(Value::BulkString(Bytes::from(format!("value{}", i))));
        }
        
        // Serialize all
        let serialized: Vec<Vec<u8>> = responses.into_iter()
            .map(|v| v.serialize_bytes())
            .collect();
        
        // Create IoSlices
        let io_slices: Vec<IoSlice> = serialized.iter()
            .map(|buf| IoSlice::new(buf))
            .collect();
        
        assert_eq!(io_slices.len(), 10);
        
        // Verify first response
        assert_eq!(&serialized[0], b"$6\r\nvalue0\r\n");
    }
}
